# Module 12 — Terraform & Infrastructure as Code
## Provision Databricks Resources Programmatically

> Every enterprise project uses IaC. Clicking through UI to create clusters and jobs doesn't scale — Terraform lets you version-control your infrastructure, reproduce environments, and deploy consistently.

---

## 12.1 Why Terraform for Databricks?

```
UI APPROACH (manual):
  ✗ Cannot reproduce exact setup across dev/test/prod
  ✗ Configuration drift — someone changes a cluster setting manually
  ✗ No audit trail for infrastructure changes
  ✗ Takes 30 minutes to set up a new environment

TERRAFORM APPROACH:
  ✅ git diff on infra changes — full audit trail
  ✅ Identical environments: apply same Terraform to dev/prod
  ✅ Destroy and recreate environments in minutes
  ✅ PR review for cluster config changes (not just code!)
```

---

## 12.2 Project Structure

```
infrastructure/
├── environments/
│   ├── dev/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── terraform.tfvars
│   ├── test/
│   │   └── terraform.tfvars
│   └── prod/
│       └── terraform.tfvars
│
├── modules/
│   ├── databricks_workspace/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── databricks_cluster/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── databricks_job/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── databricks_unity_catalog/
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
│
├── main.tf           ← Root module
├── variables.tf
├── outputs.tf
└── backend.tf        ← Remote state config
```

---

## 12.3 Provider Setup

```hcl
# main.tf — Root configuration

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.45"
    }
  }
  
  # Store Terraform state in Azure Blob (not local!)
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "sttfstate001"
    container_name       = "tfstate"
    key                  = "databricks/${var.environment}.tfstate"
  }
}

# Azure provider
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# Databricks provider (workspace level)
provider "databricks" {
  host  = azurerm_databricks_workspace.main.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.main.id
}

# Databricks provider (account level — for Unity Catalog)
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}
```

---

## 12.4 Databricks Workspace

```hcl
# modules/databricks_workspace/main.tf

resource "azurerm_resource_group" "databricks" {
  name     = "rg-databricks-${var.environment}"
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_databricks_workspace" "main" {
  name                = "adb-${var.project}-${var.environment}"
  resource_group_name = azurerm_resource_group.databricks.name
  location            = azurerm_resource_group.databricks.location
  sku                 = "premium"    # Required for Unity Catalog
  
  # VNet injection (production requirement — isolate cluster traffic)
  custom_parameters {
    virtual_network_id                                   = azurerm_virtual_network.databricks.id
    public_subnet_name                                   = "snet-databricks-public"
    private_subnet_name                                  = "snet-databricks-private"
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
    no_public_ip                                         = true  # No public IP on workers
  }
  
  tags = local.common_tags
}

# Managed Identity for workspace (for ADLS access)
resource "azurerm_user_assigned_identity" "databricks" {
  name                = "mi-databricks-${var.environment}"
  resource_group_name = azurerm_resource_group.databricks.name
  location            = var.location
}

# Grant identity access to ADLS
resource "azurerm_role_assignment" "databricks_adls" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.databricks.principal_id
}

output "workspace_url" {
  value = "https://${azurerm_databricks_workspace.main.workspace_url}"
}
```

---

## 12.5 Cluster Policies and Clusters

```hcl
# modules/databricks_cluster/main.tf

# Cluster policy — enforce governance
resource "databricks_cluster_policy" "etl_policy" {
  name = "${var.environment}-etl-policy"
  
  definition = jsonencode({
    "node_type_id" = {
      "type"         = "allowlist"
      "values"       = ["Standard_DS3_v2", "Standard_DS4_v2", "Standard_DS5_v2"]
      "defaultValue" = "Standard_DS4_v2"
    }
    "autotermination_minutes" = {
      "type"         = "range"
      "minValue"     = 30
      "maxValue"     = 120
      "defaultValue" = 60
    }
    "spark_version" = {
      "type"    = "regex"
      "pattern" = "^13\\..*lts.*"
    }
    "azure_attributes.availability" = {
      "type"  = "fixed"
      "value" = "SPOT_WITH_FALLBACK_AZURE"
    }
  })
}

# Instance pool (pre-warm VMs)
resource "databricks_instance_pool" "etl_pool" {
  instance_pool_name = "${var.environment}-etl-pool"
  min_idle_instances = var.environment == "prod" ? 4 : 0
  max_capacity       = 20
  node_type_id       = "Standard_DS4_v2"
  
  idle_instance_autotermination_minutes = 60
  
  azure_attributes {
    availability       = "SPOT_WITH_FALLBACK_AZURE"
    spot_bid_max_price = -1
  }
  
  preloaded_spark_versions = ["13.3.x-scala2.12"]
}

# All-purpose cluster for development
resource "databricks_cluster" "dev_cluster" {
  count = var.environment == "prod" ? 0 : 1  # Only in dev/test

  cluster_name            = "${var.environment}-dev-cluster"
  spark_version           = "13.3.x-scala2.12"
  instance_pool_id        = databricks_instance_pool.etl_pool.id
  autotermination_minutes = 60
  
  autoscale {
    min_workers = 1
    max_workers = 4
  }
  
  spark_conf = {
    "spark.sql.adaptive.enabled"                     = "true"
    "spark.sql.adaptive.coalescePartitions.enabled"  = "true"
    "spark.databricks.delta.preview.enabled"         = "true"
    "spark.databricks.photon.enabled"                = "true"
  }
  
  library {
    pypi {
      package = "delta-spark==2.4.0"
    }
  }
  
  library {
    pypi {
      package = "chispa==0.9.2"
    }
  }
}
```

---

## 12.6 Jobs and Workflows

```hcl
# modules/databricks_job/main.tf

resource "databricks_job" "daily_etl_pipeline" {
  name = "daily-transaction-pipeline-${var.environment}"
  
  # Job-level cluster (cheaper than all-purpose)
  job_cluster {
    job_cluster_key = "etl_cluster"
    
    new_cluster {
      spark_version   = "13.3.x-scala2.12"
      node_type_id    = "Standard_DS4_v2"
      instance_pool_id = databricks_instance_pool.etl_pool.id
      
      autoscale {
        min_workers = 2
        max_workers = 8
      }
      
      spark_conf = {
        "spark.sql.adaptive.enabled" = "true"
        "spark.sql.shuffle.partitions" = "200"
      }
      
      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }
    }
  }
  
  # Task 1: Ingest Bronze
  task {
    task_key = "ingest_bronze"
    
    notebook_task {
      notebook_path = "/Repos/ci-cd/Azure-Databricks-Learning/notebooks/ingestion/01_ingest_bronze"
      base_parameters = {
        environment = var.environment
        batch_date  = ""
      }
    }
    
    job_cluster_key = "etl_cluster"
    max_retries     = 2
    min_retry_interval_millis = 300000
  }
  
  # Task 2: Transform Silver (depends on ingest)
  task {
    task_key = "transform_silver"
    
    depends_on {
      task_key = "ingest_bronze"
    }
    
    notebook_task {
      notebook_path = "/Repos/ci-cd/Azure-Databricks-Learning/notebooks/transformation/02_transform_silver"
    }
    
    job_cluster_key = "etl_cluster"
    max_retries     = 1
  }
  
  # Task 3: Build Gold
  task {
    task_key = "build_gold"
    
    depends_on {
      task_key = "transform_silver"
    }
    
    notebook_task {
      notebook_path = "/Repos/ci-cd/Azure-Databricks-Learning/notebooks/serving/03_build_gold"
    }
    
    job_cluster_key = "etl_cluster"
  }
  
  # Schedule
  schedule {
    quartz_cron_expression = "0 0 2 * * ?"  # 2:00 AM UTC daily
    timezone_id            = "UTC"
    pause_status           = var.environment == "prod" ? "UNPAUSED" : "PAUSED"
  }
  
  # Notifications
  email_notifications {
    on_failure = [var.alert_email]
    on_success = var.environment == "prod" ? [var.alert_email] : []
  }
  
  # Timeout
  timeout_seconds = 7200  # 2 hours max
}

# SQL Warehouse for BI
resource "databricks_sql_endpoint" "bi_warehouse" {
  name              = "${var.environment}-bi-warehouse"
  cluster_size      = "X-Small"
  max_num_clusters  = var.environment == "prod" ? 3 : 1
  auto_stop_mins    = 10
  
  enable_photon     = true
  enable_serverless_compute = true
  
  tags {
    custom_tags {
      key   = "environment"
      value = var.environment
    }
    custom_tags {
      key   = "team"
      value = "bi"
    }
  }
}
```

---

## 12.7 Unity Catalog via Terraform

```hcl
# modules/databricks_unity_catalog/main.tf

# Create catalogs
resource "databricks_catalog" "prod" {
  provider = databricks.account
  name     = "${var.environment}_catalog"
  comment  = "${upper(var.environment)} data catalog"
}

# Create schemas (Medallion Architecture layers)
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.prod.name
  name         = "bronze"
  comment      = "Raw ingested data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.prod.name
  name         = "silver"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.prod.name
  name         = "gold"
}

# Access control
resource "databricks_grants" "de_team_silver" {
  schema = "${databricks_catalog.prod.name}.${databricks_schema.silver.name}"
  
  grant {
    principal  = "data-engineering-team"
    privileges = ["SELECT", "MODIFY", "CREATE TABLE", "CREATE VIEW"]
  }
}

resource "databricks_grants" "analysts_gold" {
  schema = "${databricks_catalog.prod.name}.${databricks_schema.gold.name}"
  
  grant {
    principal  = "data-analysts"
    privileges = ["SELECT"]
  }
}
```

---

## 12.8 Variables and Environments

```hcl
# variables.tf
variable "environment"  { type = string }
variable "location"     { type = string   default = "East US" }
variable "project"      { type = string   default = "dataplatform" }
variable "alert_email"  { type = string }
variable "subscription_id" { type = string   sensitive = true }
variable "databricks_account_id" { type = string   sensitive = true }

locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project
    ManagedBy   = "Terraform"
    Team        = "DataEngineering"
  }
}

# environments/prod/terraform.tfvars
environment   = "prod"
location      = "East US"
alert_email   = "data-eng@company.com"
```

---

## 12.9 Deploy Databricks with Terraform

```bash
# Initialize (downloads providers)
cd infrastructure/environments/prod
terraform init

# Plan (see what will be created)
terraform plan -var-file="terraform.tfvars" -out=plan.out

# Review plan output — check every resource before applying
# Look for: resources to create (+), destroy (-), change (~)

# Apply (create resources)
terraform apply plan.out

# Destroy (remove everything — useful for cleanup)
terraform destroy -var-file="terraform.tfvars"

# Check current state
terraform show
terraform state list

# Import existing resource into Terraform state
terraform import databricks_cluster.dev_cluster 1234-567890-abc123

# CI/CD integration (GitHub Actions)
# Use: hashicorp/setup-terraform action
# Store credentials as GitHub Secrets
# Never commit .tfstate files (use remote backend)
```

---

## 12.10 Interview Questions — IaC

**Q: Why use Terraform for Databricks instead of the UI?**
> Version control of infrastructure — every cluster config change is a PR with review. Environment consistency — dev/test/prod have identical config, just different variable values. Disaster recovery — if a workspace is deleted, run `terraform apply` to recreate everything in minutes. Audit trail — `git log` shows who changed what infrastructure and when. Team collaboration — no "who changed that cluster?" mysteries.

**Q: How do you manage Terraform state safely for a team?**
> Store state in Azure Blob Storage (remote backend) — not in Git (state contains sensitive values). Enable state locking (Azure Blob supports this natively) to prevent concurrent applies. Use separate state files per environment (`dev.tfstate`, `prod.tfstate`). Use workspaces or separate directories per environment to prevent accidental cross-environment applies. Never store `terraform.tfstate` in Git.

---
*Next: [Module 13 — Real-World Patterns](../13-Real-World-Patterns/01-design-patterns.md)*
