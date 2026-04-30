# Module 05 — Azure Integration
## ADF → Databricks, Key Vault, ADLS Gen2, Managed Identity & Secrets

---

## 5.1 Azure Data Factory (ADF) → Databricks Integration

ADF is the enterprise orchestration tool for Azure. In most real projects, ADF *calls* Databricks notebooks/jobs, not the other way around.

```
ADF Pipeline
│
├── Copy Activity: SQL Server → ADLS Bronze     ← Extract raw data
├── Databricks Activity: Run ETL Notebook       ← Transform
├── Databricks Activity: Build Gold Tables      ← Load
└── Copy Activity: Delta → Azure SQL DWH        ← Serve to BI

ADF handles: scheduling, dependency, error handling, monitoring
Databricks handles: all the heavy compute
```

---

## 5.2 Connecting ADF to Databricks — Step by Step

### Step 1: Create ADF Linked Service for Databricks (UI)

```
Azure Data Factory Studio (adf.azure.com)
→ Manage (wrench icon)
→ Linked Services
→ + New
→ Search "Azure Databricks"
→ Select "Azure Databricks"

Fill in:
  Name:              ls_databricks_prod
  Azure subscription: <your subscription>
  Databricks workspace: adb-workspace-prod
  
  Authentication:
    Select Cluster:   New job cluster  ← Always new cluster for ADF!
    Cluster Version:  13.3 LTS
    Cluster node type: Standard_DS4_v2
    Workers:          2-8 (autoscale)
    
  Access Token:   
    ← Generate in Databricks: User Settings → Access Tokens → Generate
    ← Store in ADF → Manage → Parameters → or hardcode (dev only)
    
  Better approach: Use Azure Key Vault for the token (see below)

→ Test Connection → Create
```

### Step 2: Add Databricks Notebook Activity to Pipeline

```
ADF Pipeline Designer
→ + Activity → Databricks → Notebook

Settings:
  Linked Service: ls_databricks_prod
  Notebook path:  /Repos/my-org/data-platform/notebooks/02_transform_silver
  
Base Parameters:   ← These become widget values in the notebook!
  environment:     @pipeline().parameters.Environment
  batch_date:      @formatDateTime(pipeline().parameters.TriggerDate, 'yyyy-MM-dd')
  source_table:    transactions

→ Connect success output to next activity
→ Connect failure output to "Notification" activity
```

### Step 3: Pipeline Parameters and Dynamic Content

```json
// ADF Pipeline parameters
{
    "parameters": {
        "Environment": {
            "type": "string",
            "defaultValue": "prod"
        },
        "TriggerDate": {
            "type": "string",
            "defaultValue": "@utcNow()"
        }
    }
}
```

```python
# In the Databricks notebook — read ADF parameters via widgets
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("source_table", "transactions")

ENV        = dbutils.widgets.get("environment")
BATCH_DATE = dbutils.widgets.get("batch_date")
TABLE      = dbutils.widgets.get("source_table")

print(f"Called by ADF | ENV={ENV} | DATE={BATCH_DATE} | TABLE={TABLE}")
```

### Step 4: Complete ADF Pipeline with Error Handling

```json
// pipeline_daily_etl.json — ADF pipeline definition (ARM template)
{
    "name": "daily_etl_pipeline",
    "properties": {
        "activities": [
            {
                "name": "CopyRawData",
                "type": "Copy",
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": {
                            "value": "SELECT * FROM transactions WHERE updated_date = '@{pipeline().parameters.TriggerDate}'",
                            "type": "Expression"
                        }
                    },
                    "sink": {
                        "type": "ParquetSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings"
                        }
                    }
                },
                "linkedServiceName": {"referenceName": "ls_sql_server", "type": "LinkedServiceReference"}
            },
            {
                "name": "RunBronzeIngestion",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {"activity": "CopyRawData", "dependencyConditions": ["Succeeded"]}
                ],
                "typeProperties": {
                    "notebookPath": "/Repos/my-org/data-platform/notebooks/01_ingest_bronze",
                    "baseParameters": {
                        "batch_date": {"value": "@formatDateTime(pipeline().parameters.TriggerDate, 'yyyy-MM-dd')", "type": "Expression"},
                        "environment": {"value": "@pipeline().parameters.Environment", "type": "Expression"}
                    }
                },
                "linkedServiceName": {"referenceName": "ls_databricks_prod", "type": "LinkedServiceReference"}
            },
            {
                "name": "RunSilverTransform",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {"activity": "RunBronzeIngestion", "dependencyConditions": ["Succeeded"]}
                ],
                "typeProperties": {
                    "notebookPath": "/Repos/my-org/data-platform/notebooks/02_transform_silver",
                    "baseParameters": {
                        "batch_date": {"value": "@formatDateTime(pipeline().parameters.TriggerDate, 'yyyy-MM-dd')", "type": "Expression"},
                        "environment": "prod"
                    }
                },
                "linkedServiceName": {"referenceName": "ls_databricks_prod", "type": "LinkedServiceReference"}
            },
            {
                "name": "SendFailureAlert",
                "type": "WebActivity",
                "dependsOn": [
                    {"activity": "RunSilverTransform", "dependencyConditions": ["Failed"]}
                ],
                "typeProperties": {
                    "url": "https://outlook.office.com/webhook/...",
                    "method": "POST",
                    "body": {
                        "value": "@concat('Pipeline FAILED: ', pipeline().runId, ' Error: ', activity('RunSilverTransform').error.message)",
                        "type": "Expression"
                    }
                }
            }
        ],
        "parameters": {
            "Environment": {"type": "string", "defaultValue": "prod"},
            "TriggerDate": {"type": "string"}
        }
    }
}
```

---

## 5.3 Azure Key Vault — Secrets Management

**Golden Rule:** No credential should ever appear in code, notebooks, or ADF pipelines in plain text. All secrets go to Key Vault.

### Setting Up Key Vault Backed Secret Scope

```
STEP 1: Create Azure Key Vault
  Azure Portal → Key Vaults → Create
    Resource Group: rg-databricks-prod
    Name:           kv-databricks-prod
    Region:         East US
    Pricing:        Standard
  → Review + Create

STEP 2: Add secrets to Key Vault
  kv-databricks-prod → Secrets → Generate/Import
  
  Add these secrets:
  ┌─────────────────────────────────────┬──────────────────────────────────┐
  │ Secret Name                         │ Value                            │
  ├─────────────────────────────────────┼──────────────────────────────────┤
  │ adls-service-principal-client-id    │ xxxxxxxx-xxxx-xxxx-xxxx-xxxx    │
  │ adls-service-principal-secret       │ <app-secret-value>              │
  │ adls-tenant-id                      │ xxxxxxxx-xxxx-xxxx-xxxx-xxxx    │
  │ sql-server-username                 │ etl_svc_user                    │
  │ sql-server-password                 │ <strong-password>               │
  │ databricks-pat-token                │ dapi<token-value>               │
  │ sendgrid-api-key                    │ SG.<api-key>                    │
  │ teams-webhook-url                   │ https://outlook.office.com/...  │
  └─────────────────────────────────────┴──────────────────────────────────┘

STEP 3: Grant Databricks access to Key Vault
  kv-databricks-prod → Access Policies → Add Access Policy
    Secret Permissions: Get, List
    Select Principal: Azure Databricks
    (Or your Service Principal / Managed Identity)
  → Save

STEP 4: Create Databricks Secret Scope backed by Key Vault
  In browser: https://<your-workspace>.azuredatabricks.net#secrets/createScope
  
  Scope Name:    kv-prod
  Manage Principal: All Users (or specific group)
  DNS Name:      https://kv-databricks-prod.vault.azure.net/
  Resource ID:   /subscriptions/<sub-id>/resourceGroups/rg-databricks-prod/
                 providers/Microsoft.KeyVault/vaults/kv-databricks-prod
  → Create
```

### Using Secrets in Notebooks

```python
# ─────────────────────────────────────────────────────────────
# READING SECRETS — Production patterns
# ─────────────────────────────────────────────────────────────

# Basic secret retrieval
sp_client_id = dbutils.secrets.get(scope="kv-prod", key="adls-service-principal-client-id")
sp_secret    = dbutils.secrets.get(scope="kv-prod", key="adls-service-principal-secret")
tenant_id    = dbutils.secrets.get(scope="kv-prod", key="adls-tenant-id")

# The value is MASKED in notebook output — you'll see [REDACTED] if you print it
print(sp_client_id)  # Prints: [REDACTED] — safe to have in notebooks

# ─────────────────────────────────────────────────────────────
# ADLS mount using Key Vault secrets
# ─────────────────────────────────────────────────────────────

def mount_adls_container(
    storage_account: str,
    container: str,
    mount_point: str,
    scope: str = "kv-prod"
) -> bool:
    """Mount ADLS Gen2 container using Key Vault-backed secrets."""
    
    if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        print(f"ℹ️  {mount_point} already mounted")
        return True
    
    try:
        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id":
                dbutils.secrets.get(scope=scope, key="adls-service-principal-client-id"),
            "fs.azure.account.oauth2.client.secret":
                dbutils.secrets.get(scope=scope, key="adls-service-principal-secret"),
            "fs.azure.account.oauth2.client.endpoint":
                f"https://login.microsoftonline.com/"
                f"{dbutils.secrets.get(scope=scope, key='adls-tenant-id')}/oauth2/token",
        }
        
        dbutils.fs.mount(
            source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✅ Mounted: {container} → {mount_point}")
        return True
        
    except Exception as e:
        print(f"❌ Mount failed for {container}: {e}")
        return False

# Mount all containers
mounts = [
    ("stdatalakeprod", "bronze",    "/mnt/bronze"),
    ("stdatalakeprod", "silver",    "/mnt/silver"),
    ("stdatalakeprod", "gold",      "/mnt/gold"),
    ("stdatalakeprod", "quarantine","/mnt/quarantine"),
    ("stdatalakeprod", "checkpoints","/mnt/checkpoints"),
]

for storage, container, mount in mounts:
    mount_adls_container(storage, container, mount)

# ─────────────────────────────────────────────────────────────
# JDBC connection using Key Vault secrets
# ─────────────────────────────────────────────────────────────

jdbc_url = (
    "jdbc:sqlserver://prod-sql-server.database.windows.net:1433;"
    "database=ecommerce;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30"
)

df_from_sql = spark.read.format("jdbc") \
    .option("url",      jdbc_url) \
    .option("dbtable",  "dbo.transactions") \
    .option("user",     dbutils.secrets.get("kv-prod", "sql-server-username")) \
    .option("password", dbutils.secrets.get("kv-prod", "sql-server-password")) \
    .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("numPartitions",  "8") \
    .option("partitionColumn","txn_id") \
    .option("lowerBound",     "1") \
    .option("upperBound",     "10000000") \
    .option("fetchsize",      "10000") \
    .load()
```

---

## 5.4 Managed Identity — Zero-Secret Architecture

Managed Identity is the modern, most secure way to authenticate. No secrets to rotate, no client IDs to manage.

```
TRADITIONAL (Service Principal):
  Code → reads secret from Key Vault → uses secret → authenticates to ADLS

MANAGED IDENTITY:
  Code → Azure platform handles auth automatically → authenticates to ADLS
  No secrets anywhere!
```

### Setting Up User-Assigned Managed Identity

```
STEP 1: Create Managed Identity
  Azure Portal → Managed Identities → Create
    Name: mi-databricks-etl
    Region: East US
  → Create → Copy the Principal ID

STEP 2: Grant access to ADLS
  ADLS Gen2 Storage Account → Access Control (IAM)
  → Add role assignment
  → Role: "Storage Blob Data Contributor"
  → Members: Managed Identity → Select "mi-databricks-etl"
  → Save

STEP 3: Assign to Databricks Cluster
  Databricks Workspace → Compute → Your Cluster → Edit
  → Advanced Options → Azure tab
  → Managed Identity: mi-databricks-etl
  → Confirm and Restart

STEP 4: Access storage (NO credentials in code!)
```

```python
# With Managed Identity configured on the cluster,
# access ADLS directly with NO credentials:

# Direct path access:
df = spark.read.format("parquet") \
    .load("abfss://bronze@prodlake.dfs.core.windows.net/transactions/")

# Via mount (mounted once during setup):
df = spark.read.format("parquet").load("/mnt/bronze/transactions/")

# No secrets, no configs — identity handled by Azure!
print("✅ Reading from ADLS using Managed Identity — zero credentials in code")
```

---

## 5.5 Unity Catalog External Locations (Modern Approach)

In Unity Catalog workspaces, use External Locations instead of mounts.

```sql
-- Create storage credential (one-time setup by admin)
CREATE STORAGE CREDENTIAL adls_prod_credential
  WITH AZURE_MANAGED_IDENTITY = '/subscriptions/<sub-id>/resourceGroups/<rg>/
                                  providers/Microsoft.Databricks/accessConnectors/
                                  databricks-access-connector';

-- Create external location
CREATE EXTERNAL LOCATION bronze_external
  URL 'abfss://bronze@prodlake.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL adls_prod_credential)
  COMMENT 'Bronze layer storage for production data lake';

-- Grant access to data engineering team
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION bronze_external
  TO `data-engineering-team`;
```

```python
# Access via External Location (no mounts needed)
df = spark.read.format("delta") \
    .load("abfss://silver@prodlake.dfs.core.windows.net/transactions/")

# Or register as a managed external table
spark.sql("""
    CREATE TABLE IF NOT EXISTS prod_catalog.silver.transactions
    USING DELTA
    LOCATION 'abfss://silver@prodlake.dfs.core.windows.net/transactions/'
""")
```

---

## 5.6 SQL Warehouse & Power BI Integration

### Setting Up SQL Warehouse

```
Databricks Workspace → SQL Warehouses → Create
  
  Name: pbi-reporting-warehouse
  Cluster Size: X-Small (2 workers) — for BI queries
  Auto Stop: 10 minutes (cost control)
  Scaling: 1-3 clusters (scale for concurrent BI users)
  
  Advanced:
    Serverless: ✅ Enable (faster startup, pay-per-query)
    
→ Create (takes ~2 minutes to start)
```

### Connect Power BI to Databricks

```
Method 1: Power BI Desktop (direct connection)
  Power BI Desktop → Get Data → Azure Databricks
  
  Server Hostname: adb-workspace.azuredatabricks.net
  HTTP Path:       /sql/1.0/warehouses/<warehouse-id>
                   (Find in SQL Warehouse → Connection Details)
  Authentication:  Azure Active Directory
  
  → Navigate catalog → select tables → Load

Method 2: DirectQuery (live connection — always fresh data)
  In Power BI: 
  → Get Data → Azure Databricks
  → Connection mode: DirectQuery  ← Always fresh, no import lag
  
  Add measures:
  Total Revenue = SUM('gold daily_summary'[total_volume])
  Avg Transaction = AVERAGE('gold daily_summary'[avg_transaction_value])
  High Risk % = DIVIDE(SUM([high_risk_count]), SUM([transaction_count]))

Method 3: Power BI Service + Gateway
  For scheduled refresh:
  Power BI Service → Datasets → Scheduled Refresh
  Gateway: Not needed for Azure Databricks! (native connector)
  Refresh schedule: Every 15 minutes
```

### Optimize Delta Tables for BI Queries

```sql
-- Create Gold table optimized for Power BI reporting
CREATE TABLE gold.daily_revenue_summary
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed'       = 'true',  -- For incremental PBI refresh
    'delta.dataSkippingNumIndexedCols' = '8'      -- Index more columns for BI filters
)
AS
SELECT
    txn_date,
    YEAR(txn_date)  AS year,
    MONTH(txn_date) AS month,
    payment_mode,
    currency,
    region,
    COUNT(txn_id)                            AS transaction_count,
    SUM(amount)                              AS total_revenue,
    AVG(amount)                              AS avg_transaction,
    MAX(amount)                              AS max_transaction,
    COUNT(DISTINCT customer_id)              AS unique_customers,
    SUM(CASE WHEN fraud_risk_level = 'HIGH' 
             THEN amount ELSE 0 END)         AS high_risk_revenue
FROM silver.transactions
GROUP BY txn_date, YEAR(txn_date), MONTH(txn_date), 
         payment_mode, currency, region;

-- OPTIMIZE for BI access patterns (BI usually filters by date)
OPTIMIZE gold.daily_revenue_summary
ZORDER BY (txn_date, payment_mode);

-- Create a view with business-friendly column names
CREATE OR REPLACE VIEW gold.vw_revenue_dashboard AS
SELECT
    txn_date          AS Date,
    payment_mode      AS `Payment Method`,
    currency          AS Currency,
    region            AS Region,
    transaction_count AS `Transaction Count`,
    total_revenue     AS `Total Revenue (₹)`,
    avg_transaction   AS `Avg Transaction (₹)`,
    unique_customers  AS `Unique Customers`,
    ROUND(high_risk_revenue / NULLIF(total_revenue, 0) * 100, 2) AS `Fraud Risk %`
FROM gold.daily_revenue_summary;
```

---

## 5.7 Interview Questions — Azure Integration

**Q1: What is the difference between a Service Principal and Managed Identity for Databricks-ADLS authentication?**
> A Service Principal requires creating an app registration, generating client secrets, storing them somewhere (Key Vault), and rotating them periodically — there are credentials to manage. A Managed Identity is assigned to the Azure resource (cluster) by the Azure platform — no credentials exist anywhere. Managed Identity is preferred for new projects: it eliminates secret rotation, reduces attack surface, and simplifies code. Service Principals are used when Managed Identity isn't supported (e.g., cross-subscription access).

**Q2: How does ADF pass parameters to a Databricks notebook?**
> ADF uses the "Base Parameters" field in the Databricks Notebook Activity — these appear as widget values in the notebook. In the notebook, `dbutils.widgets.get("param_name")` retrieves them. The values can use ADF dynamic content expressions like `@pipeline().parameters.StartDate` or `@utcNow()`.

**Q3: What is a SQL Warehouse and how is it different from a Databricks cluster?**
> A SQL Warehouse is a compute resource specifically optimized for SQL queries and BI tool connections — it supports ANSI SQL, ODBC/JDBC connections (for Power BI, Tableau), and has auto-start/stop for cost efficiency. Regular Databricks clusters support the full PySpark API and are designed for data engineering workloads. SQL Warehouses are stateless and serverless-capable; clusters maintain Spark session state.

**Q4: How do you prevent secrets from appearing in Databricks notebook output?**
> Use `dbutils.secrets.get()` — the return value is automatically masked in notebook output (shows `[REDACTED]`). Never pass secrets as widget default values, print them, or store them in DataFrame columns. Additionally: use Key Vault-backed secret scopes (not Databricks-managed scopes) for enterprise-grade rotation and auditing. Enable Databricks audit logs to track all secret access events.

---

*Next: [Module 09 — Unity Catalog & Data Governance](../09-Unity-Catalog/01-unity-catalog-complete.md)*
