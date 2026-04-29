# Module 01 — Azure Databricks Fundamentals
## Part 1: Workspace, Clusters & Notebooks

> **Goal:** Understand the Databricks environment deeply enough to set it up, configure it, and explain every decision in a client interview.

---

## 1.1 What is Azure Databricks?

Azure Databricks is a **unified analytics platform** built on top of Apache Spark, co-developed by Microsoft and Databricks, and deeply integrated into the Azure ecosystem.

Think of it this way:

```
Apache Spark          →  The engine (open source, powerful, but raw)
Azure Databricks      →  Spark + managed infrastructure + collaboration + Azure security
```

### Why enterprises use it (Real reasons from client projects):

| Problem Without Databricks | How Databricks Solves It |
|---|---|
| Setting up Spark clusters is complex | Fully managed clusters — one click |
| Spark has no collaboration layer | Notebooks with real-time co-editing |
| Security integration is custom work | Native Azure AD, Unity Catalog |
| Spark tuning requires deep expertise | Auto-optimization, Photon engine |
| No built-in Delta Lake support | Delta Lake is first-class citizen |

**Real client scenario (Banking):** A major bank needed to process 10TB of daily transaction data for fraud detection. They chose Databricks because: managed Spark (no DevOps overhead), Delta Lake for ACID compliance (regulatory requirement), and Unity Catalog for column-level access control on PII data.

---

## 1.2 Core Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   AZURE SUBSCRIPTION                     │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │           DATABRICKS WORKSPACE                   │   │
│  │                                                  │   │
│  │   ┌──────────┐  ┌──────────┐  ┌──────────────┐  │   │
│  │   │Notebooks │  │  Jobs    │  │  SQL Warehouse│  │   │
│  │   └──────────┘  └──────────┘  └──────────────┘  │   │
│  │                                                  │   │
│  │   ┌──────────────────────────────────────────┐   │   │
│  │   │            CLUSTERS                       │   │   │
│  │   │  Driver Node  │  Worker Node  x N         │   │   │
│  │   └──────────────────────────────────────────┘   │   │
│  │                                                  │   │
│  │   ┌──────────────────────────────────────────┐   │   │
│  │   │         DBFS / ADLS Gen2 / Delta Lake     │   │   │
│  │   └──────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌────────────┐  ┌─────────────┐  ┌──────────────────┐ │
│  │ Azure AD   │  │  Key Vault  │  │  ADLS Gen2       │ │
│  └────────────┘  └─────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

**Key components:**
- **Control Plane:** Managed by Databricks (your workspace UI, job scheduler, metadata)
- **Data Plane:** Lives in YOUR Azure subscription (clusters, storage — your data never leaves)

---

## 1.3 Creating a Databricks Workspace — Step by Step

### UI Steps (Azure Portal):

```
1. Go to portal.azure.com
2. Search "Azure Databricks" → Create
3. Fill in:
   - Subscription: <your subscription>
   - Resource Group: rg-databricks-dev
   - Workspace Name: adb-workspace-dev
   - Region: East US (choose close to your ADLS)
   - Pricing Tier: Premium (required for Unity Catalog, RBAC)
4. Click "Review + Create" → Create
5. Wait ~3 minutes → "Go to Resource" → "Launch Workspace"
```

### ⚠️ Common Mistake:
> Choosing **Standard tier** instead of **Premium**. You lose Unity Catalog, cluster policies, and RBAC — all of which are mandatory on real enterprise projects. Always use **Premium**.

---

## 1.4 Clusters — Deep Dive

A **cluster** is the compute engine — a group of virtual machines that run your Spark jobs.

### Cluster Types

#### 1. All-Purpose Clusters (Interactive)
- Used for: Notebook development, ad-hoc exploration
- Billed: Per hour while running (even when idle)
- Multi-user: Yes — multiple people can attach and run

#### 2. Job Clusters
- Used for: Running scheduled jobs/pipelines
- Billed: Only while the job is running
- Auto-terminated: Yes — immediately after job finishes
- **Best practice for production:** Always use job clusters for scheduled pipelines. Reduces cost by 60-80%.

#### 3. SQL Warehouses (Compute for SQL)
- Used for: Running SQL queries, BI tool connections
- Separate from Spark clusters
- Supports serverless option

---

### Cluster Configuration — What Every Field Means

```
Cluster Name:        prod-etl-cluster          # Use naming conventions
Cluster Mode:        Multi Node                 # Standard for ETL
                     Single Node                # For dev/testing only
Databricks Runtime:  13.3 LTS (Spark 3.4.1)   # LTS = Long Term Support
                                                # ALWAYS use LTS in production

Worker Type:         Standard_DS3_v2            # 14GB RAM, 4 cores
                     Standard_DS4_v2            # 28GB RAM, 8 cores (for heavy ETL)
Min Workers:         2
Max Workers:         8                          # Autoscaling enabled

Driver Type:         Same as worker             # Or larger for complex jobs
Enable Autoscaling:  ✅ Yes                     # Essential for variable workloads
Auto Termination:    120 minutes                # Prevent idle billing
```

### Runtime Selection — Why It Matters

```
Databricks Runtime 13.3 LTS
    └── Apache Spark 3.4.1
    └── Scala 2.12
    └── Python 3.10
    └── Delta Lake 2.4
    └── MLflow 2.6

ML Runtime (13.3 LTS ML)
    └── Everything above +
    └── PyTorch, TensorFlow, scikit-learn pre-installed
```

**Rule:** In production, **always pin to an LTS (Long Term Support) runtime**. Non-LTS runtimes have shorter support windows and introduce breaking changes.

---

### Cluster Policies (Enterprise Feature — Premium Tier)

In real client projects, **you don't give developers free-reign on cluster config**. Cluster policies enforce guardrails:

```json
{
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"],
    "defaultValue": "Standard_DS3_v2"
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 30,
    "maxValue": 120,
    "defaultValue": 60
  },
  "spark_version": {
    "type": "regex",
    "pattern": "^13\\..*\\.lts.*$"
  }
}
```

**Why this matters:** On a real project, a developer spinning up a 32-node cluster for testing can cost thousands of dollars in a day. Policies prevent this.

---

## 1.5 Notebooks — Complete Reference

### Creating and Organizing Notebooks

```
Workspace/
├── Users/
│   └── sumit@company.com/
│       └── scratch/              ← personal experiments
├── Shared/
│   └── ETL/
│       ├── ingestion/
│       │   ├── 01_raw_ingest.py
│       │   └── 02_schema_validate.py
│       ├── transformation/
│       │   └── 03_silver_transform.py
│       └── serving/
│           └── 04_gold_aggregate.py
└── Repos/                        ← Git-connected notebooks (production standard)
    └── my-org/
        └── data-platform/
```

**Best Practice:** In enterprise projects, notebooks live in **Databricks Repos** (Git integration), not in the personal workspace. This enables version control, PR reviews, and CI/CD.

---

### Magic Commands — Complete Reference

Magic commands are special directives that change the behavior of a cell.

#### `%python` — Switch to Python (default in Python notebooks)
```python
%python
df = spark.read.format("delta").load("/mnt/bronze/transactions")
df.show(5)
```

#### `%sql` — Run SQL directly in a Python notebook
```sql
%sql
SELECT 
    customer_id,
    SUM(amount) AS total_spend,
    COUNT(*) AS transaction_count
FROM bronze.transactions
WHERE transaction_date >= '2024-01-01'
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;
```

#### `%scala` — Switch to Scala in a Python notebook
```scala
%scala
val df = spark.read.format("delta").load("/mnt/bronze/transactions")
df.printSchema()
```

#### `%sh` — Run shell commands on the driver node
```bash
%sh
# Check driver disk space
df -h

# List files
ls -la /dbfs/mnt/bronze/

# Install a system package (driver only — not workers)
apt-get install -y some-package
```

#### `%fs` — Shortcut for dbutils.fs commands
```
%fs ls /mnt/bronze/

%fs head /mnt/bronze/transactions/2024/01/01/file.csv

%fs mkdirs /mnt/silver/customers/

%fs rm -r /mnt/temp/staging/
```

#### `%run` — Run another notebook inline (imports its scope)
```python
%run ./utils/common_functions

# After %run, functions defined in common_functions are available here
result = transform_data(df)
```

**Real-world use:** Create a `config.py` notebook with all environment variables, then `%run` it at the top of every ETL notebook. This keeps config centralized.

#### `%md` — Markdown documentation
```markdown
%md
# Daily Transaction Pipeline
## Overview
This notebook processes raw transaction data from ADLS Bronze layer
and applies business rules to produce Silver layer output.

**Owner:** Data Engineering Team  
**Schedule:** Daily at 2:00 AM UTC  
**SLA:** Must complete by 4:00 AM UTC
```

---

## 1.6 Cluster Configuration — Practical Setup (UI Steps)

### Step-by-Step: Create Your First Cluster

```
1. In Databricks workspace → Click "Compute" in left sidebar
2. Click "+ Create Cluster"
3. Configure:
   
   Cluster name:     dev-cluster-01
   Policy:           (Select if your workspace has policies)
   Cluster mode:     Single Node  ← for learning; Multi Node for ETL
   Databricks Runtime: 13.3 LTS (Spark 3.4.1, Scala 2.12)
   
   Worker type:      Standard_DS3_v2
   Min workers:      1
   Max workers:      4
   
   ✅ Enable autoscaling
   
   Auto termination: 60 minutes
   
4. Expand "Advanced Options":
   
   Spark Config:
   spark.databricks.delta.preview.enabled true
   spark.sql.adaptive.enabled true
   spark.sql.adaptive.coalescePartitions.enabled true
   
5. Click "Create Cluster"
6. Wait for green circle (Running state) — ~3-5 minutes
```

### Spark Config Explained (What to Set in Production)

```
# Adaptive Query Execution — automatically optimizes join strategies
spark.sql.adaptive.enabled                                true

# Automatically merge small shuffle partitions
spark.sql.adaptive.coalescePartitions.enabled             true

# Enable Delta Lake optimistic concurrency
spark.databricks.delta.preview.enabled                    true

# Set shuffle partitions (default 200 is usually wrong)
# Rule: ~2x number of CPU cores in your cluster
spark.sql.shuffle.partitions                              200

# For large data (>1TB), increase:
spark.sql.shuffle.partitions                              800

# Enable Photon (Databricks vectorized execution engine — Premium)
spark.databricks.photon.enabled                          true
```

---

## 1.7 First Notebook — Hello Spark

Attach this to your cluster and run cell by cell:

```python
# Cell 1: Check Spark version
print(f"Spark Version: {spark.version}")
print(f"Python Version: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")

# Cell 2: Create a simple DataFrame from Python data
data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob",   "Marketing",   75000),
    (3, "Carol", "Engineering", 110000),
    (4, "Dave",  "HR",          65000),
    (5, "Eve",   "Engineering", 105000),
]

columns = ["emp_id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)

# Cell 3: Basic operations
df.printSchema()
# Output:
# root
#  |-- emp_id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: long (nullable = true)

df.show()
# Output:
# +------+-----+-----------+------+
# |emp_id| name| department|salary|
# +------+-----+-----------+------+
# |     1|Alice|Engineering| 95000|
# ...

# Cell 4: First transformation
from pyspark.sql.functions import col, avg, count

dept_summary = df.groupBy("department") \
    .agg(
        count("emp_id").alias("headcount"),
        avg("salary").alias("avg_salary")
    ) \
    .orderBy("avg_salary", ascending=False)

dept_summary.show()
# Output:
# +-----------+---------+----------+
# | department|headcount|avg_salary|
# +-----------+---------+----------+
# |Engineering|        3|103333.33 |
# |  Marketing|        1|  75000.0 |
# |         HR|        1|  65000.0 |
# +-----------+---------+----------+

# Cell 5: Write to Delta (your first Delta table!)
df.write.format("delta").mode("overwrite").saveAsTable("default.employees")

# Cell 6: Read it back with SQL
%sql
SELECT department, AVG(salary) as avg_salary 
FROM default.employees 
GROUP BY department
```

---

## 1.8 Common Mistakes in This Module

| Mistake | Consequence | Fix |
|---|---|---|
| Using Standard tier workspace | Can't use Unity Catalog, RBAC | Always use Premium tier |
| Leaving clusters running | Huge cost overrun | Set auto-termination (60-120 min) |
| Using non-LTS runtime | Breaking changes, short support | Always pin to LTS |
| High default shuffle partitions (200) | Slow jobs on large data | Tune to 2x CPU cores |
| Writing notebooks in personal workspace | No version control, lost work | Use Databricks Repos |
| Not enabling AQE | Missing automatic optimizations | Set `spark.sql.adaptive.enabled=true` |

---

## 1.9 Interview Questions — Module 01

**Q1: What is the difference between All-Purpose and Job clusters?**
> All-purpose clusters are persistent, shared, and billed hourly even when idle — used for development. Job clusters are ephemeral, created fresh for each job run, terminated immediately after, and are 30-80% cheaper. In production, **always use job clusters** for scheduled pipelines to minimize cost.

**Q2: What is the Databricks Control Plane vs Data Plane?**
> The Control Plane is managed by Databricks and hosts the workspace UI, REST APIs, job scheduler, and cluster manager metadata. The Data Plane runs inside **your** Azure subscription — actual VMs, networking, and storage. Your data never leaves your subscription. This is Databricks' key enterprise security differentiator.

**Q3: Why would you use cluster policies?**
> To enforce governance — prevent developers from spinning up oversized clusters, enforce auto-termination, restrict to approved VM types, and force LTS runtimes. Critical for cost management on enterprise projects.

**Q4: What Spark configs do you always set in production?**
> `spark.sql.adaptive.enabled=true` (AQE), `spark.sql.adaptive.coalescePartitions.enabled=true`, tuned `spark.sql.shuffle.partitions` based on data volume, and `spark.databricks.delta.preview.enabled=true`.

**Q5: What is the difference between `%run` and `import` in Databricks?**
> `%run` executes another notebook **inline in the current notebook's scope**, making all its variables and functions available. `import` imports Python modules/packages. Use `%run` for config notebooks or shared utility notebooks that need to run in context.

---

*Next: [Part 2 — DBFS, Storage Mounts & ADLS Gen2 Integration](./02-dbfs-storage-mounts.md)*
