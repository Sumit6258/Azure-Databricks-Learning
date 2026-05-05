# Topics 4–9: Unity Catalog, Magic Commands, dbutils, Delta Lake, Tables & Workflows
## Complete Deep-Dive with Theory + Practical Examples

---

# TOPIC 4: UNITY CATALOG

---

## 4.1 What Problem Does Unity Catalog Solve?

Before Unity Catalog, every Databricks workspace had its own isolated Hive Metastore. Imagine your company has 3 workspaces (dev, test, prod):

```
OLD WAY (Before Unity Catalog):
  Dev Workspace  → Own Hive Metastore → Own tables → Own permissions
  Test Workspace → Own Hive Metastore → Own tables → Own permissions
  Prod Workspace → Own Hive Metastore → Own tables → Own permissions

  Problems:
  ✗ Table "silver.transactions" in dev is DIFFERENT from prod
  ✗ You must define permissions separately in each workspace
  ✗ No way to share tables across workspaces
  ✗ No column-level security
  ✗ No data lineage tracking
  ✗ No PII tagging
```

Unity Catalog is a single governance layer above ALL your workspaces:

```
NEW WAY (With Unity Catalog):
                        ┌─────────────────────────────────────┐
                        │      UNITY CATALOG METASTORE        │
                        │   (One for the whole organization)  │
                        │                                     │
                        │  Tables, Schemas, Catalogs          │
                        │  Permissions, Lineage, Tags         │
                        └─────────────────────────────────────┘
                                        │
                 ┌──────────────────────┼──────────────────────┐
                 ▼                      ▼                      ▼
          Dev Workspace          Test Workspace        Prod Workspace
          (All see same          (Same governance)     (Same governance)
           governance)
```

---

## 4.2 The Three-Level Hierarchy — VERY IMPORTANT

This is the most asked interview question about Unity Catalog.

```
HIERARCHY:  CATALOG → SCHEMA → TABLE/VIEW/FUNCTION

Think of it like a filing cabinet:
  CATALOG  = The filing cabinet itself
  SCHEMA   = A drawer in the cabinet
  TABLE    = A folder inside the drawer

REAL EXAMPLE:
  prod_catalog          ← CATALOG (top level)
  └── bronze            ← SCHEMA (like a database)
  │   ├── transactions  ← TABLE
  │   └── customers     ← TABLE
  ├── silver
  │   ├── transactions_clean
  │   └── customers_enriched
  └── gold
      ├── daily_revenue
      └── customer_360

FULLY QUALIFIED TABLE NAME:
  prod_catalog.silver.transactions_clean
  │            │      │
  │            │      └── TABLE name
  │            └────────── SCHEMA name
  └─────────────────────── CATALOG name
```

---

## 4.3 Unity Catalog in Practice — SQL Commands

```sql
-- ════════════════════════════════════════════════════════════
-- VIEWING THE HIERARCHY
-- ════════════════════════════════════════════════════════════

-- List all catalogs you can access
SHOW CATALOGS;
-- +------------------+
-- | catalog          |
-- +------------------+
-- | hive_metastore   |  ← Legacy (pre-Unity Catalog)
-- | prod_catalog     |  ← Your production catalog
-- | dev_catalog      |  ← Your dev catalog
-- | shared_catalog   |  ← Shared reference data

-- List schemas in a catalog
SHOW SCHEMAS IN prod_catalog;
-- +----------+
-- | schema   |
-- +----------+
-- | bronze   |
-- | silver   |
-- | gold     |

-- List tables in a schema
SHOW TABLES IN prod_catalog.silver;
-- +---------------------------+
-- | tableName                 |
-- +---------------------------+
-- | transactions_clean        |
-- | customers_enriched        |

-- ════════════════════════════════════════════════════════════
-- CREATING THE HIERARCHY
-- ════════════════════════════════════════════════════════════

-- Create a catalog (admin only)
CREATE CATALOG IF NOT EXISTS my_catalog
  COMMENT 'My personal learning catalog';

-- Set default catalog (so you don't type it every time)
USE CATALOG my_catalog;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw data - append only';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Business-ready aggregations';

-- Set default schema
USE SCHEMA silver;

-- Now you can reference tables without full path:
SELECT * FROM transactions_clean;
-- Instead of:
SELECT * FROM my_catalog.silver.transactions_clean;

-- ════════════════════════════════════════════════════════════
-- ACCESS CONTROL
-- ════════════════════════════════════════════════════════════

-- Grant catalog access
GRANT USE CATALOG ON CATALOG prod_catalog TO `data-engineering-team`;

-- Grant schema access
GRANT USE SCHEMA ON SCHEMA prod_catalog.silver TO `data-engineering-team`;

-- Grant table read access
GRANT SELECT ON TABLE prod_catalog.silver.transactions TO `data-analysts`;

-- Grant full access to a schema
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA prod_catalog.gold TO `data-engineers`;

-- Revoke access
REVOKE SELECT ON TABLE prod_catalog.silver.transactions FROM `junior-analysts`;

-- Check current permissions on a table
SHOW GRANTS ON TABLE prod_catalog.silver.transactions;

-- ════════════════════════════════════════════════════════════
-- PRACTICAL DEMO — Create your own catalog and tables
-- ════════════════════════════════════════════════════════════

-- Step 1: Create catalog
CREATE CATALOG IF NOT EXISTS sumit_learning;
USE CATALOG sumit_learning;

-- Step 2: Create schema
CREATE SCHEMA IF NOT EXISTS demo;
USE SCHEMA demo;

-- Step 3: Create a managed table
CREATE TABLE IF NOT EXISTS employees (
  emp_id     INT     NOT NULL,
  name       STRING  NOT NULL,
  department STRING,
  salary     DOUBLE,
  hire_date  DATE
)
USING DELTA
COMMENT 'Employee master data for learning';

-- Step 4: Insert data
INSERT INTO employees VALUES
  (1, 'Alice',  'Engineering', 95000, '2020-01-15'),
  (2, 'Bob',    'Marketing',   75000, '2019-06-01'),
  (3, 'Carol',  'Engineering', 110000,'2018-03-20'),
  (4, 'Dave',   'HR',          65000, '2021-09-10'),
  (5, 'Eve',    'Engineering', 105000,'2017-11-05');

-- Step 5: Query using full 3-level path
SELECT * FROM sumit_learning.demo.employees;

-- Step 6: Check table details
DESCRIBE TABLE EXTENDED sumit_learning.demo.employees;
```

---

# TOPIC 5: DATABRICKS MAGIC COMMANDS

---

## 5.1 What Are Magic Commands?

Magic commands are special instructions that start with `%` and change how a single cell behaves. They are NOT Python or SQL — they're Databricks-specific commands.

```
Regular cell (Python notebook):
  x = 5          ← Python code, runs as Python

%sql cell:
  %sql
  SELECT * FROM table  ← Runs as SQL, even in Python notebook

The % at the start = "Magic" — changes the cell's execution mode
```

---

## 5.2 ALL Magic Commands — Complete Reference

### `%python` — Run Python

```python
# %python is implicit in Python notebooks
# Use it explicitly to switch back to Python after using %scala or %sql

%python
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()
# +---+-----+
# | id| name|
# +---+-----+
# |  1|Alice|
# |  2|  Bob|
# +---+-----+
```

### `%sql` — Run SQL

```sql
-- Run SQL in ANY language notebook (Python, Scala, R)
-- Most useful: run SQL inside a Python notebook

%sql
-- Create a table
CREATE TABLE IF NOT EXISTS default.demo_sql_table (
  id INT,
  name STRING,
  amount DOUBLE
) USING DELTA;

-- Insert data
INSERT INTO default.demo_sql_table VALUES
  (1, 'Transaction A', 1500.00),
  (2, 'Transaction B', 250.75),
  (3, 'Transaction C', 8900.00);

-- Query
SELECT
  name,
  amount,
  CASE WHEN amount > 1000 THEN 'HIGH' ELSE 'LOW' END as category
FROM default.demo_sql_table
ORDER BY amount DESC;
```

### `%scala` — Run Scala

```scala
// Run Scala code in a Python notebook
// Useful for using Scala-only Spark features

%scala
val data = Seq((1, "Alice", 95000), (2, "Bob", 75000))
val df = spark.createDataFrame(data).toDF("id", "name", "salary")
df.printSchema()
// root
//  |-- id: integer (nullable = false)
//  |-- name: string (nullable = true)
//  |-- salary: integer (nullable = false)
```

### `%r` — Run R

```r
%r
# R code in a Python notebook (R must be installed)
x <- c(1, 2, 3, 4, 5)
mean_val <- mean(x)
print(paste("Mean:", mean_val))
# [1] "Mean: 3"
```

### `%sh` — Shell Commands (Driver Only)

```bash
# Run Linux shell commands on the DRIVER node
# NOT on workers — only the driver machine

%sh
# Check disk space on driver
df -h

# Check which Python is being used
which python3
python3 --version

# Install a system package
sudo apt-get install -y curl

# Check files on the driver filesystem
ls -la /tmp/

# Environment variables
echo $JAVA_HOME

# Network connectivity check
curl -s https://api.github.com/meta | head -20

# Run a shell script
cat > /tmp/test_script.sh << 'EOF'
#!/bin/bash
echo "Today is: $(date)"
echo "Hostname: $(hostname)"
EOF
chmod +x /tmp/test_script.sh
/tmp/test_script.sh
```

### `%fs` — DBFS File System Operations

```
# Shortcut for dbutils.fs commands
# List files
%fs ls /mnt/

# List specific path
%fs ls /mnt/bronze/

# Check file content (first 65536 bytes)
%fs head /mnt/bronze/config.json

# Create directory
%fs mkdirs /mnt/bronze/new_folder/

# Copy file
%fs cp /mnt/bronze/source.csv /mnt/bronze/backup/source.csv

# Move file
%fs mv /mnt/bronze/old_name.csv /mnt/bronze/new_name.csv

# Delete file
%fs rm /mnt/bronze/temp_file.csv

# Delete directory (recursive)
%fs rm -r /mnt/bronze/temp_folder/
```

### `%run` — Run Another Notebook

```python
# Execute another notebook inline
# Everything defined in that notebook becomes available here

# Run by relative path
%run ./utils/config

# Run by absolute workspace path
%run /Repos/my-org/project/notebooks/utils/config

# After %run, variables from the other notebook are available:
# If config.py defines: BATCH_DATE = "2024-01-15"
# Then here: print(BATCH_DATE)  → "2024-01-15"

# IMPORTANT: %run cannot pass parameters!
# Use dbutils.notebook.run() to pass parameters (shown in hands-on tasks)
```

### `%md` — Markdown Documentation

```markdown
%md
# Pipeline Documentation

## Overview
This notebook processes **daily transaction data** from ADLS Bronze layer
and produces Silver layer output with data quality checks applied.

## Pipeline Flow
```
Bronze → Validate → Deduplicate → Silver
```

## SLA
- **Schedule:** Daily at 2:00 AM UTC
- **Must complete by:** 4:00 AM UTC
- **Owner:** Data Engineering Team

| Parameter | Description | Default |
|-----------|-------------|---------|
| batch_date | Processing date | Yesterday |
| env | Environment | dev |

> ⚠️ **WARNING:** Do not modify the Silver table directly. All changes must go through this pipeline.
```

### `%pip` — Install Python Packages

```python
# Install packages on ALL nodes in the cluster (driver + workers)
# Changes persist for the lifetime of the cluster session

%pip install pandas==2.1.0
%pip install matplotlib seaborn
%pip install great-expectations==0.18.0
%pip install delta-spark==3.0.0

# Install multiple at once
%pip install pandas numpy matplotlib scikit-learn

# Install from requirements file
%pip install -r /dbfs/FileStore/requirements.txt

# IMPORTANT:
# - %pip restarts the Python interpreter
# - Run %pip BEFORE your main code cells
# - Don't use %pip inside loops or functions
# - For cluster-level packages, use cluster Libraries tab instead
```

### `%conda` — Conda Package Manager

```python
# Install conda packages (less common than %pip)
%conda install -c conda-forge pyarrow

# Show installed conda packages
%conda list
```

---

# TOPIC 6: UTILITIES (dbutils) — COMPLETE REFERENCE

---

## 6.1 What is dbutils?

`dbutils` (Databricks Utilities) is a built-in Python library available in every Databricks notebook. It provides tools for file system operations, secrets management, widget creation, and more.

```python
# View all available utilities and their methods
dbutils.help()

# Output:
# This module provides various utilities for users to interact with the
# rest of Databricks.

# submodules:
# credentials  Utilities for interacting with credentials within notebooks
# data         Utilities for understanding and interacting with datasets
# fs           Utilities for accessing and working with remote filesystems
# jobs         Utilities for leveraging jobs features
# library      Utilities for session isolated libraries
# notebook     Utilities for running notebooks
# secrets      Utilities for leveraging secrets within notebooks
# widgets      Utilities for working with notebook widgets

# Get help on a specific utility
dbutils.fs.help()
dbutils.secrets.help()
```

---

## 6.2 dbutils.fs — File System Operations

```python
# ═══════════════════════════════════════════════════
# LIST FILES
# ═══════════════════════════════════════════════════

# List a directory
files = dbutils.fs.ls("/mnt/bronze/")
for f in files:
    print(f"Name: {f.name}, Size: {f.size}, Path: {f.path}")

# Better: display as table
display(dbutils.fs.ls("/mnt/bronze/"))

# ═══════════════════════════════════════════════════
# CREATE / COPY / MOVE / DELETE
# ═══════════════════════════════════════════════════

# Create a directory
dbutils.fs.mkdirs("/mnt/bronze/new_folder/2024/01/")

# Copy a file
dbutils.fs.cp(
    "/mnt/bronze/transactions.csv",
    "/mnt/bronze/backup/transactions_backup.csv"
)

# Copy entire directory (recursive)
dbutils.fs.cp(
    "/mnt/bronze/2024/",
    "/mnt/backup/2024/",
    recurse=True
)

# Move/rename a file
dbutils.fs.mv(
    "/mnt/bronze/old_name.csv",
    "/mnt/bronze/new_name.csv"
)

# Delete a file
dbutils.fs.rm("/mnt/bronze/temp.csv")

# Delete a directory
dbutils.fs.rm("/mnt/bronze/temp_folder/", recurse=True)

# ═══════════════════════════════════════════════════
# READ FILE CONTENT
# ═══════════════════════════════════════════════════

# Read first N bytes of a file (useful for quick inspection)
content = dbutils.fs.head("/mnt/bronze/config.json")
print(content)

# Read max 100KB
content = dbutils.fs.head("/mnt/bronze/large_file.txt", maxBytes=100000)

# ═══════════════════════════════════════════════════
# WRITE SMALL FILES
# ═══════════════════════════════════════════════════

# Write text content to a file
dbutils.fs.put(
    "/mnt/bronze/checkpoint.txt",
    "Last processed: 2024-01-15",
    overwrite=True
)

# ═══════════════════════════════════════════════════
# CHECK IF PATH EXISTS (IDEMPOTENCY PATTERN)
# ═══════════════════════════════════════════════════

def path_exists(path: str) -> bool:
    """Check if a DBFS path exists."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# Use it
if not path_exists("/mnt/bronze/processed/"):
    dbutils.fs.mkdirs("/mnt/bronze/processed/")
    print("Created directory")
else:
    print("Directory already exists")

# ═══════════════════════════════════════════════════
# MOUNTS
# ═══════════════════════════════════════════════════

# List all mounts
for mount in dbutils.fs.mounts():
    print(f"{mount.mountPoint} → {mount.source}")

# Check if a specific mount exists
def is_mounted(mount_point: str) -> bool:
    return any(m.mountPoint == mount_point for m in dbutils.fs.mounts())

# Mount ADLS (idempotent)
if not is_mounted("/mnt/bronze"):
    dbutils.fs.mount(
        source="abfss://bronze@storage.dfs.core.windows.net/",
        mount_point="/mnt/bronze",
        extra_configs={
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id":
                dbutils.secrets.get("kv-scope", "sp-client-id"),
            "fs.azure.account.oauth2.client.secret":
                dbutils.secrets.get("kv-scope", "sp-secret"),
            "fs.azure.account.oauth2.client.endpoint":
                f"https://login.microsoftonline.com/{dbutils.secrets.get('kv-scope','tenant-id')}/oauth2/token"
        }
    )
    print("✅ Mounted /mnt/bronze")

# Unmount
dbutils.fs.unmount("/mnt/bronze")
```

---

## 6.3 dbutils.secrets — Secure Credentials

```python
# ═══════════════════════════════════════════════════
# SECRETS BASICS
# ═══════════════════════════════════════════════════

# NEVER hardcode secrets like this:
# password = "MyPassword123"  ← BAD, BAD, BAD

# ALWAYS use dbutils.secrets:
password = dbutils.secrets.get(scope="kv-scope", key="db-password")
# The value is MASKED in notebook output: [REDACTED]
# You cannot print the actual value — it's protected

# List available secret scopes
scopes = dbutils.secrets.listScopes()
for scope in scopes:
    print(f"Scope: {scope.name}, Backend: {scope.backendType}")

# List keys in a scope (names only, not values)
keys = dbutils.secrets.list("kv-scope")
for key in keys:
    print(f"Key: {key.key}")

# ═══════════════════════════════════════════════════
# PRACTICAL USAGE PATTERNS
# ═══════════════════════════════════════════════════

# Database connection
jdbc_url = "jdbc:sqlserver://server.database.windows.net:1433;database=mydb"
db_user  = dbutils.secrets.get("kv-scope", "sql-username")
db_pass  = dbutils.secrets.get("kv-scope", "sql-password")

df = spark.read.format("jdbc") \
    .option("url",      jdbc_url) \
    .option("dbtable",  "dbo.transactions") \
    .option("user",     db_user) \
    .option("password", db_pass) \
    .load()

# API key for external service
api_key = dbutils.secrets.get("kv-scope", "sendgrid-api-key")

# Storage account key (if not using Managed Identity)
storage_key = dbutils.secrets.get("kv-scope", "adls-storage-key")
spark.conf.set(
    "fs.azure.account.key.prodlake.dfs.core.windows.net",
    storage_key
)
```

---

## 6.4 dbutils.widgets — Parameters and Widgets

```python
# ═══════════════════════════════════════════════════
# WIDGET TYPES
# ═══════════════════════════════════════════════════

# 1. TEXT — free text input
dbutils.widgets.text(
    name="batch_date",          # Internal name
    defaultValue="2024-01-15",  # Default value
    label="Batch Date"          # Label shown in UI
)

# 2. DROPDOWN — select from a fixed list
dbutils.widgets.dropdown(
    name="environment",
    defaultValue="dev",
    choices=["dev", "test", "prod"],
    label="Environment"
)

# 3. COMBOBOX — dropdown + can type custom value
dbutils.widgets.combobox(
    name="table_name",
    defaultValue="transactions",
    choices=["transactions", "customers", "products"],
    label="Table Name"
)

# 4. MULTISELECT — select multiple values
dbutils.widgets.multiselect(
    name="regions",
    defaultValue="Mumbai",
    choices=["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"],
    label="Regions"
)

# READ WIDGET VALUES
batch_date  = dbutils.widgets.get("batch_date")
environment = dbutils.widgets.get("environment")
table_name  = dbutils.widgets.get("table_name")
regions     = dbutils.widgets.get("regions")  # Returns comma-separated string

print(f"Processing: {table_name} for {batch_date} in {environment}")

# REMOVE WIDGETS
dbutils.widgets.remove("batch_date")   # Remove one
dbutils.widgets.removeAll()            # Remove all
```

---

## 6.5 dbutils.notebook — Notebook Orchestration

```python
# ═══════════════════════════════════════════════════
# RUN ANOTHER NOTEBOOK
# ═══════════════════════════════════════════════════

# Run a notebook and get its return value
result = dbutils.notebook.run(
    path="./child_notebook",        # Relative path
    timeout_seconds=300,            # Fail if takes > 5 minutes
    arguments={                     # Pass as widget values
        "batch_date": "2024-01-15",
        "environment": "prod",
        "table_name": "transactions"
    }
)

print(f"Child notebook returned: {result}")

# In the CHILD notebook — return a value:
# dbutils.notebook.exit("SUCCESS:1500")

# ═══════════════════════════════════════════════════
# EXIT CURRENT NOTEBOOK
# ═══════════════════════════════════════════════════

# Exit with a value (visible to parent notebook or job)
dbutils.notebook.exit("SUCCESS:processed_1500_records")
dbutils.notebook.exit("FAILED:null_customer_id_found")
```

---

## 6.6 dbutils.jobs — Task Values (Between Job Tasks)

```python
# ═══════════════════════════════════════════════════
# PASS DATA BETWEEN TASKS IN A JOB
# ═══════════════════════════════════════════════════

# In TASK 1 (ingest_bronze): set a value
record_count = df.count()
dbutils.jobs.taskValues.set(key="record_count", value=str(record_count))
dbutils.jobs.taskValues.set(key="batch_date",   value="2024-01-15")
dbutils.jobs.taskValues.set(key="status",       value="SUCCESS")

# In TASK 2 (transform_silver): read the value from task 1
upstream_count = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",  # Name of the upstream task
    key="record_count",
    default="0"               # Default if key doesn't exist
)
batch_date = dbutils.jobs.taskValues.get("ingest_bronze", "batch_date", "")

print(f"Upstream ingested {upstream_count} records for {batch_date}")

# Use it for conditional logic
if int(upstream_count) == 0:
    print("No data to process — skipping")
    dbutils.notebook.exit("SKIPPED:no_data")
```

---

# TOPIC 7: DELTA LAKE

---

## 7.1 What is Delta Lake and Why Was It Created?

Before Delta Lake, data engineers faced these problems with plain Parquet/CSV files:

```
PROBLEM 1: No ACID Transactions
  Writer starts writing 100 files to a folder
  Writer crashes after 60 files
  Reader reads the folder — gets PARTIAL data
  No rollback, no recovery
  ← Delta Lake SOLVES THIS with atomic commits

PROBLEM 2: No Updates
  To "update" row 5 in Parquet:
  Step 1: Read entire file → Step 2: Modify → Step 3: Write new file
  Step 4: Delete old file ← What if crash here? Data LOST forever
  ← Delta Lake SOLVES THIS with transaction log

PROBLEM 3: No Schema Enforcement
  Someone uploads CSV with wrong column types
  Parquet happily accepts it — your table is now corrupt
  ← Delta Lake SOLVES THIS with schema enforcement

PROBLEM 4: No History
  "What was the data on Jan 15th?" → Impossible with plain Parquet
  ← Delta Lake SOLVES THIS with time travel
```

---

## 7.2 The Delta Lake Transaction Log

The `_delta_log` folder is the secret behind Delta Lake's ACID properties:

```
/mnt/silver/transactions/              ← Your Delta table
├── _delta_log/                        ← THE BRAIN of Delta Lake
│   ├── 00000000000000000000.json      ← Version 0: initial write
│   ├── 00000000000000000001.json      ← Version 1: first UPDATE
│   ├── 00000000000000000002.json      ← Version 2: MERGE operation
│   ├── 00000000000000000003.json      ← Version 3: schema change
│   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint
│
├── part-00000-abc123.snappy.parquet   ← Actual data files
├── part-00001-def456.snappy.parquet
└── part-00002-ghi789.snappy.parquet
```

Each JSON in `_delta_log` is an ATOMIC record:

```json
{
  "commitInfo": {
    "timestamp": 1705327845000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"},
    "userName": "sumit@company.com"
  },
  "add": {
    "path": "part-00000-abc123.snappy.parquet",
    "size": 1048576,
    "modificationTime": 1705327844000,
    "stats": "{\"numRecords\": 5000}"
  }
}
```

---

## 7.3 Delta Lake in Practice — All Operations

```python
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# ═══════════════════════════════════════════════════
# CREATING DELTA TABLES
# ═══════════════════════════════════════════════════

# Method 1: Write DataFrame as Delta
df.write.format("delta").mode("overwrite").save("/mnt/silver/employees/")

# Method 2: Create as managed table
df.write.format("delta").mode("overwrite").saveAsTable("silver.employees")

# Method 3: SQL DDL
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.employees (
        emp_id     INT NOT NULL,
        name       STRING NOT NULL,
        department STRING,
        salary     DOUBLE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (department)
    COMMENT 'Employee master data'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.enableChangeDataFeed'       = 'true'
    )
""")

# ═══════════════════════════════════════════════════
# READING DELTA TABLES
# ═══════════════════════════════════════════════════

# Current version
df = spark.read.format("delta").load("/mnt/silver/employees/")
df = spark.table("silver.employees")

# Specific version (TIME TRAVEL)
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/mnt/silver/employees/")

# Specific timestamp
df_jan = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 00:00:00") \
    .load("/mnt/silver/employees/")

# SQL time travel
spark.sql("SELECT * FROM silver.employees VERSION AS OF 2")
spark.sql("SELECT * FROM silver.employees TIMESTAMP AS OF '2024-01-15'")

# ═══════════════════════════════════════════════════
# HISTORY — See all past operations
# ═══════════════════════════════════════════════════

dt = DeltaTable.forPath(spark, "/mnt/silver/employees/")
history = dt.history()
history.select("version","timestamp","operation","operationParameters").show(truncate=False)

# +-------+-------------------+---------+------------------------------------------+
# |version|timestamp          |operation|operationParameters                       |
# +-------+-------------------+---------+------------------------------------------+
# |2      |2024-01-15 10:30:00|MERGE    |{predicate->id=id, matchedPredicates->...}|
# |1      |2024-01-14 02:00:00|WRITE    |{mode->Append}                            |
# |0      |2024-01-14 00:00:00|WRITE    |{mode->Overwrite}                         |

# ═══════════════════════════════════════════════════
# MERGE (UPSERT) — Insert + Update + Delete together
# ═══════════════════════════════════════════════════

# Scenario: Daily update arrives with new + modified employees
updates = spark.createDataFrame([
    (1, "Alice",  "Engineering", 100000),  # Salary change (UPDATE)
    (6, "Frank",  "Finance",      85000),  # New employee (INSERT)
], ["emp_id", "name", "department", "salary"])

target = DeltaTable.forPath(spark, "/mnt/silver/employees/")

(target.alias("tgt")
 .merge(
     updates.alias("src"),
     "tgt.emp_id = src.emp_id"    # How to match rows
 )
 .whenMatchedUpdate(set={          # When row exists → UPDATE these fields
     "salary":     "src.salary",
     "updated_at": "current_timestamp()"
 })
 .whenNotMatchedInsert(values={    # When row is new → INSERT all fields
     "emp_id":     "src.emp_id",
     "name":       "src.name",
     "department": "src.department",
     "salary":     "src.salary",
     "updated_at": "current_timestamp()"
 })
 .execute()
)

# ═══════════════════════════════════════════════════
# TIME TRAVEL USE CASES
# ═══════════════════════════════════════════════════

# USE CASE 1: Restore accidentally deleted data
# "We accidentally deleted engineering dept!"
accidentally_deleted = spark.read.format("delta") \
    .option("versionAsOf", 2) \
    .load("/mnt/silver/employees/") \
    .filter(col("department") == "Engineering")

# Re-insert
accidentally_deleted.write.format("delta").mode("append") \
    .save("/mnt/silver/employees/")

# USE CASE 2: Rollback entire table
DeltaTable.forPath(spark, "/mnt/silver/employees/").restoreToVersion(2)

# USE CASE 3: Compare versions
v3 = spark.read.format("delta").option("versionAsOf",3).load("/mnt/silver/employees/")
v2 = spark.read.format("delta").option("versionAsOf",2).load("/mnt/silver/employees/")

# What changed?
new_in_v3 = v3.join(v2, on="emp_id", how="left_anti")
print(f"New records added in v3: {new_in_v3.count()}")

# ═══════════════════════════════════════════════════
# OPTIMIZE + VACUUM
# ═══════════════════════════════════════════════════

# Compact small files into larger ones (performance)
spark.sql("OPTIMIZE silver.employees")

# Optimize + Z-order by frequently filtered column
spark.sql("OPTIMIZE silver.employees ZORDER BY (emp_id)")

# Remove old data files (CAREFUL — removes time travel capability for those versions)
# Default retention: 7 days. Let's keep 30 days:
spark.sql("VACUUM silver.employees RETAIN 720 HOURS")

# Check table details
spark.sql("DESCRIBE DETAIL silver.employees").show(vertical=True)
```

---

# TOPIC 8: MANAGED vs UNMANAGED TABLES

---

## 8.1 The Core Difference

This is one of the most important concepts — and the most misunderstood.

```
THE KEY QUESTION: "Who owns the data files?"

MANAGED TABLE:   Databricks/Spark owns BOTH metadata AND data files
EXTERNAL TABLE:  You own the data files; Databricks only owns metadata
(also called "Unmanaged")
```

---

## 8.2 Managed Tables — Deep Dive

```python
# ═══════════════════════════════════════════════════
# CREATING A MANAGED TABLE
# ═══════════════════════════════════════════════════

# Method 1: SQL
spark.sql("""
    CREATE TABLE silver.managed_employees (
        emp_id INT,
        name   STRING,
        salary DOUBLE
    ) USING DELTA
""")
# WHERE does Databricks store the data files?
# In Unity Catalog: your ADLS metastore root path
# In Hive: /user/hive/warehouse/silver.db/managed_employees/

# Method 2: DataFrame .saveAsTable() without LOCATION
df.write.format("delta").mode("overwrite").saveAsTable("silver.managed_employees")

# ═══════════════════════════════════════════════════
# WHAT HAPPENS WHEN YOU DROP A MANAGED TABLE?
# ═══════════════════════════════════════════════════

spark.sql("DROP TABLE silver.managed_employees")

# RESULT:
# ✅ Metadata is deleted from the metastore
# ✅ DATA FILES ARE ALSO DELETED from ADLS/DBFS
# ← PERMANENT DELETION. No recovery!

# ═══════════════════════════════════════════════════
# CHECK IF A TABLE IS MANAGED
# ═══════════════════════════════════════════════════

spark.sql("DESCRIBE EXTENDED silver.managed_employees") \
     .filter(col("col_name") == "Type") \
     .show()
# +--------+-------+-------+
# |col_name|  data_type|comment|
# +--------+-------+-------+
# |    Type|MANAGED|       |

spark.sql("DESCRIBE DETAIL silver.managed_employees") \
     .select("name", "location", "tableType") \
     .show(truncate=False)
# +------------------------------+--------------------------------------------+---------+
# |name                          |location                                    |tableType|
# +------------------------------+--------------------------------------------+---------+
# |silver.managed_employees      |dbfs:/user/hive/warehouse/silver.db/...     |MANAGED  |
```

---

## 8.3 Unmanaged (External) Tables — Deep Dive

```python
# ═══════════════════════════════════════════════════
# CREATING AN EXTERNAL TABLE
# ═══════════════════════════════════════════════════

# The KEY difference: you specify LOCATION (where YOUR data already is)

# Method 1: SQL with LOCATION
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.external_employees
    USING DELTA
    LOCATION 'abfss://silver@prodlake.dfs.core.windows.net/employees/'
    -- OR for DBFS:
    -- LOCATION '/mnt/silver/employees/'
""")

# Method 2: DataFrame with .save() + create table from path
df.write.format("delta").mode("overwrite").save("/mnt/silver/employees/")
# Then register as external table:
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.external_employees
    USING DELTA
    LOCATION '/mnt/silver/employees/'
""")

# ═══════════════════════════════════════════════════
# WHAT HAPPENS WHEN YOU DROP AN EXTERNAL TABLE?
# ═══════════════════════════════════════════════════

spark.sql("DROP TABLE silver.external_employees")

# RESULT:
# ✅ Metadata is deleted from the metastore
# ✅ DATA FILES REMAIN IN ADLS/DBFS  ← Data is safe!
# ← You can recreate the table by pointing to the same LOCATION

# To recreate:
spark.sql("""
    CREATE TABLE silver.external_employees
    USING DELTA
    LOCATION '/mnt/silver/employees/'
""")
# All data is back! The files were never deleted.

# ═══════════════════════════════════════════════════
# CHECK IF A TABLE IS EXTERNAL
# ═══════════════════════════════════════════════════

spark.sql("DESCRIBE DETAIL silver.external_employees") \
     .select("name", "location", "tableType") \
     .show(truncate=False)
# +--------------------------+------------------------------------------+---------+
# |name                      |location                                  |tableType|
# +--------------------------+------------------------------------------+---------+
# |silver.external_employees |abfss://silver@prodlake.dfs.core.windows  |EXTERNAL |
```

---

## 8.4 Side-by-Side Comparison

```
┌─────────────────────┬──────────────────────────┬──────────────────────────┐
│  Feature            │  MANAGED Table           │  EXTERNAL Table          │
├─────────────────────┼──────────────────────────┼──────────────────────────┤
│ Data location       │ Databricks manages       │ YOU specify the location │
│ LOCATION clause     │ NOT specified             │ REQUIRED                 │
│ DROP TABLE effect   │ Deletes data files too   │ Keeps data files safe    │
│ Who manages files   │ Databricks               │ You                      │
│ Data sharing        │ Harder (files in Databricks mgmt) │ Easy (files in your ADLS) │
│ Multiple tables     │ No (1 table = 1 path)    │ Yes (point multiple tables │
│ same data           │                          │ to same path)            │
│ Use in Production   │ For managed datasets     │ For existing data        │
│ Best for            │ Simple use cases         │ ENTERPRISE STANDARD      │
│ Accidental deletion │ Data is gone!            │ Metadata gone, data safe │
└─────────────────────┴──────────────────────────┴──────────────────────────┘

ENTERPRISE RECOMMENDATION:
  Use EXTERNAL tables for ALL production data.
  Data lives in YOUR ADLS → you control it → accidental DROP is recoverable.
  Managed tables are fine for temporary/intermediate data.
```

---

# TOPIC 9: WORKFLOWS IN DATABRICKS

---

## 9.1 What Are Workflows?

A Databricks Workflow (also called a Databricks Job) is a way to **schedule and automate** the execution of notebooks, scripts, or Delta Live Tables pipelines.

```
WITHOUT WORKFLOWS:
  Someone opens a notebook at 2AM and manually clicks Run ← Not scalable

WITH WORKFLOWS:
  Schedule: "Run every day at 2AM"
  Dependency: "Run Task B only AFTER Task A succeeds"
  Retry: "If it fails, try 2 more times"
  Alert: "Email me if it fails"
  No human intervention needed
```

---

## 9.2 Creating a Workflow — Step by Step

```
LEFT SIDEBAR → Workflows → Create Job

── BASIC SETUP ──────────────────────────────────────────
Job Name: daily-employee-pipeline
Tags:     environment=prod, team=data-engineering

── TASK 1 SETUP ─────────────────────────────────────────
Task Name:   ingest_bronze
Task Type:   Notebook
Path:        /Repos/my-org/project/notebooks/01_ingest
Cluster:     (Create new job cluster — NOT all-purpose!)

New Job Cluster:
  Runtime: 14.3 LTS
  Worker type: Standard_DS3_v2
  Min: 1, Max: 4 workers
  Auto termination: happens automatically when job ends

Parameters:  (passed as widget values)
  batch_date: {{current_date}}
  environment: prod

Max Retries: 2
Retry Interval: 5 minutes
── ──────────────────────────────────────────────────────

── TASK 2 SETUP ─────────────────────────────────────────
Task Name:   transform_silver
Task Type:   Notebook
Path:        /Repos/my-org/project/notebooks/02_transform
Cluster:     Same as ingest_bronze  ← reuse same cluster
Depends On:  ingest_bronze           ← Only runs if Task 1 succeeds!
── ──────────────────────────────────────────────────────

── SCHEDULE ─────────────────────────────────────────────
Trigger:  Scheduled
Cron:     0 2 * * *  (Every day at 2:00 AM UTC)
Timezone: UTC
── ──────────────────────────────────────────────────────

── NOTIFICATIONS ────────────────────────────────────────
On Start:   (optional)
On Success: sumit@company.com
On Failure: sumit@company.com, team-lead@company.com
── ──────────────────────────────────────────────────────
```

---

## 9.3 Multi-Task Workflow Architecture

```python
# VISUAL: How tasks connect (DAG - Directed Acyclic Graph)

"""
                     ┌─────────────────┐
                     │  START (2 AM)   │
                     └────────┬────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  ingest_txns    │  │ ingest_customers│  │ ingest_products │
│  (Bronze)       │  │  (Bronze)       │  │  (Bronze)       │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                     │
         └────────────────────┼─────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  validate_dq    │
                    │  (DQ Checks)    │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ transform_txns  │  │ transform_custs │  │ transform_prods │
│  (Silver)       │  │  (Silver)       │  │  (Silver)       │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                     │
         └────────────────────┼─────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │   build_gold    │
                    │  (Gold Layer)   │
                    └────────┬────────┘
                             │
                    ┌─────────────────┐
                    │  notify_teams   │
                    │  (Send Alert)   │
                    └─────────────────┘
"""
```

---

## 9.4 Cron Schedule Reference

```
CRON SYNTAX: minute hour day-of-month month day-of-week

EXAMPLES:
  0 2 * * *      → Every day at 2:00 AM UTC
  0 */4 * * *    → Every 4 hours
  0 8 * * 1-5    → Every weekday (Mon-Fri) at 8:00 AM
  0 0 1 * *      → First day of every month at midnight
  */15 * * * *   → Every 15 minutes
  0 6,18 * * *   → Twice a day: 6 AM and 6 PM
  0 2 * * 0      → Every Sunday at 2 AM

DATABRICKS QUARTZ SYNTAX (Databricks uses this format):
  0 0 2 * * ?    → Every day at 2:00 AM (? = any day of week)
  0 0 */4 * * ?  → Every 4 hours
  0 0 8 ? * MON-FRI → Weekdays at 8 AM
```
