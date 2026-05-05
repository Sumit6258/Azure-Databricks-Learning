# 🎓 Azure Databricks — Complete Learning Guide
## From Zero to Production-Ready in 9 Core Topics + 4 Hands-On Tasks

---

## 📚 What This Guide Covers

### Theory Modules (read these first)

| File | Topics Covered |
|---|---|
| `foundations/01-control-plane-clusters.md` | Control Plane, Data Plane, All 5 Cluster Types |
| `foundations/02-rdd-vs-dataframe.md` | RDD vs DataFrame deep dive with side-by-side code |
| `foundations/03-topics-4-to-9.md` | Unity Catalog, Magic Commands, dbutils, Delta Lake, Managed vs Unmanaged Tables, Workflows |

### Hands-On Notebooks (copy into Databricks, run cell by cell)

| File | What You Build |
|---|---|
| `hands-on/Task1_DataFrames_FileFormats.py` | Create DataFrames 5 ways, read CSV/JSON/Parquet/Delta, master show() vs display() |
| `hands-on/Task2_Parameters_Widgets.py` | All 4 widget types, multi-widget pipelines, ADF parameter flow |
| `hands-on/Task3_Child_Notebooks.py` | Parent notebook calls child, passes parameters, reads return values |
| `hands-on/Task4_Error_Handling.py` | try-except patterns, retry logic, checkpoints, production pipeline |

---

## 🗺️ How to Use This Guide

```
STEP 1 → Read a theory module (understand the concept)
STEP 2 → Open the corresponding hands-on notebook in Databricks
STEP 3 → Copy cells one by one (DON'T paste everything at once)
STEP 4 → Run each cell and READ every output before moving on
STEP 5 → Modify the examples — break them, fix them, extend them
STEP 6 → Answer the interview questions at the end of each topic
```

---

## 🧭 Topic Quick Reference

### Topic 1 & 2: Control Plane & Data Plane

```
Control Plane (Databricks manages):     Data Plane (YOU own):
  - Workspace UI                          - Cluster Virtual Machines
  - REST API Server                       - ADLS Gen2 Storage
  - Job Scheduler                         - Virtual Network
  - Cluster Manager                       - Key Vault
  - Unity Catalog Metastore               - DBFS / Mounts

KEY FACT: Your data NEVER leaves your Azure subscription.
Databricks only sends CODE instructions — never touches your data.
```

### Topic 2: Cluster Types

```
All-Purpose  → Development, always on, billed 24/7, multi-user
Job Cluster  → Production jobs, created per run, 80% cheaper
Single Node  → Learning, small data, 1 VM only
SQL Warehouse→ BI tools, SQL only, Power BI/Tableau connector

RULE: Use Job Clusters for ALL production scheduled pipelines.
```

### Topic 3: RDD vs DataFrame

```
RDD:
  - No schema (Spark can't optimize)
  - Verbose code (10 lines = 1 DataFrame line)
  - Use for: legacy code, very custom unstructured processing

DataFrame:
  - Has schema (Catalyst optimizer works its magic)
  - Concise SQL-like API
  - 2-10x faster than equivalent RDD code
  - Use for: 99% of all data engineering work

CONVERT: rdd.toDF(["col1","col2"])  |  df.rdd
```

### Topic 4: Unity Catalog Hierarchy

```
CATALOG  →  SCHEMA  →  TABLE
prod_catalog.silver.transactions
     │          │         │
     │          │         └── TABLE name
     │          └──────────── SCHEMA (like a database)
     └─────────────────────── CATALOG (top level)

Commands:
  SHOW CATALOGS
  SHOW SCHEMAS IN catalog_name
  SHOW TABLES IN catalog.schema
  USE CATALOG prod_catalog
  USE SCHEMA silver
  SELECT * FROM prod_catalog.silver.transactions
```

### Topic 5: Magic Commands

```
%sql      → Run SQL in any notebook language
%python   → Run Python
%scala    → Run Scala
%r        → Run R
%sh       → Shell commands (driver only)
%fs       → DBFS file system operations
%run      → Run another notebook (shares scope, no params)
%md       → Markdown documentation
%pip      → Install Python packages on ALL nodes
```

### Topic 6: dbutils — All Utilities

```
dbutils.fs.*        → List, copy, move, delete, read files
dbutils.secrets.*   → Get secrets from Key Vault (NEVER hardcode!)
dbutils.widgets.*   → Create/read/remove input parameters
dbutils.notebook.*  → Run child notebooks, exit with return value
dbutils.jobs.*      → Pass values between job tasks (taskValues)

Most Used:
  dbutils.fs.ls("/mnt/bronze/")
  dbutils.secrets.get("scope", "key")
  dbutils.widgets.get("param_name")
  dbutils.notebook.run("./child", 300, {"key": "value"})
  dbutils.jobs.taskValues.set(key="count", value="1000")
```

### Topic 7: Delta Lake

```
WHY DELTA:
  ✅ ACID transactions (atomic writes — no partial data)
  ✅ Time travel (query past versions)
  ✅ MERGE/UPSERT (insert + update + delete atomically)
  ✅ Schema enforcement (bad data rejected at write)
  ✅ Schema evolution (new columns added safely)
  ✅ Audit history (who changed what, when)

KEY OPERATIONS:
  Write:      df.write.format("delta").mode("append").save("/path/")
  Read:       spark.read.format("delta").load("/path/")
  Time travel:.option("versionAsOf", 3) or .option("timestampAsOf","2024-01-15")
  Merge:      DeltaTable.forPath(...).alias("t").merge(...).execute()
  History:    DeltaTable.forPath(...).history().show()
  Optimize:   spark.sql("OPTIMIZE table ZORDER BY (col)")
  Vacuum:     spark.sql("VACUUM table RETAIN 720 HOURS")
```

### Topic 8: Managed vs Unmanaged Tables

```
MANAGED TABLE:
  CREATE TABLE t (...)  ← No LOCATION clause
  DROP TABLE t          ← DELETES DATA FILES TOO (permanent!)
  Data stored:          Databricks-managed path
  Best for:             Temporary / intermediate tables

UNMANAGED (EXTERNAL) TABLE:
  CREATE TABLE t (...)  LOCATION '/mnt/silver/t/'
  DROP TABLE t          ← Only removes metadata, DATA IS SAFE
  Data stored:          Your ADLS path (you control it)
  Best for:             ALL PRODUCTION DATA

ENTERPRISE RULE: Always use External tables for production.
```

### Topic 9: Workflows

```
What is a Workflow? → Scheduled automation of notebooks/scripts

Key concepts:
  Task          → Single notebook/script to run
  Dependency    → "Task B only runs if Task A succeeds"
  Job Cluster   → Fresh cluster per run (cost efficient)
  Cron Schedule → "0 2 * * *" = every day at 2AM UTC
  Retry Policy  → Auto-retry N times on failure
  Notifications → Email on success/failure

Multi-task pattern:
  ingest_bronze  ──→  validate_dq  ──→  transform_silver  ──→  build_gold
  (parallel)          (waits for         (waits for            (waits for
                       all ingests)       validate)              silver)
```

---

## 📝 Hands-On Tasks Summary

### Task 1: DataFrames & File Formats
```python
# 5 ways to create a DataFrame:
spark.createDataFrame(python_list, schema)      # From Python data
spark.read.option("header","true").csv(path)    # From CSV
spark.read.json(path)                           # From JSON
spark.read.parquet(path)                        # From Parquet
spark.read.format("delta").load(path)           # From Delta

# show() vs display()
df.show()        # Plain text, works anywhere, use in production code
display(df)      # Rich interactive table, Databricks only, use for exploration
```

### Task 2: Parameters & Widgets
```python
# 4 widget types:
dbutils.widgets.text("name", "default", "Label")
dbutils.widgets.dropdown("env", "dev", ["dev","test","prod"], "Env")
dbutils.widgets.combobox("table", "txns", ["txns","custs"], "Table")
dbutils.widgets.multiselect("regions", "All", ["All","N","S"], "Regions")

# Always read with:
value = dbutils.widgets.get("widget_name")  # Always returns STRING
```

### Task 3: Child Notebooks
```python
# In CHILD notebook — define widgets to receive params:
dbutils.widgets.text("batch_date", "", "Date")
batch_date = dbutils.widgets.get("batch_date")
# ... do work ...
dbutils.notebook.exit(json.dumps({"status":"SUCCESS","count":1000}))

# In PARENT notebook — call child and get result:
result_str = dbutils.notebook.run(
    path="./child_notebook",
    timeout_seconds=300,
    arguments={"batch_date": "2024-01-15"}   # Populates child's widgets
)
result = json.loads(result_str)
print(result["count"])  # 1000
```

### Task 4: Error Handling
```python
# Production pattern:
try:
    df = spark.read.csv("/mnt/bronze/file.csv")
    df.write.format("delta").save("/mnt/silver/")
except AnalysisException as e:
    logger.error(f"Column/SQL error: {e}")
    # Handle gracefully
except Exception as e:
    logger.error(f"Unexpected: {e}")
    raise  # Re-raise to fail the job (triggers retry + alert)
finally:
    # Always cleanup (temp views, cache, connections)
    spark.catalog.clearCache()
```

---

## 🎯 Interview Answer Templates

**Q: Explain Control Plane vs Data Plane**
> Control Plane = Databricks manages it (UI, scheduler, metadata). Data Plane = inside YOUR Azure subscription (VMs, storage). Your data never leaves your subscription — Databricks only sends code instructions, never touches data.

**Q: Which cluster type for production pipelines?**
> Job Clusters — ephemeral (created per run, deleted after), 80% cheaper than all-purpose, clean state every run, isolated failures.

**Q: Why DataFrame over RDD?**
> DataFrames have a schema → Catalyst optimizer can rewrite and optimize the execution plan (predicate pushdown, column pruning, join reordering). RDDs are opaque — Spark can't look inside lambda functions to optimize. DataFrames are 2-10x faster for the same logic.

**Q: What's the 3-level hierarchy in Unity Catalog?**
> CATALOG → SCHEMA → TABLE. Example: `prod_catalog.silver.transactions`. Catalog = filing cabinet, Schema = drawer, Table = folder inside. Fully qualified: `catalog.schema.table`.

**Q: Managed vs External table — what happens on DROP?**
> Managed: both metadata AND data files are deleted permanently. External: only metadata is deleted, data files in ADLS remain safe. Always use External tables for production — accidental DROP is recoverable.

**Q: What is Delta Lake and name 3 key features?**
> Delta Lake is an open-source storage layer that adds ACID transactions to data lakes. Three key features: (1) MERGE/UPSERT — atomic insert+update+delete in one operation, (2) Time Travel — query data at any past version using `VERSION AS OF`, (3) Schema Enforcement — bad data with wrong types/columns is rejected at write time.
