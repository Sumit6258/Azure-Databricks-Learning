# Module 03 — Delta Lake
## Part 2: Delta Live Tables (DLT) — Declarative Pipelines

> DLT is Databricks' next-generation pipeline framework. It handles orchestration, data quality, retries, and monitoring declaratively — you focus on business logic, not pipeline plumbing.

---

## 2.1 What is Delta Live Tables?

```
TRADITIONAL APPROACH (what you've learned so far):
  • Write ETL logic in notebooks
  • Define job with task dependencies
  • Handle errors, retries, monitoring yourself
  • DQ checks are custom code

DLT APPROACH:
  • Define tables with @dlt.table decorator
  • DLT handles: execution order, retries, restarts
  • Built-in DQ with @dlt.expect decorators
  • Automatic lineage visualization
  • Automatic schema inference and enforcement
```

**When to use DLT vs traditional jobs?**
- DLT: New greenfield pipelines, streaming + batch hybrid, teams wanting managed infrastructure
- Traditional jobs: Complex multi-notebook orchestration, when you need full control, existing pipelines

---

## 2.2 DLT — First Pipeline

```python
# ── bronze_to_gold_pipeline.py ──
# This single file defines the ENTIRE pipeline: Bronze → Silver → Gold
# DLT figures out the execution order automatically

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ── CONFIGURATION ──
# In DLT, use spark.conf to pass parameters from pipeline config
BATCH_DATE = spark.conf.get("pipeline.batch_date", "")
ENV        = spark.conf.get("pipeline.environment", "dev")
BRONZE_PATH= f"abfss://bronze@{ENV}lake.dfs.core.windows.net/transactions/"

# ──────────────────────────────────────────────────────
# BRONZE: Raw ingestion (using Auto Loader)
# ──────────────────────────────────────────────────────

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data from core banking system",
    table_properties={
        "quality":   "bronze",
        "team":      "data-engineering",
        "pipeline":  "transaction-processing"
    }
)
def bronze_transactions():
    """
    Read raw CSV files using Auto Loader.
    Append-only, no transformation.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",         "csv")
            .option("cloudFiles.schemaLocation", "/mnt/checkpoints/dlt-schema/")
            .option("cloudFiles.inferColumnTypes","true")
            .option("header",                    "true")
            .option("rescuedDataColumn",         "_rescued_data")
            .load(BRONZE_PATH)
            .withColumn("_ingested_at",  current_timestamp())
            .withColumn("_source_file",  input_file_name())
    )

# ──────────────────────────────────────────────────────
# SILVER: Cleaned and validated (with DQ expectations)
# ──────────────────────────────────────────────────────

@dlt.table(
    name="silver_transactions",
    comment="Cleaned, validated transactions ready for analytics",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_transaction_id", "txn_id IS NOT NULL")
@dlt.expect("positive_amount",      "CAST(amount AS DOUBLE) > 0")
@dlt.expect_or_drop("valid_date",   "txn_date IS NOT NULL")        # Drop invalid rows
@dlt.expect_or_fail("no_duplicates","TRUE")                         # Fail on this
@dlt.expect_all({
    "valid_payment_mode": "payment_mode IN ('CARD','UPI','NETBANKING','NEFT','RTGS','IMPS')",
    "customer_id_format": "customer_id RLIKE '^(CUST|C)[0-9]+$'",
})
def silver_transactions():
    """
    Transform Bronze → Silver:
    Type casting, validation, deduplication.
    """
    return (
        dlt.read_stream("bronze_transactions")
            .filter(col("_rescued_data").isNull())  # Skip schema-mismatched rows
            .withColumn("txn_id",      col("txn_id").cast("bigint"))
            .withColumn("txn_date",    to_date(col("txn_date"), "yyyy-MM-dd"))
            .withColumn("amount",      regexp_replace(col("amount"), "[,$₹]", "").cast("double"))
            .withColumn("customer_id", upper(trim(col("customer_id"))))
            .withColumn("payment_mode",upper(trim(col("payment_mode"))))
            .withColumn("processed_at",current_timestamp())
            .dropDuplicates(["txn_id"])
    )

# ──────────────────────────────────────────────────────
# SILVER: Dimension table (static/slowly changing)
# ──────────────────────────────────────────────────────

@dlt.table(
    name="silver_customers",
    comment="Customer reference data",
)
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL AND customer_id != ''")
def silver_customers():
    """Batch read from JDBC — DLT handles full refresh."""
    return (
        spark.read.format("jdbc")
            .option("url",     "jdbc:sqlserver://prod-db.database.windows.net:1433;database=crm")
            .option("dbtable", "dbo.customers")
            .option("user",    dbutils.secrets.get("kv-prod", "sql-user"))
            .option("password",dbutils.secrets.get("kv-prod", "sql-password"))
            .load()
            .withColumn("loaded_at", current_timestamp())
    )

# ──────────────────────────────────────────────────────
# GOLD: Business aggregations
# ──────────────────────────────────────────────────────

@dlt.table(
    name="gold_daily_summary",
    comment="Daily transaction summary by payment mode for BI dashboard",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "pipelines.reset.allowed": "false"  # Prevent accidental reset
    }
)
def gold_daily_summary():
    """
    Join Silver transactions with customers, aggregate by day and payment mode.
    Reads batch from Silver (uses .read() not .read_stream()).
    """
    txns = dlt.read("silver_transactions")
    custs= dlt.read("silver_customers")
    
    return (
        txns.join(custs, on="customer_id", how="left")
            .groupBy("txn_date", "payment_mode", "customer.city")
            .agg(
                count("txn_id").alias("transaction_count"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction"),
                countDistinct("customer_id").alias("unique_customers"),
            )
            .withColumn("report_generated_at", current_timestamp())
    )

# ──────────────────────────────────────────────────────
# QUARANTINE: Capture records failing DQ
# ──────────────────────────────────────────────────────

@dlt.table(
    name="quarantine_transactions",
    comment="Records that failed data quality validation"
)
@dlt.expect_all_or_drop({
    "invalid_transaction_id": "txn_id IS NULL",         # These are INTENTIONALLY inverted
    "non_positive_amount":    "CAST(amount AS DOUBLE) <= 0",  # We WANT failures here
})
def quarantine_transactions():
    """Capture records that failed Silver DQ rules."""
    return (
        dlt.read_stream("bronze_transactions")
            .filter(
                col("txn_id").isNull() |
                (col("amount").cast("double") <= 0) |
                col("txn_date").isNull()
            )
            .withColumn("quarantine_reason", 
                when(col("txn_id").isNull(), "NULL_TXN_ID")
                .when(col("amount").cast("double") <= 0, "INVALID_AMOUNT")
                .otherwise("NULL_DATE")
            )
            .withColumn("quarantined_at", current_timestamp())
    )
```

---

## 2.3 DLT Expectations — All Modes

```python
# ── @dlt.expect — Log only (doesn't drop rows, doesn't fail pipeline) ──
@dlt.expect("check_name", "column_expression")
# Use when: monitoring/reporting only, business might send some bad rows

# ── @dlt.expect_or_drop — Drop failing rows silently ──
@dlt.expect_or_drop("check_name", "column_expression")
# Use when: bad rows should be silently excluded (common for Silver)

# ── @dlt.expect_or_fail — Fail entire pipeline ──
@dlt.expect_or_fail("check_name", "column_expression")
# Use when: a failure means the data is fundamentally broken (e.g., all amounts null)

# ── @dlt.expect_all — Apply multiple expectations (all log only) ──
@dlt.expect_all({
    "rule_1": "expression_1",
    "rule_2": "expression_2",
})

# ── @dlt.expect_all_or_drop — Multiple, drop on any failure ──
@dlt.expect_all_or_drop({
    "rule_1": "expression_1",
    "rule_2": "expression_2",
})

# ── @dlt.expect_all_or_fail — Multiple, fail on any ──
@dlt.expect_all_or_fail({
    "critical_rule": "amount IS NOT NULL",
    "no_negatives":  "amount > 0",
})
```

---

## 2.4 Creating a DLT Pipeline — UI Steps

```
Databricks Workspace → Workflows → Delta Live Tables → Create Pipeline

Settings:
  Pipeline name:  transaction-processing-pipeline
  Pipeline mode:  Triggered  (runs on schedule, like batch)
                  Continuous  (always running, for streaming)
  
  Source code:    /Repos/Sumit6258/Azure-Databricks-Learning/
                  notebooks/dlt/bronze_to_gold_pipeline
  
  Target schema:  prod_catalog.dlt_pipeline  (Unity Catalog)
                  OR
                  hive_metastore.dlt_outputs  (legacy)
  
  Storage location: abfss://dlt@prodlake.dfs.core.windows.net/pipeline-data/
  
  Cluster policy: job-clusters-policy
  
  Configuration (key-value):
    pipeline.batch_date    = {{BATCH_DATE}}
    pipeline.environment   = prod
  
  Notifications:
    Email on failure: data-eng@company.com

→ Create → Start (for development: Validate first, then Start)
```

---

## 2.5 DLT Pipeline Configuration JSON

```json
{
  "name": "transaction-processing-pipeline",
  "edition": "ADVANCED",
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 2,
        "max_workers": 8,
        "mode": "ENHANCED"
      }
    }
  ],
  "libraries": [
    {
      "notebook": {
        "path": "/Repos/Sumit6258/Azure-Databricks-Learning/notebooks/dlt/bronze_to_gold_pipeline"
      }
    }
  ],
  "target": "dlt_outputs",
  "catalog": "prod_catalog",
  "configuration": {
    "pipeline.batch_date": "",
    "pipeline.environment": "prod",
    "spark.sql.adaptive.enabled": "true"
  },
  "continuous": false,
  "development": false,
  "photon": true,
  "channel": "CURRENT"
}
```

---

## 2.6 Monitoring DLT Pipelines

```
Pipeline UI Shows:
  ┌─────────────────────────────────────────────────────────┐
  │  Pipeline Graph (DAG visualization)                      │
  │                                                          │
  │  [bronze_transactions] ──→ [silver_transactions]        │
  │                                    ↓                     │
  │                          [gold_daily_summary]            │
  │                                                          │
  │  Each node shows:                                        │
  │  ✅ Records passed DQ: 498,221                          │
  │  ⚠️  Records dropped:    4,626                          │
  │  ❌ Records failed:         0                           │
  └─────────────────────────────────────────────────────────┘

Click any table node to see:
  - DQ expectation results (pass/fail counts per rule)
  - Data lineage (upstream → downstream)
  - Last updated timestamp
  - Row counts
```

---

## 2.7 DLT vs Traditional Jobs — Comparison

```
Feature                  Traditional Jobs    Delta Live Tables
─────────────────────────────────────────────────────────────
Execution order          Manual (depends_on) Automatic (DAG inference)
Data quality             Custom code         @dlt.expect decorators
Error handling           Custom try/catch    Built-in retry + alerts
Schema evolution         Manual option()     Automatic
Lineage tracking         Manual              Automatic visualization
Streaming + batch        Separate jobs       Single pipeline, mixed
Development mode         Full cluster run    Incremental dev mode
Monitoring               Custom logging      Built-in DQ dashboard
Cost                     Full job cluster    Managed (optimized)
Complexity               More code, more control  Less code, less control
```

---

## 2.8 Interview Questions — DLT

**Q: What is the difference between @dlt.expect and @dlt.expect_or_drop?**
> `@dlt.expect` logs violations in the DLT event log and DQ metrics but keeps all rows — the pipeline continues with potentially bad data. Use for monitoring/alerting. `@dlt.expect_or_drop` silently removes rows that violate the rule — the bad rows never reach the target table. Use for Silver layer where you want clean data only. `@dlt.expect_or_fail` halts the entire pipeline if any row fails — use only for critical data integrity checks where a failure means the entire feed is corrupted.

**Q: Can you mix streaming and batch in the same DLT pipeline?**
> Yes — DLT handles both. Use `spark.readStream` (or Auto Loader) for tables that need low-latency continuous updates. Use `spark.read` (or `dlt.read()`) for batch/lookup tables like customer dimensions. DLT automatically coordinates their execution — streaming tables update continuously while batch tables refresh on the pipeline trigger schedule.

**Q: When would you choose DLT over a traditional Databricks job?**
> Choose DLT for new pipelines where: you want automatic lineage visualization (important for governance/compliance), built-in DQ tracking without writing a framework, simpler pipeline code (less boilerplate), or you're using Continuous mode for streaming. Choose traditional jobs when: you need complex multi-notebook orchestration with programmatic dependencies, you need full control over execution (custom retry logic, inter-task communication via taskValues), or you're maintaining existing pipelines.

---
*Next: [Part 3 — Change Data Feed](./03-change-data-feed.md)*
