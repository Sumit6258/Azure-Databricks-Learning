# Project 01 — End-to-End Batch Pipeline
## Banking Transaction Processing System

> **Scenario:** A mid-sized bank needs to process daily transaction data from their core banking system, apply fraud detection rules, and serve aggregated reports to their BI dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│              DAILY TRANSACTION PROCESSING PIPELINE                   │
│                                                                      │
│  Core Banking  →  ADLS Bronze  →  Databricks  →  ADLS Silver/Gold  │
│    System              │                                    │        │
│   (CSV files)     Raw files                          Delta tables   │
│                   Append-only                        Power BI        │
│                   No transforms                      ready           │
│                                                                      │
│  SCHEDULE: Daily at 2:00 AM UTC                                      │
│  SLA: Complete by 4:00 AM UTC                                        │
│  Data Volume: ~500K transactions/day, ~2GB                           │
└─────────────────────────────────────────────────────────────────────┘

Architecture Flow:
━━━━━━━━━━━━━━━━

[Task 1: Ingest Bronze]
  Core Banking CSV → ADLS Bronze (raw, append-only Delta)

[Task 2: Validate & Transform Silver]  ← depends on Task 1
  Bronze → Clean, type-cast, deduplicate, apply business rules
  Rejected records → Quarantine table

[Task 3: Fraud Detection]             ← depends on Task 2
  Silver → Apply fraud rules (velocity checks, amount thresholds)
  Flag suspicious transactions

[Task 4: Build Gold]                  ← depends on Task 3
  Silver + Fraud flags → Daily summary, customer profiles, KPIs

[Task 5: Refresh Reports]             ← depends on Task 4
  Notify BI team / trigger Power BI refresh
```

---

## Project Structure

```
project-01-batch-pipeline/
├── notebooks/
│   ├── 00_setup.py               ← Mount storage, create schemas
│   ├── 01_ingest_bronze.py       ← Task 1: Raw ingestion
│   ├── 02_transform_silver.py    ← Task 2: Clean & validate
│   ├── 03_fraud_detection.py     ← Task 3: Fraud rules
│   ├── 04_build_gold.py          ← Task 4: Aggregations
│   └── utils/
│       ├── config.py             ← Environment config
│       ├── logger.py             ← Logging utilities
│       └── data_quality.py       ← DQ check functions
├── tests/
│   ├── test_transformations.py
│   └── test_fraud_rules.py
├── job_definition.json           ← Databricks job JSON
└── README.md
```

---

## Full Implementation

### `utils/config.py`

```python
# ─────────────────────────────────────────────────────────────────────
# config.py — Centralized configuration notebook
# %run this from every pipeline notebook
# ─────────────────────────────────────────────────────────────────────

import sys
from datetime import datetime, timedelta

# Widget for environment
dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"])
ENV = dbutils.widgets.get("environment")

dbutils.widgets.text("batch_date", "")
BATCH_DATE = dbutils.widgets.get("batch_date") or \
             (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

# Storage paths
PATHS = {
    "dev": {
        "bronze": "abfss://bronze@devlake.dfs.core.windows.net",
        "silver": "abfss://silver@devlake.dfs.core.windows.net",
        "gold":   "abfss://gold@devlake.dfs.core.windows.net",
        "quarantine": "abfss://quarantine@devlake.dfs.core.windows.net",
    },
    "prod": {
        "bronze": "abfss://bronze@prodlake.dfs.core.windows.net",
        "silver": "abfss://silver@prodlake.dfs.core.windows.net",
        "gold":   "abfss://gold@prodlake.dfs.core.windows.net",
        "quarantine": "abfss://quarantine@prodlake.dfs.core.windows.net",
    }
}

BASE = PATHS.get(ENV, PATHS["dev"])

BRONZE_PATH    = f"{BASE['bronze']}/transactions/"
SILVER_PATH    = f"{BASE['silver']}/transactions/"
GOLD_PATH      = f"{BASE['gold']}/daily_summary/"
QUARANTINE_PATH = f"{BASE['quarantine']}/transactions/"

print(f"✅ Config loaded | ENV: {ENV} | BATCH_DATE: {BATCH_DATE}")
print(f"   Bronze: {BRONZE_PATH}")
print(f"   Silver: {SILVER_PATH}")
```

---

### `01_ingest_bronze.py`

```python
# ─────────────────────────────────────────────────────────────────────
# TASK 1: INGEST BRONZE
# Read raw CSV files from ADLS, append to Bronze Delta table
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, 
    col, to_date, trim
)
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

# ── Define strict schema (never use inferSchema in production) ──
RAW_SCHEMA = StructType([
    StructField("txn_id",       StringType(),  True),
    StructField("txn_date",     StringType(),  True),
    StructField("txn_time",     StringType(),  True),
    StructField("customer_id",  StringType(),  True),
    StructField("account_no",   StringType(),  True),
    StructField("amount",       StringType(),  True),  # String — raw format may have commas
    StructField("currency",     StringType(),  True),
    StructField("payment_mode", StringType(),  True),
    StructField("merchant_id",  StringType(),  True),
    StructField("status",       StringType(),  True),
    StructField("channel",      StringType(),  True),
])

# ── Source path for today's batch ──
source_path = f"abfss://landing@prodlake.dfs.core.windows.net/transactions/date={BATCH_DATE}/"

print(f"📥 Reading from: {source_path}")

# ── Check if source files exist ──
try:
    source_files = dbutils.fs.ls(source_path)
    file_count = len(source_files)
    print(f"✅ Found {file_count} source files")
except Exception as e:
    error_msg = f"Source path not found: {source_path}"
    logger.error(error_msg)
    dbutils.notebook.exit(f"FAILED:{error_msg}")
    raise FileNotFoundError(error_msg)

# ── Read raw files ──
df_raw = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .option("encoding", "UTF-8") \
    .option("emptyValue", None) \
    .schema(RAW_SCHEMA) \
    .csv(source_path)

# ── Add metadata columns (Bronze standard) ──
df_bronze = df_raw \
    .withColumn("_ingested_at",   current_timestamp()) \
    .withColumn("_source_file",   input_file_name()) \
    .withColumn("_batch_date",    lit(BATCH_DATE)) \
    .withColumn("_pipeline_run",  lit(f"daily-txn-{BATCH_DATE}"))

# ── Write to Bronze (append-only, partitioned by batch_date) ──
print(f"📝 Writing to Bronze: {BRONZE_PATH}")

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("_batch_date") \
    .save(BRONZE_PATH)

# ── Metrics ──
record_count = df_bronze.count()
print(f"✅ Bronze ingestion complete")
print(f"   Records written: {record_count:,}")
print(f"   Batch date:      {BATCH_DATE}")

# ── Pass to downstream tasks ──
dbutils.jobs.taskValues.set("bronze_record_count", str(record_count))
dbutils.jobs.taskValues.set("batch_date", BATCH_DATE)
dbutils.notebook.exit(f"SUCCESS:{record_count}")
```

---

### `02_transform_silver.py`

```python
# ─────────────────────────────────────────────────────────────────────
# TASK 2: TRANSFORM SILVER
# Clean, validate, type-cast, deduplicate Bronze data
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable

# ── Get upstream task values ──
bronze_count = int(dbutils.jobs.taskValues.get("ingest_bronze", "bronze_record_count", "0"))
batch_date   = dbutils.jobs.taskValues.get("ingest_bronze", "batch_date", BATCH_DATE)

print(f"📊 Upstream bronze records: {bronze_count:,}")

if bronze_count == 0:
    print("⚠️ No records to process. Exiting.")
    dbutils.notebook.exit("SKIPPED:no_data")

# ── Read today's bronze batch ──
df_bronze = spark.read.format("delta").load(BRONZE_PATH) \
    .filter(col("_batch_date") == batch_date)

print(f"📖 Loaded {df_bronze.count():,} bronze records")

# ── Type casting and standardization ──
df_typed = df_bronze \
    .withColumn("txn_id",      col("txn_id").cast(LongType())) \
    .withColumn("txn_datetime",to_timestamp(
        concat(col("txn_date"), lit(" "), col("txn_time")),
        "yyyy-MM-dd HH:mm:ss"
    )) \
    .withColumn("txn_date",    to_date(col("txn_date"), "yyyy-MM-dd")) \
    .withColumn("amount",      regexp_replace(col("amount"), "[,$₹]", "").cast(DoubleType())) \
    .withColumn("customer_id", upper(trim(col("customer_id")))) \
    .withColumn("account_no",  trim(col("account_no"))) \
    .withColumn("payment_mode",upper(trim(col("payment_mode")))) \
    .withColumn("status",      upper(trim(col("status")))) \
    .withColumn("currency",    upper(trim(col("currency"))))

# ── Data Quality Validation ──
print("🔍 Running data quality checks...")

# Define validation rules
VALIDATION_RULES = {
    "txn_id_not_null":      col("txn_id").isNotNull(),
    "amount_positive":      (col("amount").isNotNull()) & (col("amount") > 0),
    "amount_reasonable":    col("amount") < 10_000_000,  # < 1 crore INR
    "customer_id_format":   col("customer_id").rlike("^(CUST|C)[0-9]+$"),
    "txn_date_valid":       col("txn_date").isNotNull(),
    "payment_mode_valid":   col("payment_mode").isin(
        ["CARD", "UPI", "NETBANKING", "NEFT", "RTGS", "IMPS", "WALLET"]
    ),
    "currency_valid":       col("currency").isin(["INR", "USD", "EUR", "GBP"]),
}

# Tag records with their validation status
df_validated = df_typed
for rule_name, rule_expr in VALIDATION_RULES.items():
    df_validated = df_validated.withColumn(f"_check_{rule_name}", rule_expr)

# Split valid vs invalid
check_columns = [f"_check_{rule}" for rule in VALIDATION_RULES.keys()]
all_valid_expr = reduce(lambda a, b: a & b, [col(c) for c in check_columns])

df_valid   = df_validated.filter(all_valid_expr).drop(*check_columns)
df_invalid = df_validated.filter(~all_valid_expr)

# Identify which rule failed
failed_rule_expr = when(~col("_check_txn_id_not_null"),       "NULL_TXN_ID") \
    .when(~col("_check_amount_positive"),                      "INVALID_AMOUNT") \
    .when(~col("_check_amount_reasonable"),                    "AMOUNT_TOO_HIGH") \
    .when(~col("_check_customer_id_format"),                   "INVALID_CUSTOMER_ID") \
    .when(~col("_check_payment_mode_valid"),                   "UNKNOWN_PAYMENT_MODE") \
    .otherwise("OTHER_VALIDATION_FAILURE")

df_quarantine = df_invalid \
    .withColumn("reject_reason",   failed_rule_expr) \
    .withColumn("rejected_at",     current_timestamp()) \
    .drop(*check_columns)

valid_count   = df_valid.count()
invalid_count = df_quarantine.count()

print(f"   ✅ Valid records:    {valid_count:,}")
print(f"   ❌ Invalid records:  {invalid_count:,}")
print(f"   📊 Pass rate:        {valid_count/(valid_count+invalid_count)*100:.2f}%")

# Write quarantine records
if invalid_count > 0:
    df_quarantine.write.format("delta").mode("append") \
        .partitionBy("_batch_date") \
        .save(QUARANTINE_PATH)
    print(f"📤 {invalid_count:,} records written to quarantine")

# ── Deduplication ──
# In case source sends duplicate records, keep the latest
w_dedup = Window.partitionBy("txn_id").orderBy(col("_ingested_at").desc())

df_deduped = df_valid \
    .withColumn("_rn", row_number().over(w_dedup)) \
    .filter(col("_rn") == 1) \
    .drop("_rn", "_source_file", "_pipeline_run", "_ingested_at")

# Add silver metadata
df_silver = df_deduped \
    .withColumn("processed_at",    current_timestamp()) \
    .withColumn("processing_date", lit(batch_date))

# ── MERGE to Silver (handle late arrivals and corrections) ──
print(f"📝 MERGing to Silver: {SILVER_PATH}")

# Create Silver table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.transactions
    USING DELTA
    LOCATION '{SILVER_PATH}'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

target = DeltaTable.forPath(spark, SILVER_PATH)

merge_stats = (
    target.alias("tgt")
    .merge(
        df_silver.alias("src"),
        "tgt.txn_id = src.txn_id AND tgt.txn_date = src.txn_date"
    )
    .whenMatchedUpdate(
        condition="""
            src.status   != tgt.status OR 
            src.amount   != tgt.amount OR
            src.merchant_id != tgt.merchant_id
        """,
        set={
            "status":       "src.status",
            "amount":       "src.amount",
            "merchant_id":  "src.merchant_id",
            "processed_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✅ Silver MERGE complete")

# Pass downstream
dbutils.jobs.taskValues.set("silver_record_count", str(valid_count))
dbutils.jobs.taskValues.set("quarantine_count", str(invalid_count))
dbutils.notebook.exit(f"SUCCESS:{valid_count}")
```

---

### `03_fraud_detection.py`

```python
# ─────────────────────────────────────────────────────────────────────
# TASK 3: FRAUD DETECTION
# Apply rule-based fraud detection on Silver transactions
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *
from pyspark.sql import Window

batch_date = dbutils.jobs.taskValues.get("transform_silver", "batch_date", BATCH_DATE)

# Read today's silver data
df = spark.read.format("delta").load(SILVER_PATH) \
    .filter(col("processing_date") == batch_date)

# ── FRAUD RULE 1: High-value transaction (> ₹5 lakhs) ──
df = df.withColumn("flag_high_value", col("amount") > 500000)

# ── FRAUD RULE 2: Velocity check (> 10 transactions in 1 hour) ──
w_velocity = Window.partitionBy("customer_id") \
    .orderBy(col("txn_datetime").cast("long")) \
    .rangeBetween(-3600, 0)  # 1-hour window in seconds

df = df.withColumn(
    "txn_count_last_hour",
    count("txn_id").over(w_velocity)
).withColumn(
    "flag_high_velocity",
    col("txn_count_last_hour") > 10
)

# ── FRAUD RULE 3: Amount > 3x customer's rolling 30-day average ──
# Read 30-day history
df_history = spark.read.format("delta").load(SILVER_PATH) \
    .filter(col("txn_date") >= date_sub(lit(batch_date).cast("date"), 30))

customer_avg = df_history.groupBy("customer_id") \
    .agg(avg("amount").alias("avg_30d_amount"))

df = df.join(broadcast(customer_avg), on="customer_id", how="left") \
    .withColumn(
        "flag_amount_anomaly",
        col("amount") > (col("avg_30d_amount") * 3)
    )

# ── FRAUD RULE 4: Round amount transactions (often money laundering) ──
df = df.withColumn(
    "flag_round_amount",
    (col("amount") % 10000 == 0) & (col("amount") >= 100000)
)

# ── Compute fraud risk score ──
df_scored = df.withColumn(
    "fraud_score",
    (col("flag_high_value").cast("int")    * 30) +
    (col("flag_high_velocity").cast("int") * 40) +
    (col("flag_amount_anomaly").cast("int")* 20) +
    (col("flag_round_amount").cast("int")  * 10)
).withColumn(
    "fraud_risk_level",
    when(col("fraud_score") >= 60, "HIGH")
    .when(col("fraud_score") >= 30, "MEDIUM")
    .when(col("fraud_score") >= 10, "LOW")
    .otherwise("NORMAL")
)

# Write fraud scores back to Silver (UPDATE existing records)
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, SILVER_PATH)
fraud_updates = df_scored.select(
    "txn_id", "txn_date",
    "fraud_score", "fraud_risk_level",
    "flag_high_value", "flag_high_velocity",
    "flag_amount_anomaly", "flag_round_amount"
)

(target.alias("tgt")
 .merge(fraud_updates.alias("src"), "tgt.txn_id = src.txn_id AND tgt.txn_date = src.txn_date")
 .whenMatchedUpdateAll()
 .execute())

high_risk_count = df_scored.filter(col("fraud_risk_level") == "HIGH").count()
print(f"🚨 High-risk transactions flagged: {high_risk_count:,}")

dbutils.jobs.taskValues.set("high_risk_count", str(high_risk_count))
dbutils.notebook.exit(f"SUCCESS:{high_risk_count}")
```

---

### `04_build_gold.py`

```python
# ─────────────────────────────────────────────────────────────────────
# TASK 4: BUILD GOLD — Business aggregations for BI
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *

batch_date = dbutils.jobs.taskValues.get("fraud_detection", "batch_date", BATCH_DATE)

df_silver = spark.read.format("delta").load(SILVER_PATH) \
    .filter(col("processing_date") == batch_date)

# ── Gold Table 1: Daily Transaction Summary ──
df_daily_summary = df_silver.groupBy("txn_date", "payment_mode", "currency") \
    .agg(
        count("txn_id").alias("transaction_count"),
        sum("amount").alias("total_volume"),
        avg("amount").alias("avg_transaction_value"),
        max("amount").alias("max_transaction"),
        countDistinct("customer_id").alias("unique_customers"),
        sum(when(col("fraud_risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_count"),
        sum(when(col("fraud_risk_level") == "HIGH", col("amount")).otherwise(0)).alias("high_risk_volume"),
    ) \
    .withColumn("report_generated_at", current_timestamp())

df_daily_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"txn_date = '{batch_date}'") \
    .save(f"{GOLD_PATH}/daily_summary/")

# ── Gold Table 2: Customer Transaction Profile (for personalization) ──
df_customer_profile = df_silver.groupBy("customer_id") \
    .agg(
        count("txn_id").alias("lifetime_transactions"),
        sum("amount").alias("lifetime_spend"),
        avg("amount").alias("avg_transaction_value"),
        max("txn_date").alias("last_transaction_date"),
        collect_set("payment_mode").alias("preferred_modes"),
        sum(when(col("fraud_risk_level") == "HIGH", 1).otherwise(0)).alias("fraud_flags"),
    )

df_customer_profile.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}/customer_profiles/")

print(f"✅ Gold tables built for {batch_date}")
print(f"   Daily summary rows: {df_daily_summary.count():,}")
print(f"   Customer profiles:  {df_customer_profile.count():,}")

dbutils.notebook.exit(f"SUCCESS:gold_built")
```

---

## Job Definition (JSON for API/Terraform)

```json
{
  "name": "daily-transaction-pipeline",
  "tasks": [
    {
      "task_key": "ingest_bronze",
      "notebook_task": {
        "notebook_path": "/Repos/my-org/data-platform/notebooks/01_ingest_bronze",
        "base_parameters": {
          "environment": "prod",
          "batch_date": ""
        }
      },
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS4_v2",
        "autoscale": {"min_workers": 2, "max_workers": 6}
      },
      "max_retries": 2,
      "min_retry_interval_millis": 300000
    },
    {
      "task_key": "transform_silver",
      "depends_on": [{"task_key": "ingest_bronze"}],
      "notebook_task": {
        "notebook_path": "/Repos/my-org/data-platform/notebooks/02_transform_silver"
      },
      "job_cluster_key": "shared_cluster",
      "max_retries": 1
    },
    {
      "task_key": "fraud_detection",
      "depends_on": [{"task_key": "transform_silver"}],
      "notebook_task": {
        "notebook_path": "/Repos/my-org/data-platform/notebooks/03_fraud_detection"
      },
      "job_cluster_key": "shared_cluster"
    },
    {
      "task_key": "build_gold",
      "depends_on": [{"task_key": "fraud_detection"}],
      "notebook_task": {
        "notebook_path": "/Repos/my-org/data-platform/notebooks/04_build_gold"
      },
      "job_cluster_key": "shared_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "shared_cluster",
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS4_v2",
        "autoscale": {"min_workers": 2, "max_workers": 8}
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  },
  "email_notifications": {
    "on_failure": ["data-eng-team@company.com"],
    "on_success": ["data-eng-team@company.com"]
  },
  "timeout_seconds": 7200
}
```

---

## Expected Output

```
✅ Pipeline Run: 2024-01-15
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Task              Duration    Records     Status
─────────────────────────────────────────────
ingest_bronze       8m 32s    512,847     ✅ SUCCESS
transform_silver   12m 15s    508,221     ✅ SUCCESS (4,626 quarantined)
fraud_detection    18m 44s        847     ✅ SUCCESS (847 high-risk flagged)
build_gold          3m 22s    508,221     ✅ SUCCESS
─────────────────────────────────────────────
Total Duration:    43m 53s
SLA:               2h 00m (within SLA ✅)
```
