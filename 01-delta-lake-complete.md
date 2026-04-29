# Module 03 — Delta Lake Deep Dive
## ACID Transactions, Time Travel, Merge, Schema Evolution & Partitioning

> Delta Lake is the single most important technology in modern Azure Databricks projects. Every enterprise data lake built post-2020 uses it.

---

## 3.1 What Is Delta Lake and Why Does It Exist?

Before Delta Lake, data lakes had a fundamental problem: **they were just folders of files**.

```
PROBLEM (Plain Parquet/CSV data lake):

Writer 1 starts writing partition /date=2024-01-01/
Writer 2 starts reading — sees partial, incomplete data ← DATA CORRUPTION

Job fails halfway through writing 100 files
→ 60 files written, 40 missing ← INCOMPLETE DATA, NO ROLLBACK

"Update" means: delete old file + write new file
→ If crash happens between delete and write ← DATA LOSS

No way to know what changed, when, or who changed it ← NO AUDIT TRAIL
```

Delta Lake solves all of this with a **transaction log**.

### The Delta Lake Transaction Log

```
/mnt/silver/transactions/           ← Your Delta table
│
├── _delta_log/                     ← THE TRANSACTION LOG
│   ├── 00000000000000000000.json   ← Version 0 (initial write)
│   ├── 00000000000000000001.json   ← Version 1 (first update)
│   ├── 00000000000000000002.json   ← Version 2 (MERGE operation)
│   ├── 00000000000000000003.json   ← Version 3 (schema change)
│   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint at v10
│
├── part-00000-abc123.parquet       ← Actual data files
├── part-00001-def456.parquet
└── part-00002-ghi789.parquet
```

Each JSON in `_delta_log` records exactly what happened:
```json
{
  "commitInfo": {
    "timestamp": 1704067200000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"},
    "userName": "sumit@company.com"
  },
  "add": {
    "path": "part-00000-abc123.parquet",
    "size": 1048576,
    "stats": "{\"numRecords\": 5000, \"minValues\": {...}}"
  }
}
```

---

## 3.2 Creating Delta Tables — All Methods

```python
# Method 1: Write DataFrame as Delta
df.write.format("delta").mode("overwrite").save("/mnt/silver/transactions/")

# Method 2: Write as managed table (stored in Hive metastore)
df.write.format("delta").mode("overwrite").saveAsTable("silver.transactions")

# Method 3: Create table using SQL DDL
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.transactions (
        txn_id       BIGINT        NOT NULL,
        txn_date     DATE          NOT NULL,
        customer_id  STRING        NOT NULL,
        amount       DOUBLE        NOT NULL,
        payment_mode STRING,
        status       STRING        DEFAULT 'PENDING',
        created_at   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    PARTITIONED BY (txn_date)
    LOCATION '/mnt/silver/transactions/'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.enableChangeDataFeed'       = 'true'
    )
""")

# Method 4: Convert existing Parquet to Delta (no data rewrite!)
from delta.tables import DeltaTable

DeltaTable.convertToDelta(spark, "parquet.`/mnt/bronze/legacy_data/`")
```

---

## 3.3 ACID Transactions in Practice

### Atomicity — All or Nothing

```python
# This write either completes fully or leaves NO trace
try:
    df_large.write \
        .format("delta") \
        .mode("append") \
        .save("/mnt/silver/transactions/")
    print("✅ Write committed successfully")
except Exception as e:
    # If this fails at any point (network, OOM, etc.), 
    # Delta ROLLS BACK — no partial data
    print(f"❌ Write failed and was rolled back: {e}")
```

### Isolation — Concurrent Reads and Writes

```python
# Thread 1: Writing new data
df_new.write.format("delta").mode("append").save("/mnt/silver/transactions/")

# Thread 2: Reading at the SAME TIME — sees consistent snapshot
# (Optimistic Concurrency Control — reads are never blocked)
df_read = spark.read.format("delta").load("/mnt/silver/transactions/")
```

### Durability — Changes Survive Failures

```python
# Check table history — every operation is recorded permanently
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/silver/transactions/")

history_df = delta_table.history()
history_df.select(
    "version", "timestamp", "operation", "operationParameters", "userName"
).show(truncate=False)

# Output:
# +-------+-------------------+----------------+---------------------------------------------------+---------------------+
# |version|timestamp          |operation       |operationParameters                                |userName             |
# +-------+-------------------+----------------+---------------------------------------------------+---------------------+
# |3      |2024-01-15 10:30:00|MERGE           |{predicate -> ..., matchedPredicates -> ...}       |sumit@company.com    |
# |2      |2024-01-15 08:00:00|WRITE           |{mode -> Append, partitionBy -> []}                |adf-pipeline@svc.com |
# |1      |2024-01-14 02:00:00|OPTIMIZE        |{predicate -> [], zOrderBy -> [customer_id]}       |system               |
# |0      |2024-01-14 00:00:00|CREATE OR REPLACE|{isManaged -> false, description -> ...}          |sumit@company.com    |
```

---

## 3.4 Time Travel — Read Historical Data

```python
from delta.tables import DeltaTable

# Read a specific version
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/mnt/silver/transactions/")

# Read at a specific timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-14 00:00:00") \
    .load("/mnt/silver/transactions/")

# SQL syntax
spark.sql("""
    SELECT * FROM silver.transactions VERSION AS OF 2
""")

spark.sql("""
    SELECT * FROM silver.transactions TIMESTAMP AS OF '2024-01-14'
""")
```

### Real-World Time Travel Use Cases

```python
# USE CASE 1: Audit — what did the data look like on a specific date?
# Regulatory requirement: reproduce report from last quarter end
q3_end_data = spark.read.format("delta") \
    .option("timestampAsOf", "2024-09-30 23:59:59") \
    .load("/mnt/gold/revenue_summary/")

# USE CASE 2: Recover from accidental DELETE
# "We accidentally deleted customer C001's data!"

# Step 1: Find the last version that had C001
history = DeltaTable.forPath(spark, "/mnt/silver/transactions/").history()

# Step 2: Read the data before the deletion (version 5)
df_before_delete = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("/mnt/silver/transactions/") \
    .filter(col("customer_id") == "C001")

# Step 3: Re-insert the recovered data
df_before_delete.write \
    .format("delta") \
    .mode("append") \
    .save("/mnt/silver/transactions/")
print("✅ Data recovered from version 5")

# USE CASE 3: Rollback entire table to previous version
DeltaTable.forPath(spark, "/mnt/silver/transactions/") \
    .restoreToVersion(5)

# USE CASE 4: Compare what changed between versions
v5 = spark.read.format("delta").option("versionAsOf", 5).load("/mnt/silver/transactions/")
v6 = spark.read.format("delta").option("versionAsOf", 6).load("/mnt/silver/transactions/")

# Find new records added in v6
new_in_v6 = v6.join(v5, on="txn_id", how="left_anti")
print(f"New records added in v6: {new_in_v6.count()}")
```

### Managing Data Retention

```python
# Default retention: 30 days of history
# Configure longer retention for compliance:
spark.sql("""
    ALTER TABLE silver.transactions
    SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 90 days')
""")

# Vacuum: remove old data files no longer referenced
# WARNING: After vacuum, you cannot time travel beyond the retention period!
spark.sql("VACUUM silver.transactions RETAIN 90 HOURS")

# Check vacuum impact before running
spark.sql("VACUUM silver.transactions RETAIN 90 HOURS DRY RUN")
```

---

## 3.5 MERGE (Upsert) — The Most Important Delta Operation

`MERGE` is the backbone of every incremental ETL pipeline. It handles insert-update-delete in one atomic operation.

```python
from delta.tables import DeltaTable

# SCENARIO: Daily batch of transaction updates arrives
# Some are new transactions (INSERT)
# Some are updates to existing transactions (status changed)
# Some are cancellations (DELETE)

# Target: existing Delta table in Silver layer
target = DeltaTable.forPath(spark, "/mnt/silver/transactions/")

# Source: today's incoming batch
source = spark.read.format("parquet") \
    .load("/mnt/bronze/transactions/date=2024-01-15/")

# MERGE operation
(
    target.alias("tgt")
    .merge(
        source.alias("src"),
        condition="tgt.txn_id = src.txn_id"  # ← Join condition (how to match)
    )
    # When matched AND cancelled → DELETE
    .whenMatchedDelete(
        condition="src.status = 'CANCELLED'"
    )
    # When matched AND status changed → UPDATE
    .whenMatchedUpdate(
        condition="src.status != tgt.status OR src.amount != tgt.amount",
        set={
            "status":      "src.status",
            "amount":      "src.amount",
            "updated_at":  "current_timestamp()"
        }
    )
    # When not matched in target → INSERT
    .whenNotMatchedInsert(
        values={
            "txn_id":      "src.txn_id",
            "txn_date":    "src.txn_date",
            "customer_id": "src.customer_id",
            "amount":      "src.amount",
            "status":      "src.status",
            "created_at":  "current_timestamp()",
            "updated_at":  "current_timestamp()"
        }
    )
    .execute()
)

print("✅ MERGE completed")
```

### MERGE with SQL

```sql
MERGE INTO silver.transactions AS tgt
USING bronze.transactions_staging AS src
ON tgt.txn_id = src.txn_id

WHEN MATCHED AND src.status = 'CANCELLED' THEN
    DELETE

WHEN MATCHED AND (src.status != tgt.status OR src.amount != tgt.amount) THEN
    UPDATE SET
        tgt.status     = src.status,
        tgt.amount     = src.amount,
        tgt.updated_at = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (txn_id, txn_date, customer_id, amount, status, created_at, updated_at)
    VALUES (src.txn_id, src.txn_date, src.customer_id, src.amount, 
            src.status, current_timestamp(), current_timestamp())
```

### MERGE Performance Optimization

```python
# PROBLEM: MERGE on 1TB table is slow — scans entire target table

# SOLUTION 1: Partition pruning in MERGE condition
# Add partition column to the join condition
(
    target.alias("tgt")
    .merge(
        source.alias("src"),
        # Include partition column → Spark only scans relevant partitions
        condition="""
            tgt.txn_date = src.txn_date 
            AND tgt.txn_id = src.txn_id
        """
    )
    ...
    .execute()
)

# SOLUTION 2: Filter target before merge (for point-in-time processing)
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Create a view of only today's partition
todays_data_path = "/mnt/silver/transactions/"
todays_date = "2024-01-15"

target_filtered = DeltaTable.forPath(spark, todays_data_path)

source_deduped = source \
    .dropDuplicates(["txn_id"]) \
    .filter(col("txn_date") == todays_date)

# SOLUTION 3: SCD Type 2 MERGE (Slowly Changing Dimensions)
target_scd2 = DeltaTable.forPath(spark, "/mnt/silver/customers/")

(
    target_scd2.alias("tgt")
    .merge(
        source.alias("src"),
        "tgt.customer_id = src.customer_id AND tgt.is_current = true"
    )
    # Close the old record
    .whenMatchedUpdate(
        condition="tgt.customer_email != src.customer_email",
        set={
            "is_current": "false",
            "valid_to":   "current_date()"
        }
    )
    .execute()
)

# Insert new record (separate operation for SCD2)
new_records = source.withColumn("is_current", lit(True)) \
                    .withColumn("valid_from", current_date()) \
                    .withColumn("valid_to", lit(None).cast("date"))

new_records.write.format("delta").mode("append").saveAsTable("silver.customers")
```

---

## 3.6 Schema Evolution

Delta Lake can automatically handle schema changes — critical for evolving data sources.

```python
# SCENARIO: Source system adds new columns in next batch

# Existing table: txn_id, txn_date, customer_id, amount
# New batch has:  txn_id, txn_date, customer_id, amount, merchant_id (NEW!)

# WITHOUT schema evolution → ERROR
df_new_batch.write \
    .format("delta") \
    .mode("append") \
    .save("/mnt/silver/transactions/")
# AnalysisException: A schema mismatch detected when writing to Delta table

# WITH mergeSchema → automatically adds new columns
df_new_batch.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/silver/transactions/")
# ✅ merchant_id column added, old records have NULL for merchant_id

# Enable at table level (applies to all writes)
spark.sql("""
    ALTER TABLE silver.transactions
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

### Schema Enforcement (Prevent Bad Data)

```python
# Delta ENFORCES schema by default — protects data quality
# If source has wrong types, write fails BEFORE any data is written

# Check current schema
spark.sql("DESCRIBE silver.transactions").show()

# Evolve schema explicitly (add column)
spark.sql("""
    ALTER TABLE silver.transactions
    ADD COLUMN merchant_id STRING AFTER customer_id
""")

# Change column type (requires rewrite in some cases)
spark.sql("""
    ALTER TABLE silver.transactions
    ALTER COLUMN amount TYPE DECIMAL(18, 2)
""")

# Rename column (requires column mapping enabled)
spark.sql("""
    ALTER TABLE silver.transactions
    RENAME COLUMN txn_id TO transaction_id
""")
```

---

## 3.7 Partitioning Strategy — Critical for Performance

```python
# WRONG: Partition by a high-cardinality column
df.write.format("delta") \
    .partitionBy("customer_id") \  # 10 million customers = 10M partitions = disaster
    .save("/mnt/silver/transactions/")

# WRONG: No partitioning on a large, date-filtered table
df.write.format("delta").save("/mnt/silver/transactions/")
# Full table scan for every date-filtered query

# RIGHT: Partition by date (low cardinality, commonly filtered)
df.write.format("delta") \
    .partitionBy("txn_date") \
    .save("/mnt/silver/transactions/")
# Queries like WHERE txn_date = '2024-01-15' only read 1 partition

# RIGHT: Multi-level partition for very large tables
df.write.format("delta") \
    .partitionBy("year", "month") \
    .save("/mnt/silver/transactions/")
```

### Partition Pruning in Action

```python
# This DOES use partition pruning (fast — reads only Jan 2024 data)
df = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .filter(col("txn_date").between("2024-01-01", "2024-01-31"))

# This DOES NOT use partition pruning (full scan)
df = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .filter(col("amount") > 1000)  # amount is not a partition column

# Check query plan to verify pruning
df.explain(True)
# Look for: PartitionFilters in the scan step
```

### OPTIMIZE and Z-Ordering

```python
# OPTIMIZE: Compact small files into larger ones (fixes "small file problem")
spark.sql("OPTIMIZE silver.transactions")

# Z-ORDER: Co-locate related data within files (speeds up filtered queries)
# Z-order on most frequently filtered non-partition columns
spark.sql("""
    OPTIMIZE silver.transactions
    ZORDER BY (customer_id, payment_mode)
""")
# After Z-ordering: queries filtering by customer_id are 10-50x faster

# Auto-optimize at table level
spark.sql("""
    ALTER TABLE silver.transactions
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',  -- Optimize during writes
        'delta.autoOptimize.autoCompact'   = 'true'   -- Auto-compact small files
    )
""")
```

---

## 3.8 Medallion Architecture — Production Implementation

```
┌────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                       │
│                                                                 │
│  Source          BRONZE           SILVER            GOLD        │
│  System    →    (Raw)      →    (Cleaned)   →    (Business)    │
│                                                                 │
│  • No transforms  • Validated     • Aggregated                 │
│  • Raw format     • Deduplicated  • Joined/enriched            │
│  • Append-only    • Typed         • KPI-ready                  │
│  • Never delete   • Schema fixed  • BI-optimized               │
│  • Audit trail    • Delta format  • Serving layer              │
└────────────────────────────────────────────────────────────────┘
```

```python
# ─────────────────────────────────────────────────────────
# BRONZE LAYER: Raw ingestion, no transformation
# ─────────────────────────────────────────────────────────

def ingest_to_bronze(source_path: str, target_path: str, date: str):
    """
    Bronze layer: read raw, append as-is with metadata columns.
    No data transformation — preserve exactly as received.
    """
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false")  \  # All strings — preserve raw format
        .csv(source_path)
    
    df_bronze = df_raw \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_date", lit(date))
    
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \  # Handle new columns from source
        .partitionBy("_batch_date") \
        .save(target_path)
    
    return df_bronze.count()

# ─────────────────────────────────────────────────────────
# SILVER LAYER: Clean, validate, type-cast, deduplicate
# ─────────────────────────────────────────────────────────

def transform_to_silver(bronze_path: str, silver_path: str, batch_date: str):
    """
    Silver layer: apply business rules, validate, deduplicate.
    """
    from pyspark.sql.functions import to_date, to_timestamp, regexp_replace, col
    
    df_bronze = spark.read.format("delta").load(bronze_path) \
        .filter(col("_batch_date") == batch_date)
    
    # Type casting
    df_typed = df_bronze \
        .withColumn("txn_id",    col("txn_id").cast("bigint")) \
        .withColumn("txn_date",  to_date(col("txn_date"), "yyyy-MM-dd")) \
        .withColumn("amount",    regexp_replace(col("amount"), "[,$]", "").cast("double")) \
        .withColumn("customer_id", upper(trim(col("customer_id"))))
    
    # Validation: remove records failing business rules
    df_valid = df_typed.filter(
        col("txn_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0) &
        col("customer_id").rlike("^C[0-9]{3,}$")
    )
    
    # Track rejected records for data quality monitoring
    df_rejected = df_typed.join(df_valid, on="txn_id", how="left_anti") \
        .withColumn("reject_reason", lit("FAILED_VALIDATION"))
    
    df_rejected.write.format("delta").mode("append") \
        .save("/mnt/quarantine/transactions/")
    
    # Deduplicate: keep latest record per txn_id
    from pyspark.sql import Window
    w = Window.partitionBy("txn_id").orderBy(col("_ingested_at").desc())
    
    df_deduped = df_valid \
        .withColumn("rn", row_number().over(w)) \
        .filter(col("rn") == 1) \
        .drop("rn", "_source_file", "_batch_date")
    
    # MERGE to silver (handle late arrivals and corrections)
    target = DeltaTable.forPath(spark, silver_path)
    
    (target.alias("tgt")
     .merge(df_deduped.alias("src"), "tgt.txn_id = src.txn_id AND tgt.txn_date = src.txn_date")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

# ─────────────────────────────────────────────────────────
# GOLD LAYER: Business aggregations for BI consumption
# ─────────────────────────────────────────────────────────

def build_gold_daily_summary(silver_path: str, gold_path: str, report_date: str):
    """
    Gold layer: daily sales summary for BI reporting.
    """
    df_silver = spark.read.format("delta").load(silver_path) \
        .filter(col("txn_date") == report_date)
    
    df_gold = df_silver.groupBy(
        "txn_date",
        "payment_mode"
    ).agg(
        count("txn_id").alias("transaction_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers"),
        max("amount").alias("max_transaction")
    ).withColumn("report_generated_at", current_timestamp())
    
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"txn_date = '{report_date}'") \
        .save(gold_path)
```

---

## 3.9 Interview Questions — Delta Lake

**Q1: What makes Delta Lake ACID compliant?**
> Delta Lake uses a **transaction log** (`_delta_log/`) that records every operation atomically. Atomicity: writes either fully commit a JSON entry or are rolled back. Consistency: schema enforcement prevents bad data. Isolation: Optimistic Concurrency Control allows concurrent reads/writes with conflict detection. Durability: the transaction log persists on durable cloud storage.

**Q2: How does time travel work and when would you use it in production?**
> Every Delta write creates a versioned entry in the transaction log. You can query `VERSION AS OF N` or `TIMESTAMP AS OF '...'`. Use cases: regulatory audits (reproduce exact data at quarter-end), disaster recovery (restore accidentally deleted data using `restoreToVersion`), and pipeline debugging (compare what data looked like before/after a transformation).

**Q3: What is the difference between MERGE and a delete+insert approach?**
> MERGE is a single atomic operation — if it fails, nothing changes. Delete+insert is two operations — if the insert fails after delete, data is lost permanently. MERGE also has partition pruning intelligence to avoid scanning the entire target table. For production upsert pipelines, **always use MERGE**.

**Q4: What is Z-ordering and how does it differ from partitioning?**
> Partitioning splits data into separate folders by a column value — queries filter entire partition folders. Z-ordering co-locates related data **within** data files using a space-filling curve — useful for high-cardinality columns (like customer_id) where partitioning would create too many small partitions. Use partitioning for low-cardinality date/region columns; use Z-ordering for high-cardinality frequently-filtered columns.

**Q5: What happens if two jobs try to MERGE to the same Delta table simultaneously?**
> Delta Lake uses **Optimistic Concurrency Control**. Both jobs read the current version, compute changes, and attempt to commit. The first to commit wins. The second detects a conflict (the version changed) and automatically retries its operation from the new version. This is configurable via `spark.databricks.delta.retryWriteConflicts`.

---

*Next: [Module 04 — Performance Optimization](../04-Optimization/01-partitioning-repartition-coalesce.md)*
