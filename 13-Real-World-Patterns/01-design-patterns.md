# Module 13 — Real-World Patterns
## SCD Types, Data Modeling, Pipeline Patterns & Enterprise Recipes

---

## 13.1 Slowly Changing Dimensions (SCD) — All Types

SCD is one of the most asked-about topics in DE interviews because it appears in every data warehouse project.

```
SCENARIO: Customer changes their city from "Mumbai" to "Delhi"

SCD TYPE 1: Overwrite — just update the current record
  Before: C001 | Alice | Mumbai
  After:  C001 | Alice | Delhi
  Lost: history of Mumbai address

SCD TYPE 2: Full history — close old, create new
  Before: C001 | Alice | Mumbai | 2020-01-01 | null | TRUE
  After:  C001 | Alice | Mumbai | 2020-01-01 | 2024-01-15 | FALSE (closed)
          C001 | Alice | Delhi  | 2024-01-15 | null       | TRUE  (new current)

SCD TYPE 3: Track only previous value
  Before: C001 | Alice | Mumbai | null (no prev city)
  After:  C001 | Alice | Delhi  | Mumbai (prev_city column)
  Lost: history before Mumbai

SCD TYPE 4: Separate history table
  Current table: C001 | Alice | Delhi  (latest only)
  History table: C001 | Alice | Mumbai | 2020-01-01 | 2024-01-15
                 C001 | Alice | Delhi  | 2024-01-15 | null
```

### SCD Type 1 — Overwrite

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

def scd_type1_upsert(
    source_df,
    target_path: str,
    join_keys: list,
    update_cols: list = None
):
    """
    SCD Type 1: Update existing records, insert new ones.
    History is NOT preserved.
    """
    target = DeltaTable.forPath(spark, target_path)
    
    merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in join_keys])
    
    (target.alias("tgt")
     .merge(source_df.alias("src"), merge_condition)
     .whenMatchedUpdate(set={
         **({"updated_at": "current_timestamp()"} if "updated_at" in target.toDF().columns else {}),
         **(
             {col: f"src.{col}" for col in update_cols}
             if update_cols
             else {"*": "*"}  # Update all columns
         )
     })
     .whenNotMatchedInsertAll()
     .execute())
```

### SCD Type 2 — Full History

```python
from pyspark.sql.functions import *
from delta.tables import DeltaTable

def scd_type2_merge(
    source_df,
    target_path: str,
    business_key: str,
    track_columns: list,
    effective_date_col: str = "valid_from",
    expiry_date_col: str    = "valid_to",
    current_flag_col: str   = "is_current"
):
    """
    SCD Type 2: Close old records on change, insert new versions.
    Full history preserved.
    
    Requires target table to have:
      - business_key column
      - valid_from, valid_to, is_current columns
    """
    
    target = DeltaTable.forPath(spark, target_path)
    target_df = target.toDF()
    
    # Step 1: Find records that have CHANGED
    # Join source to current target records
    changes = source_df.alias("src").join(
        target_df.filter(col(current_flag_col) == True).alias("tgt"),
        on=business_key,
        how="left"
    ).filter(
        # Record exists but one of the tracked columns changed
        col(f"tgt.{business_key}").isNotNull() &
        reduce(
            lambda a, b: a | b,
            [col(f"src.{c}") != col(f"tgt.{c}") for c in track_columns]
        )
    ).select(f"src.{business_key}")
    
    now = current_timestamp()
    
    # Step 2: Close old records (set valid_to and is_current=False)
    (target.alias("tgt")
     .merge(
         changes.alias("src"),
         f"tgt.{business_key} = src.{business_key} AND tgt.{current_flag_col} = true"
     )
     .whenMatchedUpdate(set={
         expiry_date_col:  "current_timestamp()",
         current_flag_col: "false",
         "updated_at":     "current_timestamp()"
     })
     .execute())
    
    # Step 3: Insert new versions for changed records + brand new records
    existing_current_keys = target.toDF() \
        .filter(col(current_flag_col) == True) \
        .select(business_key)
    
    # New versions (changed records) + new records (not in target at all)
    records_to_insert = source_df.join(
        existing_current_keys,
        on=business_key,
        how="left_anti"  # Records NOT in current target
    ).withColumn(effective_date_col, now) \
     .withColumn(expiry_date_col,    lit(None).cast("timestamp")) \
     .withColumn(current_flag_col,   lit(True)) \
     .withColumn("created_at",       now)
    
    records_to_insert.write \
        .format("delta") \
        .mode("append") \
        .save(target_path)
    
    print(f"✅ SCD2 complete | Closed: {changes.count()} | Inserted: {records_to_insert.count()}")


# ── QUERY SCD2 TABLE ──

# Get current state (like a normal dimension table)
current_customers = spark.read.format("delta").load(target_path) \
    .filter(col("is_current") == True)

# Get historical state at a specific date
customers_on_jan1 = spark.read.format("delta").load(target_path) \
    .filter(
        (col("valid_from") <= "2024-01-01") &
        ((col("valid_to") > "2024-01-01") | col("valid_to").isNull())
    )

# Join transactions to customer dimension — get customer profile AT TIME OF TRANSACTION
txns = spark.read.format("delta").load("/mnt/silver/transactions/")
customers = spark.read.format("delta").load(target_path)

# Point-in-time join (the correct way to join to SCD2)
txns_with_customer = txns.join(
    customers,
    (txns["customer_id"] == customers["customer_id"]) &
    (txns["txn_date"] >= customers["valid_from"]) &
    ((txns["txn_date"] < customers["valid_to"]) | customers["valid_to"].isNull()),
    how="left"
).select(
    txns["*"],
    customers["city"].alias("customer_city_at_txn_time"),  # Historical city!
    customers["tier"].alias("customer_tier_at_txn_time"),
)
```

---

## 13.2 Watermark + Deduplication Pattern

```python
# Production incremental load with watermark and deduplication
# Handles: late arrivals, duplicate source records, partial pipeline reruns

class IncrementalPipeline:
    """
    Generic incremental pipeline pattern.
    Used in virtually every enterprise ETL project.
    """
    
    def __init__(
        self,
        pipeline_name: str,
        source_path: str,
        target_path: str,
        business_key: str,
        watermark_col: str,
        watermark_table: str = "metadata.pipeline_watermarks"
    ):
        self.pipeline_name   = pipeline_name
        self.source_path     = source_path
        self.target_path     = target_path
        self.business_key    = business_key
        self.watermark_col   = watermark_col
        self.watermark_table = watermark_table
    
    def get_watermark(self) -> str:
        """Read last processed timestamp from watermark table."""
        try:
            row = spark.sql(f"""
                SELECT watermark_value FROM {self.watermark_table}
                WHERE pipeline_name = '{self.pipeline_name}'
            """).first()
            return row["watermark_value"] if row else "1900-01-01 00:00:00"
        except Exception:
            return "1900-01-01 00:00:00"
    
    def save_watermark(self, new_value: str):
        """Update watermark after successful processing."""
        from delta.tables import DeltaTable
        
        wm_df = spark.createDataFrame(
            [(self.pipeline_name, new_value, str(datetime.utcnow()))],
            ["pipeline_name", "watermark_value", "updated_at"]
        )
        
        try:
            target = DeltaTable.forName(spark, self.watermark_table)
            (target.alias("t")
             .merge(wm_df.alias("s"), "t.pipeline_name = s.pipeline_name")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        except Exception:
            # First run — table doesn't exist yet
            wm_df.write.format("delta").mode("overwrite").saveAsTable(self.watermark_table)
    
    def run(self) -> dict:
        """Execute the incremental load."""
        last_wm = self.get_watermark()
        print(f"📅 {self.pipeline_name}: processing from {last_wm}")
        
        # Read new/changed records
        df_new = spark.read.format("delta").load(self.source_path) \
            .filter(col(self.watermark_col) > last_wm)
        
        total_new = df_new.count()
        
        if total_new == 0:
            print("✅ No new records")
            return {"records": 0, "status": "SKIPPED"}
        
        # Deduplicate (in case source sends same record multiple times)
        # Keep latest version of each business key
        w = Window.partitionBy(self.business_key).orderBy(col(self.watermark_col).desc())
        df_deduped = df_new \
            .withColumn("_rn", row_number().over(w)) \
            .filter(col("_rn") == 1) \
            .drop("_rn")
        
        deduped_count = df_deduped.count()
        duplicates    = total_new - deduped_count
        
        # MERGE to target
        target = DeltaTable.forPath(spark, self.target_path)
        merge_cond = f"tgt.{self.business_key} = src.{self.business_key}"
        
        (target.alias("tgt")
         .merge(df_deduped.alias("src"), merge_cond)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
        # Update watermark
        new_wm = df_new.agg(max(self.watermark_col)).first()[0]
        self.save_watermark(str(new_wm))
        
        stats = {
            "records_read":        total_new,
            "duplicates_removed":  duplicates,
            "records_merged":      deduped_count,
            "new_watermark":       str(new_wm),
            "status":              "SUCCESS"
        }
        
        print(f"✅ Merged {deduped_count:,} records | Dupes removed: {duplicates:,}")
        return stats


# Usage
pipeline = IncrementalPipeline(
    pipeline_name="customer_sync",
    source_path="/mnt/bronze/crm_customers/",
    target_path="/mnt/silver/customers/",
    business_key="customer_id",
    watermark_col="updated_at"
)
stats = pipeline.run()
```

---

## 13.3 Fan-Out Pipeline Pattern

```python
# One source → Multiple targets (common for domain-specific marts)
# Example: Transactions → Sales mart, Fraud mart, Finance mart

def fan_out_pipeline(source_path: str, batch_date: str):
    """
    Read source once, write to multiple downstream tables.
    Key: cache the source DataFrame!
    """
    # Read source ONCE
    df_silver = spark.read.format("delta").load(source_path) \
        .filter(col("txn_date") == batch_date)
    
    # CACHE — we'll use this multiple times
    df_silver.cache()
    df_silver.count()  # Materialize the cache
    
    print(f"📊 Cached {df_silver.count():,} records for fan-out")
    
    try:
        # Target 1: Sales Mart (only card + UPI transactions)
        df_sales = df_silver \
            .filter(col("payment_mode").isin(["CARD", "UPI"])) \
            .groupBy("txn_date", "merchant_id") \
            .agg(
                sum("amount").alias("total_sales"),
                count("txn_id").alias("transaction_count")
            )
        
        df_sales.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"txn_date = '{batch_date}'") \
            .save("/mnt/gold/sales_mart/")
        
        print(f"✅ Sales mart: {df_sales.count():,} rows")
        
        # Target 2: Fraud Mart (flagged transactions)
        df_fraud = df_silver \
            .filter(col("fraud_risk_level") != "NORMAL") \
            .select(
                "txn_id", "txn_date", "customer_id", "amount",
                "fraud_risk_level", "fraud_score"
            )
        
        df_fraud.write.format("delta") \
            .mode("append") \
            .partitionBy("txn_date") \
            .save("/mnt/gold/fraud_mart/")
        
        print(f"✅ Fraud mart: {df_fraud.count():,} flagged transactions")
        
        # Target 3: Finance Mart (settled transactions only)
        df_finance = df_silver \
            .filter(col("status") == "COMPLETED") \
            .groupBy("txn_date", "currency") \
            .agg(
                sum("amount").alias("settled_amount"),
                count("txn_id").alias("settled_count"),
                avg("amount").alias("avg_settlement")
            )
        
        df_finance.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"txn_date = '{batch_date}'") \
            .save("/mnt/gold/finance_mart/")
        
        print(f"✅ Finance mart: {df_finance.count():,} rows")
        
    finally:
        # Always release cache
        df_silver.unpersist()
        print("🧹 Cache released")
```

---

## 13.4 Config-Driven Pipeline Pattern

```python
# ── pipeline_config.py ──
# Define pipeline behavior in config — not hardcoded

PIPELINE_CONFIGS = {
    "transactions": {
        "source_path":       "/mnt/bronze/transactions/",
        "target_path":       "/mnt/silver/transactions/",
        "business_key":      "txn_id",
        "partition_col":     "txn_date",
        "watermark_col":     "updated_at",
        "z_order_cols":      ["customer_id", "payment_mode"],
        "dq_rules":          TRANSACTION_DQ_RULES,
        "sla_hours":         4,       # Must complete within 4 hours of trigger
    },
    "customers": {
        "source_path":       "/mnt/bronze/crm_customers/",
        "target_path":       "/mnt/silver/customers/",
        "business_key":      "customer_id",
        "partition_col":     None,    # No partitioning for small dimension
        "watermark_col":     "updated_at",
        "z_order_cols":      ["customer_id"],
        "dq_rules":          CUSTOMER_DQ_RULES,
        "sla_hours":         6,
    },
}

# ── generic_pipeline.py ──
# One notebook handles ALL entity types via config

dbutils.widgets.dropdown("entity", "transactions", list(PIPELINE_CONFIGS.keys()))
entity = dbutils.widgets.get("entity")

config = PIPELINE_CONFIGS[entity]
print(f"🚀 Running pipeline for: {entity}")
print(f"   Config: {config}")

# Read source
df_source = spark.read.format("delta").load(config["source_path"])

# Run DQ
dq = DataQualityEngine(entity, BATCH_DATE)
df_clean, df_bad = dq.run_checks(df_source, config["dq_rules"], QUARANTINE_PATH)

# Merge to target
pipeline = IncrementalPipeline(
    pipeline_name=entity,
    source_path=config["source_path"],
    target_path=config["target_path"],
    business_key=config["business_key"],
    watermark_col=config["watermark_col"]
)
stats = pipeline.run()

print(f"✅ {entity} pipeline complete: {stats}")
```

---

## 13.5 Dead Letter Queue Pattern

```python
# Any record that fails processing goes to DLQ instead of being lost
# Operators can investigate and reprocess DLQ records manually

class DeadLetterQueue:
    """
    Route failed records to a separate Delta table for investigation.
    """
    
    DLQ_SCHEMA = StructType([
        StructField("pipeline_name",  StringType()),
        StructField("batch_date",     StringType()),
        StructField("failure_reason", StringType()),
        StructField("original_data",  StringType()),  # JSON of original record
        StructField("error_message",  StringType()),
        StructField("failed_at",      TimestampType()),
        StructField("retry_count",    IntegerType()),
        StructField("resolved",       BooleanType()),
    ])
    
    def __init__(self, dlq_path: str, pipeline_name: str):
        self.dlq_path      = dlq_path
        self.pipeline_name = pipeline_name
    
    def write(self, df_failed: DataFrame, failure_reason: str, error_msg: str = ""):
        """Write failed records to DLQ table."""
        from pyspark.sql.functions import to_json, struct
        
        df_dlq = df_failed.withColumn(
            "original_data",  to_json(struct(*[col(c) for c in df_failed.columns]))
        ).withColumn("pipeline_name",  lit(self.pipeline_name)) \
         .withColumn("batch_date",     lit(BATCH_DATE)) \
         .withColumn("failure_reason", lit(failure_reason)) \
         .withColumn("error_message",  lit(error_msg)) \
         .withColumn("failed_at",      current_timestamp()) \
         .withColumn("retry_count",    lit(0)) \
         .withColumn("resolved",       lit(False)) \
         .select("pipeline_name","batch_date","failure_reason",
                 "original_data","error_message","failed_at","retry_count","resolved")
        
        df_dlq.write.format("delta").mode("append").save(self.dlq_path)
        print(f"  📮 {df_failed.count():,} records sent to DLQ: {failure_reason}")
    
    def get_unresolved(self, batch_date: str = None) -> DataFrame:
        """Fetch unresolved DLQ records for reprocessing."""
        df = spark.read.format("delta").load(self.dlq_path) \
            .filter(col("resolved") == False)
        
        if batch_date:
            df = df.filter(col("batch_date") == batch_date)
        
        return df
    
    def mark_resolved(self, record_ids: list):
        """Mark records as resolved after manual fix."""
        target = DeltaTable.forPath(spark, self.dlq_path)
        resolved_df = spark.createDataFrame(
            [(id,) for id in record_ids], ["_record_id"]
        )
        
        (target.alias("t")
         .merge(resolved_df.alias("s"), "t._record_id = s._record_id")
         .whenMatchedUpdate(set={"resolved": "true"})
         .execute())

# Usage in pipeline
dlq = DeadLetterQueue("/mnt/dlq/transactions/", "transaction-pipeline")

try:
    df_result = process_batch(df_input)
    df_result.write.format("delta").mode("append").save(TARGET_PATH)
except Exception as e:
    dlq.write(df_input, "PROCESSING_ERROR", str(e))
    raise
```

---

## 13.6 Interview Questions — Design Patterns

**Q: Walk me through how you'd implement SCD Type 2 in PySpark with Delta Lake.**
> Three-step MERGE approach: (1) Find changed records by joining source to current target (`is_current=true`) and comparing tracked columns. (2) Close old records with a MERGE that sets `valid_to=now()` and `is_current=false` for matching changed records. (3) Insert new versions for changed records + brand new records with `valid_from=now()`, `valid_to=null`, `is_current=true`. This requires two operations (one MERGE to close, one write to insert) because Delta MERGE can't simultaneously close a record AND insert a new version for the same business key.

**Q: What is the fan-out pattern and why is caching critical for it?**
> Fan-out: one source DataFrame is written to multiple downstream tables/marts in a single pipeline run. Without caching, Spark recomputes the source transformation from disk for each write — if you write to 5 targets, you read Silver 5 times. With `df.cache()` + a triggering action (`count()`), the source data is materialized in memory once and reused for all 5 writes. For 50GB Silver data and 5 targets, caching reduces I/O from 250GB to 50GB.

---
*Next: [Module 14 — Cheatsheets](../14-Cheatsheets/01-pyspark-cheatsheet.md)*
