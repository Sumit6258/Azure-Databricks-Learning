# Module 03 — Delta Lake
## Part 3: Change Data Feed (CDF), Liquid Clustering & Advanced Features

---

## 3.1 Change Data Feed (CDF)

CDF lets downstream consumers read only what CHANGED in a Delta table — inserts, updates, and deletes — since the last time they looked. This eliminates expensive full-table scans for incremental processing.

```
WITHOUT CDF:
  Pipeline A updates Silver table (MERGE: insert 1K, update 500, delete 100)
  Pipeline B (downstream) must: read ENTIRE Silver table → process → write Gold
  Problem: reads 50M rows to find 1,600 changes

WITH CDF:
  Pipeline B reads only the 1,600 changed rows
  10-1000x faster for downstream incremental consumers
```

### Enabling CDF

```python
# Enable on new table
spark.sql("""
    CREATE TABLE silver.transactions
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Enable on existing table
spark.sql("""
    ALTER TABLE silver.transactions
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Enable in write operation
df.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .save("/mnt/silver/transactions/")
```

### Reading CDF

```python
# Read changes since a specific version
df_changes = spark.read.format("delta") \
    .option("readChangeFeed",    "true") \
    .option("startingVersion",   5) \
    .option("endingVersion",     10) \  # Optional — defaults to latest
    .table("silver.transactions")

# Read changes since a specific timestamp
df_changes = spark.read.format("delta") \
    .option("readChangeFeed",    "true") \
    .option("startingTimestamp", "2024-01-15 02:00:00") \
    .table("silver.transactions")

# CDF adds these metadata columns:
# _change_type:      insert | update_preimage | update_postimage | delete
# _commit_version:   Delta version when this change was committed
# _commit_timestamp: Timestamp of the commit

df_changes.show()
# +-----------+----------+--------+-------------------+------------------+
# |customer_id|    amount|  status|       _change_type|  _commit_version |
# +-----------+----------+--------+-------------------+------------------+
# |       C001|    150.00| PENDING|             insert|               5  |
# |       C002|    250.00|APPROVED|  update_preimage   |               6  |
# |       C002|    250.00|COMPLETED|  update_postimage  |               6  |
# |       C003|     75.00| PENDING|             delete|               7  |

# Filter by change type
df_inserts       = df_changes.filter(col("_change_type") == "insert")
df_updates_after = df_changes.filter(col("_change_type") == "update_postimage")
df_deletes       = df_changes.filter(col("_change_type") == "delete")
```

### CDF for Incremental Gold Builds

```python
# ── Incremental Gold using CDF — replaces full Silver scan ──

def build_gold_incremental_with_cdf(
    silver_table: str,
    gold_path:    str,
    last_version: int
) -> int:
    """
    Only process records that changed in Silver since last_version.
    Much faster than reading entire Silver table.
    """
    from delta.tables import DeltaTable
    
    # Read only what changed
    df_changes = spark.read.format("delta") \
        .option("readChangeFeed",  "true") \
        .option("startingVersion", last_version + 1) \
        .table(silver_table)
    
    if df_changes.count() == 0:
        print("✅ No changes since last run")
        return last_version
    
    # We only care about the final state of changed records (postimage + inserts)
    df_latest_state = df_changes.filter(
        col("_change_type").isin(["insert", "update_postimage"])
    ).drop("_change_type", "_commit_version", "_commit_timestamp")
    
    # Get customer IDs that changed — we need to recompute their Gold aggregates
    changed_customers = df_latest_state.select("customer_id").distinct()
    
    # Read full Silver for just those customers (targeted, not full scan)
    df_silver_subset = spark.read.table(silver_table) \
        .join(broadcast(changed_customers), on="customer_id", how="inner")
    
    # Recompute Gold for changed customers
    df_gold_updates = df_silver_subset.groupBy("customer_id") \
        .agg(
            count("txn_id").alias("lifetime_transactions"),
            sum("amount").alias("lifetime_spend"),
            avg("amount").alias("avg_transaction"),
            max("txn_date").alias("last_transaction_date"),
        )
    
    # MERGE to Gold
    target = DeltaTable.forPath(spark, gold_path)
    (target.alias("tgt")
     .merge(df_gold_updates.alias("src"), "tgt.customer_id = src.customer_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    
    # Get new last version
    new_version = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1") \
        .first()["version"]
    
    print(f"✅ Processed {df_latest_state.count():,} changed records | New version: {new_version}")
    return new_version


# Read CDF as a stream (for real-time downstream propagation)
df_cdf_stream = spark.readStream.format("delta") \
    .option("readChangeFeed",    "true") \
    .option("startingVersion",   "latest") \
    .table("silver.transactions")

# Process each change event in near real-time
def process_cdf_batch(df_batch, batch_id):
    inserts = df_batch.filter(col("_change_type") == "insert")
    updates  = df_batch.filter(col("_change_type") == "update_postimage")
    deletes  = df_batch.filter(col("_change_type") == "delete")
    
    print(f"Batch {batch_id}: +{inserts.count()} ~{updates.count()} -{deletes.count()}")
    # Apply incremental logic...

df_cdf_stream.writeStream \
    .foreachBatch(process_cdf_batch) \
    .option("checkpointLocation", "/mnt/checkpoints/cdf-gold/") \
    .trigger(processingTime="5 minutes") \
    .start()
```

---

## 3.2 Liquid Clustering — The Next Evolution of Partitioning

**Problem with traditional partitioning:** You have to decide partitioning strategy at table creation time. Wrong choice = painful migration. High-cardinality columns create millions of tiny partitions.

**Liquid Clustering** is a new approach (Databricks Runtime 13.3+) that: optimizes data layout automatically over time, allows changing cluster columns without rewriting data, and handles high-cardinality columns efficiently.

```python
# Create table with Liquid Clustering
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.transactions
    USING DELTA
    CLUSTER BY (customer_id, txn_date)  -- No need for PARTITIONED BY
    TBLPROPERTIES (
        'delta.feature.clustering' = 'supported'
    )
""")

# Write data (clustering happens during OPTIMIZE, not on write)
df.write.format("delta").mode("append").saveAsTable("silver.transactions")

# Trigger clustering (run this periodically — weekly or after large writes)
spark.sql("OPTIMIZE silver.transactions")

# Change cluster columns any time — no data rewrite needed!
spark.sql("ALTER TABLE silver.transactions CLUSTER BY (txn_date, payment_mode)")
spark.sql("OPTIMIZE silver.transactions")  # Apply new clustering

# Check clustering status
spark.sql("DESCRIBE DETAIL silver.transactions").select(
    "clusteringColumns",
    "numFiles",
    "sizeInBytes"
).show()

# Liquid Clustering vs Partitioning
"""
PARTITIONING                    LIQUID CLUSTERING
═══════════════════════════════════════════════════════════
Good for low-cardinality        Good for any cardinality
cols (< 1000 distinct values)   
Must decide at creation         Can change anytime
Creates many small files        Manages file sizes automatically
  for high-cardinality          
Partition pruning at folder     Data skipping within files
  level                         (file-level statistics)
Partition filter required       Any predicate benefits
  for full pruning              
"""
```

---

## 3.3 Delta Table Constraints

```python
# Add constraints to enforce data quality at write time
# Constraints prevent bad data from entering — fail the write, not silently skip

# NOT NULL constraint
spark.sql("""
    ALTER TABLE silver.transactions
    ADD CONSTRAINT txn_id_not_null CHECK (txn_id IS NOT NULL)
""")

# Check constraint
spark.sql("""
    ALTER TABLE silver.transactions
    ADD CONSTRAINT positive_amount CHECK (amount > 0)
""")

spark.sql("""
    ALTER TABLE silver.transactions
    ADD CONSTRAINT valid_payment_mode CHECK (
        payment_mode IN ('CARD','UPI','NETBANKING','NEFT','RTGS','IMPS','WALLET')
    )
""")

# Now any write that violates a constraint FAILS:
bad_df = spark.createDataFrame(
    [(-100, "C001", "2024-01-15")],
    ["amount", "customer_id", "txn_date"]
)
try:
    bad_df.write.format("delta").mode("append").saveAsTable("silver.transactions")
except Exception as e:
    print(f"Write rejected: {e}")
    # DeltaInvariantViolationException: Constraint 'positive_amount' violated by row...

# Drop constraint
spark.sql("ALTER TABLE silver.transactions DROP CONSTRAINT positive_amount")

# List constraints
spark.sql("DESCRIBE DETAIL silver.transactions") \
    .select("tableConstraints").show(truncate=False)
```

---

## 3.4 Generated Columns — Derived Columns in Schema

```python
# Generated columns are automatically computed from other columns
# Stored physically — queries on them benefit from data skipping
# Critical for partitioning derived date parts

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.transactions (
        txn_id        BIGINT NOT NULL,
        txn_timestamp TIMESTAMP NOT NULL,
        customer_id   STRING NOT NULL,
        amount        DOUBLE NOT NULL,
        payment_mode  STRING,
        
        -- Generated columns (computed automatically)
        txn_date      DATE    GENERATED ALWAYS AS (CAST(txn_timestamp AS DATE)),
        txn_year      INT     GENERATED ALWAYS AS (YEAR(CAST(txn_timestamp AS DATE))),
        txn_month     INT     GENERATED ALWAYS AS (MONTH(CAST(txn_timestamp AS DATE))),
        amount_bucket STRING  GENERATED ALWAYS AS (
            CASE
                WHEN amount < 100    THEN 'LOW'
                WHEN amount < 1000   THEN 'MEDIUM'
                WHEN amount < 10000  THEN 'HIGH'
                ELSE                      'PREMIUM'
            END
        )
    )
    USING DELTA
    PARTITIONED BY (txn_year, txn_month)  -- Partition by generated columns!
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# When you write, only provide the base columns:
df_base = spark.createDataFrame([
    (1, "2024-01-15 10:30:00", "C001", 150.0, "CARD"),
], ["txn_id", "txn_timestamp", "customer_id", "amount", "payment_mode"]) \
    .withColumn("txn_timestamp", col("txn_timestamp").cast("timestamp"))

df_base.write.format("delta").mode("append").saveAsTable("silver.transactions")

# Delta automatically computes and stores txn_date, txn_year, txn_month, amount_bucket
# Now queries like WHERE txn_year=2024 AND txn_month=1 use partition pruning automatically!
```

---

## 3.5 Clone Tables — Zero-Copy Dev Environments

```python
# SHALLOW CLONE: Reference original data files (no copy)
# Fast (seconds), shares storage with source
# Good for: dev/test environments, quick experiments

spark.sql("""
    CREATE TABLE dev_catalog.silver.transactions
    SHALLOW CLONE prod_catalog.silver.transactions
    LOCATION 'abfss://silver@devlake.dfs.core.windows.net/transactions/'
""")
# Creates metadata + transaction log pointing to prod files
# Any writes to clone create NEW files — prod data untouched
# Reads fall through to prod data files if not overwritten in clone

# DEEP CLONE: Physically copies all data files
# Slower (proportional to data size), fully independent
# Good for: disaster recovery, long-term snapshots, cross-region copies

spark.sql("""
    CREATE TABLE backup.silver.transactions
    DEEP CLONE prod_catalog.silver.transactions
    LOCATION 'abfss://backup@secondarylake.dfs.core.windows.net/transactions/'
""")

# Incremental deep clone (sync new changes since last clone)
spark.sql("""
    CREATE OR REPLACE TABLE backup.silver.transactions
    DEEP CLONE prod_catalog.silver.transactions
    LOCATION 'abfss://backup@secondarylake.dfs.core.windows.net/transactions/'
""")
# Only copies files changed since last clone — very efficient for DR sync

# Use case: Developer wants to test a destructive transformation
spark.sql("""
    CREATE TABLE dev_catalog.silver.txns_experiment
    SHALLOW CLONE prod_catalog.silver.transactions
""")
# Run destructive operations on clone — prod untouched
# Delete the clone when done
spark.sql("DROP TABLE dev_catalog.silver.txns_experiment")
```

---

## 3.6 Delta Sharing — Cross-Org Data Sharing

```python
# Delta Sharing: share live Delta tables with external organizations
# They receive read-only access — no need to copy data
# Open protocol — works with Spark, pandas, PowerBI, Tableau

# ── PROVIDER SIDE (you are sharing data) ──
spark.sql("CREATE SHARE banking_data_share")

spark.sql("""
    ALTER SHARE banking_data_share
    ADD TABLE prod_catalog.gold.daily_revenue_summary
""")

spark.sql("""
    ALTER SHARE banking_data_share
    ADD TABLE prod_catalog.gold.customer_segments
    PARTITION FILTER region = 'IN'  -- Only share India data
""")

# Grant to recipient (external organization)
spark.sql("""
    CREATE RECIPIENT external_partner_bank
    USING ID 'recipient-id-from-partner'
""")

spark.sql("GRANT SELECT ON SHARE banking_data_share TO RECIPIENT external_partner_bank")

# ── RECIPIENT SIDE (you are receiving shared data) ──
import delta_sharing

# Activation link provided by the provider
profile_file = "/dbfs/FileStore/delta_share.profile"

# List available shares
client = delta_sharing.SharingClient(profile_file)
shares = client.list_shares()

# Load shared table into Spark DataFrame
df_shared = delta_sharing.load_as_spark(
    f"{profile_file}#banking_data_share.gold.daily_revenue_summary"
)
df_shared.show()
```

---

## 3.7 Complete Delta Table Optimization Script

```python
# ── optimize_all_tables.py ──
# Run weekly to maintain optimal table performance

def optimize_table(table_name: str, z_order_cols: list = None, partition_filter: str = None):
    """
    Full optimization routine for a Delta table:
    1. OPTIMIZE (compact small files)
    2. ZORDER (if specified)
    3. VACUUM (clean old files)
    4. ANALYZE (update statistics)
    """
    print(f"\n🔧 Optimizing: {table_name}")
    
    # OPTIMIZE + ZORDER
    z_order_clause = f"ZORDER BY ({', '.join(z_order_cols)})" if z_order_cols else ""
    where_clause   = f"WHERE {partition_filter}" if partition_filter else ""
    
    spark.sql(f"""
        OPTIMIZE {table_name}
        {where_clause}
        {z_order_clause}
    """)
    print(f"  ✅ OPTIMIZE complete")
    
    # VACUUM (remove files older than 30 days)
    spark.sql(f"VACUUM {table_name} RETAIN 720 HOURS")
    print(f"  ✅ VACUUM complete (retained 30 days)")
    
    # Update statistics for Spark optimizer
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
    print(f"  ✅ Statistics updated")
    
    # Show current state
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    print(f"  Files: {detail['numFiles']:,} | Size: {detail['sizeInBytes']/(1024**3):.2f} GB")

# Run optimization for all production tables
TABLES_TO_OPTIMIZE = [
    ("prod_catalog.silver.transactions",    ["customer_id", "payment_mode"], None),
    ("prod_catalog.silver.customers",       ["customer_id"],                 None),
    ("prod_catalog.gold.daily_revenue",     None,                            "txn_date >= '2024-01-01'"),
]

for table, z_cols, part_filter in TABLES_TO_OPTIMIZE:
    optimize_table(table, z_cols, part_filter)

print("\n✅ All tables optimized")
```

---

## 3.8 Interview Questions — Advanced Delta

**Q: What is Change Data Feed and how does it help downstream pipelines?**
> CDF records every insert, update (pre/post image), and delete in the Delta transaction log. Downstream pipelines can read only rows changed since their last run using `readChangeFeed=true` and a `startingVersion`. This eliminates full-table scans — instead of reading 50M rows to find 1,600 changes, you read exactly 1,600 rows. Especially valuable for Gold layer incremental builds and real-time data propagation to downstream systems.

**Q: What is the difference between Shallow Clone and Deep Clone?**
> Shallow Clone creates a new table that references the source's data files without copying them — creation is instant (seconds). New writes to the clone create separate files; reads fall through to source files. Deep Clone physically copies all data files to a new location — creation time scales with data size. Use Shallow Clone for development environments and experiments; use Deep Clone for disaster recovery, cross-region backups, or when you need a fully independent copy.

**Q: What are Delta table constraints and how do they differ from DQ checks in code?**
> Delta constraints (`ALTER TABLE ... ADD CONSTRAINT ... CHECK (...)`) are enforced at the storage layer — any write violating a constraint is rejected before a single byte is written. This is proactive prevention. DQ checks in code (your custom `DataQualityEngine`) run after reading data — they detect bad records and route them to quarantine. Constraints guarantee the table can never contain invalid data; code-level DQ provides flexibility, reporting, and routing of bad records.

---
*Next: [Module 04 — Optimization: Photon, Serverless & Cost Optimization](../04-Optimization/02-photon-serverless-cost.md)*
