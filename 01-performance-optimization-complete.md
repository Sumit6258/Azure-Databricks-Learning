# Module 04 — Performance Optimization
## Partitioning, Repartition vs Coalesce, Broadcast Joins, Caching, Z-Ordering & Skew Handling

> **Goal:** Turn slow, expensive Spark jobs into production-grade, cost-efficient pipelines. Every technique here comes from real client engagements where jobs were taking hours and needed to finish in minutes.

---

## 4.1 The Spark Execution Model — What Actually Happens

Before tuning, understand what Spark does with your code:

```
Your PySpark code
        ↓
Logical Plan (Catalyst builds a tree of operations)
        ↓
Optimized Logical Plan (Catalyst rewrites for efficiency: predicate pushdown, column pruning)
        ↓
Physical Plan (Selects join algorithms, sort strategies)
        ↓
RDD Execution (Stages → Tasks → Partitions)
```

### Stages and Tasks

```
JOB
├── Stage 1 (no shuffle)
│   ├── Task 1 → processes Partition 1
│   ├── Task 2 → processes Partition 2
│   └── Task N → processes Partition N
│
├── [SHUFFLE BOUNDARY — data moves between workers]
│
├── Stage 2 (post-shuffle)
│   ├── Task 1 → processes shuffled Partition 1
│   └── ...
│
└── Stage 3 (final aggregation)
    └── ...
```

**Key insight:** Each task processes ONE partition. If you have 10 cores and 10 partitions — perfect parallelism. If you have 10 cores and 200 partitions — 20 rounds of processing. If you have 10 cores and 3 partitions — 7 cores are idle.

---

## 4.2 Partitioning — The Foundation

### How Many Partitions Should You Have?

```python
# Check current partition count
df.rdd.getNumPartitions()

# RULE OF THUMB:
# Target partition size: 128MB - 256MB
# Number of partitions = Data size / Target partition size
#
# Example:
# Data: 100GB
# Target partition size: 200MB
# Ideal partitions = 100GB / 200MB = 512 partitions
#
# Also: partitions >= 2 × number of CPU cores in cluster
# 4 workers × 8 cores = 32 cores → use 64-128 partitions minimum

# Default shuffle partitions: 200 (almost always wrong)
spark.conf.get("spark.sql.shuffle.partitions")  # Returns "200"

# Set appropriately for your data size:
# Small data (< 10GB):
spark.conf.set("spark.sql.shuffle.partitions", "50")

# Medium data (10-100GB):
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Large data (100GB - 1TB):
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Very large data (> 1TB):
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# OR: Enable AQE to handle this automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# AQE will automatically merge small partitions after shuffles
```

---

## 4.3 Repartition vs Coalesce — When to Use Each

```python
# ─────────────────────────────────────────────────────────────
# repartition(n) — Full shuffle, perfectly balanced partitions
# ─────────────────────────────────────────────────────────────

# When to use repartition:
# 1. INCREASING partition count (coalesce can't increase)
# 2. Balancing severely skewed data
# 3. Before a join — co-locate data on join key

# Increase from 10 to 200 partitions
df_repart = df.repartition(200)

# Repartition BY a column (all same-key rows go to same partition)
# This is called "hash partitioning" — critical before joins
df_by_key = df.repartition(200, col("customer_id"))
# Now all rows with same customer_id are in the same partition
# Joining on customer_id = no shuffle needed!

# Check partition sizes after repartition
df_repart.rdd.mapPartitions(lambda partition: [sum(1 for _ in partition)]) \
             .collect()  # Should be roughly equal sizes

# ─────────────────────────────────────────────────────────────
# coalesce(n) — No shuffle, merges existing partitions
# ─────────────────────────────────────────────────────────────

# When to use coalesce:
# 1. REDUCING partition count (e.g., before writing output files)
# 2. When you want to avoid a full shuffle
# 3. After filtering — fewer rows means fewer partitions needed

# Bad example (too many small output files):
df.write.format("delta").save("/mnt/silver/transactions/")
# Creates 200 files × 1MB each = 200 small files (bad for readers!)

# Good example (coalesce before write):
df.coalesce(10) \
  .write.format("delta").save("/mnt/silver/transactions/")
# Creates 10 larger files (~20MB each = optimal for Delta readers)

# ─────────────────────────────────────────────────────────────
# Decision tree
# ─────────────────────────────────────────────────────────────
#
# Need more partitions?       → repartition()
# Need fewer partitions?      → coalesce() (unless data is skewed)
# Data is skewed, need fewer? → repartition() then write
# Before writing to Delta?    → coalesce(target_file_count)
# Before a join?              → repartition(n, join_key_col)
```

### The Small File Problem (And How to Fix It)

```python
# PROBLEM: Many small files kill query performance
# Symptoms:
# - Queries take longer than expected
# - Spark UI shows thousands of tasks
# - File sizes < 10MB each

# Diagnose:
files = dbutils.fs.ls("/mnt/silver/transactions/date=2024-01-15/")
file_sizes = [f.size / (1024*1024) for f in files]
print(f"Files: {len(files)}, Avg size: {sum(file_sizes)/len(file_sizes):.1f} MB")

# Fix 1: OPTIMIZE (Delta Lake — compacts small files)
spark.sql("OPTIMIZE silver.transactions WHERE txn_date = '2024-01-15'")

# Fix 2: Auto-optimize at table level
spark.sql("""
    ALTER TABLE silver.transactions
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# Fix 3: Coalesce before write (prevent the problem)
df.coalesce(
    max(1, df.count() // 500_000)  # ~500K rows per partition/file
).write.format("delta").mode("append").save("/mnt/silver/transactions/")

# Fix 4: Target file size (Databricks)
spark.conf.set("spark.databricks.delta.optimizeWrite.targetFileSize", "134217728")  # 128MB
```

---

## 4.4 Broadcast Joins — Eliminate Shuffles

```python
from pyspark.sql.functions import broadcast, col

# ─────────────────────────────────────────────────────────────
# SCENARIO: Join 500GB transaction table with 2MB country codes
# ─────────────────────────────────────────────────────────────

transactions = spark.read.format("delta").load("/mnt/silver/transactions/")   # 500GB
country_codes = spark.read.format("delta").load("/mnt/reference/countries/")  # 2MB

# BAD: Default Sort-Merge Join
# Spark sorts both tables by join key → massive shuffle → slow
result_slow = transactions.join(country_codes, on="country_code", how="left")
# Execution: 45 minutes

# GOOD: Broadcast Join
# Spark sends country_codes to ALL workers → no shuffle needed
result_fast = transactions.join(broadcast(country_codes), on="country_code", how="left")
# Execution: 4 minutes

# ─────────────────────────────────────────────────────────────
# Tune the broadcast threshold
# ─────────────────────────────────────────────────────────────

# Default: 10MB — Spark auto-broadcasts tables under this size
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # 10485760 bytes

# Increase to 100MB (for larger dimension tables)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)

# Disable auto-broadcast (when you suspect wrong auto-decisions)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# ─────────────────────────────────────────────────────────────
# Read the query plan to verify join type
# ─────────────────────────────────────────────────────────────
result_fast.explain()
# Look for: BroadcastHashJoin  ← Good, broadcast used
# vs:        SortMergeJoin     ← Expensive, full shuffle

# Or in Databricks UI: 
# Click "See Physical Plan" on any DataFrame display
# Under Details → find the join strategy

# ─────────────────────────────────────────────────────────────
# Broadcast hints (when Spark doesn't auto-detect)
# ─────────────────────────────────────────────────────────────
from pyspark.sql.functions import broadcast

# Explicit broadcast hint
result = large_df.join(
    broadcast(small_df),
    on="join_key",
    how="inner"
)

# SQL hint syntax
spark.sql("""
    SELECT /*+ BROADCAST(c) */ t.*, c.country_name
    FROM silver.transactions t
    JOIN reference.countries c ON t.country_code = c.country_code
""")

# ─────────────────────────────────────────────────────────────
# Bucket Join (for repeatedly joining large tables)
# ─────────────────────────────────────────────────────────────

# Pre-bucket both tables on the join key
# After buckets are written, future joins on that key = no shuffle

transactions.write \
    .format("parquet") \
    .bucketBy(200, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("silver.transactions_bucketed")

customers.write \
    .format("parquet") \
    .bucketBy(200, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("silver.customers_bucketed")

# Now joins are shuffle-free:
result = spark.table("silver.transactions_bucketed") \
              .join(spark.table("silver.customers_bucketed"), on="customer_id")
result.explain()
# Shows: SortMergeJoin WITHOUT Exchange (no shuffle!)
```

---

## 4.5 Caching and Persistence

```python
from pyspark import StorageLevel

# ─────────────────────────────────────────────────────────────
# WHEN TO CACHE:
# - DataFrame used multiple times in the same notebook/job
# - DataFrame is expensive to compute (many transformations, joins)
# - DataFrame fits in memory
#
# WHEN NOT TO CACHE:
# - DataFrame used only once → caching wastes time
# - DataFrame is too large for memory → spills to disk, slower
# - Streaming DataFrames
# ─────────────────────────────────────────────────────────────

# Cache in memory (default)
df_expensive = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .join(customers, "customer_id") \
    .join(products, "product_id") \
    .filter(col("txn_date") >= "2024-01-01")

df_expensive.cache()  # OR df_expensive.persist()

# Force materialization (cache is lazy!)
df_expensive.count()  # Triggers actual caching

# Now subsequent operations reuse cached data
summary_by_customer = df_expensive.groupBy("customer_id").agg(sum("amount"))
summary_by_product  = df_expensive.groupBy("product_id").agg(count("txn_id"))
# Both use cached df_expensive — no re-computation!

# Release cache when done (important — frees cluster memory)
df_expensive.unpersist()

# ─────────────────────────────────────────────────────────────
# Storage Levels — Choose Based on Cluster Resources
# ─────────────────────────────────────────────────────────────

# MEMORY_ONLY (default .cache())
# Stores deserialized in RAM. Fast access, uses most memory.
df.persist(StorageLevel.MEMORY_ONLY)

# MEMORY_AND_DISK
# Memory first, spill to disk if needed. Good default for large DataFrames.
df.persist(StorageLevel.MEMORY_AND_DISK)

# DISK_ONLY
# Write to disk. Slow but doesn't use memory. For very large DataFrames.
df.persist(StorageLevel.DISK_ONLY)

# MEMORY_AND_DISK_2 (replicated)
# Replicated across 2 nodes. Fault-tolerant caching.
df.persist(StorageLevel.MEMORY_AND_DISK_2)

# OFF_HEAP (using Tungsten's off-heap memory)
# Avoids JVM GC pressure. Best for Databricks with Photon.
df.persist(StorageLevel.OFF_HEAP)

# ─────────────────────────────────────────────────────────────
# Practical caching pattern for ETL notebooks
# ─────────────────────────────────────────────────────────────

class CacheManager:
    """Context manager for safe DataFrame caching."""
    
    def __init__(self, df, storage_level=StorageLevel.MEMORY_AND_DISK):
        self.df = df
        self.storage_level = storage_level
    
    def __enter__(self):
        self.df.persist(self.storage_level)
        self.df.count()  # Materialize
        return self.df
    
    def __exit__(self, *args):
        self.df.unpersist()
        print("✅ Cache released")

# Usage:
with CacheManager(df_expensive) as df_cached:
    result1 = df_cached.groupBy("customer_id").agg(sum("amount")).collect()
    result2 = df_cached.groupBy("payment_mode").agg(count("txn_id")).collect()
    result3 = df_cached.filter(col("amount") > 1000).count()
# df automatically unpersisted after this block
```

---

## 4.6 Handling Data Skew — The Most Difficult Problem

Data skew is when one partition has dramatically more data than others, causing one task to run 10x longer than all others — a single "straggler" holds up the entire stage.

```
Symptom in Spark UI:
Stage 3 → Tasks: 200 tasks
  Tasks 1-199: completed in ~30 seconds ✅
  Task 200:    running for 45 MINUTES ❌ (skewed partition)

Cause:
  Data has 1M rows for customer "C001" and 100 rows for others
  After groupBy("customer_id"), Task 200 gets ALL of C001's data
```

```python
from pyspark.sql.functions import col, count, lit, concat, spark_partition_id, rand, floor

# ─────────────────────────────────────────────────────────────
# DIAGNOSE SKEW
# ─────────────────────────────────────────────────────────────

# Check partition sizes
df.groupBy(spark_partition_id()).count().orderBy(col("count").desc()).show(20)
# If one partition has 10x more rows than others → skew!

# Find the skewed key
df.groupBy("customer_id").count() \
  .orderBy(col("count").desc()) \
  .show(10)
# customer_id=C001: 1,500,000 rows ← HOT KEY!
# customer_id=C002:     1,200 rows
# ...

# ─────────────────────────────────────────────────────────────
# FIX 1: SALTING (most common approach)
# ─────────────────────────────────────────────────────────────

SALT_FACTOR = 10  # Split each hot partition into 10

# Add a random salt to the hot key
df_salted = df.withColumn(
    "salted_customer_id",
    when(
        col("customer_id") == "C001",  # Hot key
        concat(col("customer_id"), lit("_"), (rand() * SALT_FACTOR).cast("int").cast("string"))
    ).otherwise(col("customer_id"))
)

# Join with small table — also needs to be exploded by salt
from pyspark.sql.functions import array, explode, sequence

df_customers_salted = customers \
    .filter(col("customer_id") == "C001") \
    .crossJoin(
        spark.range(SALT_FACTOR)
             .withColumnRenamed("id", "salt")
             .withColumn("salted_customer_id", 
                         concat(lit("C001_"), col("salt").cast("string")))
    ) \
    .union(
        customers.filter(col("customer_id") != "C001")
                 .withColumn("salted_customer_id", col("customer_id"))
    )

# Now join on salted key — C001's data spread across 10 partitions
result = df_salted.join(broadcast(df_customers_salted), on="salted_customer_id") \
                  .drop("salted_customer_id")

# ─────────────────────────────────────────────────────────────
# FIX 2: Adaptive Query Execution (AQE) — Automatic skew handling
# ─────────────────────────────────────────────────────────────

# AQE in Databricks automatically detects and splits skewed partitions
spark.conf.set("spark.sql.adaptive.enabled",                     "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",            "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# A partition is "skewed" if it's 5x larger than the median partition

# With AQE on → Spark automatically splits skewed partitions during joins!
result = df.join(customers, on="customer_id")  # AQE handles skew automatically

# ─────────────────────────────────────────────────────────────
# FIX 3: Separate processing for hot keys
# ─────────────────────────────────────────────────────────────

HOT_KEYS = ["C001", "AMAZON", "FLIPKART"]  # Known hot keys

# Process hot keys separately
df_hot    = df.filter(col("customer_id").isin(HOT_KEYS))
df_normal = df.filter(~col("customer_id").isin(HOT_KEYS))

# Hot keys: use repartition to spread data
result_hot = df_hot.repartition(200, col("customer_id")) \
                   .join(customers, "customer_id")

# Normal keys: standard join
result_normal = df_normal.join(broadcast(customers_small), "customer_id")

# Combine results
result_final = result_hot.union(result_normal)

# ─────────────────────────────────────────────────────────────
# FIX 4: Broadcast the skewed table (if feasible)
# ─────────────────────────────────────────────────────────────

# If the table causing skew can fit in memory, broadcast it
# This eliminates the skew entirely
result = df.join(broadcast(skewed_small_table), on="key")
```

---

## 4.7 Z-Ordering — Deep Dive

Z-Ordering is Delta Lake's technique to co-locate related data within files, so queries that filter on Z-ordered columns touch fewer files.

```python
# ─────────────────────────────────────────────────────────────
# Z-ORDER vs PARTITION — When to use which
# ─────────────────────────────────────────────────────────────

# PARTITIONING: Physical folder separation by column value
# Use for: Low-cardinality columns, most queries filter by this column
# Examples: date, region, status (5-100 distinct values)

# Z-ORDERING: Co-locate data within files using a space-filling curve
# Use for: High-cardinality columns, queries filter but partitioning is impractical
# Examples: customer_id, product_id, user_id (millions of distinct values)

# ─────────────────────────────────────────────────────────────
# OPTIMIZE with Z-ORDER
# ─────────────────────────────────────────────────────────────

# Basic optimization (compact small files)
spark.sql("OPTIMIZE silver.transactions")

# Z-ORDER by most commonly filtered column
spark.sql("""
    OPTIMIZE silver.transactions
    ZORDER BY (customer_id)
""")

# Multi-column Z-ORDER (first column gets more benefit)
# Order by descending importance of the filter columns
spark.sql("""
    OPTIMIZE silver.transactions
    ZORDER BY (customer_id, payment_mode)
""")

# Z-ORDER on a specific partition only (faster — doesn't touch whole table)
spark.sql("""
    OPTIMIZE silver.transactions
    WHERE txn_date = '2024-01-15'
    ZORDER BY (customer_id)
""")

# ─────────────────────────────────────────────────────────────
# Verify Z-ORDER effectiveness
# ─────────────────────────────────────────────────────────────

# Check table stats
spark.sql("DESCRIBE DETAIL silver.transactions").show()
# Look for: numFiles, sizeInBytes, minReaderVersion

# Check data skipping effectiveness in query plan
df = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .filter(col("customer_id") == "C001")

df.explain()
# Look for: numFilesAffectedByScan vs numFiles
# After Z-ordering: numFilesAffectedByScan should be much smaller

# Operational metric to track:
# "files read" in Spark UI → should decrease after Z-ordering
```

---

## 4.8 Predicate Pushdown and Column Pruning

These happen automatically with the Catalyst optimizer — but understanding them helps you write better code.

```python
# ─────────────────────────────────────────────────────────────
# COLUMN PRUNING: Only read columns you need
# ─────────────────────────────────────────────────────────────

# BAD: Read everything then select
df = spark.read.format("delta").load("/mnt/silver/transactions/")
df = df.select("customer_id", "amount")
# Spark still reads ALL columns from disk!

# GOOD: Catalyst pushes projection down to the scan
# These are equivalent in execution, Catalyst handles it:
df = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .select("customer_id", "amount")
# Spark reads ONLY customer_id and amount columns from Parquet files

# Verify column pruning in explain():
df.explain()
# Look for: ReadSchema in the scan → should show only selected columns

# ─────────────────────────────────────────────────────────────
# PREDICATE PUSHDOWN: Filter as early as possible
# ─────────────────────────────────────────────────────────────

# BAD: Filter after join (process all data, then filter)
result = transactions.join(customers, "customer_id") \
    .filter(col("city") == "Mumbai")

# GOOD: Filter before join (join smaller data)
result = transactions \
    .join(customers.filter(col("city") == "Mumbai"), "customer_id")

# For JDBC sources — filter is pushed to the database:
# Spark automatically translates .filter() to SQL WHERE clause
df_jdbc = spark.read.format("jdbc") \
    .option("dbtable", "transactions") \
    .load() \
    .filter(col("txn_date") >= "2024-01-01")
# Database executes: SELECT * FROM transactions WHERE txn_date >= '2024-01-01'
# Only matching rows transferred to Spark!

# For Delta — filter on partition columns is pushed to file scan:
df_delta = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .filter(col("txn_date") == "2024-01-15")  # txn_date is partition column
# Spark only opens the /date=2024-01-15/ partition folder
```

---

## 4.9 Adaptive Query Execution (AQE) — Production Configuration

AQE is Spark 3.x's automatic query optimizer. Enable it and let it handle many optimization decisions at runtime.

```python
# Enable all AQE features
spark.conf.set("spark.sql.adaptive.enabled",                             "true")

# Automatically coalesce small post-shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",          "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")

# Convert Sort-Merge Join to Broadcast Join when small table detected at runtime
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled",          "true")

# Handle skew automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",      "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Dynamically switch join strategies at runtime
spark.conf.set("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true")
```

---

## 4.10 Performance Optimization Checklist (For Every Production Job)

```
PRE-DEVELOPMENT:
□ Defined explicit schema (no inferSchema)?
□ Partitioning strategy chosen for write (date/region, low-cardinality)?
□ Z-ordering planned for high-cardinality filter columns?
□ AQE enabled in cluster config?

DATA READING:
□ Reading only needed columns (avoid SELECT *)?
□ Pushing filters to data source (partition pruning)?
□ Using Delta instead of CSV/JSON where possible?

TRANSFORMATIONS:
□ Using built-in functions instead of Python UDFs?
□ Filtering early (before joins)?
□ Joining on partition/Z-ordered columns?

JOINS:
□ Broadcasting small tables (< 50-100MB)?
□ Verified join type in explain() plan?
□ Handling skewed keys (salting / AQE)?
□ Pre-bucketing frequently joined large tables?

WRITES:
□ Using coalesce() before writing to prevent small files?
□ OPTIMIZE + ZORDER scheduled after large writes?
□ autoOptimize enabled on Delta tables?
□ VACUUM scheduled (retain 30+ days for production)?

MONITORING:
□ Checked Spark UI for stragglers?
□ Verified shuffle read size is reasonable?
□ Checked for GC pauses in executor logs?
□ Partition sizes between 128MB-256MB?
```

---

## 4.11 Interview Questions — Performance

**Q1: How do you diagnose a slow Spark job?**
> Go to Spark UI → Jobs → find the slow Stage → click it → look for: (1) Straggler tasks (one task much longer than others = skew), (2) Large shuffle read/write (unnecessary shuffles), (3) Garbage collection time > 10% (memory pressure, cache misses), (4) Too many or too few tasks (partition count wrong). Then check the query plan with `.explain(True)` to see if expected optimizations (broadcast joins, column pruning) are applied.

**Q2: What is the difference between partition pruning and predicate pushdown?**
> Partition pruning is specific to partitioned tables — Spark skips entire directories (partitions) that don't match a filter on the partition column. Predicate pushdown is broader — filters are pushed down to the data source layer (Delta file stats, JDBC WHERE clause, Parquet row group statistics) so data is filtered before being sent to Spark.

**Q3: When would AQE NOT help with skew?**
> AQE handles skew in shuffle-based operations (joins, aggregations). It won't help if skew exists in the source data read (before any shuffle), or if the job uses operations that bypass the Catalyst optimizer (like Python UDFs processing everything row by row). For source-level skew, manual salting or separate hot-key processing is required.

**Q4: If a job is using 200 shuffle partitions on 1TB of data, what's wrong and how do you fix it?**
> 200 partitions on 1TB = 5GB per partition — way too large. Each task would process 5GB, causing memory pressure and spills to disk. Fix: Set `spark.sql.shuffle.partitions` to `800-2000`, or enable AQE with `coalescePartitions.enabled=true` to let Spark decide dynamically. Ideal partition size is 128-256MB.

---

*Next: [Module 05 — Azure Integration (ADF, Key Vault, ADLS)](../05-ADF-Integration/01-adf-databricks-complete.md)*
