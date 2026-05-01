# Module 04 — Optimization
## Part 2: Photon Engine, Serverless Compute & Cost Optimization

---

## 2.1 Photon — Databricks Vectorized Execution Engine

Photon is Databricks' proprietary C++ vectorized query engine that replaces the JVM-based Spark executor. It's enabled on Premium workspaces.

```
TRADITIONAL SPARK (JVM):
  Process data row by row
  JVM overhead + GC pressure
  Scalar processing (one value at a time)

PHOTON:
  Processes data in columnar batches (like a CPU SIMD)
  Native C++ — no JVM, no GC
  Vectorized: processes 1024+ values per CPU instruction
  Result: 2-8x faster for SQL and DataFrame operations
```

### What Photon Accelerates

```
✅ FULLY ACCELERATED:
  - SQL queries (SELECT, JOIN, GROUP BY, ORDER BY)
  - Delta Lake reads and writes
  - Parquet / ORC reads
  - Most DataFrame operations (filter, select, withColumn, agg)
  - Window functions
  - Sort merge joins, hash joins
  - String functions (most built-ins)

⚠️ PARTIALLY ACCELERATED:
  - Python UDFs (not accelerated — Photon calls back to Python)
  - Pandas UDFs (partially accelerated)
  - Complex nested type operations

❌ NOT ACCELERATED (falls back to Spark):
  - RDD operations
  - Streaming (some operations)
  - ML operations (use ML Runtime)
```

### Enabling and Verifying Photon

```python
# Enable at cluster level (UI):
# Compute → Edit Cluster → Advanced Options → Enable Photon Acceleration

# Verify Photon is being used in a query:
df.explain()
# Look for: PhotonResultStage, PhotonSort, PhotonGroupingAgg
# vs: WholeStageCodegen (JVM fallback)

# Check if Photon is active
spark.conf.get("spark.databricks.photon.enabled")
# Returns: "true"

# Force Photon for specific operations
spark.conf.set("spark.databricks.photon.enabled", "true")

# Check Photon acceleration in Spark UI:
# Jobs → Stage → "Photon" column shows bytes processed by Photon
```

### Photon Benchmark — Real Numbers

```
Operation                    JVM Spark    Photon    Speedup
─────────────────────────────────────────────────────────────
Delta table scan (10GB)      45 sec       8 sec      5.6x
GROUP BY aggregation          2 min       18 sec      6.7x
Sort merge join (50GB×5GB)   18 min      3.5 min     5.1x
String regex operations       8 min      1.5 min     5.3x
Window functions              12 min      2 min       6x
ORDER BY (100M rows)          5 min       55 sec      5.5x

Note: actual speedup varies by data type, cardinality, and query shape
```

---

## 2.2 Serverless Compute — Zero Cluster Management

**Serverless clusters** start in seconds (vs 3-5 minutes for regular clusters), are fully managed by Databricks (no VM provisioning), and bill per DBU consumed (not per VM-hour).

```
CLASSIC CLUSTERS:             SERVERLESS CLUSTERS:
  VM provisioning: 3-5 min     Start time: ~5 seconds
  Per-VM billing               Per-DBU billing
  You choose VM type           Databricks manages hardware
  Idle cost: full VM cost      Idle cost: ~zero
  Autoscale: 1-5 minutes       Autoscale: seconds
  Fixed runtime version        Always latest runtime
```

### When to Use Serverless

```python
# SERVERLESS SQL WAREHOUSE (for BI/SQL queries):
# Best for: Power BI, Tableau, ad-hoc SQL, BI dashboards
# Benefits: Instant start, per-query billing, auto-suspend

# Create in UI:
# SQL Warehouses → Create → Type: Serverless
# Auto-stop: 5 minutes (aggressive — saves cost)

# SERVERLESS JOB COMPUTE (for ETL jobs):
# Best for: Short-to-medium jobs, burst workloads, variable schedules
# Benefits: No startup wait, pay only for compute time

# In job definition JSON:
{
  "tasks": [{
    "task_key": "etl_task",
    "notebook_task": {"notebook_path": "..."},
    "environment_key": "serverless"  // Use serverless compute
  }],
  "environments": [{
    "environment_key": "serverless",
    "spec": {
      "client": "1",
      "dependencies": ["delta-spark==2.4.0"]
    }
  }]
}
```

### Serverless vs Classic — Cost Comparison

```
Scenario: ETL job, 2 hours/day, Standard_DS4_v2 × 4 workers

CLASSIC JOB CLUSTER:
  VM cost:     $0.60/hr × 4 VMs = $2.40/hr
  DBU cost:    4 DBU/hr × $0.40/DBU = $1.60/hr
  Total:       $4.00/hr × 2hr = $8.00/day
  Monthly:     $240

SERVERLESS:
  DBU cost:    ~6 Serverless DBU/hr × $0.70/DBU = $4.20/hr
  (No VM cost — included in DBU)
  Total:       $4.20/hr × 2hr = $8.40/day
  Monthly:     $252

Serverless wins when:
  ✅ Jobs run < 1 hour (cluster startup cost amortized less)
  ✅ Variable schedule (don't pay for idle)
  ✅ Dev/test runs (start immediately = faster iteration)
  ✅ Many short jobs throughout the day

Classic wins when:
  ✅ Jobs run > 4 hours continuously
  ✅ Fixed schedule with predictable data volumes
  ✅ Need specific VM types (GPU, high memory)
  ✅ Custom init scripts required
```

---

## 2.3 Cost Optimization — Complete Playbook

### Cluster Cost Optimizations

```python
# ── 1. SPOT INSTANCES (Azure Spot / Low Priority VMs) ──
# 60-80% cheaper than on-demand. Risk: can be evicted.
# Use for: batch ETL jobs (Spark auto-retries tasks on eviction)

# Cluster config for spot instances:
cluster_config = {
  "azure_attributes": {
    "first_on_demand": 1,               # 1 on-demand driver node
    "availability": "SPOT_WITH_FALLBACK_AZURE",  # Spot, fallback to on-demand
    "spot_bid_max_price": -1            # -1 = pay current spot price
  }
}

# ── 2. AUTOSCALING ──
# Scale down when idle, scale up for peak
# Configure tight bounds for predictable workloads

# WRONG: Too wide — wastes money at low utilization
"autoscale": {"min_workers": 1, "max_workers": 32}

# RIGHT: Narrow range based on workload analysis
"autoscale": {"min_workers": 2, "max_workers": 8}

# ── 3. AUTO TERMINATION ──
# Dev clusters: 30-60 minutes
# Always-on clusters: DON'T USE — use job clusters instead

# ── 4. JOB CLUSTERS vs ALL-PURPOSE ──
# All-purpose clusters bill 24/7 even when idle
# Job clusters bill only for the job duration

# Cost difference example (2-hour daily job):
# All-purpose:  24 hrs × $4/hr = $96/day
# Job cluster:   2 hrs × $4/hr =  $8/day  ← 92% savings!

# ── 5. CLUSTER POLICIES ──
# Prevent engineers from creating oversized clusters
policy = {
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
  },
  "autotermination_minutes": {
    "type": "range", "minValue": 30, "maxValue": 120
  }
}
```

### Storage Cost Optimizations

```python
# ── 1. DELTA VACUUM ──
# Old data files accumulate — vacuum removes them
# Run weekly for active tables

# Check table size before/after vacuum
before = spark.sql("DESCRIBE DETAIL silver.transactions").first()["sizeInBytes"]

spark.sql("VACUUM silver.transactions RETAIN 720 HOURS")  # Keep 30 days

after = spark.sql("DESCRIBE DETAIL silver.transactions").first()["sizeInBytes"]
saved_gb = (before - after) / (1024**3)
print(f"Freed: {saved_gb:.2f} GB")

# ── 2. COMPRESSION ──
# Default Parquet compression: snappy (fast, ~60% compression)
# Switch to ZSTD for better ratio at similar read speed
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# ── 3. SMALL FILE COMPACTION ──
# Many small files = high storage metadata cost + slow reads
spark.sql("OPTIMIZE silver.transactions")

# ── 4. COLD STORAGE FOR OLD PARTITIONS ──
# Archive partitions older than 2 years to cheaper storage tier
spark.sql("""
    CREATE TABLE archive.transactions
    DEEP CLONE silver.transactions
    WHERE txn_date < '2022-01-01'
    LOCATION 'abfss://archive@coldlake.dfs.core.windows.net/transactions/'
""")

# Delete from hot storage after archiving
spark.sql("""
    DELETE FROM silver.transactions
    WHERE txn_date < '2022-01-01'
""")

# ── 5. CHOOSE RIGHT STORAGE TIER ──
# ADLS Gen2 Hot tier: active data (daily ETL, recent data)
# ADLS Gen2 Cool tier: infrequently accessed (> 30 days old)
# ADLS Gen2 Archive tier: compliance/long-term (> 1 year)
# Cost: Hot = $0.023/GB, Cool = $0.01/GB, Archive = $0.00099/GB
```

### Job-Level Cost Optimizations

```python
# ── 1. BROADCAST JOINS — eliminate expensive shuffles ──
# Each shuffle writes data to disk and transfers over network
# Broadcast eliminates this for small tables
result = large_df.join(broadcast(small_df), "key")

# ── 2. TUNE SHUFFLE PARTITIONS ──
# Default 200 — wrong for most workloads
# Too many partitions: task scheduling overhead dominates
# Too few partitions: tasks run out of memory

# Formula: target 128-256MB per partition
data_size_gb = 50
target_partition_mb = 200
optimal_partitions = int(data_size_gb * 1024 / target_partition_mb)
spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))

# ── 3. PREDICATE PUSHDOWN — filter at source ──
# Push filters to the database/storage — don't bring unnecessary data to Spark
df = spark.read.format("jdbc") \
    .option("query", "SELECT * FROM txns WHERE txn_date >= '2024-01-01'") \
    .load()  # Only 2024 data transferred, not entire table

# ── 4. CACHING — avoid recomputing expensive DFs ──
# Cache DataFrames used in multiple operations
df_expensive.cache()
df_expensive.count()  # Materialize

result1 = df_expensive.groupBy("a").agg(sum("b"))
result2 = df_expensive.groupBy("c").agg(count("d"))  # Reuses cache

df_expensive.unpersist()  # Release when done

# ── 5. COLUMNAR READS — avoid SELECT * ──
# Parquet/Delta stores columns separately — only read what you need
# BAD:
df = spark.read.format("delta").load(path)  # Reads ALL columns

# GOOD:
df = spark.read.format("delta").load(path).select("customer_id", "amount", "txn_date")
# Only 3 columns read from disk — 10-20x less I/O for wide tables
```

### Cost Monitoring Dashboard

```sql
-- Query cluster usage (Databricks system tables — Premium feature)
SELECT
    cluster_name,
    DATE(usage_start_time)  AS usage_date,
    SUM(usage_quantity)     AS total_dbus,
    SUM(usage_quantity * 0.40) AS estimated_cost_usd  -- $0.40/DBU approximate
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC;

-- Most expensive jobs last 7 days
SELECT
    run_name,
    DATE(start_time)                         AS run_date,
    SUM(usage_quantity)                      AS dbus_consumed,
    SUM(usage_quantity * 0.40)               AS estimated_cost_usd,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) AS avg_runtime_min
FROM system.billing.usage
JOIN system.lakeflow.job_run_timeline USING (run_id)
WHERE start_time >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY 1, 2
ORDER BY estimated_cost_usd DESC
LIMIT 20;
```

---

## 2.4 Instance Pool — Pre-Warm Clusters

```python
# Instance Pools keep VM instances ready — eliminates cold start wait

# Create an instance pool (UI: Compute → Pools → Create Pool)
pool_config = {
    "instance_pool_name": "etl-warm-pool",
    "node_type_id": "Standard_DS4_v2",
    "min_idle_instances": 4,      # Always keep 4 warm VMs
    "max_capacity": 20,           # Never exceed 20 VMs
    "idle_instance_autotermination_minutes": 60,
    "azure_attributes": {
        "availability": "SPOT_WITH_FALLBACK_AZURE"
    },
    "preloaded_spark_versions": ["13.3.x-scala2.12"]  # Pre-install runtime
}

# Use pool in cluster config
cluster_with_pool = {
    "instance_pool_id": "pool-id-here",  # From pool creation
    "spark_version": "13.3.x-scala2.12",
    "num_workers": 4,
    "autotermination_minutes": 60
}

# Cold start: Cluster from pool ready in ~30 seconds vs 3-5 minutes
# Cost: Idle VMs in pool still incur VM cost (but no DBU cost)
# Best for: Teams running many short jobs throughout the day
```

---

## 2.5 Interview Questions — Cost & Performance

**Q: How would you reduce the cost of a Databricks deployment that's running over budget?**
> Systematic approach: (1) **Switch all scheduled pipelines to Job Clusters** — biggest single saving (80-90% cost reduction vs 24/7 all-purpose clusters). (2) **Enable Spot instances** with on-demand fallback for batch jobs — 60-80% cheaper. (3) **Add cluster policies** to cap VM size and enforce auto-termination. (4) **Schedule jobs during off-peak hours** for better Spot instance availability. (5) **Run VACUUM** on all Delta tables to reclaim storage. (6) **Tune shuffle partitions** to avoid over-provisioned clusters. (7) **Use Serverless SQL Warehouses** for BI tools — pay only when running queries. (8) Use system table billing queries to identify the top 5 most expensive jobs and optimize them specifically.

**Q: What is Photon and does it replace PySpark?**
> Photon is a vectorized C++ query engine that replaces the JVM execution layer for supported operations. It does NOT replace PySpark — you still write PySpark/SQL code exactly the same way. Photon transparently accelerates supported operations (SQL aggregations, joins, Delta reads) by 2-8x without any code changes. Operations Photon can't accelerate (Python UDFs, RDDs) fall back to standard Spark JVM execution. To maximize Photon benefit: use built-in functions instead of Python UDFs, use SQL/DataFrame API instead of RDDs.

---
*Next: [Module 05 — ADF: Parameterized Pipelines & Triggers](../05-ADF-Integration/02-adf-advanced-patterns.md)*
