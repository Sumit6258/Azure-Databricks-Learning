# Module 10 — Monitoring, Spark UI & Debugging
## Spark UI Analysis, Job Logs, Cluster Logs & Error Troubleshooting

> Debugging Spark jobs is a skill that separates junior engineers from senior ones. This module teaches you how to read the Spark UI like an expert and resolve the most common production issues.

---

## 10.1 The Spark UI — Complete Navigation Guide

```
How to access Spark UI in Databricks:
  Running Cluster → "Spark UI" tab
  Or during a running job: click "View" on the progress bar

Spark UI Tabs:
┌─────────┬──────────────────────────────────────────────────────────────┐
│  Tab    │  What to Look For                                            │
├─────────┼──────────────────────────────────────────────────────────────┤
│ Jobs    │ List of all Spark jobs, duration, stage count, task count    │
│ Stages  │ Individual stages, task distribution, shuffle metrics        │
│ Tasks   │ Per-task duration, data processed, GC time                  │
│ Storage │ Cached DataFrames, size in memory, fraction cached           │
│ Environ │ Spark config, Java version, all set properties              │
│ Executors│ Per-worker metrics, memory used, GC pressure               │
│ SQL     │ Query plans, physical execution, scan details                │
│ Streaming│ (For streaming jobs) Input rate, processing rate, lag      │
└─────────┴──────────────────────────────────────────────────────────────┘
```

---

## 10.2 Reading the Stages Tab — Key Metrics

```
Stage 3: groupBy + agg
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Duration:    45 min 12 sec    ← Total stage time
Input:       150.2 GB         ← Data read from disk/memory
Output:      2.8 GB           ← Data written after aggregation
Shuffle Read: 48.5 GB         ← Data received after shuffle
Shuffle Write: 48.5 GB        ← Data sent for shuffle

Tasks Summary:
  Min: 0.2s  25th: 1.3s  Median: 1.5s  75th: 1.8s  Max: 42m 8s  ← PROBLEM!

Task Metrics:
  GC Time: 15m 32s  ← Very high GC → memory pressure

Interpretation:
  Median task: 1.5 seconds
  Max task: 42 MINUTES → massive skew!
  GC time: 15 min → memory is too full, JVM garbage collecting constantly
```

### What Each Metric Tells You

```
DURATION: Too long?
  → Look at shuffle metrics and task distribution

INPUT SIZE: Much larger than expected?
  → Column pruning not working? Reading too many partitions?

SHUFFLE READ/WRITE: Very large?
  → Wide transformation (groupBy, join) processing too much data
  → Consider broadcast join or pre-aggregation

MEDIAN vs MAX TASK TIME: Large gap?
  → Data skew! One partition has much more data than others

GC TIME > 10% of task time?
  → Memory pressure: too many cached DataFrames, or working set too large
  → Increase executor memory or reduce parallelism

TASK FAILURES (red bars)?
  → OOM errors, disk space issues, network timeouts
  → Click on failed task → see error message
```

---

## 10.3 Reading the SQL Tab — Query Plans

```python
# Get the query plan for any DataFrame
df.explain()          # Simple plan (human-readable)
df.explain("simple")  # Same as above
df.explain("extended") # Logical + physical plan
df.explain("cost")    # With cost estimates
df.explain("codegen")  # Generated Java code (advanced)
df.explain(True)      # All plans — most useful for debugging

# Sample output and what to look for:
"""
== Physical Plan ==
*(5) HashAggregate(keys=[customer_id#10], functions=[sum(amount#20)])
+- Exchange hashpartitioning(customer_id#10, 200)   ← SHUFFLE! 200 partitions
   +- *(4) HashAggregate(keys=[customer_id#10], functions=[partial_sum(amount#20)])
      +- *(4) Project [customer_id#10, amount#20]   ← Column pruning ✅
         +- *(4) Filter (isnotnull(amount#20) AND (amount#20 > 0))  ← Filter pushed down ✅
            +- *(4) ColumnarToRow
               +- FileScan parquet [customer_id#10,txn_date#11,amount#20]  ← 3 cols, not all ✅
                  PushedFilters: [IsNotNull(amount), GreaterThan(amount,0.0)]  ← Filter pushed ✅
                  PartitionFilters: [isnotnull(txn_date#11), (txn_date#11 = 2024-01-15)]  ← Partition pruning ✅
"""

# GOOD signs in a query plan:
# ✅ FileScan shows only columns you selected (column pruning)
# ✅ PushedFilters shows your .filter() conditions  
# ✅ PartitionFilters shows your partition column filters
# ✅ BroadcastHashJoin (not SortMergeJoin for small tables)
# ✅ WholeStageCodegen (batched execution)

# BAD signs in a query plan:
# ❌ FileScan reads ALL columns (SELECT * or missing projection)
# ❌ SortMergeJoin when one table is small (should be BroadcastHashJoin)
# ❌ Exchange with very high partition count (shuffle overhead)
# ❌ CartesianProduct (accidental cross join!)
# ❌ Generate (explode on large array — check cardinality)
```

---

## 10.4 Common Errors and How to Fix Them

### Error 1: OutOfMemoryError (OOM)

```
Error:
  org.apache.spark.SparkException: Task failed while writing rows.
  java.lang.OutOfMemoryError: GC overhead limit exceeded

OR:
  ERROR Executor: Exception in task 15.0 in stage 3.0
  java.lang.OutOfMemoryError: Java heap space
```

```python
# DIAGNOSIS:
# 1. Check Spark UI → Executors tab → Storage Memory Used vs Total Memory
# 2. Look for GC Time > 20% in task metrics
# 3. Check if you're caching very large DataFrames

# FIX 1: Increase executor memory via cluster config
# Cluster Settings → Advanced → Spark Config:
# spark.executor.memory 8g  (default: 4g)
# spark.driver.memory 4g

# FIX 2: Increase shuffle partitions (smaller data per partition)
spark.conf.set("spark.sql.shuffle.partitions", "800")  # More, smaller partitions

# FIX 3: Clear unused caches
df_old.unpersist()
spark.catalog.clearCache()

# FIX 4: Use disk spill (allow Spark to spill to disk)
spark.conf.set("spark.sql.shuffle.spill", "true")

# FIX 5: Check for data skew — one partition getting all data
df.rdd.mapPartitions(lambda p: [sum(1 for _ in p)]).collect()
# If one number is 100x larger → skew → apply salting

# FIX 6: Don't collect large DataFrames
# BAD:
all_data = df.collect()  # Pulls ALL data to driver — OOM if data is large

# GOOD:
df.write.format("delta").save("/mnt/silver/output/")  # Write to storage
# OR: sample for testing
sample = df.limit(1000).collect()
```

### Error 2: Job Stuck on Last Few Tasks (Stragglers)

```
Symptom:
  Stage 4: 199/200 tasks complete. Waiting on 1 task for 45 minutes.
  All other stages completed in seconds.
```

```python
# DIAGNOSIS:
# 1. Spark UI → Stages → Click the stage → Sort tasks by Duration
# 2. Find the single long-running task
# 3. Click on the task → Locality, Shuffle Read Size
# If Shuffle Read Size is 100x larger than others → SKEW

# FIX 1: Enable AQE skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# FIX 2: Identify and salt the hot key
df.groupBy("join_key").count().orderBy(col("count").desc()).show(5)
# If top key has 10x more rows → apply salting (see Module 04)

# FIX 3: Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# FIX 4: Use repartition before groupBy
df.repartition(500, col("customer_id")).groupBy("customer_id").agg(...)
```

### Error 3: AnalysisException — Schema Mismatch

```
Error:
  AnalysisException: A schema mismatch detected when writing to the Delta table:
  
  To enable schema migration, please set:
  spark.databricks.delta.schema.autoMerge.enabled = true
  or use the option 'mergeSchema' = true
```

```python
# FIX: Enable schema evolution
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \  # ← Add this
    .save("/mnt/silver/transactions/")

# Or globally:
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# DIAGNOSIS: What changed?
from delta.tables import DeltaTable
target = DeltaTable.forPath(spark, "/mnt/silver/transactions/")
print("Target schema:")
target.toDF().printSchema()

print("Source schema:")
df.printSchema()
# Compare — find the column that's different or missing
```

### Error 4: FileNotFoundException — Data Not Where Expected

```
Error:
  FileNotFoundException: Operation failed: "The specified path does not exist."
  Path: /mnt/bronze/transactions/date=2024-01-15/
```

```python
# DIAGNOSIS:
# 1. Check if path exists
try:
    files = dbutils.fs.ls("/mnt/bronze/transactions/")
    display(files)
except Exception as e:
    print(f"Path error: {e}")

# 2. Check mount is active
for mount in dbutils.fs.mounts():
    if "/mnt/bronze" in mount.mountPoint:
        print(f"Mount active: {mount.mountPoint} → {mount.source}")
        break

# 3. List what partitions exist
dbutils.fs.ls("/mnt/bronze/transactions/")
# Check if date partition exists

# FIX:
# If mount is missing → re-run mount notebook
# If partition is missing → check upstream pipeline ran successfully
# If path format wrong → verify date format

# DEFENSIVE PATTERN: Check before processing
def safe_read_with_check(path: str) -> "DataFrame | None":
    try:
        files = dbutils.fs.ls(path)
        if len(files) == 0:
            print(f"⚠️ Path exists but is empty: {path}")
            return None
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"❌ Cannot read from {path}: {e}")
        return None

df = safe_read_with_check("/mnt/bronze/transactions/date=2024-01-15/")
if df is None:
    dbutils.notebook.exit("SKIPPED:no_data_for_date")
```

### Error 5: ConcurrentModificationException in Delta MERGE

```
Error:
  io.delta.exceptions.ConcurrentWriteException: A concurrent transaction has written 
  to the same Delta table and modified files that the current transaction reads.
```

```python
# CAUSE: Two jobs writing to same Delta table simultaneously

# FIX 1: Configure retry on conflict
spark.conf.set("spark.databricks.delta.retryWriteConflicts", "true")

# FIX 2: Use Delta's conflict retry in your code
import time

def merge_with_retry(target, source, merge_condition, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            (target.alias("tgt")
             .merge(source.alias("src"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            print(f"✅ MERGE succeeded on attempt {attempt}")
            return True
        except Exception as e:
            if "ConcurrentWriteException" in str(e) and attempt < max_retries:
                wait_time = 30 * attempt
                print(f"⚠️ Concurrent write conflict (attempt {attempt}). Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    return False

# FIX 3: Schedule jobs to avoid overlap (job dependencies in workflow)
# Don't run parallel MERGE jobs on same table — use task dependencies
```

---

## 10.5 Production Logging Pattern

```python
# ── logger.py — Standardized logging for all pipeline notebooks ──

import logging
from datetime import datetime
from typing import Optional

class PipelineLogger:
    """Structured logging for Databricks ETL pipelines."""
    
    def __init__(self, pipeline_name: str, batch_date: str, environment: str):
        self.pipeline_name = pipeline_name
        self.batch_date    = batch_date
        self.environment   = environment
        self.start_time    = datetime.utcnow()
        self.step_count    = 0
        self.metrics       = {}
        
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s | {pipeline_name} | %(levelname)s | %(message)s'
        )
        self.logger = logging.getLogger(pipeline_name)
    
    def step(self, step_name: str):
        """Log the start of a processing step."""
        self.step_count += 1
        self.current_step = step_name
        self.step_start   = datetime.utcnow()
        self.logger.info(f"STEP {self.step_count}: {step_name} — STARTED")
    
    def step_done(self, records_processed: int = None):
        """Log the completion of a processing step."""
        duration = (datetime.utcnow() - self.step_start).total_seconds()
        msg = f"STEP {self.step_count}: {self.current_step} — DONE ({duration:.1f}s)"
        if records_processed is not None:
            msg += f" | records={records_processed:,}"
            self.metrics[self.current_step] = {
                "records": records_processed,
                "duration_seconds": duration
            }
        self.logger.info(msg)
    
    def error(self, message: str, exc: Exception = None):
        """Log an error with full context."""
        self.logger.error(
            f"FAILED at step '{self.current_step}': {message}",
            exc_info=exc is not None
        )
    
    def summary(self):
        """Print final pipeline summary."""
        total_duration = (datetime.utcnow() - self.start_time).total_seconds() / 60
        print("\n" + "="*60)
        print(f"PIPELINE SUMMARY: {self.pipeline_name}")
        print(f"Batch Date:  {self.batch_date}")
        print(f"Environment: {self.environment}")
        print(f"Duration:    {total_duration:.1f} minutes")
        print("-"*60)
        for step, m in self.metrics.items():
            print(f"  {step:<30} {m['records']:>10,} records | {m['duration_seconds']:.1f}s")
        print("="*60 + "\n")
    
    def log_to_delta(self, log_table: str):
        """Write pipeline run log to a Delta table for monitoring dashboards."""
        log_data = [{
            "pipeline_name":   self.pipeline_name,
            "batch_date":      self.batch_date,
            "environment":     self.environment,
            "start_time":      str(self.start_time),
            "end_time":        str(datetime.utcnow()),
            "status":          "SUCCESS",
            "total_duration":  (datetime.utcnow() - self.start_time).total_seconds(),
            "metrics":         str(self.metrics)
        }]
        
        log_df = spark.createDataFrame(log_data)
        log_df.write.format("delta").mode("append").saveAsTable(log_table)


# Usage in a pipeline notebook:
log = PipelineLogger("daily-transaction-pipeline", BATCH_DATE, ENV)

log.step("Read Bronze Data")
df_bronze = spark.read.format("delta").load(BRONZE_PATH).filter(col("_batch_date") == BATCH_DATE)
log.step_done(records_processed=df_bronze.count())

log.step("Transform Silver")
df_silver = apply_transformations(df_bronze)
log.step_done(records_processed=df_silver.count())

log.step("Merge to Silver Delta")
merge_to_silver(df_silver)
log.step_done(records_processed=df_silver.count())

log.summary()
log.log_to_delta("metadata.pipeline_run_log")
```

---

## 10.6 Cluster and Job Logs

### Accessing Logs in Databricks UI

```
CLUSTER LOGS:
  Compute → Your Cluster → Logging tab
  
  Driver Logs:
    Log4j logs: stdout/stderr from driver process
    → Look for: ERROR lines, "WARN DiskBlockManager" (disk pressure)
    → Look for: "GCDaemon" lines appearing frequently (GC pressure)
  
  Worker Logs:
    Per-executor logs (harder to find, use Spark UI Executors tab)
    → Look for: OOM errors on specific executors

JOB RUN LOGS:
  Workflows → Jobs → Click a job run
  → Click a specific task → Logs tab
  → See stdout, stderr, Driver logs for that task run
  
  Look for:
    "Task X failed" — click to see exception
    "Stage retried" — transient failure (network, storage)
    "SparkContext stopped" — driver died (OOM usually)
```

### Log Aggregation for Production Monitoring

```python
# ── Centralized error tracking using Azure Monitor ──

import requests
import json
from datetime import datetime

def send_to_azure_monitor(
    workspace_id: str,
    workspace_key: str,
    log_data: dict,
    log_type: str = "DatabricksPipelineLogs"
):
    """Send custom logs to Azure Log Analytics workspace."""
    import base64, hashlib, hmac, datetime
    
    body = json.dumps([log_data])
    content_length = len(body)
    
    rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    
    string_to_hash = f"POST\n{content_length}\napplication/json\nx-ms-date:{rfc1123date}\n/api/logs"
    bytes_to_hash  = bytes(string_to_hash, encoding="utf-8")
    decoded_key    = base64.b64decode(workspace_key)
    encoded_hash   = base64.b64encode(
        hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
    ).decode()
    
    headers = {
        'Content-Type':  'application/json',
        'Authorization': f'SharedKey {workspace_id}:{encoded_hash}',
        'Log-Type':      log_type,
        'x-ms-date':     rfc1123date,
        'time-generated-field': 'timestamp'
    }
    
    response = requests.post(
        f'https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01',
        data=body,
        headers=headers
    )
    return response.status_code == 200

# Usage: log pipeline metrics to Azure Monitor
send_to_azure_monitor(
    workspace_id=dbutils.secrets.get("kv-prod", "log-analytics-workspace-id"),
    workspace_key=dbutils.secrets.get("kv-prod", "log-analytics-key"),
    log_data={
        "timestamp":       datetime.utcnow().isoformat(),
        "pipeline_name":   "daily-transaction-pipeline",
        "batch_date":      BATCH_DATE,
        "status":          "SUCCESS",
        "records_processed": 512847,
        "duration_minutes":  43.9,
        "environment":     "prod"
    }
)
```

---

## 10.7 Performance Monitoring Dashboard

```python
# ── monitoring_dashboard.py ──
# Query pipeline run logs to build operational visibility

# Read pipeline run history
pipeline_history = spark.read.format("delta") \
    .load("/mnt/metadata/pipeline_run_log/") \
    .filter(col("pipeline_name") == "daily-transaction-pipeline")

# Daily pipeline trend
daily_trend = pipeline_history \
    .groupBy("batch_date") \
    .agg(
        avg("total_duration").alias("avg_duration_seconds"),
        max("total_duration").alias("max_duration_seconds"),
        count("*").alias("run_count"),
        sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        sum(when(col("status") == "FAILED",  1).otherwise(0)).alias("failure_count"),
    ) \
    .withColumn("success_rate_pct", 
                col("success_count") / col("run_count") * 100) \
    .orderBy("batch_date", ascending=False)

display(daily_trend)

# SLA monitoring (must complete within 2 hours = 7200 seconds)
sla_breaches = pipeline_history \
    .filter(col("total_duration") > 7200) \
    .select("batch_date", "total_duration", "status") \
    .orderBy("batch_date", ascending=False)

print(f"SLA breaches in last 30 days: {sla_breaches.count()}")
display(sla_breaches)
```

---

## 10.8 Interview Questions — Monitoring & Debugging

**Q1: How do you identify data skew in a running Spark job?**
> Open Spark UI → Jobs → click the slow job → click the slow Stage → look at the Tasks section. If the Median task duration is 1-2 seconds but the Max is 45 minutes, that's skew — one partition has far more data than others. Click on the slow task to see its Shuffle Read Size — if it's 50-100x larger than the median task, that partition is the hot key. Find the key by running `df.groupBy("join_key").count().orderBy("count", ascending=False)` on your data.

**Q2: What does a high GC time in Spark UI indicate and how do you fix it?**
> High GC time (>10-15% of task time) means the JVM is spending too much time garbage collecting — memory is almost full. Causes: too many cached DataFrames, working set too large for available memory, or too many small objects (prefer arrays/primitives over objects). Fixes: increase executor memory (`spark.executor.memory`), unpersist unused caches, reduce shuffle partition data size by increasing `spark.sql.shuffle.partitions`, or use Tungsten off-heap storage.

**Q3: How would you troubleshoot a Databricks job that succeeds locally but fails in production?**
> Systematic approach: (1) Check cluster runtime version — is it the same? (2) Check data volume — prod may have edge cases (nulls, special characters, data skew) not in dev samples. (3) Check permissions — service account vs personal account access differences. (4) Check cluster size — job cluster may have different memory than all-purpose dev cluster. (5) Look at the exact error in job run logs → driver logs → stage → failed task. (6) Run with a sample of prod data in dev environment to reproduce.

**Q4: What is the difference between driver logs and executor logs?**
> Driver logs show activity on the driver node — job submissions, DAG creation, action triggers, SparkContext errors. If the driver dies (OOM, crash), you see it in driver logs. Executor logs show activity on worker nodes — individual task execution, task OOM errors, local disk issues. Most data processing errors appear in executor logs. In Databricks, access driver logs via Cluster → Logging. Executor logs are harder — use Spark UI → Executors tab → click a failed executor → click "stderr".

---

*Next: [Module 07 — Interview Preparation: 100 Questions & Answers](../07-Interview-Prep/01-complete-interview-guide.md)*
