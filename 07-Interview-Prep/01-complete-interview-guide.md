# Module 07 — Complete Interview Preparation
## 80 Questions + Model Answers for Azure Databricks & PySpark Roles

> These questions are drawn from actual DE interviews at Infosys, TCS, Wipro, Cognizant, Capgemini, and MAANG-level data platform teams. Organized by topic and difficulty.

---

## SECTION 1: Azure Databricks Fundamentals (Beginner–Intermediate)

**Q1: What is the difference between Azure Databricks and Apache Spark?**
> Apache Spark is the open-source distributed computing engine — it processes data in parallel across a cluster but requires you to manage infrastructure, security, and deployment. Azure Databricks is a managed cloud service built on Spark, jointly developed by Databricks and Microsoft, that adds: managed cluster lifecycle (auto-scaling, auto-termination), collaborative notebooks with Git integration, Unity Catalog for governance, Delta Lake as first-class storage format, built-in security (Azure AD, RBAC), and the Photon vectorized execution engine. Think of Spark as the engine and Databricks as the fully-built car around it.

**Q2: What are the types of Databricks clusters and when do you use each?**
> (1) **All-Purpose Clusters** — persistent, shared, billed hourly even when idle. Used for interactive development, ad-hoc exploration, and collaborative analysis. (2) **Job Clusters** — ephemeral, created fresh per job run, terminated immediately after, 60-80% cheaper. Used for all scheduled production pipelines. (3) **SQL Warehouses** — dedicated compute for SQL queries and BI tool connections, supports ANSI SQL and ODBC/JDBC, separate from Spark clusters. (4) **Single Node** — driver only, no workers. Used for small data development and testing. Production ETL must use Job clusters.

**Q3: What is the Databricks Runtime and why should you always pin to an LTS version?**
> The Databricks Runtime bundles Apache Spark, Python, Delta Lake, MLflow, and Databricks-specific optimizations into a versioned package. LTS (Long Term Support) versions receive security patches and bug fixes for at least 2 years without breaking API changes. Non-LTS versions have shorter support windows and may introduce breaking changes between minor releases. In production, pinning to an LTS runtime ensures reproducibility — your job won't break when the runtime is updated. Example: `13.3 LTS (Spark 3.4.1, Scala 2.12)`.

**Q4: Explain the Control Plane and Data Plane in Databricks architecture.**
> The **Control Plane** is managed by Databricks — it runs the workspace UI, REST API server, job scheduler, notebook server, cluster manager, and stores metadata. The **Data Plane** lives entirely within your Azure subscription — actual cluster VMs, ADLS Gen2 storage, VNet, and network security groups. Your data NEVER leaves your subscription — it goes directly between cluster VMs and your storage. This architecture is Databricks' key enterprise security argument: Databricks never sees your data.

**Q5: What is DBFS and what are its limitations?**
> DBFS (Databricks File System) is a virtualized file system abstraction that maps paths like `/mnt/bronze/` to actual Azure Blob Storage or ADLS Gen2 underneath. Limitations: (1) Not a true distributed file system — it's a path abstraction. (2) Files written to `/dbfs/FileStore/` are accessible to all workspace users — no per-user access control. (3) In Unity Catalog workspaces, DBFS mounts are deprecated in favor of External Locations. (4) DBFS root is managed storage — not recommended for production data. Always use mounted ADLS Gen2 or External Locations for production data.

---

## SECTION 2: PySpark Core Concepts (Intermediate)

**Q6: What is lazy evaluation and what is its practical benefit?**
> Lazy evaluation means transformations (filter, select, join, groupBy) are not executed when called — they build a computation plan (DAG). Execution only happens when an action (count, show, write, collect) is called. Practical benefits: (1) **Catalyst Optimizer** rewrites the entire plan before execution — applies column pruning (only read needed columns from Parquet), predicate pushdown (filter at source), join reordering. (2) **Pipeline fusion** — consecutive narrow transformations are combined into a single pass rather than separate scans. (3) If you add more transformations before the action, they're all optimized together. A 6-hour job reduced to 45 minutes in one real project simply by removing unnecessary intermediate `.count()` actions that forced premature execution.

**Q7: What is the difference between narrow and wide transformations?**
> **Narrow transformations** operate on each partition independently — no data movement between partitions. Examples: `filter()`, `select()`, `withColumn()`, `map()`, `flatMap()`. These are fast because tasks can run in parallel with no coordination. **Wide transformations** require data to be shuffled across the network (data from multiple partitions must be combined). Examples: `groupBy()`, `join()`, `distinct()`, `orderBy()`, `repartition()`. Shuffles are the primary bottleneck in Spark — minimizing wide transformations is the core of Spark optimization.

**Q8: What is the difference between `persist()` and `cache()`?**
> `cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)` in Databricks (actually `MEMORY_ONLY` in vanilla Spark). `persist(level)` lets you specify the storage level explicitly: `MEMORY_ONLY` (deserialized in RAM — fastest but uses most memory), `MEMORY_AND_DISK` (spill to disk if RAM full), `DISK_ONLY` (only disk — avoids memory pressure), `OFF_HEAP` (Tungsten off-heap — avoids GC pressure, best with Photon). In practice: use `cache()` for most cases, `persist(MEMORY_AND_DISK)` for DataFrames that might not fit in RAM.

**Q9: Explain the difference between `repartition()` and `coalesce()`.**
> `repartition(n)` performs a **full shuffle** — redistributes ALL data across the network into `n` balanced partitions. Use to: increase partition count, balance skewed partitions, or partition by a specific column (`repartition(n, col)`). Expensive due to full shuffle. `coalesce(n)` **reduces** partitions without a full shuffle — merges adjacent partitions within each node. Much cheaper but can result in unbalanced sizes. Use before writing to reduce output file count without the overhead of a full shuffle. **Key rule:** coalesce to reduce, repartition to increase or balance.

**Q10: What is schema inference and why is it banned in production?**
> `inferSchema=true` causes Spark to read the entire dataset twice — once to infer column types and once to actually load data. Problems: (1) Doubles read cost (critical for TBs of data). (2) Infers incorrect types — "001" → Integer (drops leading zero), "null" → String instead of null, inconsistent dates inferred as strings. (3) Non-deterministic — schema can change if the sample data changes. Production standard: always define explicit `StructType` schemas with correct types and nullable flags.

**Q11: When would you use a Python UDF vs a Pandas UDF vs a built-in function?**
> **Built-in functions** (first choice): Run entirely in the JVM/Photon, benefit from Catalyst optimization, no serialization overhead. Use `when()`, `regexp_replace()`, `date_format()`, etc. **Pandas UDF** (second choice): When logic requires Pandas/NumPy operations (complex math, ML scoring, statistical calculations). Processes an entire Series vectorized — 10-100x faster than Python UDF. Requires `pyarrow`. **Python UDF** (last resort): When logic truly cannot be expressed otherwise. Serializes every row from JVM to Python interpreter — very slow. Use only for complex string parsing or external library calls.

---

## SECTION 3: Delta Lake (Intermediate–Advanced)

**Q12: What makes Delta Lake ACID compliant?**
> Delta Lake achieves ACID through its **transaction log** (`_delta_log/`): **Atomicity** — each write creates a new JSON commit entry atomically; if the write fails midway, no entry is created and the table is unchanged. **Consistency** — schema enforcement validates every write against the table's schema. **Isolation** — Optimistic Concurrency Control (OCC) allows concurrent reads and writes: reads see consistent snapshots (not partial writes), writes detect conflicts and retry. **Durability** — the transaction log is written to durable cloud storage (ADLS); once committed, the entry survives any failure.

**Q13: How does Delta Lake time travel work? When would you use it in production?**
> Every Delta write creates a versioned JSON entry in `_delta_log/`. You can query any historical version via `VERSION AS OF N` or `TIMESTAMP AS OF 'YYYY-MM-DD'`. Production use cases: (1) **Regulatory audit** — reproduce exact dataset at quarter-end for regulatory reporting without maintaining separate copies. (2) **Disaster recovery** — restore accidentally deleted data using `restoreToVersion(N)`. (3) **Pipeline debugging** — compare data before and after a transformation to find where an error was introduced. (4) **ML reproducibility** — retrain a model with exactly the same data it originally used. Retention is configurable (`delta.logRetentionDuration`); VACUUM removes history beyond the retention window.

**Q14: Explain MERGE and why it's preferred over DELETE+INSERT.**
> `MERGE` (UPSERT) is a **single atomic operation** — if any part fails, nothing changes. DELETE+INSERT is two operations — a crash between them leaves the table in a corrupted state (no rows at all for that period). MERGE also: (1) Supports partition pruning on the join condition — only scans relevant partitions instead of the full table. (2) Handles all three cases (INSERT/UPDATE/DELETE) in one pass. (3) Is the standard for SCD Type 1 and Type 2 patterns. (4) Maintains Delta ACID guarantees throughout.

**Q15: What is Z-Ordering and when should you use it instead of partitioning?**
> Z-Ordering uses a space-filling curve to co-locate related rows within the same data files — when you filter on a Z-ordered column, Delta reads fewer files even without partition pruning. Use partitioning for **low-cardinality** columns where most queries filter (date, region, status — 5-365 distinct values). Use Z-Ordering for **high-cardinality** columns that are frequently filtered but partitioning is impractical (customer_id with 10M values — partitioning creates 10M tiny folders). They complement each other: partition by date, Z-order by customer_id within each date partition.

**Q16: What is schema evolution in Delta Lake and what are its modes?**
> Schema evolution allows a Delta table's schema to change over time. **mergeSchema** (`option("mergeSchema", "true")` or `SET TBLPROPERTIES ('delta.autoMerge.enabled'='true')`) — adds new columns from source to the target table. Old records have null for new columns. **overwriteSchema** — completely replaces the schema (data is also replaced). **Schema enforcement** (default) — Delta rejects writes that don't match the current schema, protecting data quality. In production, use `mergeSchema` to handle source system additions gracefully, and add data quality checks to validate new columns before they reach Silver.

**Q17: What happens during a VACUUM operation and what is the risk?**
> VACUUM physically deletes data files that are no longer referenced by the transaction log and are older than the retention period (default: 7 days). This reclaims storage space. The risk: after VACUUM, you cannot time-travel beyond the retention window because the old files are permanently deleted. Best practice: set retention to at least 30 days for production (`RETAIN 720 HOURS`), run `DRY RUN` first to preview what will be deleted, and schedule VACUUM during maintenance windows. Never run `VACUUM ... RETAIN 0 HOURS` unless you're sure you don't need historical data.

---

## SECTION 4: Performance Optimization (Advanced)

**Q18: Walk me through how you would diagnose and fix a Spark job taking 6 hours when it should finish in 30 minutes.**
> **Step 1: Spark UI** → Jobs → find the slow stage. **Step 2: Check task distribution** — if 199/200 tasks finish in 30s but 1 task runs for 6 hours → data skew. Identify the hot key with `groupBy(join_key).count()`. Fix: AQE skew handling, salting, or separate hot-key processing. **Step 3: Check shuffle metrics** — if shuffle read is very large → too few shuffle partitions. Increase `spark.sql.shuffle.partitions`. **Step 4: Check join strategy** in `explain()` — if SortMergeJoin when one table is small → add `broadcast()` hint. **Step 5: Check GC time** — if >20% of task time → memory pressure. Increase executor memory or reduce partition size. **Step 6: Check partition count** of source data — if reading CSV with 2 partitions on 32 cores → 30 idle cores. Use `repartition()` after read.

**Q19: What is Adaptive Query Execution (AQE) and what does it automatically optimize?**
> AQE is Spark 3.x's runtime query optimizer that collects statistics DURING execution and adjusts the plan on the fly — unlike the static Catalyst optimizer that plans everything upfront. AQE automatically handles: (1) **Coalescing small post-shuffle partitions** — if a shuffle produces 200 small partitions, AQE merges them into fewer, larger ones. (2) **Skew join optimization** — detects and automatically splits skewed partitions into smaller sub-tasks. (3) **Dynamic join strategy switching** — converts SortMergeJoin to BroadcastHashJoin at runtime if one side turns out to be small after filtering. Enable with `spark.sql.adaptive.enabled=true`.

**Q20: What is the broadcast join threshold and how do you tune it?**
> The `spark.sql.autoBroadcastJoinThreshold` setting (default: 10MB) controls the maximum table size Spark will automatically broadcast to all workers. If a table's estimated size is below this, Spark converts the join to a BroadcastHashJoin (no shuffle). Tuning: increase to 50-100MB for larger dimension tables (`spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)`). Set to -1 to disable auto-broadcast entirely (useful to force Sort-Merge joins for consistency, or when Spark's size estimates are wrong). Use explicit `broadcast()` hint when automatic detection fails.

**Q21: Explain data skew, how to detect it, and 3 different ways to fix it.**
> **Detection:** Spark UI → Stage → Tasks — if one task runs 10-100x longer than median, it's skew. Confirm by `groupBy(join_key).count().orderBy("count", descending=True)` — if top key has 1M rows and average is 100 → skew. **Fix 1: AQE** (`spark.sql.adaptive.skewJoin.enabled=true`) — automatic, zero code change, handles most cases. **Fix 2: Salting** — append a random integer (0-N) to the hot key in the large table; explode the small table to match all salt values; join on salted key; aggregate again to remove salt effect. **Fix 3: Separate processing** — identify hot keys, process them separately with `repartition()` spread across more partitions, process normal keys with broadcast, union results. **Fix 4: Increase partitions** — `repartition(n, skewed_col)` spreads data more evenly before aggregation.

---

## SECTION 5: Streaming & Auto Loader (Advanced)

**Q22: What is the difference between Auto Loader and standard `readStream.csv()`?**
> Standard `readStream` on a directory lists all files on every trigger batch and tracks which were processed in a state store — this doesn't scale beyond hundreds of thousands of files (directory listing is slow). **Auto Loader** (`format("cloudFiles")`) uses Azure Event Grid file notifications instead of directory listing — scales to billions of files with no performance degradation. It also automatically tracks processed files in a RocksDB checkpoint, handles schema inference and evolution automatically, and supports exactly-once semantics even across cluster restarts.

**Q23: What is a watermark and what happens to data that arrives after the watermark threshold?**
> A watermark tells Spark how long to wait for late-arriving data before closing a time window. `.withWatermark("event_timestamp", "10 minutes")` means: keep the window open for 10 minutes past the current max event time. Data arriving more than 10 minutes late is **dropped** (not processed). Without a watermark, Spark keeps all window state in memory forever (unbounded state → OOM on long-running streams). The trade-off: smaller watermark = less memory but more dropped late data. In IoT or mobile app scenarios, 5-15 minutes is typical; for financial data where all events are near-real-time, 1-2 minutes may be acceptable.

**Q24: What is `foreachBatch` and when do you absolutely need it?**
> `foreachBatch(func)` passes each streaming micro-batch to a user-defined function as a static DataFrame, allowing you to use batch operations on streaming data. You NEED it when: (1) Writing with **Delta MERGE** (streaming `writeStream` doesn't support upsert semantics — only append, update, complete modes). (2) Writing to **multiple sinks** in one stream — process the batch once, write to multiple destinations. (3) Applying **complex batch logic** like SCD Type 2 updates that can't be expressed in streaming mode. (4) Idempotent operations using `batch_id` to prevent duplicate processing on retry.

**Q25: Explain exactly-once semantics in Structured Streaming.**
> Exactly-once means each event is processed precisely once despite failures. Databricks achieves this through three layers: (1) **Source tracking** — Auto Loader and Kafka connectors track exactly which files/offsets were consumed in a checkpoint. If the query restarts, it resumes from the last committed offset. (2) **Idempotent sinks** — Delta Lake writes are transactional; a partial write that fails is rolled back. The checkpoint records which batches were committed; on restart, uncommitted batches are reprocessed (the write is idempotent due to Delta). (3) **Checkpointing** — the checkpoint directory stores the query's state (offsets, aggregation state) on durable ADLS. Together, these guarantee end-to-end exactly-once even if the driver crashes mid-batch.

---

## SECTION 6: Unity Catalog & Security (Intermediate)

**Q26: What is the three-level namespace in Unity Catalog?**
> `catalog.schema.table` — Catalog is the top-level container (one per environment typically: `prod_catalog`, `dev_catalog`); Schema is a logical grouping (Bronze/Silver/Gold layers or business domains); Table is the data object. This adds one level above the traditional Hive `database.table` two-level namespace. The benefit: you can have the same schema and table names across multiple catalogs (environments), and access control can be applied at any level.

**Q27: What is column masking vs row filtering in Unity Catalog?**
> **Column masking**: A SQL function is attached to a specific column using `ALTER TABLE ... ALTER COLUMN ... SET MASK`. When any user queries that column, the function runs automatically — privileged users see the real value, others see a masked version (e.g., PAN card → `XXXXX1234X`). The data on disk is unchanged; masking is applied at query time. **Row filtering**: A SQL function is attached to a table using `ALTER TABLE ... ADD ROW FILTER`. It returns true/false for each row — false rows are invisible to the querying user. Used for data regionalization (India team sees only `region='IN'` rows) or sensitivity levels. Both are transparent to the application — no special query syntax needed.

**Q28: What is the difference between a Service Principal and Managed Identity?**
> A **Service Principal** is an Azure AD identity with client ID + client secret credentials — you must store, protect, and rotate the secret. A **Managed Identity** is an identity automatically managed by Azure assigned to a resource (VM, cluster) — there is NO credential, Azure handles authentication internally. For new projects: always use Managed Identity for Databricks-to-ADLS authentication. Use Service Principals only when cross-subscription or cross-tenant access is required (Managed Identity has Azure subscription scope).

---

## SECTION 7: Jobs, ADF & DevOps (Intermediate–Advanced)

**Q29: How do you design an idempotent notebook?**
> An idempotent notebook produces the same result whether run once or multiple times. Patterns: (1) **Delta MERGE instead of append** — MERGE naturally handles duplicates. (2) **Checkpoint files** — write a `.done` marker file on success, check for it before processing. (3) **replaceWhere for partition overwrite** — `write.option("replaceWhere", f"date='{batch_date}'"`) overwrites only the target partition, same result on re-run. (4) **Deduplication** — always deduplicate on business key before writing, even if upstream sends duplicates. Idempotency is critical because job retries are automatic and you cannot guarantee a notebook runs exactly once.

**Q30: How does ADF pass parameters to a Databricks notebook and how does the notebook receive them?**
> ADF Databricks Notebook activity has a **Base Parameters** section where you define key-value pairs — these can include ADF dynamic expressions like `@pipeline().parameters.StartDate`. In the notebook, `dbutils.widgets.text("start_date", "")` defines a widget receiver. The value is retrieved with `dbutils.widgets.get("start_date")`. The notebook can return a value via `dbutils.notebook.exit("SUCCESS:500000")` which ADF captures in `@activity('notebook_name').output.runOutput`.

**Q31: Describe a complete Dev → Test → Prod deployment workflow.**
> (1) Developer creates a feature branch in Git, writes notebooks in **Databricks Repos** (Git-connected). (2) Opens a Pull Request to `main` branch — triggers **GitHub Actions** CI pipeline: runs `pytest` unit tests, linting (flake8), and an integration test against Dev environment. (3) PR is reviewed by peer, merged to `main`. (4) CI auto-deploys to **Test environment**: updates Repos to `main` branch, runs the pipeline once with test data, validates output. (5) QA/DE lead reviews test run results, approves deployment. (6) Manual workflow trigger deploys to **Prod**: `databricks repos update --branch main`, updates job definition via `databricks jobs update`. All environment-specific config is handled via notebook parameters — same code, different parameter values.

---

## SECTION 8: System Design Scenarios (Advanced)

**Q32: Design a data platform for a bank processing 50M transactions daily.**

```
REQUIREMENTS:
- 50M transactions/day (≈580/second)
- Fraud detection within 5 minutes of transaction
- Historical data for 7 years (regulatory)
- BI reports available by 6 AM
- 99.9% pipeline reliability

ARCHITECTURE:

Transaction Sources (Core Banking, ATMs, Mobile App)
    ↓ (Kafka / Event Hub)
Auto Loader (Structured Streaming) → Bronze Delta
    ↓ (5-minute micro-batches)
Silver Layer:
  - Type casting and validation
  - PII masking (column masks in Unity Catalog)
  - Deduplication on txn_id
  - Fraud Detection Stream (rule-based + ML scoring)
    ↓ (Daily batch at 2 AM)
Gold Layer:
  - Daily summary by payment mode, region, channel
  - Customer 360 profile
  - Regulatory reports (RBI SAR, etc.)
    ↓
SQL Warehouse + Power BI (available by 6 AM)

STORAGE:
  Bronze: 7-year retention, WORM storage (regulatory)
  Silver: 3-year retention
  Gold:   5-year retention

SECURITY:
  Unity Catalog with row-level filters by region
  Column masking on account_no, PAN
  PII data tagged and audited

COST OPTIMIZATION:
  Job clusters (not all-purpose) for all pipelines
  Spot instances for batch workloads (80% savings)
  Delta VACUUM with 90-day retention
  Z-ordering on customer_id, txn_date
```

**Q33: How would you handle late-arriving data in a streaming pipeline?**
> Three-part strategy: (1) **Watermarks** — define acceptable lateness (`withWatermark("event_ts", "15 minutes")`). Data arriving within 15 minutes of window close is included; beyond that is dropped. (2) **Reconciliation batch** — run a daily batch job that reads the full day's Bronze data (including late arrivals) and re-MERGEs to Silver. This handles events that arrived after the streaming watermark. (3) **Change Data Feed** — enable `delta.enableChangeDataFeed` on Silver; downstream Gold can use CDF to incrementally process corrections from the reconciliation batch. Result: near-real-time freshness with eventual correctness for late data.

---

## SECTION 9: Scenario-Based Coding Questions

**Q34: Write PySpark code to find the top 3 customers by spend in each city, for the last 30 days.**

```python
from pyspark.sql.functions import col, sum, dense_rank, current_date, date_sub
from pyspark.sql import Window

# Read Silver transactions (last 30 days)
df = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .filter(col("txn_date") >= date_sub(current_date(), 30))

# Join with customers to get city
customers = spark.read.format("delta").load("/mnt/silver/customers/") \
    .select("customer_id", "city")

df_joined = df.join(broadcast(customers), on="customer_id", how="inner")

# Aggregate spend per customer per city
customer_spend = df_joined.groupBy("city", "customer_id") \
    .agg(sum("amount").alias("total_spend"))

# Rank within each city
w = Window.partitionBy("city").orderBy(col("total_spend").desc())

top_3 = customer_spend \
    .withColumn("city_rank", dense_rank().over(w)) \
    .filter(col("city_rank") <= 3) \
    .orderBy("city", "city_rank")

top_3.show()
```

**Q35: Write code to detect customers who have made more than 5 transactions within any 1-hour window.**

```python
from pyspark.sql.functions import col, count, window, to_timestamp
from pyspark.sql import Window

# Use windowed aggregation on event time
df = spark.read.format("delta").load("/mnt/silver/transactions/") \
    .withColumn("txn_timestamp", to_timestamp(col("txn_datetime")))

# Using window functions (batch approach)
w = Window.partitionBy("customer_id") \
    .orderBy(col("txn_timestamp").cast("long")) \
    .rangeBetween(-3600, 0)  # 1-hour window in seconds

velocity_df = df \
    .withColumn("txn_count_last_hour", count("txn_id").over(w)) \
    .filter(col("txn_count_last_hour") > 5) \
    .select("customer_id", "txn_timestamp", "txn_id", "txn_count_last_hour") \
    .distinct()

print(f"Suspicious customers: {velocity_df.select('customer_id').distinct().count()}")
velocity_df.show()
```

**Q36: Write an incremental load function with watermark tracking.**

```python
from pyspark.sql.functions import max, col
from delta.tables import DeltaTable

def incremental_load(
    source_path: str,
    target_path: str,
    watermark_table: str,
    pipeline_name: str,
    key_column: str,
    watermark_column: str = "updated_at"
) -> int:
    """
    Generic incremental load with watermark tracking.
    Returns: number of records processed
    """
    # Read last watermark
    try:
        wm = spark.sql(f"""
            SELECT watermark_value FROM {watermark_table}
            WHERE pipeline_name = '{pipeline_name}'
        """).first()
        last_wm = wm["watermark_value"] if wm else "1900-01-01"
    except:
        last_wm = "1900-01-01"
    
    print(f"📅 Processing from: {last_wm}")
    
    # Read incremental data
    df_new = spark.read.format("delta").load(source_path) \
        .filter(col(watermark_column) > last_wm)
    
    record_count = df_new.count()
    
    if record_count == 0:
        print("✅ No new records")
        return 0
    
    # Deduplicate
    from pyspark.sql import Window
    w = Window.partitionBy(key_column).orderBy(col(watermark_column).desc())
    df_deduped = df_new \
        .withColumn("_rn", row_number().over(w)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")
    
    # MERGE to target
    target = DeltaTable.forPath(spark, target_path)
    (target.alias("tgt")
     .merge(df_deduped.alias("src"), f"tgt.{key_column} = src.{key_column}")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    
    # Update watermark
    new_wm = df_new.agg(max(watermark_column)).first()[0]
    
    wm_update = spark.createDataFrame(
        [(pipeline_name, str(new_wm))],
        ["pipeline_name", "watermark_value"]
    )
    DeltaTable.forPath(spark, watermark_table) \
        .alias("t") \
        .merge(wm_update.alias("s"), "t.pipeline_name = s.pipeline_name") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print(f"✅ Loaded {record_count:,} records | New watermark: {new_wm}")
    return record_count
```

---

## Quick Reference: Most Asked Topics by Company Type

```
INFOSYS / WIPRO / COGNIZANT (Service Companies):
  → Medallion Architecture explanation (always asked)
  → Delta Lake MERGE code (always asked)
  → PySpark transformations vs actions
  → Repartition vs coalesce
  → Broadcast join explanation
  → ADF + Databricks integration
  → Key Vault secrets management

TCS / CAPGEMINI:
  → All of above +
  → Auto Loader configuration
  → Unity Catalog governance
  → Window functions (ranking + lag/lead)
  → SCD Type 2 implementation
  → Error handling patterns

PRODUCT / MAANG-ADJACENT:
  → All of above +
  → Streaming (watermarks, foreachBatch, exactly-once)
  → Skew handling (salting, AQE)
  → System design (design a data platform)
  → Spark UI interpretation
  → CI/CD for Databricks
  → Data quality frameworks
  → Unity Catalog column masking / row filtering
```

---

*You've completed the full Azure Databricks & PySpark Learning Program.*  
*Start the projects, push everything to GitHub, and begin interviewing.*
