# Module 14 — Cheatsheets
## Interview Questions by Difficulty — 120 Q&As

---

## 🟢 BEGINNER (L1/L2 — 0-2 Years Experience)

**Q1: What is Apache Spark and why is it faster than Hadoop MapReduce?**
> Spark processes data in-memory across a cluster instead of writing intermediate results to disk after every step like MapReduce. A multi-stage job that takes 10 hours in MapReduce (disk I/O between each stage) takes 30 minutes in Spark (data stays in RAM between stages). Spark also has a DAG execution engine that optimizes the entire computation plan before running, whereas MapReduce has no such global optimization.

**Q2: What is a DataFrame in PySpark?**
> A DataFrame is a distributed collection of data organized into named columns — conceptually identical to a SQL table or a Pandas DataFrame, but distributed across a cluster and processing in parallel. Unlike RDDs, DataFrames have a schema (known column names and types), enabling the Catalyst optimizer to apply query optimizations automatically.

**Q3: What is the difference between a transformation and an action?**
> Transformations (filter, select, join, groupBy) are lazy — they build a computation plan (DAG) but execute nothing. Actions (count, show, write, collect) trigger actual execution of the entire accumulated plan. This lazy evaluation allows Spark's Catalyst optimizer to rewrite and optimize the entire plan before any computation begins.

**Q4: What does `df.show()` do and why is it an action?**
> `show()` prints the top N rows of a DataFrame to the console — it must actually execute the computation to retrieve those rows, making it an action. It's the most common debugging tool but should be removed from production code (replaced with logging the count or writing to a target).

**Q5: What is HDFS and how does ADLS Gen2 replace it in Azure?**
> HDFS (Hadoop Distributed File System) is the distributed storage system that comes with Hadoop — data is stored across cluster nodes' disks. ADLS Gen2 (Azure Data Lake Storage Gen2) is Azure's cloud-native equivalent — object storage with a hierarchical namespace, practically unlimited scale, and decoupled from compute (clusters can be shut down without losing data). ADLS Gen2 is cheaper, more durable (11 9s), and better integrated with Azure security.

**Q6: What is a partition in Spark?**
> A partition is a chunk of data that a single task processes. Spark splits a large DataFrame into many partitions and processes them in parallel across cluster nodes. Each core processes one partition at a time. Too few partitions = underutilized cores. Too many partitions = task scheduling overhead. Optimal: 2-4 partitions per CPU core, with each partition 128-256MB.

**Q7: What is `select *` and why should you avoid it in production?**
> `SELECT *` reads all columns from storage. In columnar formats like Parquet and Delta Lake, columns are stored separately — reading only the columns you need is dramatically faster. For a 100-column table where you need 5 columns, `SELECT *` reads 20x more data than necessary. Always specify column names explicitly in production.

**Q8: What is the difference between `count()` and `countDistinct()`?**
> `count(col)` counts all non-null values in the column (duplicates included). `countDistinct(col)` counts unique non-null values. `count(*)` counts total rows including those where the column is null. In production with huge datasets, use `approx_count_distinct(col, 0.05)` for ~5% error but 10-100x faster execution.

**Q9: What is Delta Lake?**
> Delta Lake is an open-source storage layer that adds ACID transaction guarantees to data lakes. It uses a transaction log (_delta_log/) to record every operation atomically. On top of standard Parquet files, Delta adds: ACID transactions, schema enforcement, upserts (MERGE), time travel (query historical versions), and auto-optimization. It's the default format for all production Databricks tables.

**Q10: What is the Medallion Architecture?**
> A three-layer data organization pattern: Bronze (raw, as-is from source — append-only, never transform), Silver (cleaned, validated, typed, deduplicated — business rules applied), Gold (aggregated, business-ready — KPIs, reporting tables, BI-optimized). Each layer is a set of Delta tables. Data flows Bronze→Silver→Gold via scheduled ETL pipelines.

---

## 🟡 INTERMEDIATE (L2/L3 — 2-4 Years Experience)

**Q11: Explain lazy evaluation with a real example showing its benefit.**
> You write: `df.filter(col("date")=="2024-01-15").select("customer_id","amount").groupBy("customer_id").agg(sum("amount"))`. With lazy evaluation, Spark doesn't execute this line by line. It builds one combined plan where: only "customer_id" and "amount" columns are read from disk (column pruning), the date filter is applied while reading (predicate pushdown), and the groupBy processes already-filtered data. Without lazy evaluation, each step would execute separately — reading all columns, filtering all rows, then selecting. Combined: 5-10x less I/O.

**Q12: What is data skew and how do you detect and fix it?**
> Data skew is when one partition has dramatically more rows than others — one task takes 100x longer than the rest, becoming a straggler that blocks the entire stage. Detect: Spark UI → Stage → Tasks → if Max duration >> Median duration. Find hot key: `df.groupBy("join_key").count().orderBy("count",ascending=False)`. Fix: (1) Enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled=true`), (2) Salt the hot key by appending a random number (0-N), (3) Process hot keys separately with `repartition()`.

**Q13: How does a broadcast join work and when do you use it?**
> In a broadcast join, Spark sends the entire smaller table to every worker node's memory. Each worker then joins its partition of the large table against the local copy of the small table — no data shuffling across the network. Use when one table is small enough to fit in worker memory (< 50-100MB typically). `df_large.join(broadcast(df_small), "key")`. Verify it's used: `df.explain()` should show `BroadcastHashJoin` not `SortMergeJoin`.

**Q14: What is schema evolution in Delta Lake and how do you enable it?**
> Schema evolution allows a Delta table's schema to automatically update when new columns appear in incoming data. Enable with `.option("mergeSchema","true")` on write, or globally with `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")`. New columns are added to the table; existing records have null for new columns. Schema enforcement (default) blocks writes that don't match the current schema — this protects data quality at the cost of flexibility. Use `overwriteSchema=true` to completely replace the schema.

**Q15: What is the difference between MERGE and overwrite in Delta Lake?**
> Overwrite replaces all data (or a partition) with new data — fast but loses previous records. MERGE (UPSERT) is an atomic operation that handles three cases: update matching rows, insert new rows, optionally delete rows — in a single statement. MERGE preserves existing records not in the source, making it ideal for incremental loads. Overwrite is for full refreshes; MERGE is for incremental updates.

**Q16: Explain Adaptive Query Execution (AQE) and what it automatically handles.**
> AQE is Spark 3.x's runtime optimizer that collects statistics DURING execution (not just upfront). It automatically: (1) Coalesces small post-shuffle partitions into fewer, larger ones, (2) Converts SortMergeJoin to BroadcastHashJoin if one side turns out small after filtering, (3) Splits skewed partitions into smaller sub-tasks. Enable: `spark.sql.adaptive.enabled=true`. Real impact: jobs that take 2 hours with AQE off finish in 30 minutes with it on.

**Q17: How does time travel work in Delta Lake?**
> Every Delta operation creates a versioned JSON entry in `_delta_log/`. Query historical data with `VERSION AS OF N` or `TIMESTAMP AS OF 'YYYY-MM-DD'`. Restore to a previous version with `RESTORE TABLE t TO VERSION AS OF N`. History is retained until VACUUM removes old files. Use cases: regulatory audits, accidental deletion recovery, ML reproducibility, pipeline debugging (compare data before/after a transformation).

**Q18: What is a SQL Warehouse and how does it differ from a Spark cluster?**
> SQL Warehouse: dedicated compute for SQL queries only, supports ANSI SQL, ODBC/JDBC connections (Power BI, Tableau), auto-start/stop, serverless option, optimized for concurrent BI queries, no PySpark API. Spark Cluster: full Spark engine, supports PySpark/Scala/SQL, used for ETL jobs and notebook development. SQL Warehouses are for data analysts and BI tools; Spark clusters are for data engineers running pipelines.

**Q19: How do you pass parameters to a notebook in a Databricks job?**
> Define `dbutils.widgets.text("param_name","default")` in the notebook. In the Databricks Job UI, under Task → Notebook Task → Base Parameters, add key-value pairs. ADF passes parameters via the Databricks Notebook Activity's "Base Parameters" field supporting dynamic expressions like `@pipeline().parameters.StartDate`. Retrieve in notebook: `dbutils.widgets.get("param_name")`.

**Q20: What is Auto Loader and why use it instead of `readStream` on a directory?**
> Auto Loader (`format("cloudFiles")`) uses Azure Event Grid notifications to detect new files — scales to billions of files with no performance degradation. Standard `readStream` on a directory lists ALL files every trigger to find new ones — O(N) overhead that becomes unusable at millions of files. Auto Loader also handles exactly-once semantics, schema inference, schema evolution, and rescue data (schema-mismatch rows) automatically.

---

## 🔴 ADVANCED (L3/L4 — 4+ Years Experience)

**Q21: A Spark job runs fine in dev (1GB data) but fails with OOM in prod (500GB). Walk me through your debugging approach.**
> Systematic investigation: (1) Check Spark UI Executors tab — what's the memory usage? Are executors running at 95%+? (2) Check GC time — >20% means memory pressure. (3) Check shuffle read size — is one partition getting a disproportionate amount of data (skew)? (4) Check partition count — 500GB / 200 partitions = 2.5GB per partition (too large, causes OOM). Solution: increase shuffle.partitions to 2000, enable AQE, check for hot keys causing skew. If join-related: broadcast the smaller side, bucket large tables. If aggregation-related: pre-aggregate before the main agg, or salt hot keys.

**Q22: Design a data platform for a company processing 100M events/day with < 5-minute latency for fraud detection.**
> Architecture: Events arrive via Kafka → Auto Loader (streaming) → Bronze Delta (30-second micro-batches, ~50K events/batch) → Silver transformation stream (clean, validate, enrich) → Fraud Detection stream (rule-based engine + ML model scoring via Pandas UDF) → Alert stream to Kafka for real-time action + Gold Delta for batch reporting. Key decisions: watermark of 5 minutes for late data, foreachBatch for ML scoring (batch API calls more efficient), separate cluster for streaming (never share with batch), checkpoint on ADLS for exactly-once, Z-order Gold by customer_id for fast point queries.

**Q23: How would you implement exactly-once processing in a streaming pipeline?**
> Three-layer guarantee: (1) Source — Auto Loader tracks processed files in RocksDB checkpoint; Kafka connectors commit offsets only after successful batch write. (2) Processing — Spark Structured Streaming uses write-ahead log in checkpoint; if query restarts, uncommitted batches are reprocessed. (3) Sink — Delta Lake transactional writes ensure partial writes are rolled back; idempotent writes (MERGE with batch_id deduplication) prevent duplicate records even if a batch is reprocessed. Together: source tracking + idempotent processing + transactional sink = end-to-end exactly-once.

**Q24: How do you design a cost-efficient Databricks architecture for a startup with variable workloads?**
> Key decisions: (1) Job clusters (not all-purpose) for all scheduled jobs — 80% cost reduction. (2) Spot instances with on-demand fallback — 60% VM cost reduction. (3) Serverless SQL Warehouse for BI (pay per query, zero startup cost). (4) Autoscaling with tight bounds (2-8 workers based on workload profiling). (5) Cluster policies to prevent oversized dev clusters. (6) Instance pools for dev clusters (fast start, small idle pool). (7) Schedule batch jobs at off-peak hours for cheaper Spot instances. (8) Delta VACUUM weekly + OPTIMIZE monthly to control storage costs. Expected: 60-80% cost reduction vs default configuration.

**Q25: How do you handle PII (personally identifiable information) in a data lake?**
> Defense-in-depth approach: (1) **Detection** — scan incoming data for PII using regex patterns and Unity Catalog column tags (`pii='true'`). (2) **Access control** — column masking functions in Unity Catalog show masked values (`XXXXX1234X`) to non-privileged users; row filters restrict data by user's region/role. (3) **Encryption** — encrypt PII columns at rest using Azure Key Vault-managed keys; Databricks encryption at rest for ADLS. (4) **Audit** — Unity Catalog logs every access to PII-tagged columns; retain logs for 1+ year for compliance. (5) **Deletion** — maintain a deletion log; GDPR right-to-erasure: MERGE + DELETE + VACUUM to physically remove data; propagate deletions via CDF.

**Q26: What is the small file problem in Delta Lake and how do you solve it?**
> When many small files (< 10MB) accumulate (from frequent small appends, streaming micro-batches, or many small partitions), query performance degrades because: each file requires a separate network request to open, metadata overhead grows, and Spark creates one task per file (many tiny tasks = scheduling overhead). Solutions: (1) `OPTIMIZE` command compacts small files into 128MB target files. (2) `autoOptimize.optimizeWrite=true` table property — Databricks automatically bins writes into optimal sizes. (3) `autoOptimize.autoCompact=true` — automatically runs OPTIMIZE after large write operations. (4) `coalesce()` before write to control output file count in batch pipelines. (5) Liquid Clustering manages file sizes automatically without manual OPTIMIZE runs.

**Q27: How would you debug a MERGE operation that's taking 45 minutes on a 500GB table?**
> Diagnose first: (1) Run `df_source.count()` and `df_target.count()` — how much data on each side? (2) `EXPLAIN` the MERGE — is it doing a full table scan or partition-pruned? (3) Check if partition column is in the MERGE condition — `tgt.txn_date = src.txn_date AND tgt.txn_id = src.txn_id`. Without `txn_date` in condition, Spark scans all 500GB. Fix: (1) Add partition column to MERGE condition for partition pruning. (2) Run `OPTIMIZE ZORDER BY (join_key)` before MERGE — reduces files scanned via data skipping. (3) Deduplicate source before MERGE — smaller source = faster scan. (4) Enable AQE — may convert inner join part to broadcast. (5) Check for skew on join key — if one customer has 10M rows, salt it.

**Q28: Design a Unity Catalog governance model for a financial services company.**
> Three-catalog model: `prod_catalog` (production data), `dev_catalog` (developer sandbox), `shared_catalog` (reference/lookup data). Schema per layer: bronze, silver, gold, quarantine. Access groups: `data-engineers` (SELECT+MODIFY on all layers), `data-analysts` (SELECT on gold only), `compliance-team` (SELECT on all + unmask PII columns), `bi-team` (SELECT on gold + SQL Warehouse access). PII handling: column masking on account numbers, PAN cards, email — `data-engineers` and `compliance-team` see real values, others see masked. Row filters: regional teams see only their region's data. All access logged to system tables. Monthly access review via SHOW GRANTS queries.

---

## 🎯 CODING CHALLENGES (Solve in Interview)

**Challenge 1: Find the second highest salary per department.**
```sql
SELECT department, salary
FROM (
    SELECT department, salary,
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
    FROM employees
)
WHERE rnk = 2;
```

**Challenge 2: Find customers who transacted in January but NOT in February.**
```python
jan = df.filter(month("txn_date")==1).select("customer_id").distinct()
feb = df.filter(month("txn_date")==2).select("customer_id").distinct()
result = jan.join(feb, on="customer_id", how="left_anti")
```

**Challenge 3: Calculate 7-day rolling average per customer.**
```python
w = Window.partitionBy("customer_id").orderBy("txn_date").rowsBetween(-6, 0)
df.withColumn("rolling_7d_avg", avg("amount").over(w))
```

**Challenge 4: Flatten a nested JSON column with array of items into one row per item.**
```python
df.withColumn("item", explode("items")) \
  .select("order_id", "item.sku", "item.qty", "item.price")
```

**Challenge 5: Detect duplicate transactions (same customer, same amount, within 5 minutes).**
```python
w = Window.partitionBy("customer_id").orderBy(col("txn_timestamp").cast("long"))
df.withColumn("prev_amount", lag("amount",1).over(w)) \
  .withColumn("prev_time",   lag("txn_timestamp",1).over(w)) \
  .withColumn("is_duplicate",
      (col("amount") == col("prev_amount")) &
      (col("txn_timestamp").cast("long") - col("prev_time").cast("long") < 300)
  )
```

---

## 📋 SYSTEM DESIGN TEMPLATE

```
For any "Design a data platform" question, answer in this structure:

1. REQUIREMENTS CLARIFICATION (2 minutes)
   - Data volume: How many records/day? How much GB?
   - Latency: Batch (hours), Near-real-time (minutes), Real-time (seconds)?
   - Consumers: BI tools? APIs? ML models?
   - Retention: How many years?
   - Compliance: PII? GDPR? SOC2?

2. HIGH-LEVEL ARCHITECTURE (draw it)
   Source → Ingestion → Bronze → Silver → Gold → Consumption

3. TECHNOLOGY CHOICES (justify each)
   - Why Delta Lake (ACID, time travel, MERGE)
   - Why Auto Loader (scale, exactly-once, schema evolution)
   - Why Unity Catalog (governance, column masking, lineage)
   - Why job clusters (cost — 80% savings over all-purpose)

4. DATA QUALITY
   - DQ framework with ERROR/WARNING/INFO severity
   - Quarantine table for bad records
   - Volume anomaly detection

5. PERFORMANCE
   - Partitioning strategy (date for most tables)
   - Z-ordering (customer_id, product_id)
   - Broadcast joins for dimension tables
   - AQE enabled

6. SECURITY
   - Key Vault for secrets (no hardcoded credentials)
   - Managed Identity for cluster-to-ADLS auth
   - Column masking on PII
   - Row filters for regional access

7. OPERATIONS
   - Monitoring: Spark UI + custom pipeline log table
   - Alerting: Teams/email on failure
   - CI/CD: GitHub Actions → Test → Prod
   - Cost: Spot instances + job clusters + auto-termination
```
