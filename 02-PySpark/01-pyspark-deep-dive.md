# Module 02 — PySpark Deep Dive
## Part 1: RDD vs DataFrame vs Dataset, Transformations vs Actions, Lazy Evaluation

---

## 2.1 The Spark Programming Model — Big Picture

Before writing a single line of PySpark, you need to understand **how Spark thinks**.

```
Your PySpark Code
       ↓
    DAG Builder         ← Spark analyzes your code, builds a Directed Acyclic Graph
       ↓
    Catalyst Optimizer  ← Rewrites your query for maximum efficiency
       ↓
    Tungsten Engine     ← Executes using optimized bytecode
       ↓
    Cluster Execution   ← Distributed across worker nodes
```

This entire flow is **lazy** — Spark doesn't execute anything until you call an **action**.

---

## 2.2 RDD vs DataFrame vs Dataset

### The Evolution

```
Spark 1.0  → RDDs      (powerful but verbose, no optimization)
Spark 1.3  → DataFrames (SQL-like, Catalyst optimizer, schema-aware)
Spark 1.6  → Datasets  (type-safe DataFrames — Scala/Java only)
Spark 2.0  → DataFrame = Dataset[Row] (unified API)
```

### RDD (Resilient Distributed Dataset)

```python
# Creating an RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Map: apply a function to each element
squared = rdd.map(lambda x: x ** 2)

# Filter
evens = rdd.filter(lambda x: x % 2 == 0)

# Reduce (this is an ACTION — triggers execution)
total = rdd.reduce(lambda a, b: a + b)
print(total)  # 55

# Word count (classic RDD example)
text_rdd = spark.sparkContext.parallelize([
    "spark is fast", 
    "spark is powerful",
    "databricks uses spark"
])

word_count = text_rdd \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print(word_count.collect())
# [('spark', 3), ('is', 2), ('fast', 1), ('powerful', 1), ('databricks', 1), ('uses', 1)]
```

**When you encounter RDDs on client projects:**
- Legacy code (pre-2018 pipelines)
- Very custom transformations that DataFrames can't express
- Graph processing, ML pipelines internally

**99% of new work uses DataFrames.** But you must understand RDDs to debug issues.

---

### DataFrame — The Standard

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum, avg, count, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Create from Python data
transactions = [
    (1, "2024-01-01", "C001", 150.00, "CARD"),
    (2, "2024-01-01", "C002", 250.50, "UPI"),
    (3, "2024-01-02", "C001", 75.25,  "CARD"),
    (4, "2024-01-02", "C003", 500.00, "NETBANKING"),
    (5, "2024-01-03", "C002", 125.75, "UPI"),
]

schema = StructType([
    StructField("txn_id",      IntegerType(), nullable=False),
    StructField("txn_date",    StringType(),  nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("amount",      DoubleType(),  nullable=False),
    StructField("payment_mode",StringType(),  nullable=True),
])

df = spark.createDataFrame(transactions, schema)

# DataFrame is schema-aware — this is the advantage over RDD
df.printSchema()
# root
#  |-- txn_id: integer (nullable = false)
#  |-- txn_date: string (nullable = false)
#  |-- customer_id: string (nullable = false)
#  |-- amount: double (nullable = false)
#  |-- payment_mode: string (nullable = true)

df.show()
# +------+----------+-----------+------+----------+
# |txn_id|  txn_date|customer_id|amount|payment_mode|
# +------+----------+-----------+------+----------+
# |     1|2024-01-01|       C001|150.0 |CARD      |
# ...
```

---

## 2.3 Transformations vs Actions — The Most Important Concept

This is the single most important concept in Spark. **Get this wrong and you'll write terribly inefficient code.**

### Transformations — Build the Plan (Lazy)

Transformations **return a new DataFrame** and **do not execute immediately**. They just add a step to the computation plan.

```python
# NONE of these lines actually run — they just build a plan
df_filtered   = df.filter(col("amount") > 100)           # Transformation
df_selected   = df_filtered.select("customer_id", "amount")  # Transformation
df_grouped    = df_selected.groupBy("customer_id")        # Transformation
df_aggregated = df_grouped.agg(sum("amount").alias("total"))  # Transformation

# At this point: ZERO data has been processed. Spark has built a DAG.
print("No execution yet!")  # This prints instantly
```

**Narrow Transformations** (no data shuffle — fast):
```python
df.filter(col("amount") > 100)          # Each partition processed independently
df.select("customer_id", "amount")      # Column projection — no shuffle
df.withColumn("tax", col("amount") * 0.18)  # Row-wise operation
df.map(...)                             # Row-wise function
```

**Wide Transformations** (require data shuffle — expensive):
```python
df.groupBy("customer_id").agg(sum("amount"))  # Must move data across nodes
df.join(other_df, "customer_id")              # Data must be co-located
df.distinct()                                 # Requires global deduplication
df.orderBy("amount")                          # Requires global sort
df.repartition(100)                           # Reshuffles all data
```

### Actions — Trigger Execution

Actions **force Spark to execute** the accumulated plan and return results.

```python
# These ALL trigger execution:

# 1. collect() — bring all data to driver (DANGEROUS for large datasets)
rows = df_aggregated.collect()
print(rows)  # [Row(customer_id='C001', total=225.25), ...]

# 2. show() — print top N rows
df.show(10)
df.show(10, truncate=False)  # Don't truncate long strings

# 3. count() — count rows
n = df.count()
print(f"Total transactions: {n}")

# 4. first() / head() — get first row(s)
first_row = df.first()
top_5 = df.head(5)

# 5. take() — get N rows as Python list
sample = df.take(3)

# 6. write — save data (most common action in ETL)
df.write.format("delta").mode("overwrite").save("/mnt/silver/transactions/")

# 7. foreach() / foreachPartition()
df.foreach(lambda row: print(row))

# 8. toPandas() — convert to Pandas (DANGEROUS for large data)
pandas_df = df.toPandas()  # Only for small DataFrames (<1M rows)
```

### The Lazy Evaluation Payoff — Why It Matters

```python
# WITHOUT lazy evaluation (hypothetical):
# Step 1: Read 10TB of data → 10TB in memory
# Step 2: Filter where amount > 100 → 5TB
# Step 3: Select 2 columns → 500GB
# Total work: 10TB read + 5TB filter + 500GB select = wasteful

# WITH lazy evaluation (what Spark actually does):
# Spark COMBINES steps 1+2+3 into a single pass
# Step 1: Read ONLY the needed columns (column pruning)
# Step 2: While reading, immediately filter amount > 100
# Total work: Read only relevant columns, filter while reading = 10x faster
```

Real-world impact: On a banking project, a 6-hour daily job was reduced to 45 minutes simply by understanding this — by not calling unnecessary `.count()` actions inside loops.

---

## 2.4 Reading Data — All Formats

```python
# CSV
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .option("encoding", "UTF-8") \
    .csv("/mnt/bronze/transactions/*.csv")

# Parquet (preferred for ETL)
df_parquet = spark.read.parquet("/mnt/bronze/transactions/")

# JSON
df_json = spark.read \
    .option("multiLine", "true") \
    .json("/mnt/bronze/events/*.json")

# Delta Lake
df_delta = spark.read.format("delta").load("/mnt/silver/transactions/")

# JDBC (SQL Server / Oracle / MySQL)
df_jdbc = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://server:1433;databaseName=mydb") \
    .option("dbtable", "dbo.transactions") \
    .option("user", dbutils.secrets.get("kv-scope", "sql-user")) \
    .option("password", dbutils.secrets.get("kv-scope", "sql-password")) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("fetchsize", "10000") \
    .load()

# Excel (via spark-excel library)
df_excel = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/bronze/report.xlsx")

# Define schema explicitly (production standard — never use inferSchema in prod)
from pyspark.sql.types import *

schema = StructType([
    StructField("txn_id",      IntegerType(), False),
    StructField("txn_date",    DateType(),    False),
    StructField("customer_id", StringType(),  False),
    StructField("amount",      DoubleType(),  False),
    StructField("payment_mode",StringType(),  True),
])

df_typed = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/mnt/bronze/transactions/*.csv")
```

**Why `inferSchema=true` is banned in production:**
```python
# inferSchema reads the ENTIRE dataset twice — once to infer types, once to load
# For 1TB data: doubles your read cost
# Also: infers wrong types (e.g., "001" → Integer, losing leading zeros)
# Always define schema explicitly in production
```

---

## 2.5 Core DataFrame Operations

### Selecting and Renaming Columns

```python
from pyspark.sql.functions import col

# Select specific columns
df.select("txn_id", "amount", "customer_id")

# Select with col() — preferred in production (avoids ambiguity)
df.select(col("txn_id"), col("amount"), col("customer_id"))

# Rename while selecting
df.select(
    col("txn_id").alias("transaction_id"),
    col("amount").alias("txn_amount"),
    col("customer_id")
)

# Select all + add column
df.select("*", (col("amount") * 1.18).alias("amount_with_tax"))

# Rename a column
df.withColumnRenamed("txn_id", "transaction_id")

# Select using string expressions
df.selectExpr(
    "txn_id as transaction_id",
    "amount * 1.18 as amount_with_tax",
    "UPPER(customer_id) as customer_id"
)
```

### Adding and Modifying Columns

```python
from pyspark.sql.functions import col, lit, when, to_date, current_timestamp

# Add a constant column
df_with_source = df.withColumn("source_system", lit("ONLINE"))

# Conditional column (CASE WHEN equivalent)
df_with_category = df.withColumn(
    "amount_category",
    when(col("amount") < 100, "LOW")
    .when(col("amount") < 500, "MEDIUM")
    .when(col("amount") < 1000, "HIGH")
    .otherwise("PREMIUM")
)

# Type casting
df_cast = df.withColumn("txn_date", to_date(col("txn_date"), "yyyy-MM-dd"))

# String operations
from pyspark.sql.functions import upper, lower, trim, regexp_replace, substring

df_cleaned = df \
    .withColumn("customer_id", upper(trim(col("customer_id")))) \
    .withColumn("payment_mode", lower(col("payment_mode"))) \
    .withColumn("amount", regexp_replace(col("amount").cast("string"), ",", "").cast("double"))

# Add ingestion timestamp
df_timestamped = df.withColumn("ingested_at", current_timestamp())

# Drop columns
df_trimmed = df.drop("payment_mode", "ingested_at")
```

### Filtering

```python
# Filter by condition
df.filter(col("amount") > 100)
df.where(col("amount") > 100)   # Equivalent

# Multiple conditions
df.filter(
    (col("amount") > 100) & 
    (col("payment_mode") == "CARD") & 
    (col("txn_date") >= "2024-01-01")
)

# NOT condition
df.filter(~col("payment_mode").isin(["FAILED", "PENDING"]))

# Null handling
df.filter(col("payment_mode").isNotNull())
df.filter(col("amount").isNull())

# String matching
df.filter(col("customer_id").startsWith("C0"))
df.filter(col("payment_mode").contains("CARD"))
df.filter(col("customer_id").rlike("^C[0-9]{3}$"))  # Regex
```

---

## 2.6 Joins — Deep Dive

Joins are where most performance issues originate. Understand every type.

### Join Types

```python
customers = spark.createDataFrame([
    ("C001", "Alice",   "Mumbai"),
    ("C002", "Bob",     "Delhi"),
    ("C003", "Carol",   "Pune"),
    ("C004", "Dave",    "Chennai"),   # No transactions
], ["customer_id", "name", "city"])

transactions = spark.createDataFrame([
    (1, "C001", 150.0),
    (2, "C002", 250.0),
    (3, "C001", 75.0),
    (5, "C999", 500.0),   # No matching customer
], ["txn_id", "customer_id", "amount"])

# INNER JOIN — only matching rows
inner = transactions.join(customers, on="customer_id", how="inner")
# Result: C001(×2), C002(×1) — excludes C999 and Dave

# LEFT JOIN — all transactions, matching customer info where available
left = transactions.join(customers, on="customer_id", how="left")
# Result: C001(×2), C002(×1), C999(null customer info)

# RIGHT JOIN — all customers, matching transactions where available
right = transactions.join(customers, on="customer_id", how="right")
# Result: C001(×2), C002(×1), C003(null txn), C004(null txn)

# FULL OUTER JOIN — all rows from both sides
full = transactions.join(customers, on="customer_id", how="full")
# Result: All of the above

# SEMI JOIN — transactions WHERE a customer exists (no customer columns returned)
semi = transactions.join(customers, on="customer_id", how="left_semi")
# Result: Only C001 and C002 transactions — no customer columns

# ANTI JOIN — transactions WHERE customer DOES NOT exist
anti = transactions.join(customers, on="customer_id", how="left_anti")
# Result: Only C999 transaction

# CROSS JOIN (Cartesian) — every combination. Use with extreme caution!
cross = customers.crossJoin(transactions)
# Result: 4 customers × 4 transactions = 16 rows
```

### Join on Multiple Keys

```python
# Single key
df1.join(df2, on="customer_id", how="inner")

# Multiple keys (list)
df1.join(df2, on=["customer_id", "date"], how="inner")

# Different column names in each table
df1.join(
    df2,
    df1["cust_id"] == df2["customer_id"],
    how="inner"
)

# Complex join condition
df1.join(
    df2,
    (df1["customer_id"] == df2["customer_id"]) &
    (df1["txn_date"] >= df2["valid_from"]) &
    (df1["txn_date"] <= df2["valid_to"]),
    how="inner"
)
```

### Join Optimization — Critical for Production

```python
from pyspark.sql.functions import broadcast

# PROBLEM: Default join strategy for large tables = Sort Merge Join (expensive)
# 2 large tables → sort both → merge → SLOW

# SOLUTION 1: Broadcast Join (when one table is small, < 10MB typically)
# Spark sends the small table to ALL workers — no shuffle needed
result = large_df.join(broadcast(small_df), on="customer_id", how="inner")

# Broadcast threshold (default: 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB

# SOLUTION 2: Bucketing (for repeatedly joining large tables on the same key)
# Write both tables bucketed by the join key — Spark can skip the sort step
large_df.write \
    .format("delta") \
    .bucketBy(100, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("bucketed_transactions")

# SOLUTION 3: Partition Pruning (filter before join)
# BAD:
result = transactions.join(customers, "customer_id") \
    .filter(col("city") == "Mumbai")

# GOOD: filter first, then join — less data to shuffle
result = transactions \
    .filter(col("txn_date") >= "2024-01-01") \
    .join(customers.filter(col("city") == "Mumbai"), "customer_id")
```

### The Shuffle Explained

```
Without optimization (Sort Merge Join):

Worker 1 has: C001_txn1, C003_txn5
Worker 2 has: C002_txn2, C001_txn3

JOIN on customer_id:
→ All C001 data must go to same worker (SHUFFLE)
→ All C002 data must go to same worker (SHUFFLE)
→ Network traffic = bottleneck

With Broadcast Join:
→ Small customers table (1MB) sent to ALL workers
→ Each worker looks up its customers locally
→ ZERO network shuffle
→ 10-100x faster
```

---

## 2.7 Window Functions — Real Enterprise Use Cases

Window functions perform calculations **across rows related to the current row** without collapsing the result into a single row (unlike GROUP BY).

```python
from pyspark.sql.functions import (
    row_number, rank, dense_rank,
    lag, lead,
    sum, avg, max, min, count,
    first, last,
    percent_rank, ntile
)
from pyspark.sql import Window

# Sample data: customer transactions over time
txn_data = [
    ("C001", "2024-01-01", 150.0),
    ("C001", "2024-01-05", 200.0),
    ("C001", "2024-01-10", 75.0),
    ("C002", "2024-01-02", 300.0),
    ("C002", "2024-01-08", 450.0),
    ("C003", "2024-01-01", 100.0),
]

txn_df = spark.createDataFrame(txn_data, ["customer_id", "txn_date", "amount"])

# ─────────────────────────────────────────────────────────
# RANKING FUNCTIONS
# ─────────────────────────────────────────────────────────

# Window spec: partition by customer, order by amount descending
w_rank = Window.partitionBy("customer_id").orderBy(col("amount").desc())

ranked = txn_df.withColumn("row_number", row_number().over(w_rank)) \
               .withColumn("rank",       rank().over(w_rank)) \
               .withColumn("dense_rank", dense_rank().over(w_rank))

ranked.show()
# +-----------+----------+------+----------+----+----------+
# |customer_id|  txn_date|amount|row_number|rank|dense_rank|
# +-----------+----------+------+----------+----+----------+
# |       C001|2024-01-05| 200.0|         1|   1|         1|
# |       C001|2024-01-01| 150.0|         2|   2|         2|
# |       C001|2024-01-10|  75.0|         3|   3|         3|
# ...

# REAL USE CASE: Get the most recent transaction per customer
w_latest = Window.partitionBy("customer_id").orderBy(col("txn_date").desc())

latest_txns = txn_df \
    .withColumn("rn", row_number().over(w_latest)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# ─────────────────────────────────────────────────────────
# LAG / LEAD — Compare with previous/next row
# ─────────────────────────────────────────────────────────

w_time = Window.partitionBy("customer_id").orderBy("txn_date")

with_lag_lead = txn_df \
    .withColumn("prev_amount", lag("amount", 1).over(w_time)) \
    .withColumn("next_amount", lead("amount", 1).over(w_time)) \
    .withColumn("amount_change", col("amount") - lag("amount", 1, 0).over(w_time))

with_lag_lead.show()
# REAL USE CASE: Detect if current transaction is 3x more than previous
# (fraud detection pattern)
fraud_check = txn_df \
    .withColumn("prev_amount", lag("amount", 1).over(w_time)) \
    .withColumn("is_suspicious",
        when(col("amount") > col("prev_amount") * 3, True).otherwise(False)
    )

# ─────────────────────────────────────────────────────────
# RUNNING TOTALS / MOVING AVERAGES
# ─────────────────────────────────────────────────────────

# Running total per customer (cumulative sum)
w_cumulative = Window.partitionBy("customer_id") \
    .orderBy("txn_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_total = txn_df.withColumn(
    "running_total",
    sum("amount").over(w_cumulative)
)

# 3-transaction rolling average
w_rolling = Window.partitionBy("customer_id") \
    .orderBy("txn_date") \
    .rowsBetween(-2, 0)  # Current row + 2 preceding

rolling_avg = txn_df.withColumn(
    "rolling_3_avg",
    avg("amount").over(w_rolling)
)

# ─────────────────────────────────────────────────────────
# NTILE — Divide into buckets
# ─────────────────────────────────────────────────────────

w_global = Window.orderBy(col("amount").desc())

with_quartiles = txn_df.withColumn(
    "quartile",
    ntile(4).over(w_global)
)
# 1 = top 25% spenders, 4 = bottom 25%
```

---

## 2.8 UDFs vs Built-in Functions

### Always Prefer Built-in Functions

```python
# BAD: Python UDF (slow — serializes/deserializes every row)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def categorize_amount_udf(amount):
    if amount < 100:   return "LOW"
    elif amount < 500: return "MEDIUM"
    else:              return "HIGH"

df.withColumn("category", categorize_amount_udf(col("amount")))
# 🐌 Slow: Python interpreter called for EVERY ROW

# GOOD: Built-in (fast — runs in JVM, no serialization)
from pyspark.sql.functions import when

df.withColumn("category",
    when(col("amount") < 100, "LOW")
    .when(col("amount") < 500, "MEDIUM")
    .otherwise("HIGH")
)
# ⚡ Fast: Catalyst optimizer handles this natively
```

### When UDFs Are Necessary

```python
# When business logic cannot be expressed with built-ins
import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Example: Custom hashing for PII masking
@udf(returnType=StringType())
def mask_pan_card(pan):
    """Mask PAN card: ABCDE1234F → XXXXX1234X"""
    if pan and len(pan) == 10:
        return f"XXXXX{pan[5:9]}X"
    return None

df.withColumn("masked_pan", mask_pan_card(col("pan_card")))
```

### Pandas UDF (Vectorized UDF) — Best of Both Worlds

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Pandas UDF: processes a Pandas Series (vectorized — much faster than regular UDF)
@pandas_udf(DoubleType())
def compute_percentile_score(amounts: pd.Series) -> pd.Series:
    """Normalize amounts to percentile score 0-100"""
    return amounts.rank(pct=True) * 100

df.withColumn("percentile_score", compute_percentile_score(col("amount")))
```

**Performance comparison:**

| Method | Speed | When to Use |
|---|---|---|
| Built-in functions | ⚡⚡⚡ Fastest | Whenever possible — always first choice |
| Pandas UDF | ⚡⚡ Fast | Complex logic needing Pandas/NumPy |
| Python UDF | ⚡ Slow | Last resort for truly custom logic |
| Regular Python loop | 🐌 Terrible | Never on large datasets |

---

## 2.9 Aggregations — Complete Reference

```python
from pyspark.sql.functions import (
    count, countDistinct, sum, avg, mean,
    max, min, variance, stddev,
    collect_list, collect_set,
    first, last,
    approx_count_distinct  # ← faster than countDistinct for huge datasets
)

# Basic aggregations
summary = df.groupBy("customer_id") \
    .agg(
        count("txn_id").alias("total_transactions"),
        countDistinct("payment_mode").alias("distinct_payment_modes"),
        sum("amount").alias("total_spend"),
        avg("amount").alias("avg_transaction"),
        max("amount").alias("largest_transaction"),
        min("amount").alias("smallest_transaction"),
        stddev("amount").alias("spend_stddev"),
        collect_list("payment_mode").alias("all_payment_modes"),  # List (with duplicates)
        collect_set("payment_mode").alias("unique_payment_modes"), # Set (no duplicates)
        first("txn_date").alias("first_transaction_date"),
        last("txn_date").alias("latest_transaction_date"),
        approx_count_distinct("txn_id", 0.05).alias("approx_txn_count")  # 5% error, much faster
    )

# Aggregation with filter (FILTER clause)
from pyspark.sql.functions import expr

conditional_agg = df.groupBy("customer_id") \
    .agg(
        count("txn_id").alias("total_txns"),
        count(when(col("payment_mode") == "CARD", 1)).alias("card_txns"),
        sum(when(col("amount") > 500, col("amount")).otherwise(0)).alias("high_value_sum")
    )

# Global aggregation (no group by)
total = df.agg(
    sum("amount").alias("total_revenue"),
    count("txn_id").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers")
)

# Rollup (hierarchical aggregation)
rollup = df.rollup("payment_mode", "customer_id") \
    .agg(sum("amount").alias("total")) \
    .orderBy("payment_mode", "customer_id")

# Pivot table
pivot = df.groupBy("customer_id") \
    .pivot("payment_mode", ["CARD", "UPI", "NETBANKING"]) \
    .agg(sum("amount"))
```

---

## 2.10 Interview Questions — PySpark

**Q1: What is lazy evaluation and how does it help performance?**
> Lazy evaluation means Spark doesn't execute transformations when they're called — it builds a DAG (computation plan). When an action is called, Spark optimizes the entire plan using the Catalyst optimizer before executing. This allows Spark to apply optimizations like column pruning (only read needed columns), predicate pushdown (filter as early as possible), and join reordering — all automatically.

**Q2: What is the difference between `repartition()` and `coalesce()`?**
> `repartition(n)` creates exactly `n` partitions by doing a **full shuffle** — data is redistributed evenly. Expensive but balanced. `coalesce(n)` reduces partitions by **merging existing ones** without a full shuffle — it's much faster but can result in uneven partition sizes. Use `coalesce` to reduce partition count (e.g., before writing small output), use `repartition` to increase or evenly redistribute.

**Q3: Why should you avoid Python UDFs?**
> Python UDFs require serialization from JVM to Python interpreter for every row — this causes massive overhead. Built-in Spark functions run entirely in the JVM (or with Photon), never leaving optimized execution. If you must use custom logic, use Pandas UDFs which process an entire Pandas Series vectorized — much faster than row-by-row Python UDFs.

**Q4: When would you use a broadcast join?**
> When joining a large table (>millions of rows) with a small/lookup table (<10-50MB). Broadcast sends the small table to all workers, eliminating the shuffle. Classic example: joining 1TB transaction data with a 500KB country code lookup table.

**Q5: What is the difference between `rank()`, `row_number()`, and `dense_rank()`?**
> For values [100, 100, 90]:  `row_number()` = [1, 2, 3] (unique, arbitrary tiebreak), `rank()` = [1, 1, 3] (ties get same rank, next rank skips), `dense_rank()` = [1, 1, 2] (ties get same rank, next rank does NOT skip).

---

*Next: [Module 03 — Delta Lake Deep Dive](../03-Delta-Lake/01-acid-transactions.md)*
