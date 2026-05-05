# Topic 3 — RDD vs DataFrame
## Complete Guide: Understanding Spark's Data Abstractions

---

## 3.1 The Evolution Story

Spark was built in 2009 at UC Berkeley. The team needed a way to represent distributed data. They created RDD. Then as Spark grew, they realized RDD had limitations — so they built DataFrames on top. Then they unified everything into Datasets (Scala/Java only). Here's how it evolved:

```
2009 → RDD (Resilient Distributed Dataset)
         Raw power. Full control. No optimization.

2013 → DataFrame
         SQL-like. Catalyst Optimizer. Schema-aware.
         10-100x faster than RDD for most operations.

2015 → Dataset (Scala/Java only)
         Type-safe DataFrame. Compile-time checks.
         Python doesn't have this — DataFrame IS Dataset[Row] in Python.

TODAY → For Python (PySpark):
  Use DataFrame for 99% of work
  Use RDD only for very specific use cases
```

---

## 3.2 RDD — Resilient Distributed Dataset

### What is an RDD?

RDD is the **lowest-level data structure** in Spark. Think of it as a list that is:
- **Resilient**: if a piece of data is lost (node crash), Spark recreates it from the original source
- **Distributed**: the list is split across multiple machines (partitions)
- **Dataset**: it's a collection of data elements

```
YOUR DATA (1 million rows):
┌─────────────────────────────────────────────────────────┐
│                     RDD                                  │
│                                                          │
│  Partition 1         Partition 2         Partition 3    │
│  [row1,row2,...250K] [row250K,...500K]  [row500K,...1M] │
│        ↓                   ↓                   ↓        │
│  Worker Node 1       Worker Node 2       Worker Node 3  │
│                                                          │
│  Each partition is processed independently in parallel  │
└─────────────────────────────────────────────────────────┘
```

### RDD Characteristics:

```
✅ Immutable: once created, never changed (new RDD created for each transform)
✅ Lazy: transformations don't execute until an action is called
✅ Fault-tolerant: knows how to recreate lost partitions (lineage graph)
✅ Type-flexible: can hold ANY Python object (strings, dicts, custom classes)
✅ Full control: you control every operation explicitly

❌ No schema: Spark doesn't know what's inside (just "objects")
❌ No optimization: Spark can't look inside and optimize your code
❌ Verbose: 10 lines of RDD code = 1 line of DataFrame code
❌ Slower: no Catalyst optimizer, no Tungsten engine
❌ Hard to debug: errors appear only at runtime
```

### Creating and Using RDDs:

```python
# CREATING AN RDD

# Method 1: From a Python list (parallelize)
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd_numbers = spark.sparkContext.parallelize(numbers)
# Spark splits this list into partitions across the cluster

# Method 2: From a file
rdd_from_file = spark.sparkContext.textFile("/mnt/bronze/transactions.csv")
# Each LINE of the file becomes one element of the RDD

# Method 3: From another RDD (transformation)
rdd_doubled = rdd_numbers.map(lambda x: x * 2)

# Check partition count
print(f"Number of partitions: {rdd_numbers.getNumPartitions()}")
# → Number of partitions: 8 (depends on your cluster cores)

# ─────────────────────────────────────────────────────────────────
# RDD TRANSFORMATIONS (Lazy — build the plan, don't execute yet)
# ─────────────────────────────────────────────────────────────────

data = [
    "Alice,Engineering,95000",
    "Bob,Marketing,75000",
    "Carol,Engineering,110000",
    "Dave,HR,65000",
    "Eve,Engineering,105000",
    "Frank,Marketing,80000",
]

rdd = spark.sparkContext.parallelize(data)

# map() — apply a function to EVERY element, returns same number of elements
# Think: transform each row
rdd_split = rdd.map(lambda line: line.split(","))
# ['Alice,Engineering,95000'] → ['Alice', 'Engineering', '95000']

# filter() — keep only elements where function returns True
rdd_engineers = rdd_split.filter(lambda row: row[1] == "Engineering")

# flatMap() — apply function, FLATTEN the result (one element can become many)
words_rdd = spark.sparkContext.parallelize(["hello world", "good morning"])
flat = words_rdd.flatMap(lambda sentence: sentence.split(" "))
# ["hello world"] → ["hello", "world"]  (2 elements from 1)

# mapPartitions() — apply function to an entire PARTITION at once
# More efficient than map() when function has setup cost
def process_partition(partition_iter):
    # This runs once per partition, not once per row
    rows = list(partition_iter)
    yield f"Partition had {len(rows)} rows"

rdd.mapPartitions(process_partition).collect()

# distinct() — remove duplicates
depts = rdd_split.map(lambda row: row[1]).distinct()

# union() — combine two RDDs
rdd1 = spark.sparkContext.parallelize([1, 2, 3])
rdd2 = spark.sparkContext.parallelize([4, 5, 6])
combined = rdd1.union(rdd2)  # [1, 2, 3, 4, 5, 6]

# groupByKey() and reduceByKey() — for key-value pair RDDs
# First, create key-value pairs
rdd_kv = rdd_split.map(lambda row: (row[1], int(row[2])))
# ('Engineering', 95000), ('Marketing', 75000), ...

# groupByKey() — group values by key (EXPENSIVE — shuffles all data)
rdd_grouped = rdd_kv.groupByKey()
# ('Engineering', [95000, 110000, 105000])

# reduceByKey() — aggregate by key (EFFICIENT — partial aggregation on each node first)
rdd_total_salary = rdd_kv.reduceByKey(lambda a, b: a + b)
# ('Engineering', 310000), ('Marketing', 155000), ('HR', 65000)

# ─────────────────────────────────────────────────────────────────
# RDD ACTIONS (Trigger execution — these actually run the computation)
# ─────────────────────────────────────────────────────────────────

# collect() — bring ALL elements to driver as a Python list
# ⚠️ DANGEROUS for large datasets — can crash driver with OOM
result_list = rdd_total_salary.collect()
print(result_list)
# [('Engineering', 310000), ('Marketing', 155000), ('HR', 65000)]

# count() — count number of elements
n = rdd.count()
print(f"Total records: {n}")  # 6

# first() — get the first element
first = rdd_split.first()
print(first)  # ['Alice', 'Engineering', '95000']

# take(n) — get first n elements as Python list
sample = rdd.take(3)

# reduce() — aggregate ALL elements using a function
total = rdd_numbers.reduce(lambda a, b: a + b)  # 55

# foreach() — apply function to each element (no return value, for side effects)
rdd.foreach(lambda x: print(x))  # Prints on workers — you won't see it in driver!

# saveAsTextFile() — write to disk
rdd.saveAsTextFile("/mnt/bronze/rdd_output/")

# ─────────────────────────────────────────────────────────────────
# CLASSIC RDD EXAMPLE: Word Count
# ─────────────────────────────────────────────────────────────────

text_data = [
    "databricks is great",
    "spark is fast",
    "databricks uses spark",
    "spark databricks together",
]

rdd_text = spark.sparkContext.parallelize(text_data)

word_count_rdd = (
    rdd_text
    .flatMap(lambda line: line.split(" "))    # Split each line into words
    .map(lambda word: (word, 1))              # Pair each word with 1
    .reduceByKey(lambda a, b: a + b)          # Sum up the 1s per word
    .sortBy(lambda pair: pair[1], ascending=False)  # Sort by count
)

results = word_count_rdd.collect()
for word, count in results:
    print(f"{word}: {count}")

# databricks: 3
# spark: 3
# is: 2
# great: 1
# fast: 1
# uses: 1
# together: 1
```

---

## 3.3 DataFrame — The Modern Way

### What is a DataFrame?

A DataFrame is a **distributed collection of data organized into named columns** — like a SQL table or a spreadsheet. Unlike RDD, Spark KNOWS the structure (schema) of a DataFrame, which lets it optimize your code automatically.

```
RDD (No Schema — Spark is blind):
  Partition 1: ["Alice", "Engineering", "95000"]
               ["Bob",   "Marketing",   "75000"]
  Spark doesn't know what these are — just generic objects

DataFrame (With Schema — Spark understands):
  +--------+-------------+------+
  |  name  | department  |salary|
  +--------+-------------+------+    ← Spark knows:
  | Alice  | Engineering | 95000|       name = String
  | Bob    | Marketing   | 75000|       department = String
  +--------+-------------+------+       salary = Integer

  Now Spark can:
  - Only read needed columns from disk (column pruning)
  - Push filters to storage (predicate pushdown)
  - Choose optimal join strategy
  - Optimize the entire execution plan
```

### DataFrame Characteristics:

```
✅ Schema-aware: named columns with defined data types
✅ Optimized: Catalyst optimizer rewrites your query for performance
✅ SQL-like: use familiar SQL concepts (SELECT, WHERE, GROUP BY, JOIN)
✅ Tungsten engine: vectorized execution, memory optimization
✅ Language-agnostic: same performance in Python, Scala, Java, R
✅ Interoperable: easily convert to/from SQL, Pandas, Delta
✅ Concise: 1 line of DataFrame = 10 lines of equivalent RDD

❌ Less flexible: schema must be defined (feature, not bug)
❌ Python UDFs lose optimization benefits (use built-ins instead)
```

### Creating DataFrames — All Methods:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, sum, count, when

# ─────────────────────────────────────────────────────────────────
# METHOD 1: From a Python list (for small data, testing)
# ─────────────────────────────────────────────────────────────────

data = [
    (1, "Alice",  "Engineering", 95000),
    (2, "Bob",    "Marketing",   75000),
    (3, "Carol",  "Engineering", 110000),
    (4, "Dave",   "HR",          65000),
    (5, "Eve",    "Engineering", 105000),
    (6, "Frank",  "Marketing",   80000),
]

# With column names only (types inferred)
df = spark.createDataFrame(data, ["emp_id", "name", "department", "salary"])

# With explicit schema (PRODUCTION STANDARD — never guess types)
schema = StructType([
    StructField("emp_id",     IntegerType(), nullable=False),
    StructField("name",       StringType(),  nullable=False),
    StructField("department", StringType(),  nullable=False),
    StructField("salary",     IntegerType(), nullable=True),
])
df = spark.createDataFrame(data, schema)

df.printSchema()
# root
#  |-- emp_id: integer (nullable = false)
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = false)
#  |-- salary: integer (nullable = true)

df.show()
# +------+-----+------------+------+
# |emp_id| name|  department|salary|
# +------+-----+------------+------+
# |     1|Alice| Engineering| 95000|
# |     2|  Bob|   Marketing| 75000|
# |     3|Carol| Engineering|110000|
# ...

# ─────────────────────────────────────────────────────────────────
# METHOD 2: From CSV file
# ─────────────────────────────────────────────────────────────────
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/bronze/employees.csv")

# ─────────────────────────────────────────────────────────────────
# METHOD 3: From Delta table (most common in production)
# ─────────────────────────────────────────────────────────────────
df_delta = spark.read.format("delta").load("/mnt/silver/employees/")
# OR
df_delta = spark.table("silver.employees")
# OR
df_delta = spark.sql("SELECT * FROM silver.employees")

# ─────────────────────────────────────────────────────────────────
# COMMON OPERATIONS on DataFrame
# ─────────────────────────────────────────────────────────────────

# SELECT — choose columns
df.select("name", "department", "salary").show()

# SELECT with expressions
df.select(
    col("name"),
    col("salary"),
    (col("salary") * 1.1).alias("salary_with_raise")
).show()

# FILTER / WHERE — filter rows
df.filter(col("salary") > 80000).show()
df.filter(col("department") == "Engineering").show()
df.filter((col("salary") > 80000) & (col("department") == "Engineering")).show()

# GROUP BY — aggregate
df.groupBy("department").agg(
    count("emp_id").alias("headcount"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary_cost")
).show()

# ORDER BY — sort
df.orderBy(col("salary").desc()).show()
df.orderBy("department", col("salary").desc()).show()

# ADD COLUMN
df_with_bonus = df.withColumn(
    "annual_bonus",
    when(col("department") == "Engineering", col("salary") * 0.20)
    .when(col("department") == "Marketing",  col("salary") * 0.15)
    .otherwise(col("salary") * 0.10)
)

# JOIN — combine two DataFrames
dept_budget = spark.createDataFrame([
    ("Engineering", 500000),
    ("Marketing",   300000),
    ("HR",          150000),
], ["department", "budget"])

df_joined = df.join(dept_budget, on="department", how="inner")
df_joined.show()

# DISTINCT — remove duplicates
df.select("department").distinct().show()

# DESCRIBE — quick statistics
df.describe("salary").show()
# +-------+------------------+
# |summary|            salary|
# +-------+------------------+
# |  count|                 6|
# |   mean|          88333.33|
# | stddev|          17200.58|
# |    min|             65000|
# |    max|            110000|
```

---

## 3.4 Side-by-Side Comparison: Same Task in RDD vs DataFrame

**Task: Find the average salary per department, sorted by average salary descending**

```python
# ─────────────────────────────────────────────────────────────────
# RDD APPROACH (verbose, no optimization)
# ─────────────────────────────────────────────────────────────────

data = [
    "Alice,Engineering,95000",
    "Bob,Marketing,75000",
    "Carol,Engineering,110000",
    "Dave,HR,65000",
    "Eve,Engineering,105000",
    "Frank,Marketing,80000",
]

rdd = spark.sparkContext.parallelize(data)

# Step 1: Parse each line
rdd_parsed = rdd.map(lambda line: line.split(","))

# Step 2: Create (department, salary) pairs
rdd_dept_salary = rdd_parsed.map(lambda row: (row[1], int(row[2])))

# Step 3: Group by department and collect all salaries
rdd_grouped = rdd_dept_salary.groupByKey()

# Step 4: Calculate average for each group
rdd_avg = rdd_grouped.map(lambda kv: (kv[0], sum(kv[1]) / len(list(kv[1]))))

# Step 5: Sort by average salary descending
rdd_sorted = rdd_avg.sortBy(lambda kv: kv[1], ascending=False)

# Step 6: Collect and print
for dept, avg_sal in rdd_sorted.collect():
    print(f"{dept}: {avg_sal:.2f}")

# Total lines of code: 6 transformations + parsing
# Optimization: NONE — Spark executes exactly as written

# ─────────────────────────────────────────────────────────────────
# DATAFRAME APPROACH (concise, optimized)
# ─────────────────────────────────────────────────────────────────

df.groupBy("department") \
  .agg(avg("salary").alias("avg_salary")) \
  .orderBy(col("avg_salary").desc()) \
  .show()

# Total: 4 lines
# Optimization: Catalyst rewrites this for maximum efficiency
# Performance: 2-10x faster for same data volume

# OUTPUT (same for both):
# +------------+---------+
# | department |avg_salary
# +------------+---------+
# |Engineering |103333.33|
# |Marketing   |77500.0  |
# |HR          |65000.0  |
# +------------+---------+
```

---

## 3.5 Converting Between RDD and DataFrame

```python
# ─────────────────────────────────────────────────────────────────
# RDD → DataFrame
# ─────────────────────────────────────────────────────────────────

# Method 1: toDF() with column names
rdd_parsed = spark.sparkContext.parallelize([
    ("Alice", "Engineering", 95000),
    ("Bob",   "Marketing",   75000),
])
df = rdd_parsed.toDF(["name", "department", "salary"])

# Method 2: createDataFrame()
df = spark.createDataFrame(rdd_parsed, ["name", "department", "salary"])

# Method 3: With explicit schema
schema = StructType([
    StructField("name",       StringType()),
    StructField("department", StringType()),
    StructField("salary",     IntegerType()),
])
df = spark.createDataFrame(rdd_parsed, schema)

# ─────────────────────────────────────────────────────────────────
# DataFrame → RDD
# ─────────────────────────────────────────────────────────────────

# .rdd property gives you the underlying RDD
rdd_from_df = df.rdd

# Each element is a Row object (not a plain list)
print(rdd_from_df.first())
# Row(name='Alice', department='Engineering', salary=95000)

# Access by name or index
row = rdd_from_df.first()
print(row.name)        # Alice
print(row["name"])     # Alice
print(row[0])          # Alice
```

---

## 3.6 When to Use RDD vs DataFrame

```
USE DATAFRAME (99% of cases):
  ✅ Any ETL pipeline
  ✅ Data aggregations and analysis
  ✅ SQL-equivalent operations
  ✅ Reading/writing files (CSV, Parquet, Delta)
  ✅ Machine learning feature preparation
  ✅ When performance matters

USE RDD (1% of cases — rare special situations):
  ✅ Processing unstructured data where schema doesn't apply
     Example: log files with unpredictable formats
  ✅ Very custom transformations impossible with built-in functions
  ✅ Low-level partition control (mapPartitionsWithIndex)
  ✅ Working with legacy Spark 1.x codebases
  ✅ When you're doing graph processing (GraphX uses RDDs)
  ✅ Certain ML algorithm implementations that require RDDs

NEVER USE RDD WHEN:
  ❌ You can express the same logic with DataFrame operations
  ❌ Performance is a concern (DataFrame is faster)
  ❌ You want readable code (DataFrame is clearer)
```

---

## 3.7 The Catalyst Optimizer — Why DataFrame is Faster

```
YOUR DATAFRAME CODE:
  df.filter(col("dept") == "Engineering")
    .select("name", "salary")
    .groupBy("dept")
    .agg(avg("salary"))

CATALYST OPTIMIZER STEPS:
  Step 1: PARSE    → Create logical plan tree from your code
  Step 2: ANALYZE  → Check column names exist, infer types
  Step 3: OPTIMIZE → Apply 1000+ rewrite rules:
    - Push filter BEFORE groupBy (less data to group)
    - Push projection BEFORE filter (read fewer columns)
    - Select most efficient join strategy
    - Combine multiple transforms into one scan
  Step 4: PLAN     → Create physical execution plan
  Step 5: EXECUTE  → Run with Tungsten (vectorized, native memory)

RDD CODE:
  Spark has no insight into your lambda functions
  It executes EXACTLY what you wrote, in the order you wrote it
  No rewriting, no optimization

REAL PERFORMANCE DIFFERENCE:
  100GB join operation:
    RDD:       45 minutes
    DataFrame: 4 minutes  (same logic, 10x faster)
```

---

## 3.8 Interview Questions

**Q: What is the difference between RDD, DataFrame, and Dataset?**
> RDD is the original low-level distributed data structure — untyped, no schema, no optimization. DataFrame adds a named-column schema and the Catalyst optimizer — queries are automatically optimized. Dataset combines both — type-safe (compile-time checks) DataFrame, available in Scala/Java only. In PySpark, there's no Dataset type — DataFrame is effectively Dataset[Row]. For day-to-day work, always use DataFrames.

**Q: Why is DataFrame faster than RDD for most operations?**
> DataFrames go through the Catalyst optimizer which rewrites the execution plan using 1000+ optimization rules before running — predicate pushdown, column pruning, join reordering, etc. It also uses the Tungsten execution engine which processes data in native memory (off-heap) avoiding JVM garbage collection overhead. RDD executes exactly what you write — Spark can't look inside your lambda functions to optimize them.

**Q: Give a real scenario where you'd use RDD instead of DataFrame.**
> Parsing a log file where each line has a different and unpredictable format — some lines might be JSON, some plain text, some binary. You need custom Python parsing logic that can't be expressed with Spark's built-in functions. Use `textFile()` to get an RDD of strings, then `mapPartitions()` with your custom parser. Another case: computing a custom metric that requires iterating over an entire partition's worth of data as a Python list.
