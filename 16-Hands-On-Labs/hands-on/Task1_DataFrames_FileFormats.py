# HANDS-ON TASK 1
## Create DataFrames: From Scratch, File Formats & show() vs display()

> **Copy every cell below into a NEW Databricks notebook. Run cell by cell. Read every comment.**

---

## CELL 1 — Setup: Create Sample Data Files First

```python
# ════════════════════════════════════════════════════════════════
# STEP 0: CREATE SAMPLE FILES TO UPLOAD
# Run this cell to create data files in DBFS that we'll read later
# ════════════════════════════════════════════════════════════════

import json

# ── Create a CSV file ─────────────────────────────────────────
csv_content = """emp_id,name,department,salary,hire_date
1,Sumit,Engineering,95000,2020-01-15
2,Priya,Marketing,75000,2019-06-01
3,Rahul,Engineering,110000,2018-03-20
4,Anjali,HR,65000,2021-09-10
5,Vikram,Engineering,105000,2017-11-05
6,Deepika,Marketing,80000,2022-02-28
7,Arjun,Finance,90000,2020-07-15
8,Kavya,Engineering,98000,2021-03-01
9,Rohit,HR,70000,2019-12-20
10,Sneha,Finance,85000,2022-08-10"""

dbutils.fs.put("/FileStore/sample_data/employees.csv", csv_content, overwrite=True)
print("✅ CSV file created at /FileStore/sample_data/employees.csv")

# ── Create a JSON file ─────────────────────────────────────────
json_data = [
    {"txn_id": 1001, "customer_id": "C001", "amount": 1500.00, "status": "SUCCESS", "date": "2024-01-15", "payment_mode": "CARD"},
    {"txn_id": 1002, "customer_id": "C002", "amount": 250.75,  "status": "SUCCESS", "date": "2024-01-15", "payment_mode": "UPI"},
    {"txn_id": 1003, "customer_id": "C001", "amount": 8900.00, "status": "FAILED",  "date": "2024-01-16", "payment_mode": "NETBANKING"},
    {"txn_id": 1004, "customer_id": "C003", "amount": 450.00,  "status": "SUCCESS", "date": "2024-01-16", "payment_mode": "UPI"},
    {"txn_id": 1005, "customer_id": "C002", "amount": 3200.50, "status": "SUCCESS", "date": "2024-01-17", "payment_mode": "CARD"},
    {"txn_id": 1006, "customer_id": "C004", "amount": 125.00,  "status": "PENDING", "date": "2024-01-17", "payment_mode": "UPI"},
    {"txn_id": 1007, "customer_id": "C001", "amount": 9999.99, "status": "SUCCESS", "date": "2024-01-18", "payment_mode": "CARD"},
    {"txn_id": 1008, "customer_id": "C003", "amount": 670.00,  "status": "SUCCESS", "date": "2024-01-18", "payment_mode": "NETBANKING"},
]
dbutils.fs.put("/FileStore/sample_data/transactions.json",
               json.dumps(json_data), overwrite=True)
print("✅ JSON file created at /FileStore/sample_data/transactions.json")

# ── Verify both files exist ────────────────────────────────────
print("\n📂 Files in /FileStore/sample_data/:")
display(dbutils.fs.ls("/FileStore/sample_data/"))
```

---

## CELL 2 — Method 1: DataFrame from Python List (Most Common for Testing)

```python
# ════════════════════════════════════════════════════════════════
# CREATING A DATAFRAME FROM A PYTHON LIST
# This is how you create test data without any files
# ════════════════════════════════════════════════════════════════

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType, BooleanType
)
from pyspark.sql.functions import col, avg, sum, count, when, round

# ── Option A: Just provide column names (types are INFERRED) ──
# AVOID in production — types may be wrong
data_simple = [
    (1, "Sumit",   "Engineering", 95000),
    (2, "Priya",   "Marketing",   75000),
    (3, "Rahul",   "Engineering", 110000),
    (4, "Anjali",  "HR",          65000),
    (5, "Vikram",  "Engineering", 105000),
]

df_simple = spark.createDataFrame(data_simple, ["emp_id", "name", "department", "salary"])

print("Schema (inferred — notice the types):")
df_simple.printSchema()
# root
#  |-- emp_id: long (nullable = true)      ← should be int, but inferred as long
#  |-- name: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: long (nullable = true)

# ── Option B: Explicit Schema (PRODUCTION STANDARD) ──────────
# Always define schema explicitly — never guess types
schema = StructType([
    StructField("emp_id",     IntegerType(), nullable=False),  # Cannot be null
    StructField("name",       StringType(),  nullable=False),  # Cannot be null
    StructField("department", StringType(),  nullable=True),   # Can be null
    StructField("salary",     DoubleType(),  nullable=True),   # Can be null
])

data_typed = [
    (1, "Sumit",   "Engineering", 95000.0),
    (2, "Priya",   "Marketing",   75000.0),
    (3, "Rahul",   "Engineering", 110000.0),
    (4, "Anjali",  "HR",          65000.0),
    (5, "Vikram",  "Engineering", 105000.0),
    (6, "Deepika", "Marketing",   80000.0),
    (7, "Arjun",   "Finance",     90000.0),
    (8, "Kavya",   "Engineering", 98000.0),
]

df_employees = spark.createDataFrame(data_typed, schema)

print("\nSchema (explicit — correct types):")
df_employees.printSchema()
# root
#  |-- emp_id: integer (nullable = false)  ← Correct!
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)

print(f"\nTotal employees: {df_employees.count()}")
print(f"Columns: {df_employees.columns}")
print(f"Partitions: {df_employees.rdd.getNumPartitions()}")
```

---

## CELL 3 — show() vs display() — THE KEY DIFFERENCE

```python
# ════════════════════════════════════════════════════════════════
# show() vs display() — One of the most asked beginner questions
# ════════════════════════════════════════════════════════════════

# ────────────────────────────────────────────────────────────────
# show() — Standard PySpark / Apache Spark method
# ────────────────────────────────────────────────────────────────

# Basic show — prints top 20 rows as plain text table
df_employees.show()
# +------+-------+------------+--------+
# |emp_id|   name|  department|  salary|
# +------+-------+------------+--------+
# |     1|  Sumit| Engineering| 95000.0|
# |     2|  Priya|   Marketing| 75000.0|
# ...

# show() with parameters
df_employees.show(3)                         # Show only 3 rows
df_employees.show(3, truncate=False)         # Show 3 rows, don't truncate long strings
df_employees.show(3, truncate=True)          # Truncate strings > 20 chars (default)
df_employees.show(truncate=50)               # Truncate strings > 50 chars
df_employees.show(5, vertical=True)          # Show as vertical (one column per line)

print("\n--- Vertical format (good for many columns) ---")
df_employees.show(2, vertical=True)
# -RECORD 0------------------
#  emp_id     | 1
#  name        | Sumit
#  department  | Engineering
#  salary      | 95000.0
# -RECORD 1------------------
#  emp_id     | 2
#  name        | Priya
#  department  | Marketing
#  salary      | 75000.0

# ────────────────────────────────────────────────────────────────
# display() — Databricks-specific (ONLY works in Databricks)
# ────────────────────────────────────────────────────────────────

# Rich interactive table — you can sort, filter, paginate in the UI
display(df_employees)
# You'll see a formatted table with:
#   - Click column headers to sort
#   - Pagination (navigate pages)
#   - Search/filter
#   - Can switch to chart view (bar chart, pie chart, etc.)
#   - Download as CSV button
```

---

## CELL 4 — Complete show() vs display() Comparison

```python
# ════════════════════════════════════════════════════════════════
# COMPREHENSIVE COMPARISON TABLE
# ════════════════════════════════════════════════════════════════

comparison_data = [
    ("What it is",        "Standard PySpark method",       "Databricks-ONLY method"),
    ("Output type",       "Plain text table in output",    "Rich interactive HTML table"),
    ("Sorting",           "Not possible",                  "Click column to sort"),
    ("Filtering",         "Not possible",                  "Built-in search/filter"),
    ("Pagination",        "Shows N rows only",             "Navigate all pages"),
    ("Charts",            "Not possible",                  "Switch to bar/pie/line chart"),
    ("Download",          "Not possible",                  "Download as CSV button"),
    ("Works outside ADB", "YES (any Spark)",               "NO (Databricks only)"),
    ("For DataFrames",    "Yes",                           "Yes"),
    ("For files list",    "No (need .toDF() first)",       "Yes — display(dbutils.fs.ls(...))"),
    ("For Pandas df",     "No",                            "Yes — display(pandas_df)"),
    ("In production code","Preferred (no UI dependency)",  "Avoid (breaks outside ADB)"),
    ("For debugging",     "Quick text output",             "Best — interactive exploration"),
]

comp_df = spark.createDataFrame(
    comparison_data,
    ["Feature", "show()", "display()"]
)
display(comp_df)

# ── PRACTICAL RULE OF THUMB ──
print("""
┌─────────────────────────────────────────────────────────────────┐
│  WHEN TO USE WHICH:                                             │
│                                                                 │
│  Use show()    → In production scripts/notebooks               │
│                 → When you need specific N rows                 │
│                 → In code that might run outside Databricks     │
│                 → Quick sanity checks                           │
│                                                                 │
│  Use display() → During data exploration and development       │
│                 → When you want to see charts                   │
│                 → When showing results to business stakeholders │
│                 → For displaying file listings from dbutils     │
└─────────────────────────────────────────────────────────────────┘
""")
```

---

## CELL 5 — Method 2: Read from CSV File

```python
# ════════════════════════════════════════════════════════════════
# READING A CSV FILE INTO A DATAFRAME
# ════════════════════════════════════════════════════════════════

from pyspark.sql.types import *

# ── BASIC READ (quick, uses inferSchema — for development only)
df_csv_basic = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/FileStore/sample_data/employees.csv")

print("Basic CSV read schema (inferred):")
df_csv_basic.printSchema()

# ── PRODUCTION READ (always define schema explicitly)
employee_schema = StructType([
    StructField("emp_id",     IntegerType(), nullable=False),
    StructField("name",       StringType(),  nullable=False),
    StructField("department", StringType(),  nullable=True),
    StructField("salary",     DoubleType(),  nullable=True),
    StructField("hire_date",  StringType(),  nullable=True),  # Read as string, convert later
])

df_csv = spark.read \
    .option("header",    "true") \
    .option("delimiter", ",") \      # Default is comma — explicit is better
    .option("quote",     '"') \      # Handle quoted fields
    .option("escape",    '"') \      # Handle escaped quotes
    .option("nullValue", "NULL") \   # Treat "NULL" string as null
    .option("emptyValue","") \       # Treat empty string as ""
    .option("mode",      "PERMISSIVE") \  # Don't fail on bad rows
    .option("columnNameOfCorruptRecord", "_bad_record") \  # Capture bad rows
    .schema(employee_schema) \
    .csv("/FileStore/sample_data/employees.csv")

print("\nProduction CSV read schema:")
df_csv.printSchema()

# Show data
print("\nFirst 5 rows using show():")
df_csv.show(5)

print("\nAll rows using display() — try clicking column headers to sort:")
display(df_csv)

# Basic statistics
print("\nSalary statistics:")
df_csv.describe("salary").show()

# Department summary
print("\nHeadcount and average salary by department:")
df_csv.groupBy("department") \
    .agg(
        count("emp_id").alias("headcount"),
        round(avg("salary"), 2).alias("avg_salary")
    ) \
    .orderBy("avg_salary", ascending=False) \
    .show()
```

---

## CELL 6 — Method 3: Read from JSON File

```python
# ════════════════════════════════════════════════════════════════
# READING JSON INTO A DATAFRAME
# ════════════════════════════════════════════════════════════════

# ── BASIC READ (schema inferred from JSON structure)
df_json = spark.read \
    .option("multiLine", "true") \  # Our file is one JSON array — needs multiLine
    .json("/FileStore/sample_data/transactions.json")

print("JSON schema (auto-detected from file):")
df_json.printSchema()
# root
#  |-- amount: double (nullable = true)
#  |-- customer_id: string (nullable = true)
#  |-- date: string (nullable = true)
#  |-- payment_mode: string (nullable = true)
#  |-- status: string (nullable = true)
#  |-- txn_id: long (nullable = true)

print("\nAll transactions:")
display(df_json)

# ── ANALYSIS on the JSON data
print("\nTransaction summary by payment mode:")
df_json.groupBy("payment_mode") \
    .agg(
        count("txn_id").alias("num_transactions"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount")
    ) \
    .orderBy("total_amount", ascending=False) \
    .show()

# ── Filter: Only successful transactions
print("\nSuccessful transactions only:")
df_successful = df_json.filter(col("status") == "SUCCESS")
df_successful.show()

print(f"\nSuccessful: {df_successful.count()}")
print(f"Failed/Pending: {df_json.filter(col('status') != 'SUCCESS').count()}")

# ── Add a category column
df_categorized = df_json.withColumn(
    "amount_category",
    when(col("amount") < 500,  "SMALL")
    .when(col("amount") < 2000,"MEDIUM")
    .when(col("amount") < 5000,"LARGE")
    .otherwise("VERY_LARGE")
)
display(df_categorized)
```

---

## CELL 7 — Method 4: Write to Parquet and Read Back

```python
# ════════════════════════════════════════════════════════════════
# WRITE TO PARQUET AND READ BACK
# Parquet is the industry standard for analytical data
# ════════════════════════════════════════════════════════════════

# Write the employees DataFrame to Parquet
df_employees.write \
    .mode("overwrite") \
    .parquet("/FileStore/sample_data/employees_parquet/")

print("✅ Written to Parquet")
print("\nFiles created:")
display(dbutils.fs.ls("/FileStore/sample_data/employees_parquet/"))

# Read it back
df_parquet = spark.read.parquet("/FileStore/sample_data/employees_parquet/")

print("\nParquet schema (preserved from write — no inferSchema needed):")
df_parquet.printSchema()
# Schema is stored inside the Parquet file itself!
# No need to specify schema when reading Parquet

display(df_parquet)
```

---

## CELL 8 — Method 5: Write to Delta and Read Back

```python
# ════════════════════════════════════════════════════════════════
# WRITE TO DELTA AND READ BACK
# Delta is the enterprise standard — ALWAYS use Delta in production
# ════════════════════════════════════════════════════════════════

# Write to Delta format
df_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/FileStore/sample_data/employees_delta/")

print("✅ Written to Delta")

# Read back
df_delta = spark.read.format("delta").load("/FileStore/sample_data/employees_delta/")

print("\nDelta table data:")
display(df_delta)

# Delta gives you transaction history!
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/FileStore/sample_data/employees_delta/")
print("\nDelta transaction history:")
display(dt.history())

# Time travel — read old version
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/FileStore/sample_data/employees_delta/")
print(f"\nVersion 0 has {df_v0.count()} rows")

# Add some rows and write again
new_employees = spark.createDataFrame([
    (9,  "Rohit", "HR",          70000.0),
    (10, "Sneha", "Finance",     85000.0),
], schema)

new_employees.write.format("delta").mode("append") \
    .save("/FileStore/sample_data/employees_delta/")

print("✅ Appended 2 more employees")

df_v1 = spark.read.format("delta").load("/FileStore/sample_data/employees_delta/")
print(f"Current version has {df_v1.count()} rows")
display(df_v1)
```

---

## CELL 9 — Register as a Table and Query with SQL

```python
# ════════════════════════════════════════════════════════════════
# REGISTER DATAFRAME AS A TEMP VIEW → QUERY WITH SQL
# Temp views last for the duration of your Spark session
# ════════════════════════════════════════════════════════════════

# Create a temporary view (session-scoped, no disk storage)
df_employees.createOrReplaceTempView("temp_employees")

# Now you can use SQL!
result = spark.sql("""
    SELECT
        department,
        COUNT(*)                    AS headcount,
        ROUND(AVG(salary), 2)       AS avg_salary,
        MAX(salary)                 AS max_salary,
        MIN(salary)                 AS min_salary
    FROM temp_employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")

print("Department summary via SQL:")
display(result)

# Create a global temp view (accessible across notebooks in same cluster)
df_employees.createOrReplaceGlobalTempView("global_employees")

# Access with special database prefix: global_temp
global_result = spark.sql("SELECT * FROM global_temp.global_employees WHERE salary > 90000")
display(global_result)

# Save as permanent table (requires Unity Catalog or Hive)
df_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.employees_permanent")

print("\n✅ Saved as permanent table: default.employees_permanent")
print("This table survives notebook restarts and cluster terminations!")
```

---

## CELL 10 — Key DataFrame Operations Summary

```python
# ════════════════════════════════════════════════════════════════
# ESSENTIAL DATAFRAME OPERATIONS — Quick Reference
# ════════════════════════════════════════════════════════════════

from pyspark.sql.functions import *

df = df_employees  # Use our employee DataFrame

print("=" * 60)
print("1. SCHEMA AND METADATA")
print("=" * 60)
df.printSchema()                    # Tree view of schema
print(f"Columns:    {df.columns}")  # ['emp_id', 'name', ...]
print(f"Row count:  {df.count()}")  # 8
print(f"Partitions: {df.rdd.getNumPartitions()}")

print("\n" + "=" * 60)
print("2. VIEWING DATA")
print("=" * 60)
df.show(3)                          # First 3 rows
df.show(3, truncate=False)          # Without truncation
df.show(3, vertical=True)           # Vertical format
display(df)                         # Rich interactive table (Databricks only)
df.first()                          # Get first row as Row object
df.head(3)                          # Get first 3 rows as list
df.take(3)                          # Same as head

print("\n" + "=" * 60)
print("3. SELECTING COLUMNS")
print("=" * 60)
df.select("name", "department").show(3)
df.select(col("name"), col("salary") * 1.1).show(3)
df.selectExpr("name", "salary * 1.1 as new_salary").show(3)

print("\n" + "=" * 60)
print("4. FILTERING ROWS")
print("=" * 60)
df.filter(col("salary") > 90000).show()
df.filter("salary > 90000").show()    # SQL-style string filter
df.where(col("department") == "Engineering").show()

print("\n" + "=" * 60)
print("5. ADDING/MODIFYING COLUMNS")
print("=" * 60)
df.withColumn("salary_in_k", col("salary") / 1000) \
  .withColumn("level", when(col("salary") >= 100000, "Senior")
                       .when(col("salary") >= 80000,  "Mid")
                       .otherwise("Junior")) \
  .show()

print("\n" + "=" * 60)
print("6. AGGREGATIONS")
print("=" * 60)
df.groupBy("department").agg(
    count("emp_id").alias("headcount"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary")
).show()

print("\n" + "=" * 60)
print("7. SORTING")
print("=" * 60)
df.orderBy("salary").show(3)
df.orderBy(col("salary").desc()).show(3)
df.orderBy("department", col("salary").desc()).show(5)

print("\n" + "=" * 60)
print("8. NULL HANDLING")
print("=" * 60)
df.filter(col("salary").isNull()).show()    # Rows with null salary
df.fillna({"salary": 0}).show(3)            # Replace nulls with 0
df.dropna(subset=["salary"]).show(3)        # Drop rows where salary is null

print("\n" + "=" * 60)
print("9. DEDUPLICATION")
print("=" * 60)
df.select("department").distinct().show()   # Unique departments
df.dropDuplicates(["department"]).show()    # Keep first of each dept

print("\n" + "=" * 60)
print("10. STATISTICS")
print("=" * 60)
df.describe("salary").show()               # count, mean, std, min, max
df.summary().show()                         # More stats (quartiles too)
```

