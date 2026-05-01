# Module 14 — Cheatsheets
## PySpark + Databricks + Delta Lake — Quick Reference

> Print this, pin it, keep it open during development.

---

## 🔥 PySpark DataFrame API Cheatsheet

```python
# ══════════════════════════════════════════════════════════════
# READING DATA
# ══════════════════════════════════════════════════════════════

spark.read.format("delta").load("/path/")
spark.read.format("parquet").load("/path/")
spark.read.option("header","true").schema(schema).csv("/path/*.csv")
spark.read.json("/path/").option("multiLine","true")
spark.read.format("jdbc").option("url","jdbc:...").option("dbtable","...").load()
spark.table("catalog.schema.table")
spark.sql("SELECT * FROM table")

# ══════════════════════════════════════════════════════════════
# WRITING DATA
# ══════════════════════════════════════════════════════════════

df.write.format("delta").mode("append").save("/path/")
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save()
df.write.format("delta").mode("overwrite").option("replaceWhere","date='2024-01-15'").save()
df.write.format("delta").saveAsTable("catalog.schema.table")
df.coalesce(10).write.format("delta").mode("append").partitionBy("date").save("/path/")

# ══════════════════════════════════════════════════════════════
# SELECT & FILTER
# ══════════════════════════════════════════════════════════════

df.select("col1", "col2")
df.select(col("col1"), col("col2").alias("c2"))
df.select("*", (col("a") + col("b")).alias("sum"))
df.selectExpr("col1", "col1 + col2 as total", "UPPER(col3) as upper_col")

df.filter(col("amount") > 0)
df.filter((col("a") > 0) & (col("b").isNotNull()))
df.filter(col("status").isin(["A","B","C"]))
df.filter(~col("status").isin(["X","Y"]))
df.filter(col("name").startsWith("A"))
df.filter(col("email").rlike(r"^[\w.]+@[\w.]+\.\w+$"))
df.filter(col("amount").between(100, 1000))

# ══════════════════════════════════════════════════════════════
# ADDING / MODIFYING COLUMNS
# ══════════════════════════════════════════════════════════════

df.withColumn("new_col", col("a") * 2)
df.withColumn("flag", when(col("a") > 0, "POS").when(col("a") < 0, "NEG").otherwise("ZERO"))
df.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd"))
df.withColumn("ts",   to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
df.withColumn("year", year(col("date")))
df.withColumn("now",  current_timestamp())
df.withColumn("src",  lit("ONLINE"))
df.withColumn("hash", sha2(col("email"), 256))
df.withColumnRenamed("old", "new")
df.drop("col1", "col2")

# ══════════════════════════════════════════════════════════════
# AGGREGATIONS
# ══════════════════════════════════════════════════════════════

df.groupBy("a","b").agg(
    count("id").alias("cnt"),
    countDistinct("id").alias("unique_cnt"),
    sum("amount").alias("total"),
    avg("amount").alias("average"),
    max("amount").alias("maximum"),
    min("amount").alias("minimum"),
    stddev("amount").alias("std"),
    collect_list("item").alias("items"),
    collect_set("category").alias("categories"),
    first("name").alias("first_name"),
    approx_count_distinct("id", 0.05).alias("approx_cnt"),
    count(when(col("status")=="SUCCESS", 1)).alias("success_cnt"),
    sum(when(col("amount")>1000, col("amount")).otherwise(0)).alias("high_val_sum"),
)

df.agg(sum("amount"), max("date"), count("*"))  # Global aggregation

# ══════════════════════════════════════════════════════════════
# JOINS
# ══════════════════════════════════════════════════════════════

df1.join(df2, on="key",           how="inner")
df1.join(df2, on=["k1","k2"],     how="left")
df1.join(df2, df1.k == df2.id,    how="right")
df1.join(df2, on="key",           how="full")
df1.join(df2, on="key",           how="left_semi")  # Filter, no df2 cols
df1.join(df2, on="key",           how="left_anti")  # Keys NOT in df2
df1.join(broadcast(df2), on="key", how="inner")     # Broadcast join
df1.crossJoin(df2)                                  # Cartesian (careful!)

# ══════════════════════════════════════════════════════════════
# WINDOW FUNCTIONS
# ══════════════════════════════════════════════════════════════

from pyspark.sql import Window

w_part  = Window.partitionBy("customer_id").orderBy(col("date").desc())
w_range = Window.partitionBy("c").orderBy("date").rowsBetween(-6, 0)  # 7-row rolling
w_unb   = Window.partitionBy("c").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("rank",         rank().over(w_part))
df.withColumn("row_num",      row_number().over(w_part))
df.withColumn("dense_rank",   dense_rank().over(w_part))
df.withColumn("lag1",         lag("amount", 1).over(w_part))
df.withColumn("lead1",        lead("amount", 1).over(w_part))
df.withColumn("running_sum",  sum("amount").over(w_unb))
df.withColumn("rolling_avg",  avg("amount").over(w_range))
df.withColumn("quartile",     ntile(4).over(w_part))

# Get latest record per key (most common pattern)
df.withColumn("rn", row_number().over(w_part)).filter(col("rn")==1).drop("rn")

# ══════════════════════════════════════════════════════════════
# STRING FUNCTIONS
# ══════════════════════════════════════════════════════════════

trim(col("name"))          ; ltrim() ; rtrim()
upper(col("s"))            ; lower() ; initcap()
length(col("s"))
concat(col("a"), lit("-"), col("b"))
concat_ws("|", col("a"), col("b"), col("c"))
split(col("s"), "-")                   # Returns array
split(col("s"), "-")[0]               # First element
substring(col("s"), 1, 5)             # Start 1-indexed, length 5
regexp_extract(col("s"), r"(\d+)", 1) # Group 1
regexp_replace(col("s"), r"[^A-Z]", "")
regexp_like(col("s"), r"^\d{10}$")    # Returns boolean
instr(col("s"), "search")             # Position (1-indexed), 0=not found
lpad(col("num").cast("string"), 8, "0")
format_string("%.2f", col("amount"))

# ══════════════════════════════════════════════════════════════
# DATE FUNCTIONS
# ══════════════════════════════════════════════════════════════

current_date()          ; current_timestamp()
to_date(col("s"), "yyyy-MM-dd")
to_timestamp(col("s"), "yyyy-MM-dd HH:mm:ss")
date_format(col("d"), "dd/MM/yyyy")
year(col("d"))    ; month() ; dayofmonth() ; dayofweek() ; quarter() ; weekofyear()
date_add(col("d"), 7)   ; date_sub(col("d"), 30)
add_months(col("d"), 3) ; months_between(col("d1"), col("d2"))
datediff(col("d1"), col("d2"))           # days difference
last_day(col("d"))                       # last day of month
date_trunc("month", col("d"))            # first day of month
from_unixtime(col("epoch"))              # Unix epoch → timestamp
unix_timestamp(col("ts"))                # timestamp → epoch
from_utc_timestamp(col("ts"), "Asia/Kolkata")

# ══════════════════════════════════════════════════════════════
# NULL HANDLING
# ══════════════════════════════════════════════════════════════

col("a").isNull()       ; col("a").isNotNull()
col("a").eqNullSafe(col("b"))    # null-safe equality
coalesce(col("a"), col("b"), lit(0))   # First non-null
df.fillna({"a": 0, "b": "UNKNOWN"})
df.dropna(how="any", subset=["a","b"])
df.dropna(thresh=3)    # Keep rows with at least 3 non-null values

# ══════════════════════════════════════════════════════════════
# ARRAY / STRUCT / MAP
# ══════════════════════════════════════════════════════════════

explode(col("arr"))                          # One row per element
explode_outer(col("arr"))                    # Keep null/empty arrays
posexplode(col("arr"))                       # With index
size(col("arr"))                             # Array length
array_contains(col("arr"), "val")
sort_array(col("arr"))
slice(col("arr"), 1, 3)                      # Start 1-based, length 3
flatten(col("nested_arr"))
filter(col("arr"), lambda x: x > 0)
transform(col("arr"), lambda x: x * 2)
aggregate(col("arr"), lit(0), lambda acc,x: acc + x)
exists(col("arr"), lambda x: x > 100)
forall(col("arr"), lambda x: x > 0)
col("struct_col.nested_field")
col("struct_col.*")                          # Expand all struct fields
map_keys(col("m"))    ; map_values(col("m"))
map_contains_key(col("m"), "key")
col("map_col")["key"]                        # Access map value

# ══════════════════════════════════════════════════════════════
# SCHEMA / METADATA
# ══════════════════════════════════════════════════════════════

df.printSchema()
df.schema         # StructType object
df.dtypes         # List of (column, type) tuples
df.columns        # List of column names
df.count()
df.rdd.getNumPartitions()
df.explain()      ; df.explain(True)
df.show(n=20, truncate=False, vertical=True)
display(df)       # Databricks-specific rich display

# ══════════════════════════════════════════════════════════════
# PARTITIONING
# ══════════════════════════════════════════════════════════════

df.repartition(200)                    # Full shuffle, N partitions
df.repartition(200, col("customer_id")) # Partition by column
df.coalesce(10)                        # Reduce partitions (no shuffle)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ══════════════════════════════════════════════════════════════
# CACHING
# ══════════════════════════════════════════════════════════════

df.cache()                             # Memory + disk
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()                             # Trigger materialization
df.unpersist()                         # Release
spark.catalog.clearCache()             # Clear all caches
```

---

## 🔥 Delta Lake Cheatsheet

```sql
-- CREATE
CREATE TABLE t USING DELTA LOCATION '/path/' PARTITIONED BY (date);
CREATE TABLE t USING DELTA CLUSTER BY (customer_id);  -- Liquid Clustering

-- ALTER
ALTER TABLE t ADD COLUMN new_col STRING;
ALTER TABLE t ADD CONSTRAINT chk CHECK (amount > 0);
ALTER TABLE t SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- MERGE
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED AND s.status = 'DELETED' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- TIME TRAVEL
SELECT * FROM t VERSION AS OF 5;
SELECT * FROM t TIMESTAMP AS OF '2024-01-15';
RESTORE TABLE t TO VERSION AS OF 5;

-- MAINTENANCE
OPTIMIZE t ZORDER BY (customer_id);
VACUUM t RETAIN 720 HOURS;
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;

-- INFO
DESCRIBE DETAIL t;
DESCRIBE HISTORY t;
SHOW TBLPROPERTIES t;

-- CDF
SELECT * FROM table_changes('t', 5);          -- From version 5
SELECT * FROM table_changes('t', null, 10);  -- Up to version 10
```

---

## 🔥 Databricks Magic Commands

```python
%python   # Switch to Python
%sql      # Run SQL in Python notebook
%scala    # Switch to Scala
%r        # Switch to R
%sh       # Shell command (driver node)
%fs       # DBFS operations (alias for dbutils.fs)
%run ./path/notebook          # Run another notebook inline
%md       # Markdown documentation cell
%pip install package          # Install package on cluster
%conda install package        # Conda install

%fs ls /mnt/bronze/
%fs cp /src /dst
%fs rm /path --recurse true
```

---

## 🔥 dbutils Cheatsheet

```python
# FILE SYSTEM
dbutils.fs.ls("/mnt/")
dbutils.fs.mkdirs("/mnt/new/")
dbutils.fs.cp("/src", "/dst", recurse=True)
dbutils.fs.mv("/old", "/new")
dbutils.fs.rm("/path", recurse=True)
dbutils.fs.head("/file.txt")
dbutils.fs.mounts()

# SECRETS
dbutils.secrets.get(scope="scope-name", key="secret-key")
dbutils.secrets.list("scope-name")
dbutils.secrets.listScopes()

# WIDGETS
dbutils.widgets.text("name", "default", "Label")
dbutils.widgets.dropdown("env", "dev", ["dev","test","prod"])
dbutils.widgets.get("name")
dbutils.widgets.removeAll()

# NOTEBOOK
dbutils.notebook.run("path", timeout=300, arguments={"k":"v"})
dbutils.notebook.exit("return_value")

# JOB TASK VALUES
dbutils.jobs.taskValues.set(key="count", value="1000")
dbutils.jobs.taskValues.get(taskKey="upstream_task", key="count", default="0")
```

---

## 🔥 Spark Configuration Cheatsheet

```python
# READ CONFIG
spark.conf.get("key")
spark.sparkContext.getConf().getAll()

# SET CONFIG
spark.conf.set("spark.sql.shuffle.partitions",              "200")
spark.conf.set("spark.sql.adaptive.enabled",                "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",       "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",      str(50*1024*1024))
spark.conf.set("spark.databricks.delta.preview.enabled",    "true")
spark.conf.set("spark.databricks.photon.enabled",           "true")
spark.conf.set("spark.sql.parquet.compression.codec",       "zstd")

# ADAPTIVE QUERY EXECUTION (AQE) — enable all
spark.conf.set("spark.sql.adaptive.enabled",                      "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",   "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",             "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled",   "true")
```

---

## 🔥 Unity Catalog Quick Reference

```sql
-- CATALOG HIERARCHY
SHOW CATALOGS;
SHOW SCHEMAS IN catalog;
SHOW TABLES IN catalog.schema;
USE CATALOG prod_catalog;
USE SCHEMA silver;

-- 3-LEVEL NAMESPACE
SELECT * FROM prod_catalog.silver.transactions;

-- GRANTS
GRANT USE CATALOG ON CATALOG cat TO `group`;
GRANT USE SCHEMA  ON SCHEMA cat.schema TO `group`;
GRANT SELECT ON TABLE cat.schema.table TO `user`;
GRANT SELECT, MODIFY ON ALL TABLES IN SCHEMA cat.schema TO `group`;
REVOKE SELECT ON TABLE t FROM `group`;
SHOW GRANTS ON TABLE t;
SHOW GRANTS TO `group`;

-- TAGS
ALTER TABLE t ALTER COLUMN email SET TAGS ('pii'='true');
ALTER TABLE t ADD ROW FILTER fn ON (region);
ALTER TABLE t ALTER COLUMN pan SET MASK masking_fn;
```

---

## 🔥 Common Patterns Quick Reference

```python
# DEDUP — keep latest
Window.partitionBy("key").orderBy(col("updated_at").desc())
df.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")

# INCREMENTAL — read only new
df.filter(col("updated_at") > last_watermark)

# PARTITION OVERWRITE — replace one partition
df.write.format("delta").mode("overwrite").option("replaceWhere", "date='2024-01-15'").save()

# NULL SAFE JOIN
df1.join(df2, df1.key.eqNullSafe(df2.key))

# BROADCAST JOIN
df_large.join(broadcast(df_small), "key")

# CONDITIONAL AGGREGATION
count(when(col("status")=="SUCCESS", 1)).alias("success_count")
sum(when(col("amount")>1000, col("amount")).otherwise(0)).alias("high_value")

# FLATTEN STRUCT
df.select(col("nested.*"))

# EXPLODE ARRAY + KEEP PARENT
df.withColumn("item", explode("items")).select("order_id", "item.*")

# RUNNING TOTAL PER GROUP
Window.partitionBy("c").orderBy("d").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("cumsum", sum("amount").over(w))

# TOP N PER GROUP
Window.partitionBy("category").orderBy(col("amount").desc())
df.withColumn("rank", row_number().over(w)).filter(col("rank") <= 3)

# PIVOT
df.groupBy("month").pivot("category", ["A","B","C"]).agg(sum("amount"))

# STRINGIFY NULL SAFELY
coalesce(col("name").cast("string"), lit("UNKNOWN"))
```

---

## 🔥 Performance Quick Wins

```
PROBLEM                     SOLUTION
────────────────────────────────────────────────────────────────
Slow query on large table   → Add filter before join, use partition pruning
One task runs 100x longer   → Data skew: salt the hot key or enable AQE
OOM errors on executors     → Increase shuffle.partitions, reduce partition size
200 output files per day    → coalesce(10) before write
Query reads all columns     → SELECT only needed cols (column pruning)
Shuffle too large           → Broadcast small table, bucket large tables
Slow string operations      → Replace UDFs with built-in regexp_ functions
Re-reading same data        → df.cache() + df.count() to materialize
Join on string keys slow    → Cast to integer if possible
```
