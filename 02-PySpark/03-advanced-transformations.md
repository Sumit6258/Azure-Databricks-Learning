# Module 02 — PySpark
## Part 3: Advanced Transformations — Arrays, Maps, Structs & Complex Operations

---

## 3.1 Working with Complex Data Types

Modern data sources — APIs, Kafka, Cosmos DB — send nested JSON. Knowing how to flatten, explode, and reshape complex types is a core DE skill.

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Sample nested data representing an e-commerce order
orders_data = [
    (
        "ORD001", "C001", "2024-01-15",
        {"name": "Alice", "city": "Mumbai", "tier": "gold"},
        [{"sku": "SKU01", "qty": 2, "price": 499.0},
         {"sku": "SKU02", "qty": 1, "price": 1299.0}],
        {"express": True, "gift_wrap": False, "notes": "Handle with care"}
    )
]

schema = StructType([
    StructField("order_id",   StringType()),
    StructField("customer_id",StringType()),
    StructField("order_date", StringType()),
    StructField("customer",   StructType([
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("tier", StringType()),
    ])),
    StructField("items", ArrayType(StructType([
        StructField("sku",   StringType()),
        StructField("qty",   IntegerType()),
        StructField("price", DoubleType()),
    ]))),
    StructField("options", MapType(StringType(), StringType())),
])

df = spark.createDataFrame(orders_data, schema)
df.printSchema()
```

---

## 3.2 Struct Operations — Nested Fields

```python
# Access struct fields using dot notation
df.select(
    col("order_id"),
    col("customer.name").alias("customer_name"),
    col("customer.city").alias("customer_city"),
    col("customer.tier").alias("customer_tier"),
).show()

# Update a field inside a struct (requires rebuilding the struct)
from pyspark.sql.functions import struct

df_updated = df.withColumn(
    "customer",
    struct(
        col("customer.name").alias("name"),
        upper(col("customer.city")).alias("city"),   # modified
        col("customer.tier").alias("tier"),
    )
)

# Create a new struct column
df_with_audit = df.withColumn(
    "audit",
    struct(
        current_timestamp().alias("processed_at"),
        lit("v1.0").alias("pipeline_version"),
        lit("prod").alias("environment")
    )
)

# Flatten struct to top-level columns (using * expansion)
df_flat = df.select(
    col("order_id"),
    col("order_date"),
    col("customer.*"),  # Expands all struct fields as separate columns
)
```

---

## 3.3 Array Operations — Explode, Transform, Filter

```python
# ── EXPLODE — one row per array element ──
df_items = df.select(
    col("order_id"),
    col("customer_id"),
    explode(col("items")).alias("item")
).select(
    col("order_id"),
    col("customer_id"),
    col("item.sku").alias("sku"),
    col("item.qty").alias("quantity"),
    col("item.price").alias("unit_price"),
    (col("item.qty") * col("item.price")).alias("line_total")
)

# ── EXPLODE vs POSEXPLODE — include array index ──
df_with_position = df.select(
    col("order_id"),
    posexplode(col("items")).alias("item_position", "item")
).select(
    col("order_id"),
    col("item_position"),  # 0, 1, 2, ...
    col("item.sku"),
    col("item.qty")
)

# ── EXPLODE_OUTER — keep rows with empty/null arrays ──
df_outer = df.select(
    col("order_id"),
    explode_outer(col("items")).alias("item")
)
# Orders with no items still appear, with item=null

# ── ARRAY FUNCTIONS ──
df_array = df.select(
    col("order_id"),
    
    # Size of array
    size(col("items")).alias("item_count"),
    
    # Extract element by index
    col("items")[0].alias("first_item"),
    
    # Check if value exists in array
    array_contains(
        col("items.sku"),  # array of SKUs
        "SKU01"
    ).alias("has_sku01"),
    
    # Sort array
    sort_array(col("items.price")).alias("prices_sorted"),
    
    # Slice (subset)
    slice(col("items"), 1, 2).alias("first_two_items"),  # start=1-based
    
    # Flatten nested arrays
    flatten(col("items.sku")).alias("all_skus"),  # If items had nested arrays
)

# ── AGGREGATE OVER ARRAY WITHOUT EXPLODE ──
df_agg = df.select(
    col("order_id"),
    aggregate(
        col("items"),
        lit(0.0),  # initial value
        lambda acc, item: acc + (item["qty"] * item["price"])  # accumulator
    ).alias("order_total"),
    
    # Filter array elements
    filter(col("items"), lambda item: item["price"] > 500).alias("expensive_items"),
    
    # Transform each element
    transform(col("items"), lambda item: 
        struct(
            item["sku"].alias("sku"),
            (item["qty"] * item["price"]).alias("line_total")
        )
    ).alias("items_with_totals"),
    
    # forall — all elements match a condition
    forall(col("items"), lambda item: item["qty"] > 0).alias("all_positive_qty"),
    
    # exists — any element matches
    exists(col("items"), lambda item: item["sku"] == "SKU01").alias("has_sku01"),
)

# ── COLLECT ITEMS BACK INTO ARRAY (reverse of explode) ──
df_exploded = df.select("order_id", explode("items").alias("item"))

# After processing, reconstruct the array
df_rebuilt = df_exploded \
    .groupBy("order_id") \
    .agg(collect_list("item").alias("items"))
```

---

## 3.4 Map Operations

```python
# Maps (key-value dictionaries) are common in event data and options

df_map = df.select(
    col("order_id"),
    col("options"),
    
    # Get value by key
    col("options")["express"].alias("is_express"),
    map_values(col("options")).alias("all_values"),
    map_keys(col("options")).alias("all_keys"),
    
    # Check if key exists
    map_contains_key(col("options"), "gift_wrap").alias("has_gift_wrap"),
    
    # Size of map
    map_size(col("options")).alias("option_count"),
)

# Explode map into key-value rows
df_map_exploded = df.select(
    col("order_id"),
    explode(col("options")).alias("option_key", "option_value")
)

# Create a map from two arrays of keys/values
df_from_arrays = spark.createDataFrame(
    [("A001", ["k1","k2","k3"], [1, 2, 3])],
    ["id", "keys", "values"]
).withColumn("my_map", map_from_arrays(col("keys"), col("values")))

# Convert struct to map
df_to_map = df.select(
    col("order_id"),
    map_from_entries(
        array(
            struct(lit("city"), col("customer.city")),
            struct(lit("tier"), col("customer.tier"))
        )
    ).alias("customer_map")
)
```

---

## 3.5 String Functions — Complete Reference

```python
from pyspark.sql.functions import (
    length, ltrim, rtrim, trim, lpad, rpad,
    upper, lower, initcap,
    concat, concat_ws, split,
    regexp_extract, regexp_replace, regexp_like,
    substring, substring_index, instr, locate,
    translate, overlay,
    format_string, printf,
    encode, decode,
    sha2, md5, hash, xxhash64,
    soundex, levenshtein,
    sentences, words, ngrams
)

df_str = spark.createDataFrame(
    [(" Alice  Kumar ", "MUMBAI-400001", "pan:ABCDE1234F:active", "hello world foo bar")],
    ["name", "location", "data", "text"]
)

df_str.select(
    # Whitespace and case
    trim(col("name")).alias("trimmed"),
    upper(col("name")).alias("upper"),
    initcap(col("name")).alias("title_case"),  # "Alice  Kumar"
    
    # Padding
    lpad(lit("42"), 8, "0").alias("padded"),      # "00000042"
    rpad(col("name"), 20, "_").alias("rpadded"),
    
    # Splitting and extracting
    split(col("location"), "-").alias("parts"),   # ["MUMBAI", "400001"]
    split(col("location"), "-")[0].alias("city"),
    split(col("location"), "-")[1].alias("pincode"),
    substring(col("location"), 1, 6).alias("city_substr"),
    
    # Regex
    regexp_extract(col("data"), r"pan:([A-Z0-9]+):", 1).alias("pan_number"),
    regexp_replace(col("data"), r":[a-z]+$", "").alias("cleaned_data"),
    regexp_like(col("data"), r"pan:[A-Z]{5}\d{4}[A-Z]").alias("is_pan_format"),
    
    # Locate
    instr(col("location"), "-").alias("dash_position"),  # 7
    locate("400", col("location")).alias("pincode_start"),
    
    # Hashing (for anonymization)
    sha2(col("name"), 256).alias("name_hash"),
    md5(col("name")).alias("name_md5"),
    
    # String similarity
    levenshtein(lit("Mumbai"), lit("Mumbay")).alias("edit_distance"),  # 1
    
    # Concatenation
    concat_ws(" | ", col("name"), col("location")).alias("combined"),
    format_string("%s lives in %s", trim(col("name")), col("location")).alias("sentence"),
    
    # Word operations
    length(col("text")).alias("char_count"),
    size(split(col("text"), " ")).alias("word_count"),
    
).show(truncate=False)
```

---

## 3.6 Date and Time Functions — Complete Reference

```python
from pyspark.sql.functions import (
    current_date, current_timestamp, now,
    to_date, to_timestamp,
    date_format, date_trunc,
    year, month, dayofmonth, dayofweek, dayofyear,
    quarter, weekofyear,
    hour, minute, second,
    date_add, date_sub, add_months, months_between,
    datediff, timestampdiff,
    last_day, next_day, trunc,
    from_unixtime, unix_timestamp, from_utc_timestamp, to_utc_timestamp,
    make_date, make_timestamp,
)

df_dates = spark.createDataFrame(
    [("2024-01-15", "2024-01-15 14:30:45", 1705327845)],
    ["date_str",   "datetime_str",          "epoch_seconds"]
)

df_dates.select(
    # Parsing
    to_date(col("date_str"), "yyyy-MM-dd").alias("parsed_date"),
    to_timestamp(col("datetime_str"), "yyyy-MM-dd HH:mm:ss").alias("parsed_ts"),
    from_unixtime(col("epoch_seconds")).alias("from_epoch"),
    
    # Current
    current_date().alias("today"),
    current_timestamp().alias("now"),
    
    # Extracting parts
    year(col("parsed_date")).alias("year"),
    month(col("parsed_date")).alias("month"),
    dayofmonth(col("parsed_date")).alias("day"),
    dayofweek(col("parsed_date")).alias("day_of_week"),  # 1=Sunday
    quarter(col("parsed_date")).alias("quarter"),
    weekofyear(col("parsed_date")).alias("week"),
    
    # Arithmetic
    date_add(col("parsed_date"), 7).alias("next_week"),
    date_sub(col("parsed_date"), 30).alias("month_ago"),
    add_months(col("parsed_date"), 3).alias("next_quarter"),
    
    # Difference
    datediff(current_date(), col("parsed_date")).alias("days_since"),
    months_between(current_date(), col("parsed_date")).alias("months_since"),
    
    # Formatting
    date_format(col("parsed_date"), "dd/MM/yyyy").alias("uk_format"),
    date_format(col("parsed_date"), "MMMM yyyy").alias("month_year"),  # January 2024
    date_format(col("parsed_date"), "E").alias("day_name"),            # Mon, Tue...
    
    # Truncation (useful for time-based grouping)
    date_trunc("month", col("parsed_date")).alias("month_start"),
    date_trunc("week",  col("parsed_date")).alias("week_start"),
    date_trunc("hour",  col("parsed_ts")).alias("hour_start"),
    
    # Last/Next
    last_day(col("parsed_date")).alias("end_of_month"),
    next_day(col("parsed_date"), "Monday").alias("next_monday"),
    
    # Timezone handling (critical for global apps)
    from_utc_timestamp(col("parsed_ts"), "Asia/Kolkata").alias("ist_time"),
    to_utc_timestamp(col("parsed_ts"),   "Asia/Kolkata").alias("utc_from_ist"),
    
).show(truncate=False)

# ── REAL USE CASE: Business calendar aggregations ──
df_with_calendar = df.select(
    col("txn_date"),
    
    # Fiscal year (April–March for Indian companies)
    when(month(col("txn_date")) >= 4,
         year(col("txn_date"))
    ).otherwise(
         year(col("txn_date")) - 1
    ).alias("fiscal_year"),
    
    # Fiscal quarter
    when(month(col("txn_date")).isin([4,5,6]),   "Q1")
    .when(month(col("txn_date")).isin([7,8,9]),   "Q2")
    .when(month(col("txn_date")).isin([10,11,12]),"Q3")
    .otherwise("Q4").alias("fiscal_quarter"),
    
    # Is weekend?
    dayofweek(col("txn_date")).isin([1, 7]).alias("is_weekend"),
    
    # Days until end of month (for month-end reporting)
    datediff(last_day(col("txn_date")), col("txn_date")).alias("days_to_month_end"),
)
```

---

## 3.7 Pivot and Unpivot

```python
# PIVOT — rows to columns (wide format for BI)
df_txns = spark.createDataFrame([
    ("2024-01", "CARD",      15000.0),
    ("2024-01", "UPI",       8500.0),
    ("2024-01", "NETBANKING",22000.0),
    ("2024-02", "CARD",      18000.0),
    ("2024-02", "UPI",       12000.0),
], ["month", "payment_mode", "total"])

# Pivot: one column per payment_mode
df_pivot = df_txns.groupBy("month") \
    .pivot("payment_mode", ["CARD", "UPI", "NETBANKING"]) \  # Specify values for perf
    .agg(sum("total"))

df_pivot.show()
# +-------+-------+------+-----------+
# |  month|   CARD|   UPI|NETBANKING |
# +-------+-------+------+-----------+
# |2024-01|15000.0|8500.0|  22000.0  |
# |2024-02|18000.0|12000.0|    null  |

# UNPIVOT — columns to rows (stack)
# Use stack() function to unpivot
df_unpivot = df_pivot.select(
    col("month"),
    expr("stack(3, 'CARD', CARD, 'UPI', UPI, 'NETBANKING', NETBANKING) as (payment_mode, total)")
).filter(col("total").isNotNull())
```

---

## 3.8 Handling Nulls — Production Patterns

```python
# ── NULL DETECTION ──
df.filter(col("amount").isNull()).count()
df.filter(col("amount").isNotNull()).count()

# Count nulls per column
null_counts = df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
)
null_counts.show()

# ── NULL FILLING ──
# Fill specific columns
df.fillna({"amount": 0.0, "status": "UNKNOWN", "notes": ""})

# Fill all string columns
df.fillna("UNKNOWN", subset=["status", "category", "region"])

# Fill with column-level defaults
df.withColumn("amount", coalesce(col("amount"), lit(0.0))) \
  .withColumn("status", coalesce(col("status"), lit("PENDING")))

# ── NULL IN JOINS (very tricky!) ──
# NULL != NULL in SQL — rows with null join keys DON'T match
df1 = spark.createDataFrame([(1, None), (2, "A"), (3, "B")], ["id", "key"])
df2 = spark.createDataFrame([(None, "X"), ("A", "Y")], ["key", "value"])

# Regular join — null keys never match
df1.join(df2, on="key", how="left").show()
# id=1 (null key) will have null value — never matches df2's null key row

# Null-safe equality join
df1.join(df2, df1["key"].eqNullSafe(df2["key"]), how="inner").show()
# Now null == null → id=1 matches df2's null row

# ── DROPNA — Remove rows with nulls ──
df.dropna(how="all")                         # Drop only if ALL columns null
df.dropna(how="any")                         # Drop if ANY column null
df.dropna(subset=["txn_id", "amount"])       # Drop if specific cols null
df.dropna(thresh=3)                          # Drop if fewer than 3 non-null values
```

---

## 3.9 Interview Questions — Advanced Transformations

**Q: How do you flatten a deeply nested JSON structure in PySpark?**
> Use `explode()` for array fields to create one row per array element. Use dot notation (`col("parent.child.grandchild")`) to access nested struct fields. Use `col("struct.*")` to expand all struct fields to top-level columns. For deeply nested or dynamic schemas, use `from_json()` with an explicit schema. String columns containing JSON should be parsed with `from_json()` before any field access. For maps, use `explode()` which produces key-value rows.

**Q: What is the difference between `explode()` and `explode_outer()`?**
> `explode(array_col)` drops rows where the array is null or empty — you lose that record entirely. `explode_outer(array_col)` retains rows with null/empty arrays, producing a single row with null values for the array element columns. In production ETL, `explode_outer` is safer because you don't silently lose data — the parent record is still present for auditing.

**Q: How do you perform a running total that resets at the start of each month?**
> Use a window function partitioned by the month: `Window.partitionBy(month("txn_date"), year("txn_date")).orderBy("txn_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)`, then `sum("amount").over(w)`. The partition boundary resets the cumulative sum for each month automatically.

---
*Next: [Part 4 — Data Quality Framework](./04-data-quality-framework.md)*
