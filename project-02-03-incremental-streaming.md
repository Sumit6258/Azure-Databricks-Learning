# Project 02 — Incremental Data Pipeline
## E-Commerce Product Catalog Sync with SCD Type 2

> **Scenario:** An e-commerce platform needs to sync product catalog changes from an operational database to their analytics layer, preserving full history of price/inventory changes.

---

## Architecture

```
Source DB (PostgreSQL)
  ├── products table (10M rows, updated frequently)
  └── Change Tracking enabled

Pipeline:
  [Every 15 minutes]
  PostgreSQL CDC / High-Watermark Extract
      → ADLS Bronze (incremental parquet files)
      → Silver (SCD Type 2 Delta table — full history)
      → Gold (Current state for BI + Price history for analytics)
```

---

## Full Implementation

### `incremental_ingest.py` — Watermark-Based Extraction

```python
# ─────────────────────────────────────────────────────────────────────
# INCREMENTAL INGESTION USING HIGH-WATERMARK PATTERN
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *
from delta.tables import DeltaTable

WATERMARK_TABLE = "metadata.pipeline_watermarks"
PIPELINE_NAME   = "product_catalog_sync"
BRONZE_PATH     = f"{BASE['bronze']}/products/"
SILVER_PATH     = f"{BASE['silver']}/products_scd2/"

# ── Step 1: Read last watermark ──
def get_last_watermark(pipeline_name: str) -> str:
    """Get the last successfully processed timestamp."""
    try:
        wm_df = spark.sql(f"""
            SELECT watermark_value 
            FROM {WATERMARK_TABLE}
            WHERE pipeline_name = '{pipeline_name}'
        """)
        if wm_df.count() > 0:
            return wm_df.first()["watermark_value"]
    except:
        pass
    return "1970-01-01 00:00:00"  # First run: process all history

last_watermark = get_last_watermark(PIPELINE_NAME)
print(f"📅 Last watermark: {last_watermark}")

# ── Step 2: Extract only changed records from source ──
jdbc_url = "jdbc:postgresql://prod-db.company.com:5432/ecommerce"

df_changes = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", dbutils.secrets.get("kv-scope", "db-user")) \
    .option("password", dbutils.secrets.get("kv-scope", "db-password")) \
    .option("numPartitions", 8) \
    .option("partitionColumn", "product_id") \
    .option("lowerBound", 1) \
    .option("upperBound", 10000000) \
    .option("dbtable", f"""
        (SELECT * FROM products 
         WHERE updated_at > '{last_watermark}'
         ORDER BY updated_at) AS incremental_data
    """) \
    .load()

new_watermark_value = df_changes.agg(max("updated_at")).first()[0]

if df_changes.count() == 0:
    print("✅ No changes since last run")
    dbutils.notebook.exit("SKIPPED:no_changes")

print(f"📊 Found {df_changes.count():,} changed records")

# ── Step 3: Write to Bronze ──
run_timestamp = current_timestamp()

df_bronze = df_changes \
    .withColumn("_extracted_at", current_timestamp()) \
    .withColumn("_watermark",    lit(str(last_watermark))) \
    .withColumn("_batch_id",     lit(f"{PIPELINE_NAME}_{BATCH_DATE}"))

df_bronze.write.format("delta").mode("append").save(BRONZE_PATH)

# ── Step 4: SCD Type 2 MERGE to Silver ──
# SCD Type 2: When a product's price/attributes change,
# close the old record and insert a new one, preserving history

# Prepare source with change detection
df_source = df_bronze.select(
    "product_id", "product_name", "category", "brand",
    "price", "cost_price", "inventory_qty", "status",
    "_extracted_at"
)

target = DeltaTable.forPath(spark, SILVER_PATH)

# Step A: Close old records that have changed
(target.alias("tgt")
 .merge(
     df_source.alias("src"),
     """tgt.product_id = src.product_id 
        AND tgt.is_current = true
        AND (tgt.price != src.price OR tgt.status != src.status)"""
 )
 .whenMatchedUpdate(set={
     "is_current": "false",
     "valid_to":   "src._extracted_at",
     "updated_at": "current_timestamp()"
 })
 .execute()
)

# Step B: Insert new records (new versions + brand new products)
existing_current = spark.read.format("delta").load(SILVER_PATH) \
    .filter(col("is_current") == True) \
    .select("product_id")

df_new_versions = df_source \
    .join(existing_current, on="product_id", how="left_anti") \
    .withColumn("is_current",  lit(True)) \
    .withColumn("valid_from",  col("_extracted_at")) \
    .withColumn("valid_to",    lit(None).cast("timestamp")) \
    .withColumn("created_at",  current_timestamp()) \
    .withColumn("updated_at",  current_timestamp()) \
    .drop("_extracted_at")

df_new_versions.write.format("delta").mode("append").save(SILVER_PATH)

print(f"✅ SCD2 MERGE complete")
print(f"   New versions created: {df_new_versions.count():,}")

# ── Step 5: Update watermark ──
new_wm_df = spark.createDataFrame([
    (PIPELINE_NAME, str(new_watermark_value), BATCH_DATE)
], ["pipeline_name", "watermark_value", "last_run_date"])

DeltaTable.forPath(spark, f"{BASE['silver']}/pipeline_watermarks/") \
    .alias("tgt") \
    .merge(new_wm_df.alias("src"), "tgt.pipeline_name = src.pipeline_name") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(f"📅 Watermark updated to: {new_watermark_value}")
```

### Query SCD2 Data

```python
# Current state of all products
current_products = spark.read.format("delta").load(SILVER_PATH) \
    .filter(col("is_current") == True)

# Full price history for a specific product
product_history = spark.read.format("delta").load(SILVER_PATH) \
    .filter(col("product_id") == "P12345") \
    .orderBy("valid_from")

product_history.select(
    "product_id", "product_name", "price",
    "valid_from", "valid_to", "is_current"
).show(truncate=False)

# What was the price on a specific date? (Point-in-time query)
def get_product_price_on_date(product_id: str, as_of_date: str):
    return spark.read.format("delta").load(SILVER_PATH) \
        .filter(
            (col("product_id") == product_id) &
            (col("valid_from") <= lit(as_of_date)) &
            ((col("valid_to") > lit(as_of_date)) | col("valid_to").isNull())
        ) \
        .select("product_id", "product_name", "price", "valid_from")

price_on_jan1 = get_product_price_on_date("P12345", "2024-01-01")
```

---

---

# Project 03 — Real-Time Streaming Pipeline
## IoT Sensor Data Processing with Auto Loader + Structured Streaming

> **Scenario:** A logistics company needs to process real-time GPS and temperature sensor data from 50,000 delivery vehicles, detecting anomalies and updating delivery status in near real-time.

---

## Architecture

```
IoT Devices (50K vehicles)
    │  GPS + temp readings every 30 seconds
    ▼
Azure Event Hub ──→ ADLS Landing Zone (JSON files)
    │                      │
    │                      ▼
    │              Auto Loader (Structured Streaming)
    │                      │
    │                 Bronze Delta (append streaming)
    │                      │
    │              Stream Processing
    │                ├── Windowed Aggregations
    │                ├── Anomaly Detection
    │                └── State Management
    │                      │
    │                 Silver Delta (upsert)
    │                      │
    │                 Gold Delta (real-time dashboard)
    └──────────────────────┘
                    ↑
             Checkpointing
           (exactly-once semantics)
```

---

## Full Implementation

### `01_auto_loader_bronze.py` — Streaming Ingestion

```python
# ─────────────────────────────────────────────────────────────────────
# AUTO LOADER: Continuously ingest new sensor files
# Exactly-once processing with checkpointing
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Sensor event schema
SENSOR_SCHEMA = StructType([
    StructField("device_id",    StringType(),  True),
    StructField("vehicle_id",   StringType(),  True),
    StructField("timestamp",    LongType(),    True),   # Unix epoch ms
    StructField("latitude",     DoubleType(),  True),
    StructField("longitude",    DoubleType(),  True),
    StructField("speed_kmh",    DoubleType(),  True),
    StructField("temperature_c",DoubleType(),  True),
    StructField("fuel_level_pct",DoubleType(), True),
    StructField("engine_status",StringType(),  True),
    StructField("cargo_weight_kg",DoubleType(),True),
])

LANDING_PATH    = "abfss://landing@prodlake.dfs.core.windows.net/sensor-data/"
BRONZE_PATH     = "abfss://bronze@prodlake.dfs.core.windows.net/sensor-events/"
CHECKPOINT_PATH = "abfss://checkpoints@prodlake.dfs.core.windows.net/sensor-bronze/"

# ── AUTO LOADER: The smarter way to ingest streaming files ──
# Auto Loader automatically:
# - Discovers new files (no manual listing)
# - Handles exactly-once semantics
# - Scales to millions of files
# - Handles schema evolution

df_stream = (
    spark.readStream
        .format("cloudFiles")               # ← Auto Loader format
        .option("cloudFiles.format", "json") # ← Source file format
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")  # Schema inference tracking
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle new fields
        .option("maxFilesPerTrigger", 1000)  # Batch size (backfill control)
        .schema(SENSOR_SCHEMA)
        .load(LANDING_PATH)
)

# Add processing metadata
df_enriched = df_stream \
    .withColumn("event_timestamp", (col("timestamp") / 1000).cast("timestamp")) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("event_date", to_date(col("event_timestamp"))) \
    .withColumn("source_file", input_file_name())

# Write to Bronze Delta (append-only streaming write)
bronze_query = (
    df_enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")  # Micro-batch every 30s
        .start(BRONZE_PATH)
)

print(f"🚀 Auto Loader streaming started")
print(f"   Source:     {LANDING_PATH}")
print(f"   Target:     {BRONZE_PATH}")
print(f"   Checkpoint: {CHECKPOINT_PATH}")

# In production, this runs continuously. 
# For notebook dev: bronze_query.awaitTermination(timeout=300)
```

### `02_stream_processing_silver.py` — Windowed Aggregations

```python
# ─────────────────────────────────────────────────────────────────────
# STRUCTURED STREAMING: Process Bronze events → Silver aggregations
# Window functions, anomaly detection, stateful processing
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *
from pyspark.sql.types import *

BRONZE_PATH      = "abfss://bronze@prodlake.dfs.core.windows.net/sensor-events/"
SILVER_PATH      = "abfss://silver@prodlake.dfs.core.windows.net/vehicle-metrics/"
CHECKPOINT_PATH  = "abfss://checkpoints@prodlake.dfs.core.windows.net/sensor-silver/"

# ── Read Bronze stream ──
df_bronze = (
    spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")   # Change Data Feed — only process new events
        .option("startingVersion", "latest")
        .load(BRONZE_PATH)
)

# ── Anomaly Detection ──
df_anomalies = df_bronze \
    .withColumn("is_speed_anomaly",    col("speed_kmh") > 120) \
    .withColumn("is_temp_anomaly",     col("temperature_c") > 45) \
    .withColumn("is_fuel_critical",    col("fuel_level_pct") < 10) \
    .withColumn("is_engine_issue",     col("engine_status") != "RUNNING") \
    .withColumn(
        "alert_level",
        when(col("is_speed_anomaly") | col("is_engine_issue"), "CRITICAL")
        .when(col("is_temp_anomaly") | col("is_fuel_critical"), "WARNING")
        .otherwise("NORMAL")
    )

# ── Windowed Aggregations (5-minute windows with 1-minute slide) ──
df_windowed = df_anomalies \
    .withWatermark("event_timestamp", "10 minutes") \  # Late data tolerance
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("vehicle_id")
    ) \
    .agg(
        count("device_id").alias("event_count"),
        avg("speed_kmh").alias("avg_speed"),
        max("speed_kmh").alias("max_speed"),
        avg("temperature_c").alias("avg_temp"),
        min("fuel_level_pct").alias("min_fuel"),
        last("latitude").alias("last_latitude"),
        last("longitude").alias("last_longitude"),
        sum(col("is_speed_anomaly").cast("int")).alias("speed_violations"),
        sum(col("is_temp_anomaly").cast("int")).alias("temp_alerts"),
        last("alert_level").alias("current_alert_level")
    ) \
    .select(
        col("vehicle_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "*"
    ).drop("window")

# ── Write to Silver (complete mode for aggregations) ──
silver_query = (
    df_windowed.writeStream
        .format("delta")
        .outputMode("append")             # ← append for sliding window
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start(SILVER_PATH)
)

# ── ALSO: Write real-time alerts to separate table ──
ALERTS_PATH      = "abfss://silver@prodlake.dfs.core.windows.net/vehicle-alerts/"
ALERT_CHECKPOINT = "abfss://checkpoints@prodlake.dfs.core.windows.net/sensor-alerts/"

df_alerts = df_anomalies \
    .filter(col("alert_level") != "NORMAL") \
    .select(
        "vehicle_id", "device_id", "event_timestamp",
        "alert_level", "speed_kmh", "temperature_c",
        "fuel_level_pct", "engine_status",
        "latitude", "longitude"
    )

alerts_query = (
    df_alerts.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", ALERT_CHECKPOINT)
        .trigger(processingTime="30 seconds")  # Alerts checked every 30s
        .start(ALERTS_PATH)
)

print("🚀 Streaming processing started")
print("   Vehicle metrics: 5-min windowed aggregations")
print("   Alerts:          Real-time (30s latency)")
```

### `03_gold_realtime_dashboard.py` — Gold for BI

```python
# ─────────────────────────────────────────────────────────────────────
# GOLD LAYER: Real-time fleet dashboard
# Current vehicle positions + status
# ─────────────────────────────────────────────────────────────────────

%run ./utils/config

from pyspark.sql.functions import *
from pyspark.sql import Window

SILVER_PATH  = "abfss://silver@prodlake.dfs.core.windows.net/vehicle-metrics/"
GOLD_PATH    = "abfss://gold@prodlake.dfs.core.windows.net/fleet-dashboard/"
CHECKPOINT   = "abfss://checkpoints@prodlake.dfs.core.windows.net/fleet-gold/"

df_silver = spark.readStream.format("delta").load(SILVER_PATH)

# Get latest state per vehicle (foreachBatch for MERGE support)
def upsert_to_gold(df_batch, batch_id):
    """
    foreachBatch allows using Delta MERGE in streaming.
    Regular streaming writeStream doesn't support MERGE.
    """
    from delta.tables import DeltaTable
    
    # Latest record per vehicle in this micro-batch
    w = Window.partitionBy("vehicle_id").orderBy(col("window_end").desc())
    
    df_latest = df_batch \
        .withColumn("rn", row_number().over(w)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .withColumn("dashboard_updated_at", current_timestamp())
    
    target = DeltaTable.forPath(spark, GOLD_PATH)
    
    (target.alias("tgt")
     .merge(df_latest.alias("src"), "tgt.vehicle_id = src.vehicle_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    
    print(f"✅ Batch {batch_id}: Updated {df_latest.count()} vehicle records in Gold")

# foreachBatch: process each micro-batch with custom function
gold_query = (
    df_silver.writeStream
        .foreachBatch(upsert_to_gold)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="2 minutes")
        .start()
)

print("🚀 Real-time fleet dashboard streaming started")
print("   Updates every: 2 minutes")
print("   BI tool:        Power BI Direct Query on Gold table")
```

---

## Monitoring Streaming Jobs

```python
# Check streaming query status
for query in spark.streams.active:
    print(f"Query: {query.name or query.id}")
    print(f"  Status:   {query.status}")
    print(f"  Progress: {query.lastProgress}")
    print()

# Check lag (how far behind are we?)
# In Spark UI: Streaming tab → Input Rate vs Processing Rate
# If processing rate < input rate → you have backpressure → scale up cluster

# Stop a specific query
bronze_query.stop()

# Check if query is still running
print(f"Bronze query active: {bronze_query.isActive}")
```

---

## Interview Questions — Streaming

**Q1: What is the difference between Auto Loader and `spark.readStream.csv()`?**
> Auto Loader (`cloudFiles` format) uses file notifications (via Azure Event Grid) or directory listing to discover new files — it's far more efficient for cloud storage at scale. Standard `readStream.csv()` polls the directory every trigger — inefficient for millions of files. Auto Loader also tracks processed files in a RocksDB-based state, handles exactly-once processing, and supports schema evolution automatically.

**Q2: What is a watermark in structured streaming?**
> A watermark tells Spark how long to wait for late-arriving data before closing a time window. `.withWatermark("event_timestamp", "10 minutes")` means: accept events up to 10 minutes late. After that, the window is finalized and late records are dropped. Without watermarks, Spark keeps all state in memory forever — causing OOM on long-running streams.

**Q3: What is `foreachBatch` and when do you need it?**
> `foreachBatch` lets you apply batch operations (like Delta MERGE) to each streaming micro-batch. You need it when the standard streaming write modes (`append`, `update`, `complete`) don't support your use case — specifically when you need upsert semantics on the output. The function receives a DataFrame and a batch ID.

**Q4: What is exactly-once processing and how does Databricks achieve it?**
> Exactly-once means each event is processed exactly one time — no duplicates, no missed events. Databricks achieves this by: (1) checkpointing — recording processed offsets so crashes don't reprocess events, (2) Delta Lake — transactional writes ensure partial writes are rolled back, (3) Auto Loader file tracking — each file is processed exactly once even if the query restarts.
