# Module 02 — PySpark
## Part 2: Data Ingestion Patterns — Every Format, Every Source

---

## 2.1 The Ingestion Landscape

```
Data Sources You'll Meet on Real Projects:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATABASES       SQL Server, Oracle, MySQL, PostgreSQL, DB2
FILES           CSV, Parquet, ORC, JSON, Avro, Excel, XML
STREAMING       Kafka, Azure Event Hub, Azure IoT Hub
APIS            REST APIs, GraphQL
SAAS            Salesforce, SAP, ServiceNow (via ADF connectors)
CLOUD STORAGE   ADLS Gen2, Azure Blob, S3, GCS
OTHER AZURE     Azure SQL, Cosmos DB, Azure Synapse
```

---

## 2.2 File Ingestion — Every Format

### CSV — Production-Grade Reading

```python
from pyspark.sql.types import *
from pyspark.sql.functions import col, input_file_name, current_timestamp

# FULL production CSV read — every option explained
df_csv = spark.read \
    .option("header",           "true")    \  # First row = column names
    .option("delimiter",        ",")        \  # Separator (use "|" for pipe)
    .option("quote",            '"')        \  # Quote character
    .option("escape",           '"')        \  # Escape character within quotes
    .option("multiLine",        "true")     \  # Allow newlines inside quoted fields
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace","true") \
    .option("nullValue",        "NULL")    \  # Treat "NULL" string as null
    .option("emptyValue",       "")         \  # Treat empty string as ""
    .option("nanValue",         "NaN")      \
    .option("positiveInf",      "Inf")      \
    .option("negativeInf",      "-Inf")     \
    .option("dateFormat",       "yyyy-MM-dd") \
    .option("timestampFormat",  "yyyy-MM-dd HH:mm:ss") \
    .option("encoding",         "UTF-8")    \
    .option("comment",          "#")        \  # Lines starting with # are comments
    .option("maxColumns",       "2048")     \
    .option("maxCharsPerColumn","100000")   \
    .option("mode",             "PERMISSIVE") \  # PERMISSIVE / DROPMALFORMED / FAILFAST
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(your_schema)                       \  # Always explicit in production
    .csv("/mnt/bronze/transactions/*.csv")

# Handle corrupt records
bad_records = df_csv.filter(col("_corrupt_record").isNotNull())
good_records = df_csv.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

print(f"Good: {good_records.count()} | Bad: {bad_records.count()}")

# Read multiple CSVs with wildcard — Spark reads them all in parallel
df_all = spark.read.schema(schema).csv("/mnt/bronze/transactions/year=2024/month=*/")
```

### Parquet — Enterprise Standard

```python
# Parquet is the preferred format for all intermediate and final data
# Reasons: columnar (only reads needed columns), compressed, schema embedded

# Read — simplest format
df_parquet = spark.read.parquet("/mnt/bronze/transactions/")

# Read with schema enforcement
df_typed = spark.read.schema(schema).parquet("/mnt/bronze/transactions/")

# Read specific partitions (partition pruning)
df_jan = spark.read.parquet("/mnt/bronze/transactions/") \
    .filter(col("year") == 2024) \
    .filter(col("month") == 1)
# Spark reads ONLY /year=2024/month=1/ directory

# Write Parquet (various compression options)
df.write.parquet("/mnt/bronze/output/",
    compression="snappy")   # Best for read speed

df.write.parquet("/mnt/bronze/archive/",
    compression="gzip")     # Best for storage size (30% smaller)

df.write.parquet("/mnt/bronze/fast/",
    compression="zstd")     # Best balance (Spark 3.x default)
```

### JSON — Nested and Semi-Structured

```python
# Flat JSON
df_json = spark.read.json("/mnt/bronze/events/")

# Multi-line JSON (each file is one JSON object, not one object per line)
df_multiline = spark.read \
    .option("multiLine", "true") \
    .json("/mnt/bronze/api_responses/")

# Nested JSON — flattening is the main challenge
# Input:
# {"order_id": "O001", "customer": {"id": "C001", "name": "Alice"},
#  "items": [{"sku": "SKU1", "qty": 2}, {"sku": "SKU2", "qty": 1}]}

from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import ArrayType, StructType, StructField

# Access nested fields using dot notation
df_nested = spark.read.option("multiLine", "true").json("/mnt/bronze/orders/")

df_flat = df_nested.select(
    col("order_id"),
    col("customer.id").alias("customer_id"),
    col("customer.name").alias("customer_name"),
    col("items")  # Still an array at this point
)

# Explode array — one row per array element
df_exploded = df_flat.withColumn("item", explode(col("items"))) \
    .select(
        col("order_id"),
        col("customer_id"),
        col("customer_name"),
        col("item.sku").alias("sku"),
        col("item.qty").alias("quantity")
    )

# Parse JSON stored as a STRING column in a table
# Common when Kafka sends JSON payloads as strings
event_schema = StructType([
    StructField("event_type", StringType()),
    StructField("user_id",    StringType()),
    StructField("timestamp",  LongType()),
    StructField("payload",    MapType(StringType(), StringType()))
])

df_parsed = df.withColumn(
    "event_data",
    from_json(col("event_payload_string"), event_schema)
).select("event_data.*")
```

### Avro — Kafka Schema Registry

```python
# Avro is common when reading from Kafka with Schema Registry

# Install: cluster libraries → Maven → org.apache.spark:spark-avro_2.12:3.4.1

df_avro = spark.read.format("avro").load("/mnt/bronze/kafka-avro/")

# With schema (from Schema Registry)
avro_schema = open("schemas/transaction_v1.avsc").read()

df_with_schema = spark.read.format("avro") \
    .option("avroSchema", avro_schema) \
    .load("/mnt/bronze/kafka-avro/")

# Convert Avro to Delta (one-time migration)
df_avro.write.format("delta").mode("overwrite").save("/mnt/silver/transactions/")
```

### Excel — Client Reports and Finance

```python
# Install spark-excel library:
# Cluster → Libraries → Maven → com.crealytics:spark-excel_2.12:3.4.1_0.20.3

# Read Excel
df_excel = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header",       "true") \
    .option("inferSchema",  "false") \
    .option("dataAddress",  "'Sheet1'!A1")  \  # Specific sheet + cell range
    .option("treatEmptyValuesAsNulls", "true") \
    .load("/mnt/bronze/finance_report.xlsx")

# Read specific range (skip header rows, read from row 3)
df_range = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("dataAddress", "'Revenue'!B3:F500") \
    .load("/mnt/bronze/report.xlsx")

# Write Excel (for reporting output to business stakeholders)
df_gold.write \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("dataAddress", "'Summary'!A1") \
    .mode("overwrite") \
    .save("/mnt/gold/monthly_report.xlsx")
```

---

## 2.3 JDBC Ingestion — Databases

### Full Load with Parallelism

```python
# CRITICAL: Without numPartitions, Spark uses 1 connection — very slow!

df_full = spark.read.format("jdbc") \
    .option("url",             "jdbc:sqlserver://server.database.windows.net:1433;database=mydb") \
    .option("dbtable",         "dbo.transactions") \
    .option("user",            dbutils.secrets.get("kv-prod", "sql-user")) \
    .option("password",        dbutils.secrets.get("kv-prod", "sql-password")) \
    .option("driver",          "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("numPartitions",   "8")          \  # 8 parallel connections
    .option("partitionColumn", "txn_id")     \  # Must be numeric
    .option("lowerBound",      "1")          \
    .option("upperBound",      "10000000")   \  # Max txn_id value
    .option("fetchsize",       "10000")      \  # Rows per network fetch
    .load()

# Spark creates 8 queries:
# SELECT * FROM dbo.transactions WHERE txn_id >= 1       AND txn_id < 1250001
# SELECT * FROM dbo.transactions WHERE txn_id >= 1250001 AND txn_id < 2500001
# ... etc.
```

### Incremental JDBC Load

```python
def jdbc_incremental_load(
    jdbc_url: str,
    source_table: str,
    target_path: str,
    watermark_column: str,
    last_watermark: str
) -> int:
    """Load only rows changed after last_watermark."""
    
    # Push the WHERE clause to the database (not Spark)
    query = f"""(
        SELECT *
        FROM {source_table}
        WHERE {watermark_column} > '{last_watermark}'
    ) AS incremental_data"""
    
    df = spark.read.format("jdbc") \
        .option("url",           jdbc_url) \
        .option("dbtable",       query) \
        .option("user",          dbutils.secrets.get("kv-prod", "sql-user")) \
        .option("password",      dbutils.secrets.get("kv-prod", "sql-password")) \
        .option("driver",        "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("numPartitions", "4") \
        .option("fetchsize",     "50000") \
        .load()
    
    count = df.count()
    if count == 0:
        return 0
    
    # Add ingestion metadata
    df_bronze = df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_watermark",   lit(last_watermark))
    
    df_bronze.write.format("delta").mode("append").save(target_path)
    return count

# JDBC for different databases
DRIVERS = {
    "sqlserver": {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url":    "jdbc:sqlserver://{host}:1433;database={db}"
    },
    "oracle": {
        "driver": "oracle.jdbc.driver.OracleDriver",
        "url":    "jdbc:oracle:thin:@{host}:1521:{db}"
    },
    "postgresql": {
        "driver": "org.postgresql.Driver",
        "url":    "jdbc:postgresql://{host}:5432/{db}"
    },
    "mysql": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "url":    "jdbc:mysql://{host}:3306/{db}"
    }
}
```

### Writing Back to SQL Database

```python
# Write Spark DataFrame to SQL Server (useful for Gold → DWH)
df_gold.write.format("jdbc") \
    .option("url",         "jdbc:sqlserver://dwh.database.windows.net:1433;database=analytics") \
    .option("dbtable",     "dbo.daily_revenue_summary") \
    .option("user",        dbutils.secrets.get("kv-prod", "dwh-user")) \
    .option("password",    dbutils.secrets.get("kv-prod", "dwh-password")) \
    .option("driver",      "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("batchsize",   "10000") \
    .option("numPartitions","4") \
    .mode("append") \
    .save()

# Truncate and reload pattern (for full refresh)
df_gold.write.format("jdbc") \
    .option("url",              "jdbc:sqlserver://...") \
    .option("dbtable",          "dbo.daily_revenue_summary") \
    .option("truncate",         "true") \  # Truncate before insert (preserves schema/indexes)
    .mode("overwrite") \
    .save()
```

---

## 2.4 Azure Event Hub & Kafka Ingestion

```python
# Read from Azure Event Hub (Kafka-compatible protocol)
# Install: com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22

EH_NAMESPACE = "your-eventhub-namespace"
EH_NAME      = "transactions"
EH_CONN_STR  = dbutils.secrets.get("kv-prod", "eventhub-connection-string")

# Kafka-protocol connection string
SASL_CONFIG = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{EH_CONN_STR}";'
)

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",        f"{EH_NAMESPACE}.servicebus.windows.net:9093") \
    .option("kafka.security.protocol",        "SASL_SSL") \
    .option("kafka.sasl.mechanism",           "PLAIN") \
    .option("kafka.sasl.jaas.config",         SASL_CONFIG) \
    .option("subscribe",                      EH_NAME) \
    .option("startingOffsets",                "latest") \
    .option("maxOffsetsPerTrigger",           "50000") \  # Rate limiting
    .option("kafka.request.timeout.ms",       "60000") \
    .option("kafka.session.timeout.ms",       "30000") \
    .load()

# Event Hub messages: key + value (bytes), need to decode
from pyspark.sql.functions import col, from_json, cast

EVENT_SCHEMA = StructType([
    StructField("txn_id",      StringType()),
    StructField("amount",      DoubleType()),
    StructField("customer_id", StringType()),
    StructField("timestamp",   LongType()),
])

df_decoded = df_stream \
    .select(
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    ) \
    .select("message_key", "data.*", "topic", "offset", "kafka_timestamp")

# Write to Bronze Delta
df_decoded.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/eventhub-bronze/") \
    .trigger(processingTime="30 seconds") \
    .start("/mnt/bronze/transactions-stream/")
```

---

## 2.5 Auto Loader — Deep Dive

```python
# Auto Loader is the recommended way to ingest files in Databricks
# It beats plain readStream because:
# 1. Scales to BILLIONS of files (uses cloud notifications, not directory listing)
# 2. Exactly-once semantics via RocksDB checkpointing
# 3. Schema inference + schema evolution built in
# 4. Supports rescue data (schema mismatch rows saved to _rescued_data column)

# ── DIRECTORY LISTING MODE (for < 10M files or non-Azure storage) ──
df_listing = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format",          "csv") \
    .option("cloudFiles.schemaLocation",  "/mnt/checkpoints/autoloader-schema/") \
    .option("cloudFiles.useNotifications","false") \  # Force listing mode
    .option("maxFilesPerTrigger",         "1000") \
    .schema(schema) \
    .load("/mnt/landing/transactions/")

# ── NOTIFICATION MODE (recommended for Azure ADLS) ──
# Auto Loader uses Azure Event Grid + Queue Storage to get file arrival notifications
# Setup: One-time configuration via Azure Portal or Terraform

df_notification = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format",               "json") \
    .option("cloudFiles.schemaLocation",       "/mnt/checkpoints/autoloader-schema/") \
    .option("cloudFiles.useNotifications",     "true") \
    .option("cloudFiles.subscriptionId",       dbutils.secrets.get("kv", "subscription-id")) \
    .option("cloudFiles.resourceGroup",        "rg-datalake") \
    .option("cloudFiles.tenantId",             dbutils.secrets.get("kv", "tenant-id")) \
    .option("cloudFiles.clientId",             dbutils.secrets.get("kv", "sp-client-id")) \
    .option("cloudFiles.clientSecret",         dbutils.secrets.get("kv", "sp-secret")) \
    .option("cloudFiles.connectionString",     dbutils.secrets.get("kv", "queue-conn-str")) \
    .option("cloudFiles.queueName",            "autoloader-queue") \
    .load("/mnt/landing/transactions/")

# ── SCHEMA EVOLUTION OPTIONS ──
df_evolving = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format",              "json") \
    .option("cloudFiles.schemaLocation",      "/mnt/checkpoints/schema/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  \  # add / rescue / failOnNewColumns / none
    .option("cloudFiles.inferColumnTypes",    "true") \
    .option("rescuedDataColumn",              "_rescued_data") \  # Store schema-mismatch data here
    .load("/mnt/landing/")

# _rescued_data contains JSON of any column that didn't match the schema
# Great for debugging without losing data

# ── BACKFILL SPECIFIC FILES ──
# If you need to reprocess specific files:
dbutils.fs.ls("/mnt/landing/transactions/2024-01-15/")

# Auto Loader tracks processed files in the checkpoint
# To reprocess: delete checkpoint and restart
dbutils.fs.rm("/mnt/checkpoints/autoloader-schema/", recurse=True)

# ── MONITORING AUTO LOADER ──
# In streaming query UI:
# - Input Rate: files/sec arriving at source
# - Processing Rate: files/sec processed
# - Backlog: files waiting to be processed
# Input Rate > Processing Rate = you have a backlog = scale up cluster
```

---

## 2.6 REST API Ingestion — Calling External APIs

```python
# Common in real projects: call external REST APIs and save to Bronze
# Example: fetch exchange rates API, CRM API, weather API

import requests
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

def ingest_from_rest_api(
    api_url: str,
    api_key: str,
    target_path: str,
    batch_date: str,
    page_size: int = 1000
) -> int:
    """
    Paginated REST API ingestion pattern.
    Works for any API that supports offset/limit pagination.
    """
    all_records = []
    page = 1
    
    while True:
        response = requests.get(
            api_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json"
            },
            params={
                "page":       page,
                "page_size":  page_size,
                "date":       batch_date
            },
            timeout=30
        )
        
        response.raise_for_status()
        data = response.json()
        
        records = data.get("data", data.get("results", []))
        if not records:
            break
        
        all_records.extend(records)
        
        # Check if more pages
        if len(records) < page_size:
            break
        
        page += 1
        print(f"  Fetched page {page-1}: {len(records)} records (total: {len(all_records)})")
    
    if not all_records:
        print("No data returned from API")
        return 0
    
    # Convert to Spark DataFrame (driver collects, then distributes)
    # For large APIs, consider writing page by page to avoid driver OOM
    df = spark.createDataFrame([Row(**record) for record in all_records]) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_batch_date",  lit(batch_date)) \
        .withColumn("_api_url",     lit(api_url))
    
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_batch_date") \
        .save(target_path)
    
    print(f"✅ Ingested {len(all_records):,} records from API")
    return len(all_records)

# Usage
api_key = dbutils.secrets.get("kv-prod", "crm-api-key")
count = ingest_from_rest_api(
    api_url="https://api.crm-system.com/v2/customers",
    api_key=api_key,
    target_path="/mnt/bronze/crm_customers/",
    batch_date="2024-01-15"
)
```

---

## 2.7 Cosmos DB Ingestion

```python
# Azure Cosmos DB → Databricks
# Install: com.azure.cosmos.spark:azure-cosmos-spark_3-3_2-12:4.25.1

# Read from Cosmos DB
df_cosmos = spark.read.format("cosmos.oltp") \
    .option("spark.cosmos.accountEndpoint",    dbutils.secrets.get("kv", "cosmos-endpoint")) \
    .option("spark.cosmos.accountKey",         dbutils.secrets.get("kv", "cosmos-key")) \
    .option("spark.cosmos.database",           "ecommerce") \
    .option("spark.cosmos.container",          "orders") \
    .option("spark.cosmos.read.inferSchema.enabled", "true") \
    .load()

# Incremental read using Cosmos change feed
df_changes = spark.read.format("cosmos.oltp.changeFeed") \
    .option("spark.cosmos.accountEndpoint",    dbutils.secrets.get("kv", "cosmos-endpoint")) \
    .option("spark.cosmos.accountKey",         dbutils.secrets.get("kv", "cosmos-key")) \
    .option("spark.cosmos.database",           "ecommerce") \
    .option("spark.cosmos.container",          "orders") \
    .option("spark.cosmos.changeFeed.startFrom","Beginning") \
    .option("spark.cosmos.changeFeed.mode",    "Incremental") \
    .load()
```

---

## 2.8 Ingestion Best Practices Checklist

```
BEFORE WRITING INGESTION CODE:
□ Schema defined explicitly? (No inferSchema in production)
□ Null values handled? (nullValue option for CSV)
□ Corrupt records handled? (PERMISSIVE mode + _corrupt_record column)
□ Secrets from Key Vault? (No hardcoded credentials)
□ Idempotent? (Can re-run without duplicates?)
□ Metadata columns added? (_ingested_at, _source_file, _batch_date)
□ Partitioned by date for efficient querying?
□ mergeSchema enabled for new column handling?

AFTER INGESTION:
□ Record count logged?
□ Schema validation run?
□ Quarantine check — any corrupt records?
□ Downstream task notified via taskValues?
□ Watermark updated?
```

---

## 2.9 Interview Questions — Ingestion

**Q: What is the difference between batch ingestion and Auto Loader? When do you use each?**
> Batch ingestion reads files that exist at a point in time — you schedule it (e.g., daily at 2AM) and it processes all files for that batch. Auto Loader is a continuous streaming ingestion that processes files as soon as they land — latency is seconds to minutes vs hours. Use batch when: data arrives in scheduled drops, processing window is acceptable (daily/hourly). Use Auto Loader when: data arrives continuously, near-real-time freshness needed, or file volumes are very high (millions/day).

**Q: How do you handle schema mismatches when ingesting from an external API or source system that adds new columns?**
> Three layers: (1) At Auto Loader level: `cloudFiles.schemaEvolutionMode=addNewColumns` + `rescuedDataColumn=_rescued_data` — new columns are added to the table, schema-mismatched data is captured without dropping records. (2) At Delta write level: `.option("mergeSchema", "true")` ensures new columns are added to the Bronze table schema. (3) At Silver level: add `mergeSchema=true` but also run a schema comparison check and alert if unexpected columns appear — new columns need to be mapped to the business schema intentionally.

---
*Next: [Part 3 — Advanced Transformations: Arrays, Maps, Structs, Complex Joins](./03-advanced-transformations.md)*
