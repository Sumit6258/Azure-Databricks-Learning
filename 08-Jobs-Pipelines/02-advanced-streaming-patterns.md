# Module 08 — Jobs & Pipelines
## Part 2: Advanced Streaming — Stateful Processing, Kafka & Production Patterns

---

## 2.1 Stateful Streaming — Beyond Simple Aggregations

Most streaming tutorials show stateless operations (filter, map). Real production streaming requires STATE — remembering information across micro-batches.

```
STATELESS: Process each event independently
  Input: txn arrives → check if amount > 1000 → flag it
  No memory of previous events

STATEFUL: Process events considering history
  Input: txn arrives → compare with customer's last 10 transactions
  Requires: remembering previous transactions per customer
  Challenge: state grows over time → must manage/expire it
```

---

## 2.2 mapGroupsWithState — Fully Custom Stateful Logic

```python
# SCENARIO: Real-time customer session tracking
# Track each customer's active session (ended if no transaction for 30 minutes)

from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql.functions import *
from typing import Iterator, Tuple
from datetime import datetime
import json

# Define session state
from dataclasses import dataclass, asdict

@dataclass
class CustomerSession:
    customer_id:     str
    session_start:   str
    last_event_time: str
    event_count:     int
    total_amount:    float
    is_active:       bool

def update_session_state(
    customer_id: str,
    events: Iterator,
    state: GroupState
) -> Iterator[Tuple]:
    """
    Update session state for one customer on each micro-batch.
    Called once per customer per micro-batch.
    """
    SESSION_TIMEOUT_MS = 30 * 60 * 1000  # 30 minutes
    
    # Load existing state or create new
    if state.exists:
        session = CustomerSession(**json.loads(state.get))
    else:
        session = CustomerSession(
            customer_id=customer_id,
            session_start=str(datetime.utcnow()),
            last_event_time=str(datetime.utcnow()),
            event_count=0,
            total_amount=0.0,
            is_active=True
        )
    
    # Check if timed out
    if state.hasTimedOut:
        session.is_active = False
        state.remove()
        yield (customer_id, session.session_start, session.last_event_time,
               session.event_count, session.total_amount, "TIMED_OUT")
        return
    
    # Process new events
    for event in events:
        session.event_count  += 1
        session.total_amount += float(event.amount or 0)
        session.last_event_time = str(event.event_timestamp)
    
    # Update state
    state.update(json.dumps(asdict(session)))
    
    # Set timeout — if no events for 30 min, session ends
    state.setTimeoutDuration(SESSION_TIMEOUT_MS)
    
    yield (customer_id, session.session_start, session.last_event_time,
           session.event_count, session.total_amount, "ACTIVE")


# Apply to stream
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

output_schema = StructType([
    StructField("customer_id",     StringType()),
    StructField("session_start",   StringType()),
    StructField("last_event_time", StringType()),
    StructField("event_count",     IntegerType()),
    StructField("total_amount",    DoubleType()),
    StructField("session_status",  StringType()),
])

df_sessions = df_stream \
    .withWatermark("event_timestamp", "35 minutes") \
    .groupBy("customer_id") \
    .applyInPandasWithState(
        update_session_state,
        output_schema,
        "customer_id STRING, session_data STRING",  # State schema
        "EventTimeTimeout",
        GroupStateTimeout.EventTimeTimeout
    )

df_sessions.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "/mnt/checkpoints/sessions/") \
    .trigger(processingTime="1 minute") \
    .start("/mnt/gold/customer_sessions/")
```

---

## 2.3 Stream-Stream Joins

```python
# JOIN TWO STREAMS TOGETHER
# Use case: Match payment events with authorization events within 5 minutes

# Stream 1: Payment authorizations
df_auth = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "...") \
    .option("subscribe", "payment-authorizations") \
    .load() \
    .select(
        from_json(col("value").cast("string"), auth_schema).alias("data")
    ).select("data.*") \
    .withWatermark("auth_timestamp", "10 minutes")

# Stream 2: Payment completions
df_payments = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "...") \
    .option("subscribe", "payment-completions") \
    .load() \
    .select(
        from_json(col("value").cast("string"), payment_schema).alias("data")
    ).select("data.*") \
    .withWatermark("payment_timestamp", "10 minutes")

# Stream-stream join with time constraint
# Match authorization → payment within 5 minutes
df_matched = df_auth.join(
    df_payments,
    expr("""
        auth.transaction_id = payment.transaction_id
        AND payment.payment_timestamp >= auth.auth_timestamp
        AND payment.payment_timestamp <= auth.auth_timestamp + INTERVAL 5 MINUTES
    """),
    how="left"
)

# Unmatched authorizations (payment not received within 5 min)
df_unmatched = df_auth.join(
    df_payments,
    expr("""
        auth.transaction_id = payment.transaction_id
        AND payment.payment_timestamp >= auth.auth_timestamp
        AND payment.payment_timestamp <= auth.auth_timestamp + INTERVAL 5 MINUTES
    """),
    how="left_anti"
)

df_unmatched.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/unmatched/") \
    .trigger(processingTime="1 minute") \
    .start("/mnt/alerts/unmatched_authorizations/")
```

---

## 2.4 Kafka Integration — Production Patterns

```python
# ── READ FROM KAFKA ──
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092") \
    .option("subscribe",               "transactions,payments")  # Multiple topics
    .option("startingOffsets",         "latest") \
    .option("maxOffsetsPerTrigger",    "100000") \  # Backpressure control
    .option("kafka.group.id",          "databricks-etl-group") \
    .option("failOnDataLoss",          "false") \  # Don't fail if Kafka deletes old data
    .option("kafka.security.protocol", "SASL_SSL") \  # For secured Kafka
    .load()

# Kafka DataFrame columns:
# key: binary, value: binary, topic: string, partition: int,
# offset: long, timestamp: timestamp, timestampType: int

# Decode value (most common case: JSON payload)
from pyspark.sql.functions import from_json, col

EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType()),
    StructField("event_type", StringType()),
    StructField("payload",    StringType()),
    StructField("ts",         LongType()),
])

df_decoded = df_kafka.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event")
).select("topic", "partition", "offset", "kafka_timestamp", "event.*")

# ── WRITE TO KAFKA ──
df_alerts = df_fraud_detected.select(
    col("transaction_id").cast("string").alias("key"),
    to_json(struct(
        col("transaction_id"),
        col("customer_id"),
        col("amount"),
        col("fraud_score"),
        col("fraud_risk_level"),
        current_timestamp().alias("alert_time")
    )).alias("value")
)

df_alerts.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("topic", "fraud-alerts") \
    .option("checkpointLocation", "/mnt/checkpoints/kafka-fraud-alerts/") \
    .trigger(processingTime="30 seconds") \
    .start()

# ── BATCH REPLAY FROM KAFKA ──
# Re-read historical Kafka data (for backfill)
df_historical = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", json.dumps({
        "transactions": {str(partition): start_offset}
        for partition, start_offset in partition_offsets.items()
    })) \
    .option("endingOffsets", "latest") \
    .load()
```

---

## 2.5 Multi-Source Streaming Pipeline

```python
# PRODUCTION PATTERN: Multiple streams → unified Bronze table
# Each source writes to the same Bronze Delta table
# Downstream Silver/Gold reads from unified Bronze

from pyspark.sql import functions as F

# Source 1: Core banking Kafka
df_banking_stream = (
    spark.readStream.format("kafka")
    .option("subscribe", "core-banking-txns")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .load()
    .select(from_json(col("value").cast("string"), TXN_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("_source", F.lit("core_banking"))
    .withColumn("_ingested_at", current_timestamp())
)

# Source 2: Mobile app Auto Loader
df_mobile_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/mobile-schema/")
    .load("/mnt/landing/mobile-events/")
    .withColumn("_source", F.lit("mobile_app"))
    .withColumn("_ingested_at", current_timestamp())
)

# Source 3: POS terminals (Event Hub)
df_pos_stream = (
    spark.readStream.format("kafka")
    .option("subscribe", "pos-transactions")
    .option("kafka.bootstrap.servers", EVENTHUB_BROKERS)
    .load()
    .select(from_json(col("value").cast("string"), POS_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("_source", F.lit("pos_terminal"))
    .withColumn("_ingested_at", current_timestamp())
)

# Union all streams → write to unified Bronze
# (Union of streaming DFs supported in Spark 3.x)
df_unified = df_banking_stream \
    .unionByName(df_mobile_stream, allowMissingColumns=True) \
    .unionByName(df_pos_stream, allowMissingColumns=True)

df_unified.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/unified-bronze/") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="30 seconds") \
    .start("/mnt/bronze/transactions-unified/")
```

---

## 2.6 Streaming Monitoring and Alerting

```python
# Monitor streaming query health

def monitor_streaming_query(query, query_name: str, max_lag_minutes: int = 10):
    """
    Monitor a streaming query and alert if lag exceeds threshold.
    Call this periodically from a separate notebook or job.
    """
    if not query.isActive:
        send_alert(f"🔴 Streaming query '{query_name}' is NOT ACTIVE!")
        return
    
    progress = query.lastProgress
    if progress is None:
        print(f"⚠️  {query_name}: No progress yet (just started?)")
        return
    
    # Check input rate vs processing rate
    input_rows_per_sec    = progress.get("inputRowsPerSecond", 0)
    processed_rows_per_sec= progress.get("processedRowsPerSecond", 0)
    
    print(f"\n📊 {query_name} Progress:")
    print(f"   Input rate:      {input_rows_per_sec:.0f} rows/sec")
    print(f"   Processing rate: {processed_rows_per_sec:.0f} rows/sec")
    
    # Check batch duration
    batch_duration_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
    print(f"   Batch duration:  {batch_duration_ms/1000:.1f} sec")
    
    # Check watermark lag
    event_time = progress.get("eventTime", {})
    watermark  = event_time.get("watermark", "")
    
    if watermark:
        from datetime import datetime
        wm_dt  = datetime.fromisoformat(watermark.replace("Z",""))
        lag_min= (datetime.utcnow() - wm_dt).total_seconds() / 60
        print(f"   Watermark lag:   {lag_min:.1f} minutes")
        
        if lag_min > max_lag_minutes:
            send_alert(
                f"⚠️  {query_name}: Watermark lag {lag_min:.1f}min > {max_lag_minutes}min threshold!\n"
                f"Input rate: {input_rows_per_sec:.0f}/s, Processing: {processed_rows_per_sec:.0f}/s\n"
                f"Consider scaling up cluster."
            )
    
    # Input rate >> Processing rate = backlog building
    if input_rows_per_sec > processed_rows_per_sec * 1.5:
        send_alert(
            f"⚠️  {query_name}: BACKLOG BUILDING!\n"
            f"Input ({input_rows_per_sec:.0f}/s) > Processing ({processed_rows_per_sec:.0f}/s)\n"
            f"Scale up workers."
        )

# Check all active streaming queries
for q in spark.streams.active:
    print(f"Active: {q.name or q.id} | Status: {q.status['message']}")

# ── Graceful streaming shutdown ──
def graceful_stop(query, wait_seconds: int = 60):
    """Stop a streaming query gracefully — wait for current batch to finish."""
    print(f"⏹️  Stopping query {query.id}...")
    query.stop()
    query.awaitTermination(timeout=wait_seconds)
    print(f"✅ Query stopped cleanly")
```

---

## 2.7 Streaming Checkpointing — Deep Dive

```python
# WHY CHECKPOINTS?
# Checkpoint stores:
#   1. Streaming query state (offsets, aggregation state)
#   2. Kafka/file offsets (where to resume after restart)
#   3. RocksDB state store (for stateful operations)

# CHECKPOINT DIRECTORY STRUCTURE:
# /mnt/checkpoints/my-query/
# ├── commits/           ← Committed batch IDs
# ├── offsets/           ← Source offsets per batch
# ├── sources/           ← Source-specific metadata
# └── state/             ← Stateful operation state
#     └── 0/             ← Operator 0 state
#         └── 1/         ← Partition 1 state

# BEST PRACTICES:
# 1. Always on durable storage (ADLS), never local disk
# 2. Never share checkpoints between queries
# 3. Keep checkpoint if you want to resume from where you left off
# 4. Delete checkpoint to start fresh (process all data from beginning)
# 5. Use separate checkpoint per environment (dev/test/prod)

CHECKPOINT_BASE = "abfss://checkpoints@prodlake.dfs.core.windows.net"

query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/{query_name}/") \
    .start(target_path)

# Changing schema when checkpoint exists:
# Option 1: Delete checkpoint (reprocess from beginning — safe for idempotent writes)
# dbutils.fs.rm(f"{CHECKPOINT_BASE}/{query_name}/", recurse=True)

# Option 2: Use checkpoint migration (advanced)
# Only works for compatible schema changes

# Option 3: New query name = new checkpoint (keeps old data)
```

---

## 2.8 Interview Questions — Advanced Streaming

**Q: What is the difference between `processingTime` trigger and `availableNow` trigger?**
> `processingTime("30 seconds")` runs micro-batches continuously on a fixed interval — the query runs forever. `availableNow()` processes all available data at query start, then stops — it's like a batch job that uses streaming infrastructure. Use `availableNow` for scheduled catch-up runs (hourly batch that processes accumulated files), use `processingTime` for true continuous streaming.

**Q: How do you handle a streaming query that has built up a large backlog?**
> Three approaches: (1) **Scale up the cluster** — add workers to increase processing throughput. (2) **Increase `maxOffsetsPerTrigger`** — process more data per batch (increases memory requirement). (3) **Batch catch-up** — stop the streaming query, run a batch job to process the backlog, then restart streaming from the current position. For Kafka, set `startingOffsets` to the current latest offset after the batch catch-up completes.

**Q: What happens to stateful streaming operations when a cluster restarts?**
> State is preserved in the checkpoint directory on ADLS (durable storage). When the streaming query restarts with the same `checkpointLocation`, Spark restores the state store from checkpoint, reads the last committed offsets, and resumes processing from exactly where it left off. This guarantees exactly-once processing — no data is lost or double-processed. The restart typically causes a slight delay while state is loaded from ADLS.

---
*Next: [Module 14 — Cheatsheet: Interview Questions by Difficulty](../14-Cheatsheets/02-interview-by-difficulty.md)*
