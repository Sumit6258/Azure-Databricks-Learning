# HANDS-ON TASK 4
## Error Handling with try-except — From Basics to Production Patterns

> **Create a NEW notebook. Copy each cell. Run cell by cell and observe every output.**

---

## CELL 1 — Why Error Handling Matters in Data Engineering

```python
# ════════════════════════════════════════════════════════════════
# THE PROBLEM: Without error handling, ONE bad record crashes
#              your ENTIRE pipeline that processes MILLIONS of rows
# ════════════════════════════════════════════════════════════════

# SCENARIO: You have a pipeline running daily at 2AM
# It processes 500,000 records
# On day 47, ONE record has a bad value
# WITHOUT error handling: entire pipeline fails, nothing gets written
# WITH error handling:    bad record is captured, rest is processed

print("ERROR HANDLING IS NOT OPTIONAL IN PRODUCTION.")
print("It is the difference between a reliable pipeline and a fragile script.")
print()
print("Three things error handling must do:")
print("  1. Catch errors without crashing the whole pipeline")
print("  2. Log WHAT failed and WHY (for debugging)")
print("  3. Decide: continue, retry, or stop (with alerting)")
```

---

## CELL 2 — Python try-except Basics (Foundation)

```python
# ════════════════════════════════════════════════════════════════
# BASIC try-except STRUCTURE
# ════════════════════════════════════════════════════════════════

# ── SIMPLEST FORM ────────────────────────────────────────────
print("=== Example 1: Basic try-except ===")

try:
    result = 10 / 0          # This will throw ZeroDivisionError
    print("This never prints")
except:
    print("Something went wrong")  # Generic — catches EVERYTHING
    # BAD PRACTICE: too generic, hides what actually went wrong

# ── BETTER: Catch specific exception ─────────────────────────
print("\n=== Example 2: Specific exception type ===")

try:
    result = 10 / 0
except ZeroDivisionError as e:
    print(f"Division error: {e}")   # ZeroDivisionError: division by zero
except Exception as e:
    print(f"Some other error: {e}")

# ── MULTIPLE EXCEPTION TYPES ─────────────────────────────────
print("\n=== Example 3: Multiple except blocks ===")

def risky_operation(value):
    try:
        number = int(value)          # Might throw ValueError
        result = 100 / number        # Might throw ZeroDivisionError
        items  = [1, 2, 3]
        item   = items[number]       # Might throw IndexError
        return result
    except ValueError as e:
        print(f"  [ValueError] Cannot convert '{value}' to integer: {e}")
        return None
    except ZeroDivisionError:
        print(f"  [ZeroDivisionError] Cannot divide by zero")
        return None
    except IndexError as e:
        print(f"  [IndexError] Index out of range: {e}")
        return None
    except Exception as e:
        print(f"  [Unexpected Error] {type(e).__name__}: {e}")
        return None

risky_operation("hello")   # ValueError
risky_operation("0")       # ZeroDivisionError
risky_operation("10")      # IndexError
risky_operation("2")       # Works fine → returns 50.0

# ── try-except-else-finally ───────────────────────────────────
print("\n=== Example 4: try-except-else-finally ===")

def read_value(data, key):
    try:
        value = data[key]           # Try this
    except KeyError as e:
        print(f"  Key not found: {e}")
        value = None
    except TypeError as e:
        print(f"  Type error: {e}")
        value = None
    else:
        # Runs ONLY if NO exception was raised
        print(f"  Success! Value = {value}")
    finally:
        # ALWAYS runs — with or without exception
        # Use for cleanup: close connections, release locks, etc.
        print(f"  [finally] Cleanup done for key: {key}")
    
    return value

read_value({"name": "Alice", "age": 30}, "name")   # Success
read_value({"name": "Alice", "age": 30}, "salary")  # KeyError
```

---

## CELL 3 — Common PySpark Errors and How to Handle Them

```python
# ════════════════════════════════════════════════════════════════
# PYSPARK-SPECIFIC ERROR HANDLING
# ════════════════════════════════════════════════════════════════

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.functions import col, to_date

# ── ERROR 1: Reading a file that doesn't exist ────────────────
print("=== Error 1: File Not Found ===")

def safe_read_csv(path: str) -> DataFrame:
    """Read CSV safely — return None if file doesn't exist."""
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path)
        # count() triggers actual reading — will fail if path doesn't exist
        row_count = df.count()
        print(f"  ✅ Read {row_count:,} rows from {path}")
        return df
    except Exception as e:
        if "Path does not exist" in str(e) or "FileNotFoundException" in str(e):
            print(f"  ❌ File not found: {path}")
        else:
            print(f"  ❌ Unexpected error reading {path}: {e}")
        return None

df_ok     = safe_read_csv("/FileStore/sample_data/employees.csv")  # Works
df_missing= safe_read_csv("/FileStore/does_not_exist.csv")         # Fails safely

# ── ERROR 2: Column doesn't exist (AnalysisException) ─────────
print("\n=== Error 2: Column Does Not Exist ===")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("emp_id",     IntegerType()),
    StructField("name",       StringType()),
    StructField("department", StringType()),
    StructField("salary",     DoubleType()),
])

df = spark.createDataFrame([
    (1, "Sumit", "Engineering", 95000.0),
    (2, "Priya", "Marketing",   75000.0),
], schema)

# Try to select a column that doesn't exist
try:
    result = df.select(col("nonexistent_column"))
    result.show()
except AnalysisException as e:
    print(f"  ❌ Analysis error: Column 'nonexistent_column' does not exist")
    print(f"     Available columns: {df.columns}")
except Exception as e:
    print(f"  ❌ Unexpected error: {e}")

# ── ERROR 3: Schema mismatch when writing to Delta ────────────
print("\n=== Error 3: Schema Mismatch (Delta write) ===")

# Write initial Delta table
df.write.format("delta").mode("overwrite").save("/FileStore/test_error/employees/")

# Try to write with DIFFERENT schema (extra column)
df_extra = spark.createDataFrame([
    (3, "Rahul", "Engineering", 110000.0, "Senior"),  # Extra 'level' column
], ["emp_id", "name", "department", "salary", "level"])

try:
    df_extra.write.format("delta").mode("append") \
        .save("/FileStore/test_error/employees/")
except Exception as e:
    if "schema mismatch" in str(e).lower() or "SchemaMismatch" in str(e):
        print("  ❌ Schema mismatch!")
        print("  Fix: Add .option('mergeSchema', 'true') to allow schema evolution")
        # Fix:
        df_extra.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .save("/FileStore/test_error/employees/")
        print("  ✅ Fixed: wrote with mergeSchema=true")
    else:
        print(f"  ❌ Other error: {e}")

# ── ERROR 4: Type casting error ──────────────────────────────
print("\n=== Error 4: Type Casting ===")

bad_data = spark.createDataFrame([
    (1, "150.50"),      # Good
    (2, "not_a_number"),# Bad — can't cast to double
    (3, ""),            # Empty string — becomes null after cast
    (4, "9999.99"),     # Good
], ["id", "amount_str"])

# WRONG approach: cast and hope for the best
df_cast = bad_data.withColumn("amount", col("amount_str").cast("double"))
print("  After casting (nulls = failed casts):")
df_cast.show()

# BETTER approach: validate before casting
from pyspark.sql.functions import when, regexp_like

df_validated = bad_data.withColumn(
    "is_valid_amount",
    regexp_like(col("amount_str"), r"^\d+(\.\d+)?$")
).withColumn(
    "amount",
    when(col("is_valid_amount"), col("amount_str").cast("double"))
    .otherwise(None)
)

print("  After validation + cast:")
df_validated.show()

invalid_count = df_validated.filter(~col("is_valid_amount")).count()
print(f"  Invalid records that need investigation: {invalid_count}")
```

---

## CELL 4 — Production Error Handling Patterns

```python
# ════════════════════════════════════════════════════════════════
# PATTERN 1: Retry with exponential backoff
# Use when: transient errors (network timeout, storage unavailable)
# ════════════════════════════════════════════════════════════════

import time
import random

def with_retry(func, max_retries: int = 3, base_wait: int = 5):
    """
    Execute a function with automatic retry on failure.
    Wait: 5s → 10s → 20s (exponential backoff)
    
    Args:
        func:        The function to run (no-argument callable)
        max_retries: Maximum number of retry attempts
        base_wait:   Base wait time in seconds (doubles each retry)
    
    Returns:
        Result of func() if successful
    Raises:
        Last exception if all retries fail
    """
    last_exception = None
    
    for attempt in range(1, max_retries + 1):
        try:
            result = func()
            if attempt > 1:
                print(f"  ✅ Succeeded on attempt {attempt}")
            return result
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = base_wait * (2 ** (attempt - 1))  # 5, 10, 20 seconds
                print(f"  ⚠️ Attempt {attempt}/{max_retries} failed: {e}")
                print(f"     Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ All {max_retries} attempts failed")
    
    raise last_exception  # Re-raise the last exception


# Simulate a flaky operation (fails 2/3 of the time)
attempt_count = [0]

def flaky_operation():
    attempt_count[0] += 1
    if attempt_count[0] < 3:  # Fail first 2 attempts
        raise ConnectionError(f"Storage temporarily unavailable (attempt {attempt_count[0]})")
    return f"Success on attempt {attempt_count[0]}"

print("=== Retry Pattern Demo ===")
try:
    # For demo, use very short waits
    result = with_retry(lambda: flaky_operation(), max_retries=3, base_wait=1)
    print(f"Final result: {result}")
except Exception as e:
    print(f"Permanently failed: {e}")
```

---

## CELL 5 — Pattern 2: Checkpoint (Idempotency)

```python
# ════════════════════════════════════════════════════════════════
# PATTERN 2: Checkpoint to prevent double-processing on retry
# Use when: pipeline can be re-run (idempotency requirement)
# ════════════════════════════════════════════════════════════════

class PipelineCheckpoint:
    """
    Track which batches have been successfully processed.
    If a job fails and is retried, already-processed batches
    are skipped — preventing duplicates.
    """
    
    def __init__(self, checkpoint_base: str):
        self.checkpoint_base = checkpoint_base
    
    def _get_path(self, batch_id: str) -> str:
        return f"{self.checkpoint_base}/batch_{batch_id}.checkpoint"
    
    def is_processed(self, batch_id: str) -> bool:
        """Return True if this batch was already successfully processed."""
        try:
            dbutils.fs.ls(self._get_path(batch_id))
            return True
        except Exception:
            return False
    
    def mark_complete(self, batch_id: str, metadata: dict = None):
        """Mark a batch as successfully completed."""
        import json
        from datetime import datetime
        content = json.dumps({
            "batch_id":    batch_id,
            "completed_at": str(datetime.utcnow()),
            "metadata":    metadata or {}
        })
        dbutils.fs.put(self._get_path(batch_id), content, overwrite=True)
    
    def mark_failed(self, batch_id: str, error: str):
        """Record a failed batch (for auditing)."""
        from datetime import datetime
        content = f"FAILED at {datetime.utcnow()}: {error}"
        dbutils.fs.put(
            f"{self.checkpoint_base}/batch_{batch_id}.failed",
            content,
            overwrite=True
        )


# Demo usage
cp = PipelineCheckpoint("/FileStore/test_checkpoints")
BATCH_DATE = "2024-01-15"

print(f"=== Checkpoint Pattern Demo for batch: {BATCH_DATE} ===")

if cp.is_processed(BATCH_DATE):
    print(f"  ⏭️ Batch {BATCH_DATE} already processed — SKIPPING")
else:
    print(f"  ▶ Processing batch {BATCH_DATE}...")
    try:
        # Simulate processing
        df = spark.createDataFrame(
            [(1,"txn1",100.0),(2,"txn2",200.0)],
            ["id","txn_id","amount"]
        )
        record_count = df.count()
        
        # Mark as complete
        cp.mark_complete(BATCH_DATE, {"record_count": record_count})
        print(f"  ✅ Processed {record_count} records")
    except Exception as e:
        cp.mark_failed(BATCH_DATE, str(e))
        print(f"  ❌ Failed: {e}")
        raise

# Second run — will skip
print(f"\nRunning again (simulating a retry):")
if cp.is_processed(BATCH_DATE):
    print(f"  ⏭️ Batch {BATCH_DATE} already processed — SKIPPING (no duplicates!)")
```

---

## CELL 6 — Pattern 3: Complete Production Pipeline with Full Error Handling

```python
# ════════════════════════════════════════════════════════════════
# COMPLETE PRODUCTION PIPELINE: Combining all error handling patterns
# This is the pattern used in REAL enterprise pipelines
# ════════════════════════════════════════════════════════════════

import logging
import traceback
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ── SETUP LOGGING ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ETL_Pipeline")


# ── THE PIPELINE FUNCTION ─────────────────────────────────────
def run_daily_etl(batch_date: str, environment: str) -> dict:
    """
    Run the complete daily ETL pipeline with full error handling.
    
    Returns: dict with status, record_count, and error details
    """
    pipeline_start = datetime.utcnow()
    pipeline_result = {
        "batch_date":      batch_date,
        "environment":     environment,
        "status":          "STARTED",
        "started_at":      str(pipeline_start),
        "completed_at":    None,
        "records_ingested":0,
        "records_cleaned": 0,
        "records_rejected":0,
        "error":           None,
        "error_step":      None,
    }
    
    logger.info(f"Pipeline started | date={batch_date} | env={environment}")
    
    # ══ STEP 1: INGEST BRONZE ════════════════════════════════
    logger.info("Step 1: Ingesting Bronze data...")
    try:
        # Simulate reading from source
        raw_data = [
            (1, "2024-01-15", "C001", "1500.0",  "CARD",       "SUCCESS"),
            (2, "2024-01-15", "C002", "250.75",  "UPI",        "SUCCESS"),
            (3, "2024-01-15", "C003", "INVALID", "NETBANKING", "FAILED"),  # Bad amount
            (4, "2024-01-15", None,   "500.0",   "UPI",        "SUCCESS"),  # Null customer
            (5, "2024-01-15", "C001", "9999.99", "CARD",       "SUCCESS"),
        ]
        
        raw_schema = StructType([
            StructField("txn_id",       IntegerType()),
            StructField("txn_date",     StringType()),
            StructField("customer_id",  StringType()),
            StructField("amount_str",   StringType()),
            StructField("payment_mode", StringType()),
            StructField("status",       StringType()),
        ])
        
        df_raw = spark.createDataFrame(raw_data, raw_schema)
        pipeline_result["records_ingested"] = df_raw.count()
        logger.info(f"  Ingested {pipeline_result['records_ingested']} raw records")
    
    except Exception as e:
        # FATAL: If we can't read source data, entire pipeline must stop
        error_msg = f"FATAL: Cannot read source data: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        pipeline_result.update({"status": "FAILED", "error": error_msg, "error_step": "INGEST"})
        return pipeline_result  # Stop pipeline completely
    
    # ══ STEP 2: VALIDATE AND CLEAN ═══════════════════════════
    logger.info("Step 2: Validating and cleaning...")
    try:
        # Validate amount can be cast to double
        df_validated = df_raw.withColumn(
            "_is_valid_amount",
            regexp_like(col("amount_str"), r"^\d+(\.\d+)?$")
        ).withColumn(
            "_is_valid_customer",
            col("customer_id").isNotNull()
        )
        
        # Split valid vs invalid
        df_valid = df_validated.filter(
            col("_is_valid_amount") & col("_is_valid_customer")
        )
        df_invalid = df_validated.filter(
            ~col("_is_valid_amount") | ~col("_is_valid_customer")
        )
        
        invalid_count = df_invalid.count()
        valid_count   = df_valid.count()
        
        logger.info(f"  Valid:   {valid_count} records")
        logger.info(f"  Invalid: {invalid_count} records → quarantine")
        
        # Warn but DON'T stop if invalid rate is under 5%
        total = pipeline_result["records_ingested"]
        invalid_pct = (invalid_count / total * 100) if total > 0 else 0
        
        if invalid_pct > 5.0:
            # Over 5% invalid → WARNING (log and continue, but alert)
            logger.warning(f"  HIGH REJECTION RATE: {invalid_pct:.1f}% — expected < 5%")
        
        if invalid_pct > 50.0:
            # Over 50% invalid → FATAL (something is fundamentally wrong)
            raise ValueError(
                f"Rejection rate {invalid_pct:.1f}% exceeds 50% maximum. "
                "Source data appears corrupt."
            )
        
        # Type-cast the clean data
        df_clean = df_valid.withColumn(
            "amount", col("amount_str").cast("double")
        ).drop("amount_str", "_is_valid_amount", "_is_valid_customer")
        
        pipeline_result["records_cleaned"]  = valid_count
        pipeline_result["records_rejected"] = invalid_count
    
    except ValueError as e:
        # Fatal validation error
        error_msg = f"FATAL validation: {str(e)}"
        logger.error(error_msg)
        pipeline_result.update({"status": "FAILED", "error": error_msg, "error_step": "VALIDATE"})
        return pipeline_result
    except Exception as e:
        error_msg = f"Unexpected validation error: {str(e)}"
        logger.error(error_msg)
        pipeline_result.update({"status": "FAILED", "error": error_msg, "error_step": "VALIDATE"})
        return pipeline_result
    
    # ══ STEP 3: WRITE TO SILVER ═══════════════════════════════
    logger.info("Step 3: Writing to Silver...")
    try:
        silver_path = f"/FileStore/{environment}/silver/transactions/"
        
        df_clean.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(silver_path)
        
        logger.info(f"  Written {valid_count} records to {silver_path}")
    
    except Exception as e:
        error_msg = f"Failed to write Silver: {str(e)}"
        logger.error(error_msg)
        # Non-fatal if it's a schema issue — try with mergeSchema
        try:
            logger.info("  Retrying with overwriteSchema...")
            df_clean.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(silver_path)
            logger.info("  Retry succeeded!")
        except Exception as retry_e:
            pipeline_result.update({
                "status":     "FAILED",
                "error":      f"Write failed after retry: {str(retry_e)}",
                "error_step": "WRITE_SILVER"
            })
            return pipeline_result
    
    # ══ PIPELINE COMPLETE ════════════════════════════════════
    pipeline_result.update({
        "status":       "SUCCESS",
        "completed_at": str(datetime.utcnow()),
    })
    
    duration = (datetime.utcnow() - pipeline_start).total_seconds()
    logger.info(f"Pipeline completed in {duration:.1f}s | "
                f"Ingested={pipeline_result['records_ingested']} | "
                f"Clean={pipeline_result['records_cleaned']} | "
                f"Rejected={pipeline_result['records_rejected']}")
    
    return pipeline_result


# ── RUN THE PIPELINE ─────────────────────────────────────────
print("Running production pipeline with error handling...\n")

final_result = run_daily_etl(
    batch_date="2024-01-15",
    environment="dev"
)

print("\n" + "=" * 55)
print("  PIPELINE RESULT SUMMARY")
print("=" * 55)
for key, value in final_result.items():
    print(f"  {key:<22}: {value}")
print("=" * 55)

if final_result["status"] == "SUCCESS":
    print("\n✅ PIPELINE SUCCEEDED")
else:
    print(f"\n❌ PIPELINE FAILED at step: {final_result['error_step']}")
    print(f"   Error: {final_result['error']}")
```

---

## CELL 7 — Custom Exception Classes

```python
# ════════════════════════════════════════════════════════════════
# CUSTOM EXCEPTIONS — Make errors self-documenting
# ════════════════════════════════════════════════════════════════

# Define custom exception hierarchy for your pipeline
class PipelineError(Exception):
    """Base class for all pipeline errors."""
    def __init__(self, message: str, step: str = None, batch_date: str = None):
        self.step       = step
        self.batch_date = batch_date
        full_message    = f"[{step or 'UNKNOWN'}][{batch_date or 'NO_DATE'}] {message}"
        super().__init__(full_message)

class DataIngestionError(PipelineError):
    """Raised when data cannot be read from source."""
    pass

class DataValidationError(PipelineError):
    """Raised when data quality checks fail critically."""
    def __init__(self, message, step=None, batch_date=None, rejection_rate=None):
        self.rejection_rate = rejection_rate
        super().__init__(message, step, batch_date)

class DataWriteError(PipelineError):
    """Raised when data cannot be written to target."""
    pass

class SchemaEvolutionError(PipelineError):
    """Raised when schema changes are incompatible."""
    pass


# ── Using custom exceptions ───────────────────────────────────

def validate_rejection_rate(valid: int, total: int, max_pct: float, batch_date: str):
    rejection_pct = ((total - valid) / total * 100) if total > 0 else 0
    if rejection_pct > max_pct:
        raise DataValidationError(
            message=f"Rejection rate {rejection_pct:.1f}% exceeds maximum {max_pct:.1f}%",
            step="VALIDATE",
            batch_date=batch_date,
            rejection_rate=rejection_pct
        )

# Test it
print("=== Custom Exceptions Demo ===\n")

try:
    validate_rejection_rate(valid=30, total=100, max_pct=10.0, batch_date="2024-01-15")
except DataValidationError as e:
    print(f"Caught DataValidationError: {e}")
    print(f"  Rejection rate was: {e.rejection_rate:.1f}%")
    print(f"  Failed at step:     {e.step}")
    print(f"  Batch date:         {e.batch_date}")
except PipelineError as e:
    print(f"Caught generic PipelineError: {e}")

try:
    validate_rejection_rate(valid=95, total=100, max_pct=10.0, batch_date="2024-01-16")
    print("\nValidation passed: 5% rejection rate is within 10% limit ✅")
except DataValidationError as e:
    print(f"Should not reach here: {e}")
```

---

## CELL 8 — Context Managers for Resource Cleanup

```python
# ════════════════════════════════════════════════════════════════
# CONTEXT MANAGERS (with statement) — Guaranteed cleanup
# ════════════════════════════════════════════════════════════════

from contextlib import contextmanager

@contextmanager
def pipeline_timer(step_name: str):
    """
    Context manager that times a pipeline step and logs it.
    Guarantees cleanup even if an exception occurs.
    """
    start = datetime.utcnow()
    print(f"\n⏱  [{step_name}] Starting...")
    try:
        yield
        duration = (datetime.utcnow() - start).total_seconds()
        print(f"  ✅ [{step_name}] Completed in {duration:.2f}s")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        print(f"  ❌ [{step_name}] FAILED after {duration:.2f}s: {e}")
        raise  # Re-raise so the caller knows it failed

@contextmanager
def temp_view(df, view_name: str):
    """
    Create a temp view and automatically remove it when done.
    Prevents temp view accumulation across many pipeline runs.
    """
    df.createOrReplaceTempView(view_name)
    print(f"  Created temp view: {view_name}")
    try:
        yield view_name
    finally:
        spark.catalog.dropTempView(view_name)
        print(f"  Dropped temp view: {view_name}")


# Demo: Using context managers
print("=== Context Manager Demo ===")

sample_df = spark.createDataFrame(
    [(1, "Alice", 95000.0), (2, "Bob", 75000.0)],
    ["id", "name", "salary"]
)

with pipeline_timer("Bronze Ingestion"):
    # Simulate processing
    row_count = sample_df.count()
    print(f"    Processed {row_count} rows")

with pipeline_timer("Silver Transform"):
    from pyspark.sql.functions import col as _col
    df_transformed = sample_df.withColumn("salary_k", _col("salary") / 1000)
    df_transformed.count()  # trigger execution

# Temp view cleanup
print("\n--- Temp View with automatic cleanup ---")
with temp_view(sample_df, "temp_employees_ctx") as view:
    result = spark.sql(f"SELECT name, salary FROM {view} WHERE salary > 80000")
    result.show()
# temp_employees_ctx is automatically dropped here

# Verify it's gone
try:
    spark.sql("SELECT * FROM temp_employees_ctx").show()
except Exception:
    print("✅ Confirmed: temp view was automatically cleaned up")
```

---

## CELL 9 — Summary: Error Handling Cheat Sheet

```python
# ════════════════════════════════════════════════════════════════
# QUICK REFERENCE SUMMARY
# ════════════════════════════════════════════════════════════════

summary = """
┌─────────────────────────────────────────────────────────────────────┐
│           ERROR HANDLING IN DATABRICKS — QUICK REFERENCE            │
├─────────────────────────────────────────────────────────────────────┤
│  STRUCTURE                                                           │
│  try:          ← Code that might fail                               │
│  except X:     ← Handle specific error type X                       │
│  except Y:     ← Handle another type Y                              │
│  else:         ← Runs ONLY if NO exception occurred                 │
│  finally:      ← ALWAYS runs (cleanup, close connections)           │
├─────────────────────────────────────────────────────────────────────┤
│  COMMON SPARK/DATABRICKS EXCEPTIONS                                  │
│  AnalysisException   → Bad column name, bad SQL, missing table      │
│  ParseException      → Invalid SQL syntax                           │
│  IllegalArgumentException → Invalid Spark operation parameters      │
│  FileNotFoundException    → Path doesn't exist in DBFS/ADLS         │
│  ConnectionError          → Network/storage temporarily unavailable │
├─────────────────────────────────────────────────────────────────────┤
│  PRODUCTION PATTERNS                                                 │
│  ✅ Retry with backoff    → For transient errors (network, storage) │
│  ✅ Checkpoint            → For idempotent re-runs (no duplicates)  │
│  ✅ Custom exceptions     → Self-documenting errors                  │
│  ✅ Context managers      → Guaranteed resource cleanup              │
│  ✅ Severity levels       → FATAL vs WARNING vs INFO errors         │
├─────────────────────────────────────────────────────────────────────┤
│  GOLDEN RULES                                                        │
│  1. ALWAYS catch specific exceptions — never bare "except:"         │
│  2. Log the full traceback for unexpected errors                     │
│  3. Decide: skip, retry, or fail the entire pipeline                │
│  4. Write rejected records to a quarantine/dead-letter table        │
│  5. Use finally for cleanup — it ALWAYS runs                        │
│  6. Test error paths, not just the happy path                       │
└─────────────────────────────────────────────────────────────────────┘
"""
print(summary)
```

