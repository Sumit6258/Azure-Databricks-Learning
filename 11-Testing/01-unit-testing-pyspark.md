# Module 11 — Testing & Code Quality
## Unit Testing PySpark Pipelines with pytest & chispa

> Testing is what separates production code from scripts. Enterprise clients require 80%+ test coverage. This module shows you exactly how to test PySpark transformations.

---

## 11.1 Why Test PySpark Code?

```
WITHOUT TESTS:
  ✗ Breaking changes discovered in production (3 AM incident)
  ✗ Business logic bugs found by analysts weeks later
  ✗ Refactoring is terrifying — nothing to catch regressions
  ✗ Code review is a gut-feel exercise

WITH TESTS:
  ✅ Breaking changes caught in CI before merge
  ✅ Business logic encoded in executable specs
  ✅ Refactor confidently — tests are your safety net
  ✅ Self-documenting: tests show how functions should behave
```

---

## 11.2 Test Setup — conftest.py

```python
# tests/conftest.py
# Shared fixtures for all test files

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    scope="session" means one session for ALL tests — much faster.
    """
    builder = (
        SparkSession.builder
            .master("local[2]")            # Use 2 local cores
            .appName("DataPlatformTests")
            .config("spark.sql.shuffle.partitions", "4")   # Small — tests are small data
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.preview.enabled", "true")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Suppress noisy logs in test output
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="function")
def sample_transactions(spark):
    """Standard transaction test data reused across tests."""
    return spark.createDataFrame([
        (1, "2024-01-15", "C001", 150.0,   "CARD",       "SUCCESS"),
        (2, "2024-01-15", "C002", 250.50,  "UPI",        "SUCCESS"),
        (3, "2024-01-16", "C001", -50.0,   "CARD",       "REFUND"),    # Negative amount
        (4, "2024-01-16", "C003", None,    "NETBANKING",  "PENDING"),   # Null amount
        (5, "2024-01-16", None,   500.0,   "UPI",        "SUCCESS"),   # Null customer
        (6, "2024-01-17", "C001", 75.25,   "UNKNOWN_MODE","SUCCESS"),   # Invalid mode
        (7, "2024-01-17", "C002", 1200.0,  "CARD",       "SUCCESS"),
    ], ["txn_id", "txn_date", "customer_id", "amount", "payment_mode", "status"])


@pytest.fixture(scope="function")
def sample_customers(spark):
    """Customer reference data for join tests."""
    return spark.createDataFrame([
        ("C001", "Alice",  "Mumbai",  "gold"),
        ("C002", "Bob",    "Delhi",   "silver"),
        ("C003", "Carol",  "Pune",    "bronze"),
    ], ["customer_id", "name", "city", "tier"])


@pytest.fixture(scope="function")
def temp_delta_path(tmp_path):
    """Temporary path for Delta table operations in tests."""
    return str(tmp_path / "delta_test")
```

---

## 11.3 Testing Transformations

```python
# tests/unit/test_transformations.py

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import *
from chispa import assert_df_equality      # pip install chispa — best PySpark test lib
from chispa.dataframe_comparer import DataFrameComparer

# Import the actual transformation functions from your src/
import sys
sys.path.insert(0, "src/")
from data_platform.transformations import (
    clean_transactions,
    apply_amount_categorization,
    calculate_customer_lifetime_value,
    deduplicate_by_key,
)


class TestCleanTransactions:
    """Tests for the clean_transactions transformation function."""
    
    def test_removes_null_amounts(self, spark, sample_transactions):
        """Rows with null amounts must be dropped from output."""
        result = clean_transactions(sample_transactions)
        
        null_count = result.filter(F.col("amount").isNull()).count()
        assert null_count == 0, "Clean transactions should have no null amounts"
    
    def test_removes_null_customers(self, spark, sample_transactions):
        """Rows with null customer_id must be dropped."""
        result = clean_transactions(sample_transactions)
        
        null_count = result.filter(F.col("customer_id").isNull()).count()
        assert null_count == 0
    
    def test_removes_negative_amounts(self, spark, sample_transactions):
        """Negative amounts (refunds) should be flagged, not silently included."""
        result = clean_transactions(sample_transactions)
        
        negative = result.filter(F.col("amount") < 0).count()
        assert negative == 0, "No negative amounts in clean output"
    
    def test_removes_invalid_payment_modes(self, spark, sample_transactions):
        """Rows with unknown payment modes should be rejected."""
        VALID_MODES = ["CARD", "UPI", "NETBANKING", "NEFT", "RTGS", "IMPS"]
        result = clean_transactions(sample_transactions)
        
        invalid = result.filter(~F.col("payment_mode").isin(VALID_MODES)).count()
        assert invalid == 0
    
    def test_valid_records_retained(self, spark, sample_transactions):
        """Valid records must all survive cleaning."""
        result = clean_transactions(sample_transactions)
        
        # txn_id 1, 2, 7 are all valid
        valid_ids = [1, 2, 7]
        retained = result.filter(F.col("txn_id").isin(valid_ids)).count()
        assert retained == 3, f"Expected 3 valid records, got {retained}"
    
    def test_output_record_count(self, spark, sample_transactions):
        """Exactly 3 records should pass all validations."""
        result = clean_transactions(sample_transactions)
        assert result.count() == 3
    
    def test_schema_preserved(self, spark, sample_transactions):
        """Output schema should match input schema (no accidental column drops)."""
        result = clean_transactions(sample_transactions)
        
        expected_cols = set(sample_transactions.columns)
        actual_cols   = set(result.columns)
        
        assert expected_cols == actual_cols, \
            f"Schema mismatch. Missing: {expected_cols - actual_cols}"
    
    def test_empty_dataframe_handled(self, spark):
        """Function should handle empty DataFrames without errors."""
        schema = StructType([
            StructField("txn_id",      IntegerType()),
            StructField("customer_id", StringType()),
            StructField("amount",      DoubleType()),
            StructField("payment_mode",StringType()),
        ])
        empty_df = spark.createDataFrame([], schema)
        
        result = clean_transactions(empty_df)
        assert result.count() == 0  # Should return empty, not error


class TestAmountCategorization:
    """Tests for amount bucket categorization."""
    
    def test_low_amount_category(self, spark):
        """Amounts < 100 should be categorized as LOW."""
        df = spark.createDataFrame([(1, 50.0), (2, 99.99)], ["id", "amount"])
        result = apply_amount_categorization(df)
        
        lows = result.filter(F.col("amount_category") == "LOW").count()
        assert lows == 2
    
    def test_medium_amount_category(self, spark):
        """Amounts 100-999.99 → MEDIUM."""
        df = spark.createDataFrame([(1, 100.0), (2, 500.0), (3, 999.99)], ["id", "amount"])
        result = apply_amount_categorization(df)
        
        mediums = result.filter(F.col("amount_category") == "MEDIUM").count()
        assert mediums == 3
    
    def test_boundary_values(self, spark):
        """Test exact boundary values (classic off-by-one bug location)."""
        df = spark.createDataFrame([
            (1, 99.99,   "LOW"),     # just below LOW threshold
            (2, 100.0,   "MEDIUM"),  # exact MEDIUM boundary
            (3, 999.99,  "MEDIUM"),  # just below HIGH
            (4, 1000.0,  "HIGH"),    # exact HIGH boundary
            (5, 10000.0, "PREMIUM"), # exact PREMIUM boundary
        ], ["id", "amount", "expected_category"])
        
        result = apply_amount_categorization(df)
        
        # Use chispa for DataFrame equality comparison
        for row in result.collect():
            assert row["amount_category"] == row["expected_category"], \
                f"txn {row['id']}: expected {row['expected_category']}, got {row['amount_category']}"
    
    def test_null_amount_returns_null_category(self, spark):
        """Null amount should result in null category, not error."""
        df = spark.createDataFrame([(1, None)], ["id", "amount"])
        result = apply_amount_categorization(df)
        
        null_cat = result.filter(F.col("amount_category").isNull()).count()
        assert null_cat == 1


class TestDeduplication:
    """Tests for deduplication logic."""
    
    def test_keeps_latest_by_updated_at(self, spark):
        """When duplicates exist, keep the most recently updated record."""
        from datetime import datetime
        
        df = spark.createDataFrame([
            (1, "C001", 150.0, "PENDING",   datetime(2024,1,15,10,0,0)),
            (1, "C001", 150.0, "COMPLETED", datetime(2024,1,15,12,0,0)),  # Later
            (2, "C002", 250.0, "SUCCESS",   datetime(2024,1,15,9,0,0)),
        ], ["txn_id", "customer_id", "amount", "status", "updated_at"])
        
        result = deduplicate_by_key(df, key_col="txn_id", order_col="updated_at")
        
        assert result.count() == 2  # Only 2 unique txn_ids
        
        # txn_id=1 should have COMPLETED status (the later record)
        status = result.filter(F.col("txn_id") == 1).first()["status"]
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"
    
    def test_no_duplicates_unchanged(self, spark):
        """If no duplicates, all records should pass through unchanged."""
        df = spark.createDataFrame([
            (1, "SUCCESS"), (2, "PENDING"), (3, "FAILED")
        ], ["txn_id", "status"])
        
        result = deduplicate_by_key(df, key_col="txn_id", order_col="txn_id")
        assert result.count() == 3


class TestCustomerLifetimeValue:
    """Tests for CLV calculation (window function-based)."""
    
    def test_running_total_correct(self, spark):
        """Running total should accumulate correctly within customer partition."""
        from datetime import date
        
        df = spark.createDataFrame([
            ("C001", date(2024,1,1), 100.0),
            ("C001", date(2024,1,5), 200.0),
            ("C001", date(2024,1,10), 150.0),
            ("C002", date(2024,1,1), 300.0),  # Different customer — separate running total
        ], ["customer_id", "txn_date", "amount"])
        
        result = calculate_customer_lifetime_value(df)
        result_sorted = result.orderBy("customer_id", "txn_date")
        
        rows = result_sorted.collect()
        
        # C001 running totals: 100, 300, 450
        assert rows[0]["running_total"] == 100.0
        assert rows[1]["running_total"] == 300.0
        assert rows[2]["running_total"] == 450.0
        
        # C002: starts fresh at 300
        assert rows[3]["running_total"] == 300.0
    
    def test_clv_resets_per_customer(self, spark):
        """Running total must reset for each customer (partition by customer_id)."""
        df = spark.createDataFrame([
            ("C001", "2024-01-01", 1000.0),
            ("C002", "2024-01-01",  500.0),
        ], ["customer_id", "txn_date", "amount"])
        
        result = calculate_customer_lifetime_value(df)
        
        # C002's first transaction should show 500, not 1500
        c002_total = result.filter(F.col("customer_id") == "C002") \
            .first()["running_total"]
        assert c002_total == 500.0
```

---

## 11.4 Testing Delta Operations

```python
# tests/unit/test_delta_operations.py

import pytest
import os
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from chispa import assert_df_equality


class TestDeltaMerge:
    """Tests for MERGE/upsert operations on Delta tables."""
    
    def test_merge_inserts_new_records(self, spark, temp_delta_path):
        """New records not in target should be inserted."""
        # Create target table with 2 records
        target_data = spark.createDataFrame([
            (1, "C001", 100.0, "PENDING"),
            (2, "C002", 200.0, "SUCCESS"),
        ], ["txn_id", "customer_id", "amount", "status"])
        
        target_data.write.format("delta").save(temp_delta_path)
        
        # Source has 1 new record (txn_id=3)
        source_data = spark.createDataFrame([
            (3, "C003", 300.0, "PENDING"),  # New
        ], ["txn_id", "customer_id", "amount", "status"])
        
        # Run merge
        target = DeltaTable.forPath(spark, temp_delta_path)
        (target.alias("t")
         .merge(source_data.alias("s"), "t.txn_id = s.txn_id")
         .whenNotMatchedInsertAll()
         .execute())
        
        result = spark.read.format("delta").load(temp_delta_path)
        assert result.count() == 3  # Original 2 + 1 new
        
        new_record = result.filter(F.col("txn_id") == 3).first()
        assert new_record["customer_id"] == "C003"
    
    def test_merge_updates_existing_records(self, spark, temp_delta_path):
        """Existing records with changed status should be updated."""
        target_data = spark.createDataFrame([
            (1, "C001", 100.0, "PENDING"),
        ], ["txn_id", "customer_id", "amount", "status"])
        
        target_data.write.format("delta").save(temp_delta_path)
        
        # Same txn_id but status changed
        source_data = spark.createDataFrame([
            (1, "C001", 100.0, "COMPLETED"),  # Status updated
        ], ["txn_id", "customer_id", "amount", "status"])
        
        target = DeltaTable.forPath(spark, temp_delta_path)
        (target.alias("t")
         .merge(source_data.alias("s"), "t.txn_id = s.txn_id")
         .whenMatchedUpdateAll()
         .execute())
        
        result = spark.read.format("delta").load(temp_delta_path)
        assert result.count() == 1  # Still 1 record
        assert result.first()["status"] == "COMPLETED"
    
    def test_merge_is_idempotent(self, spark, temp_delta_path):
        """Running the same merge twice should produce identical results."""
        initial = spark.createDataFrame(
            [(1, 100.0)], ["id", "amount"]
        )
        initial.write.format("delta").save(temp_delta_path)
        
        new_data = spark.createDataFrame([(2, 200.0)], ["id", "amount"])
        
        def do_merge():
            target = DeltaTable.forPath(spark, temp_delta_path)
            (target.alias("t")
             .merge(new_data.alias("s"), "t.id = s.id")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        
        do_merge()  # First run
        count_after_first = spark.read.format("delta").load(temp_delta_path).count()
        
        do_merge()  # Second run (same data)
        count_after_second = spark.read.format("delta").load(temp_delta_path).count()
        
        assert count_after_first == count_after_second == 2  # Idempotent!
    
    def test_time_travel_version(self, spark, temp_delta_path):
        """After a write, previous version should be accessible via time travel."""
        v0_data = spark.createDataFrame([(1, "original")], ["id", "value"])
        v0_data.write.format("delta").save(temp_delta_path)
        
        v1_data = spark.createDataFrame([(1, "updated")], ["id", "value"])
        v1_data.write.format("delta").mode("overwrite").save(temp_delta_path)
        
        # Current version
        current = spark.read.format("delta").load(temp_delta_path).first()["value"]
        assert current == "updated"
        
        # Version 0
        v0 = spark.read.format("delta") \
            .option("versionAsOf", 0) \
            .load(temp_delta_path) \
            .first()["value"]
        assert v0 == "original"
```

---

## 11.5 Testing with chispa — DataFrame Equality

```python
# chispa provides clean DataFrame comparison with good error messages
# pip install chispa

from chispa import assert_df_equality
from chispa.dataframe_comparer import DataFrameComparer

def test_transformation_output(spark):
    """Use chispa for full DataFrame equality assertion."""
    input_df = spark.createDataFrame([
        (1, "hello world"),
        (2, "  SPACES  "),
    ], ["id", "text"])
    
    # Apply transformation
    result = input_df.withColumn("cleaned", F.trim(F.lower(F.col("text"))))
    
    # Expected output
    expected = spark.createDataFrame([
        (1, "hello world", "hello world"),
        (2, "  SPACES  ",  "spaces"),
    ], ["id", "text", "cleaned"])
    
    # Assert with chispa (handles row ordering, ignores partition differences)
    assert_df_equality(
        result,
        expected,
        ignore_row_order=True,      # Don't care about row ordering
        ignore_nullable=True,       # Ignore nullable flag differences
        ignore_column_order=False,  # Column order matters
    )

def test_with_schema_assertion(spark):
    """Assert both schema AND data match."""
    result   = some_transform(input_df)
    expected = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(
        result, expected,
        ignore_row_order=True,
        ignore_nullable=True,
    )
```

---

## 11.6 Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov chispa pyspark==3.4.1 delta-spark==2.4.0

# Run all tests
pytest tests/ -v

# Run specific test class
pytest tests/unit/test_transformations.py::TestCleanTransactions -v

# Run with coverage report
pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

# Run only fast unit tests (exclude integration)
pytest tests/unit/ -v -m "not slow"

# Run in parallel (faster CI)
pip install pytest-xdist
pytest tests/unit/ -v -n auto  # Use all available CPUs

# Output example:
# tests/unit/test_transformations.py::TestCleanTransactions::test_removes_null_amounts PASSED
# tests/unit/test_transformations.py::TestCleanTransactions::test_output_record_count PASSED
# tests/unit/test_delta_operations.py::TestDeltaMerge::test_merge_is_idempotent PASSED
# 
# ======= 23 passed in 45.3s =======
# Coverage: 87%
```

---

## 11.7 The Source Functions Being Tested

```python
# src/data_platform/transformations.py
# These are the functions tested above

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, row_number
from pyspark.sql import Window

VALID_PAYMENT_MODES = ["CARD", "UPI", "NETBANKING", "NEFT", "RTGS", "IMPS", "WALLET"]

def clean_transactions(df: DataFrame) -> DataFrame:
    """Remove invalid records from transaction DataFrame."""
    return df.filter(
        col("txn_id").isNotNull() &
        col("amount").isNotNull() &
        col("customer_id").isNotNull() &
        (col("amount") > 0) &
        col("payment_mode").isin(VALID_PAYMENT_MODES)
    )

def apply_amount_categorization(df: DataFrame) -> DataFrame:
    """Add amount_category column based on transaction amount."""
    return df.withColumn(
        "amount_category",
        when(col("amount") < 100,    "LOW")
        .when(col("amount") < 1000,  "MEDIUM")
        .when(col("amount") < 10000, "HIGH")
        .when(col("amount").isNotNull(), "PREMIUM")
        .otherwise(None)
    )

def deduplicate_by_key(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """Keep the most recent record for each unique key."""
    w = Window.partitionBy(key_col).orderBy(col(order_col).desc())
    return df.withColumn("_rn", row_number().over(w)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")

def calculate_customer_lifetime_value(df: DataFrame) -> DataFrame:
    """Calculate running total spend per customer."""
    from pyspark.sql.functions import sum as spark_sum
    
    w = Window.partitionBy("customer_id") \
              .orderBy("txn_date") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    return df.withColumn("running_total", spark_sum("amount").over(w))
```

---

## 11.8 Interview Questions — Testing

**Q: How do you unit test PySpark code that runs on a cluster?**
> Use `SparkSession.builder.master("local[N]")` to create a local Spark session in your test environment — no cluster needed. The API is identical; the difference is execution is local. Use `scope="session"` in pytest fixtures so Spark starts once for all tests (not once per test — very slow). Use `chispa` library for DataFrame equality assertions — it handles row ordering, nullable flags, and gives descriptive error messages when tests fail.

**Q: What would you test in a MERGE operation?**
> Four test scenarios: (1) **Insert new records** — source has a key not in target → target grows by that count. (2) **Update existing records** — source has matching key with changed values → target has updated values, same row count. (3) **No-op on unchanged records** — source has matching key with identical values → target unchanged. (4) **Idempotency** — run the merge twice with same source → identical result both times (critical for retry safety). Also test: merge with empty source, merge with null keys (null != null in joins).

---
*Next: [Module 12 — Terraform & Infrastructure as Code for Databricks](../12-CLI-Terraform/01-terraform-databricks.md)*
