# HANDS-ON TASK 3
## Child Notebooks — Pass Parameters, Get Return Values

> **You need TWO notebooks for this task.**
> Notebook A = Parent (this file, top section)
> Notebook B = Child  (this file, bottom section)
> Create them separately in Databricks.

---

# ════════════════════════════════════════════════════════════════
# NOTEBOOK B — THE CHILD NOTEBOOK
# ════════════════════════════════════════════════════════════════
# Save this as:  /Users/your-email/child_notebook
# OR in Repos:   /Repos/your-org/project/notebooks/child_notebook

## CHILD CELL 1 — Define widgets (receive parameters from parent)

```python
# ════════════════════════════════════════════════════════════════
# CHILD NOTEBOOK
# This notebook is meant to be called FROM a parent notebook.
# It receives parameters, does work, and returns a result.
# ════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, count, sum, avg, round
from pyspark.sql.types import *
import json

# ── STEP 1: Define widgets to receive parameters ──────────────
# These widgets will be populated by the PARENT's arguments{}
dbutils.widgets.text("table_name",  "transactions",  "Table Name")
dbutils.widgets.text("batch_date",  "2024-01-15",    "Batch Date")
dbutils.widgets.text("environment", "dev",           "Environment")
dbutils.widgets.text("threshold",   "1000",          "Amount Threshold")

# ── STEP 2: Read the parameter values ─────────────────────────
table_name  = dbutils.widgets.get("table_name")
batch_date  = dbutils.widgets.get("batch_date")
environment = dbutils.widgets.get("environment")
threshold   = float(dbutils.widgets.get("threshold"))

print("=" * 50)
print("  CHILD NOTEBOOK STARTED")
print("=" * 50)
print(f"  Table     : {table_name}")
print(f"  Date      : {batch_date}")
print(f"  Env       : {environment}")
print(f"  Threshold : {threshold}")
print("=" * 50)
```

## CHILD CELL 2 — Do the actual work

```python
# ── STEP 3: Do the processing (the child notebook's job) ──────

# Create sample data for this example
# In production, you'd read from: /mnt/{environment}/bronze/{table_name}/
sample_data = [
    (1001, "C001", 1500.0, "2024-01-15", "CARD",       "SUCCESS"),
    (1002, "C002",  250.0, "2024-01-15", "UPI",        "SUCCESS"),
    (1003, "C001", 8900.0, "2024-01-16", "NETBANKING", "FAILED"),
    (1004, "C003",  450.0, "2024-01-15", "UPI",        "SUCCESS"),
    (1005, "C002", 3200.0, "2024-01-15", "CARD",       "SUCCESS"),
    (1006, "C004",  125.0, "2024-01-15", "UPI",        "PENDING"),
    (1007, "C001", 9999.0, "2024-01-15", "CARD",       "SUCCESS"),
    (1008, "C003",  670.0, "2024-01-15", "NETBANKING", "SUCCESS"),
]

schema = StructType([
    StructField("txn_id",       IntegerType()),
    StructField("customer_id",  StringType()),
    StructField("amount",       DoubleType()),
    StructField("txn_date",     StringType()),
    StructField("payment_mode", StringType()),
    StructField("status",       StringType()),
])

df = spark.createDataFrame(sample_data, schema)

# Filter by the date passed from parent
df_filtered = df.filter(col("txn_date") == batch_date)
print(f"\nRecords for {batch_date}: {df_filtered.count()}")

# Apply the threshold passed from parent
df_above = df_filtered.filter(col("amount") >= threshold)
df_below = df_filtered.filter(col("amount") < threshold)

print(f"Above threshold (₹{threshold}): {df_above.count()}")
print(f"Below threshold (₹{threshold}): {df_below.count()}")

# Calculate summary
summary = df_filtered.agg(
    count("txn_id").alias("total_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    count(col("txn_id") if col("status") == "SUCCESS" else None).alias("success_count")
).first()

total_count  = df_filtered.count()
success_count= df_filtered.filter(col("status") == "SUCCESS").count()
total_amount = df_filtered.select(sum("amount")).first()[0]

print(f"\nSummary:")
print(f"  Total transactions: {total_count}")
print(f"  Successful:         {success_count}")
print(f"  Total amount:       ₹{total_amount:,.2f}")

display(df_filtered)
```

## CHILD CELL 3 — Return result to parent

```python
# ── STEP 4: Return a result to the PARENT notebook ────────────

# Build a result dictionary — we'll JSON-encode it as a string
# because dbutils.notebook.exit() only accepts a STRING

result = {
    "status":         "SUCCESS",
    "table_name":     table_name,
    "batch_date":     batch_date,
    "records_processed": total_count,
    "success_records":   success_count,
    "failed_records":    total_count - success_count,
    "total_amount":      round(total_amount, 2),
    "above_threshold":   df_above.count(),
    "below_threshold":   df_below.count()
}

result_json = json.dumps(result)
print(f"\n✅ Child notebook complete. Returning result to parent:")
print(result_json)

# THIS IS THE RETURN VALUE — parent notebook will receive this string
dbutils.notebook.exit(result_json)
```

---

---

# ════════════════════════════════════════════════════════════════
# NOTEBOOK A — THE PARENT NOTEBOOK
# ════════════════════════════════════════════════════════════════
# Save this as: /Users/your-email/parent_notebook
# The child notebook path above must match exactly

## PARENT CELL 1 — Setup parameters

```python
# ════════════════════════════════════════════════════════════════
# PARENT NOTEBOOK
# Calls the child notebook with parameters and reads the result
# ════════════════════════════════════════════════════════════════

import json
from datetime import datetime, timedelta

# ── Parent defines its own widgets (from ADF or manual input) ──
dbutils.widgets.removeAll()

dbutils.widgets.text(
    "run_date",
    (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
    "📅 Run Date"
)
dbutils.widgets.dropdown(
    "env",
    "dev",
    ["dev", "test", "prod"],
    "🌍 Environment"
)

run_date = dbutils.widgets.get("run_date")
env      = dbutils.widgets.get("env")

print("=" * 55)
print("  🚀 PARENT NOTEBOOK STARTED")
print("=" * 55)
print(f"  Run Date    : {run_date}")
print(f"  Environment : {env}")
print("=" * 55)
```

## PARENT CELL 2 — Call child notebook (single call)

```python
# ════════════════════════════════════════════════════════════════
# CALLING A CHILD NOTEBOOK
# dbutils.notebook.run(path, timeout, arguments)
# ════════════════════════════════════════════════════════════════

print("\n[Step 1] Calling child notebook for 'transactions'...")

# ── THE CALL ──────────────────────────────────────────────────
result_str = dbutils.notebook.run(
    path="./child_notebook",    # Path to child notebook
                                # Can be relative: ./child_notebook
                                # Or absolute: /Users/you@email.com/child_notebook
                                # Or Repos path: /Repos/org/project/notebooks/child

    timeout_seconds=600,        # Fail if child takes > 10 minutes
                                # This prevents hung jobs blocking everything

    arguments={                 # These become widget VALUES in child
        "table_name":  "transactions",
        "batch_date":  run_date,     # Pass parent's widget value to child
        "environment": env,          # Pass environment too
        "threshold":   "1000"        # Child will read this as string
    }
)

print(f"[Step 1] ✅ Child returned: {result_str}")

# ── PARSE THE RESULT ──────────────────────────────────────────
result = json.loads(result_str)

print(f"\n📊 Results from child notebook:")
print(f"  Status:             {result['status']}")
print(f"  Table:              {result['table_name']}")
print(f"  Date:               {result['batch_date']}")
print(f"  Records processed:  {result['records_processed']:,}")
print(f"  Successful:         {result['success_records']:,}")
print(f"  Failed:             {result['failed_records']:,}")
print(f"  Total amount:       ₹{result['total_amount']:,.2f}")
print(f"  Above ₹1000:        {result['above_threshold']:,}")
print(f"  Below ₹1000:        {result['below_threshold']:,}")
```

## PARENT CELL 3 — Call multiple child notebooks in sequence

```python
# ════════════════════════════════════════════════════════════════
# CALLING MULTIPLE TABLES — SEQUENTIAL (one after another)
# ════════════════════════════════════════════════════════════════

tables_to_process = ["transactions", "customers", "products"]
all_results = []

print("Processing tables sequentially:")
print("-" * 45)

for table in tables_to_process:
    print(f"\n  ▶ Processing: {table}")
    
    try:
        result_str = dbutils.notebook.run(
            path="./child_notebook",
            timeout_seconds=300,
            arguments={
                "table_name":  table,
                "batch_date":  run_date,
                "environment": env,
                "threshold":   "500"
            }
        )
        
        result = json.loads(result_str)
        all_results.append(result)
        
        print(f"  ✅ {table}: {result['records_processed']} records, "
              f"₹{result['total_amount']:,.2f}")
    
    except Exception as e:
        print(f"  ❌ {table}: FAILED — {str(e)}")
        all_results.append({
            "table_name": table,
            "status": "FAILED",
            "error": str(e)
        })

print("\n" + "=" * 45)
print("ALL TABLES PROCESSED")
print("=" * 45)
for r in all_results:
    status = r.get("status", "UNKNOWN")
    icon   = "✅" if status == "SUCCESS" else "❌"
    print(f"  {icon} {r['table_name']}: {status}")
```

## PARENT CELL 4 — Error handling when child fails

```python
# ════════════════════════════════════════════════════════════════
# ROBUST PARENT: Handle child notebook failures gracefully
# ════════════════════════════════════════════════════════════════

def run_child_notebook(
    notebook_path: str,
    parameters: dict,
    timeout: int = 300
) -> dict:
    """
    Run a child notebook and return its result as a dict.
    Handles errors gracefully — never crashes the parent.
    
    Returns:
        dict with 'success' key: True if ran OK, False if failed
    """
    try:
        result_str = dbutils.notebook.run(
            path=notebook_path,
            timeout_seconds=timeout,
            arguments=parameters
        )
        
        # Try to parse as JSON
        try:
            result = json.loads(result_str)
            result["success"] = True
            return result
        except json.JSONDecodeError:
            # Child returned plain string, not JSON
            return {
                "success": True,
                "raw_result": result_str
            }
    
    except Exception as e:
        error_msg = str(e)
        print(f"  ⚠️ Child notebook failed: {error_msg}")
        return {
            "success": False,
            "error_message": error_msg,
            "notebook": notebook_path
        }


# Usage
print("Running child notebook with error handling:\n")

result = run_child_notebook(
    notebook_path="./child_notebook",
    parameters={
        "table_name":  "transactions",
        "batch_date":  run_date,
        "environment": env,
        "threshold":   "1000"
    },
    timeout=300
)

if result["success"]:
    print(f"✅ Success! Processed {result.get('records_processed', 0)} records")
    print(f"   Total amount: ₹{result.get('total_amount', 0):,.2f}")
else:
    print(f"❌ Failed! Error: {result.get('error_message', 'Unknown error')}")
    print("   Consider: checking child notebook, verifying path, checking permissions")

# Pass result to downstream job tasks
dbutils.jobs.taskValues.set(
    key="ingestion_summary",
    value=json.dumps(result)
)
print("\n✅ Result stored in task values for downstream tasks")
```

## PARENT CELL 5 — The %run alternative (simpler but no parameters)

```python
# ════════════════════════════════════════════════════════════════
# %run vs dbutils.notebook.run() — KEY DIFFERENCES
# ════════════════════════════════════════════════════════════════

comparison = spark.createDataFrame([
    ("Parameters",       "Cannot pass parameters",    "Can pass via arguments dict"),
    ("Return value",     "Cannot get return value",   "Gets return value as string"),
    ("Execution scope",  "Shares current scope",      "Runs in separate scope"),
    ("Variables",        "Child vars available here", "Child vars NOT available"),
    ("Error handling",   "Crashes parent on error",   "Can wrap in try/except"),
    ("Timeout",          "No timeout support",        "Configurable timeout"),
    ("Async/parallel",   "No (sequential only)",      "No (sequential only)"),
    ("Best use case",    "Shared utils/config",       "Parameterized sub-pipelines"),
], ["%run", "dbutils.notebook.run()", "Difference"])

print("Comparison: %run vs dbutils.notebook.run()")
display(comparison)

# ── WHEN TO USE %run ─────────────────────────────────────────
# Use %run for configuration/utility notebooks
# Example: common_functions.py defines reusable functions

# In common_functions.py:
# def clean_string(s): return s.strip().upper()
# BRONZE_PATH = "/mnt/bronze"
# SILVER_PATH = "/mnt/silver"

# In your main notebook:
# %run ./common_functions
# Then immediately:
# cleaned = clean_string("  hello  ")  # Works!
# df.write.save(SILVER_PATH)           # Works!

# ── WHEN TO USE dbutils.notebook.run() ──────────────────────
# Use for sub-pipelines that need parameters and return results
# Example: calling a notebook for each table in a loop (like our example above)
print("\n✅ Rule: use %run for shared code, dbutils.notebook.run() for sub-pipelines")
```

