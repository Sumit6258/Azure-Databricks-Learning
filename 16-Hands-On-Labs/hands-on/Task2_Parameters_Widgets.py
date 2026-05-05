# HANDS-ON TASK 2
## Notebook Parameters, Widgets — All Types with Live Examples

> **Create a NEW notebook in Databricks. Copy each cell block. Run cell by cell.**

---

## CELL 1 — Understanding Parameters and Widgets

```python
# ════════════════════════════════════════════════════════════════
# WHAT ARE WIDGETS?
# ════════════════════════════════════════════════════════════════

# A widget is a UI input element (text box, dropdown, etc.)
# that appears at the TOP of your notebook.
#
# WHY USE WIDGETS?
#   Without widgets → you hardcode values in your code:
#       batch_date = "2024-01-15"   ← must edit code to change
#
#   With widgets → value comes from the UI / from ADF / from job:
#       batch_date = dbutils.widgets.get("batch_date")  ← dynamic!
#
# THREE WAYS WIDGETS RECEIVE VALUES:
#   1. You type/select in the Databricks UI (interactive use)
#   2. ADF (Azure Data Factory) passes them as "Base Parameters"
#   3. A Databricks Job passes them as "Notebook Task Parameters"
#   4. Another notebook passes them via dbutils.notebook.run()

print("Widgets are Databricks' way of parameterizing notebooks!")
print("They make notebooks work like functions that accept arguments.")
```

---

## CELL 2 — Widget Type 1: TEXT

```python
# ════════════════════════════════════════════════════════════════
# TEXT WIDGET — Free-form text input
# ════════════════════════════════════════════════════════════════

# After running this cell, a text box appears at the TOP of the notebook
# You can type anything into it

dbutils.widgets.text(
    name="batch_date",          # Internal name — use this to get the value
    defaultValue="2024-01-15",  # What appears in the box by default
    label="📅 Batch Date"       # Label shown in the UI next to the box
)

# READ THE VALUE
batch_date = dbutils.widgets.get("batch_date")

print(f"Batch date from widget: {batch_date}")
print(f"Type: {type(batch_date)}")  # Always returns STRING — cast if needed

# IMPORTANT: Widget values are ALWAYS strings — cast them!
from datetime import datetime, timedelta

# Convert to date if needed
from datetime import date
processing_date = datetime.strptime(batch_date, "%Y-%m-%d").date()
print(f"As Python date: {processing_date}")
print(f"Type now: {type(processing_date)}")

# Calculate previous day (common pattern in ETL)
previous_date = (datetime.strptime(batch_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
print(f"Previous day: {previous_date}")

# Use in your code
print(f"\n--- Using the widget value in a query ---")
df = spark.createDataFrame([
    ("2024-01-14", "TXN001", 1500.0),
    ("2024-01-15", "TXN002", 2500.0),
    ("2024-01-15", "TXN003", 800.0),
    ("2024-01-16", "TXN004", 3200.0),
], ["txn_date", "txn_id", "amount"])

# Filter using the widget value
result = df.filter(df.txn_date == batch_date)
print(f"Transactions for {batch_date}:")
result.show()
```

---

## CELL 3 — Widget Type 2: DROPDOWN

```python
# ════════════════════════════════════════════════════════════════
# DROPDOWN WIDGET — Select from a fixed list of options
# ════════════════════════════════════════════════════════════════

dbutils.widgets.dropdown(
    name="environment",
    defaultValue="dev",                  # Must be one of the choices
    choices=["dev", "test", "prod"],     # The list of options shown
    label="🌍 Environment"
)

# READ
environment = dbutils.widgets.get("environment")
print(f"Selected environment: {environment}")

# USE IT — Route to different paths based on environment
storage_config = {
    "dev":  {
        "bronze_path": "/FileStore/dev/bronze/",
        "silver_path": "/FileStore/dev/silver/",
        "gold_path":   "/FileStore/dev/gold/",
        "parallelism": 2
    },
    "test": {
        "bronze_path": "abfss://bronze@testlake.dfs.core.windows.net/",
        "silver_path": "abfss://silver@testlake.dfs.core.windows.net/",
        "gold_path":   "abfss://gold@testlake.dfs.core.windows.net/",
        "parallelism": 4
    },
    "prod": {
        "bronze_path": "abfss://bronze@prodlake.dfs.core.windows.net/",
        "silver_path": "abfss://silver@prodlake.dfs.core.windows.net/",
        "gold_path":   "abfss://gold@prodlake.dfs.core.windows.net/",
        "parallelism": 16
    }
}

config = storage_config[environment]
print(f"\nConfiguration for '{environment}':")
for key, value in config.items():
    print(f"  {key}: {value}")

bronze_path  = config["bronze_path"]
silver_path  = config["silver_path"]
parallelism  = config["parallelism"]

print(f"\nWill read from:  {bronze_path}")
print(f"Will write to:   {silver_path}")
print(f"Parallelism set: {parallelism}")
```

---

## CELL 4 — Widget Type 3: COMBOBOX

```python
# ════════════════════════════════════════════════════════════════
# COMBOBOX WIDGET — Dropdown + can also type a custom value
# ════════════════════════════════════════════════════════════════

# DIFFERENCE from Dropdown:
#   Dropdown  → can ONLY select from the given list
#   Combobox  → can select from list OR type a custom value

dbutils.widgets.combobox(
    name="table_name",
    defaultValue="transactions",
    choices=["transactions", "customers", "products", "orders"],
    label="📋 Table Name"
)

# READ
table_name = dbutils.widgets.get("table_name")
print(f"Selected table: {table_name}")

# Real usage: build dynamic path or query
print(f"\nWill process table: {table_name}")
print(f"Bronze source path: /mnt/bronze/{table_name}/")
print(f"Silver target path: /mnt/silver/{table_name}_clean/")

# Config-driven approach using the widget
table_configs = {
    "transactions": {
        "primary_key": "txn_id",
        "partition_col": "txn_date",
        "watermark_col": "updated_at"
    },
    "customers": {
        "primary_key": "customer_id",
        "partition_col": None,
        "watermark_col": "modified_at"
    },
    "products": {
        "primary_key": "product_id",
        "partition_col": "category",
        "watermark_col": "updated_at"
    }
}

if table_name in table_configs:
    cfg = table_configs[table_name]
    print(f"\nTable config:")
    print(f"  Primary key:   {cfg['primary_key']}")
    print(f"  Partition col: {cfg['partition_col']}")
    print(f"  Watermark col: {cfg['watermark_col']}")
else:
    print(f"\n⚠️ Custom table '{table_name}' — using default config")
```

---

## CELL 5 — Widget Type 4: MULTISELECT

```python
# ════════════════════════════════════════════════════════════════
# MULTISELECT WIDGET — Select multiple values from a list
# ════════════════════════════════════════════════════════════════

dbutils.widgets.multiselect(
    name="departments",
    defaultValue="Engineering",
    choices=["Engineering", "Marketing", "HR", "Finance", "Operations"],
    label="🏢 Departments"
)

# READ — returns a COMMA-SEPARATED STRING, not a list!
departments_str = dbutils.widgets.get("departments")
print(f"Raw value from widget: '{departments_str}'")
print(f"Type: {type(departments_str)}")   # str

# CONVERT to Python list
departments_list = [d.strip() for d in departments_str.split(",")]
print(f"As Python list: {departments_list}")

# Use the list in a filter
employee_data = [
    (1, "Sumit",   "Engineering", 95000.0),
    (2, "Priya",   "Marketing",   75000.0),
    (3, "Rahul",   "Engineering", 110000.0),
    (4, "Anjali",  "HR",          65000.0),
    (5, "Vikram",  "Engineering", 105000.0),
    (6, "Arjun",   "Finance",     90000.0),
]

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

schema = StructType([
    StructField("emp_id",     IntegerType()),
    StructField("name",       StringType()),
    StructField("department", StringType()),
    StructField("salary",     DoubleType()),
])

df_emp = spark.createDataFrame(employee_data, schema)

# Filter by selected departments
df_filtered = df_emp.filter(col("department").isin(departments_list))
print(f"\nEmployees in selected departments: {departments_list}")
df_filtered.show()
print(f"Total: {df_filtered.count()}")
```

---

## CELL 6 — Using All 4 Widgets Together (Real Pipeline Pattern)

```python
# ════════════════════════════════════════════════════════════════
# REAL-WORLD PATTERN: Multiple widgets for a production pipeline
# ════════════════════════════════════════════════════════════════

# First, remove all previous widgets and start fresh
dbutils.widgets.removeAll()

# ── DEFINE ALL WIDGETS ────────────────────────────────────────
dbutils.widgets.text(
    "p_batch_date",
    (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
    "📅 Batch Date (YYYY-MM-DD)"
)

dbutils.widgets.dropdown(
    "p_environment",
    "dev",
    ["dev", "test", "prod"],
    "🌍 Environment"
)

dbutils.widgets.combobox(
    "p_source_table",
    "transactions",
    ["transactions", "customers", "products"],
    "📋 Source Table"
)

dbutils.widgets.multiselect(
    "p_regions",
    "All",
    ["All", "North", "South", "East", "West"],
    "🗺️ Regions"
)

# ── READ ALL WIDGETS ──────────────────────────────────────────
batch_date   = dbutils.widgets.get("p_batch_date")
environment  = dbutils.widgets.get("p_environment")
source_table = dbutils.widgets.get("p_source_table")
regions_str  = dbutils.widgets.get("p_regions")

# ── PROCESS VALUES ────────────────────────────────────────────
regions_list = [r.strip() for r in regions_str.split(",")]
include_all_regions = "All" in regions_list

# ── PRINT PIPELINE CONFIGURATION ─────────────────────────────
print("=" * 55)
print("  🚀 PIPELINE CONFIGURATION")
print("=" * 55)
print(f"  Batch Date  : {batch_date}")
print(f"  Environment : {environment.upper()}")
print(f"  Source Table: {source_table}")
print(f"  Regions     : {'ALL REGIONS' if include_all_regions else regions_list}")
print("=" * 55)

# ── SIMULATED PIPELINE EXECUTION ─────────────────────────────
print(f"\n[Step 1] Reading {source_table} data for {batch_date}...")
print(f"         Source: /mnt/{environment}/bronze/{source_table}/date={batch_date}/")

print(f"\n[Step 2] Applying business rules...")
if include_all_regions:
    print("         Region filter: NONE (processing all regions)")
else:
    print(f"         Region filter: {regions_list}")

print(f"\n[Step 3] Writing to Silver layer...")
print(f"         Target: /mnt/{environment}/silver/{source_table}_clean/")

print(f"\n✅ Pipeline configuration complete. Ready to execute.")
```

---

## CELL 7 — How ADF Passes Parameters to Widgets

```python
# ════════════════════════════════════════════════════════════════
# HOW ADF (Azure Data Factory) PASSES PARAMETERS
# ════════════════════════════════════════════════════════════════

# In ADF → Pipeline → Databricks Notebook Activity → Base Parameters:
# {
#     "p_batch_date":   "@formatDateTime(pipeline().parameters.TriggerDate, 'yyyy-MM-dd')",
#     "p_environment":  "prod",
#     "p_source_table": "@pipeline().parameters.TableName",
#     "p_regions":      "All"
# }
#
# These BASE PARAMETERS override the widget defaultValue
# The notebook reads them exactly the same way — via dbutils.widgets.get()

# SIMULATION: What happens when ADF calls this notebook
print("When ADF calls this notebook:")
print("  1. ADF creates a Job Cluster (fresh, no leftover state)")
print("  2. ADF sends Base Parameters: {'p_batch_date': '2024-01-15', ...}")
print("  3. Databricks assigns these as widget VALUES")
print("  4. Your code calls dbutils.widgets.get('p_batch_date')")
print("  5. Returns '2024-01-15' (the ADF-supplied value)")
print("")
print("The SAME code works for:")
print("  ✅ Interactive use (you type in the widget box)")
print("  ✅ ADF pipeline (ADF fills the widget values)")
print("  ✅ Databricks Job (job fills the widget values)")
print("  ✅ Parent notebook (passes via dbutils.notebook.run())")
```

---

## CELL 8 — Manage Widgets: View, Remove, Remove All

```python
# ════════════════════════════════════════════════════════════════
# MANAGING WIDGETS
# ════════════════════════════════════════════════════════════════

# See current widget values (as a dictionary)
# Note: dbutils.widgets does not have a "list" method
# You access values one by one

current_batch_date  = dbutils.widgets.get("p_batch_date")
current_environment = dbutils.widgets.get("p_environment")

print("Current widget values:")
print(f"  p_batch_date:   {current_batch_date}")
print(f"  p_environment:  {current_environment}")

# Remove a single widget
dbutils.widgets.remove("p_regions")
print("\n✅ Removed 'p_regions' widget")

# Try to access removed widget — this FAILS
try:
    val = dbutils.widgets.get("p_regions")
except Exception as e:
    print(f"Expected error: {e}")

# Remove ALL widgets at once
dbutils.widgets.removeAll()
print("\n✅ Removed all widgets")

# Confirm they're gone
try:
    val = dbutils.widgets.get("p_batch_date")
except Exception as e:
    print(f"Expected error: widgets are gone: {e}")

print("\nNote: Always call removeAll() before redefining widgets in same notebook")
print("      Otherwise you get 'Widget already exists' error on re-run")
```

---

## CELL 9 — Default Values and Null Safety Pattern

```python
# ════════════════════════════════════════════════════════════════
# PRODUCTION PATTERN: Safe widget reading with defaults
# ════════════════════════════════════════════════════════════════

dbutils.widgets.removeAll()

# Re-create with sensible defaults
dbutils.widgets.text("run_date",    "", "Run Date (empty = use yesterday)")
dbutils.widgets.text("max_records", "", "Max Records (empty = no limit)")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG","INFO","WARNING","ERROR"])

# SAFE READING WITH FALLBACK LOGIC
def get_widget_or_default(name: str, default_value):
    """
    Read a widget value. If empty, return default_value.
    This handles the case where ADF doesn't pass the parameter.
    """
    try:
        value = dbutils.widgets.get(name).strip()
        return value if value else default_value
    except Exception:
        return default_value

# Use the helper
from datetime import date, timedelta

run_date = get_widget_or_default(
    "run_date",
    (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
)

max_records_str = get_widget_or_default("max_records", "0")
max_records     = int(max_records_str) if max_records_str.isdigit() else 0
log_level       = get_widget_or_default("log_level", "INFO")

print(f"Run date    : {run_date}")
print(f"Max records : {'No limit' if max_records == 0 else max_records}")
print(f"Log level   : {log_level}")

# Type-safe conversions with validation
def validate_date(date_str: str) -> bool:
    """Check if a string is a valid yyyy-MM-dd date."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

if not validate_date(run_date):
    raise ValueError(f"Invalid date format: '{run_date}'. Expected yyyy-MM-dd")

print(f"\n✅ All parameters validated successfully")
print(f"   Pipeline will process data for: {run_date}")
```

