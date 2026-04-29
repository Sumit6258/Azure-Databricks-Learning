# Module 08 — Databricks Jobs & Production Pipeline Design
## Creating Jobs, Multi-Task Workflows, Dependency Handling & Retry Policies

---

## 8.1 What Are Databricks Jobs?

A **Databricks Job** is the production scheduling and orchestration layer. It replaces manually running notebooks.

```
Development       →    Production
(Run notebook     →    (Job runs notebook on schedule,
 manually)              with logging, retries, alerting)
```

### Jobs vs Workflows vs Pipelines

| Concept | What It Is | When to Use |
|---|---|---|
| **Job** | Scheduled execution of 1+ tasks | Any automated data processing |
| **Multi-task Workflow** | Job with multiple tasks + dependencies | ETL pipelines with stages |
| **Delta Live Tables Pipeline** | Declarative streaming/batch pipeline | When using DLT framework |

---

## 8.2 Creating a Job — Step by Step (UI)

```
1. Databricks Workspace → "Workflows" (left sidebar) → "Create Job"

2. Job Name: daily-transaction-pipeline

3. Task 1: Ingest Bronze
   Task name:   ingest_bronze
   Type:        Notebook
   Path:        /Repos/my-org/data-platform/ingestion/01_raw_ingest
   Cluster:     Create new job cluster (NOT all-purpose!)
   
   New Job Cluster Config:
     Runtime: 13.3 LTS (Spark 3.4.1)
     Worker type: Standard_DS4_v2
     Min workers: 2
     Max workers: 8
     
4. Task 2: Transform Silver
   Task name:    transform_silver
   Type:         Notebook
   Path:         /Repos/my-org/data-platform/transformation/02_silver_transform
   Depends on:   ingest_bronze  ← dependency!
   Cluster:      Same as ingest_bronze (reuse cluster)
   
5. Task 3: Build Gold
   Task name:    build_gold
   Type:         Notebook
   Path:         /Repos/my-org/data-platform/serving/03_gold_aggregate
   Depends on:   transform_silver
   Cluster:      Same as ingest_bronze
   
6. Schedule:
   Type: Cron
   Cron syntax: 0 2 * * *     ← Every day at 2:00 AM UTC
   Timezone: UTC
   
7. Email Notifications:
   On Start:    (optional)
   On Success:  sumit@company.com
   On Failure:  sumit@company.com, team-lead@company.com
   
8. Click "Create"
```

---

## 8.3 Multi-Task Workflow — Architecture

```
         ┌─────────────────────────────────────────────┐
         │          daily-transaction-pipeline          │
         └─────────────────────────────────────────────┘
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ ingest_txns  │  │ingest_customers│ │ ingest_prods │
    │  (Bronze)    │  │  (Bronze)    │  │  (Bronze)    │
    └──────────────┘  └──────────────┘  └──────────────┘
              │               │                │
              └───────────────┼────────────────┘
                              ▼
                    ┌──────────────────┐
                    │ validate_quality │
                    │   (DQ Checks)    │
                    └──────────────────┘
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │transform_txn │  │transform_cust│  │transform_prod│
    │  (Silver)    │  │  (Silver)    │  │  (Silver)    │
    └──────────────┘  └──────────────┘  └──────────────┘
              │               │                │
              └───────────────┼────────────────┘
                              ▼
                    ┌──────────────────┐
                    │   build_gold     │
                    │ (Aggregations)   │
                    └──────────────────┘
                              │
                    ┌──────────────────┐
                    │  refresh_report  │
                    │  (Power BI)      │
                    └──────────────────┘
```

---

## 8.4 Parameterized Notebooks — Production Standard

Never hardcode dates, environments, or table names. Use parameters.

```python
# ── 01_raw_ingest.py ──────────────────────────────────────

# Define parameters with defaults
dbutils.widgets.text("batch_date",   defaultValue="", label="Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("environment",  defaultValue="dev", label="Environment")
dbutils.widgets.text("source_table", defaultValue="transactions", label="Source Table")

# Read parameters
batch_date   = dbutils.widgets.get("batch_date")
environment  = dbutils.widgets.get("environment")
source_table = dbutils.widgets.get("source_table")

# Derive date if not provided (use yesterday for daily jobs)
from datetime import datetime, timedelta

if not batch_date:
    batch_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"🚀 Starting ingestion: {source_table} | Date: {batch_date} | Env: {environment}")

# Environment-based config
config = {
    "dev":  {"bronze_path": "abfss://bronze@devlake.dfs.core.windows.net/",   "parallelism": 2},
    "test": {"bronze_path": "abfss://bronze@testlake.dfs.core.windows.net/",  "parallelism": 4},
    "prod": {"bronze_path": "abfss://bronze@prodlake.dfs.core.windows.net/",  "parallelism": 8},
}

env_config = config[environment]
bronze_path = env_config["bronze_path"]

# Your ETL logic here
source_path = f"{bronze_path}{source_table}/date={batch_date}/"

try:
    df = spark.read.parquet(source_path)
    record_count = df.count()
    
    # Store output for downstream tasks
    dbutils.jobs.taskValues.set(key="record_count", value=str(record_count))
    dbutils.jobs.taskValues.set(key="batch_date",   value=batch_date)
    
    print(f"✅ Ingested {record_count:,} records from {source_table}")
    dbutils.notebook.exit(f"SUCCESS:{record_count}")
    
except Exception as e:
    error_msg = f"FAILED: {str(e)}"
    print(f"❌ {error_msg}")
    dbutils.notebook.exit(error_msg)
    raise  # Re-raise so the job marks this task as FAILED
```

### Reading Task Values in Downstream Tasks

```python
# ── 02_silver_transform.py ──────────────────────────────────────

# Read values set by upstream task
record_count = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",
    key="record_count",
    default="0"
)
batch_date = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",
    key="batch_date",
    default=""
)

print(f"Upstream ingested {record_count} records for batch {batch_date}")

# Abort if no data
if int(record_count) == 0:
    print("⚠️ No records to process. Skipping Silver transformation.")
    dbutils.notebook.exit("SKIPPED:no_data")
```

---

## 8.5 Retry Policies and Error Handling

```python
# ── Retry configuration in Job UI ──
# Task Settings → Retries
# Max retries: 2
# Retry on timeout: ✅
# Min retry interval: 5 minutes

# ── Robust error handling in notebooks ──

import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_read_delta(path: str, max_retries: int = 3) -> "DataFrame":
    """
    Retry wrapper for Delta reads — handles transient storage issues.
    """
    import time
    
    for attempt in range(1, max_retries + 1):
        try:
            df = spark.read.format("delta").load(path)
            logger.info(f"✅ Successfully read from {path} on attempt {attempt}")
            return df
        except Exception as e:
            if attempt < max_retries:
                wait_seconds = 30 * attempt  # Exponential backoff
                logger.warning(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                logger.error(f"❌ All {max_retries} attempts failed for {path}: {e}")
                raise


def process_with_checkpoint(
    process_fn,
    checkpoint_path: str,
    batch_id: str
) -> bool:
    """
    Idempotent processing using a checkpoint file.
    Prevents duplicate processing if job is retried.
    """
    checkpoint_file = f"{checkpoint_path}/batch_{batch_id}.done"
    
    # Check if already processed
    try:
        dbutils.fs.ls(checkpoint_file)
        logger.info(f"⏭️ Batch {batch_id} already processed. Skipping.")
        return True
    except:
        pass  # File doesn't exist — proceed with processing
    
    try:
        process_fn()
        
        # Write checkpoint on success
        dbutils.fs.put(
            checkpoint_file,
            f"Processed at {datetime.utcnow().isoformat()}",
            overwrite=True
        )
        logger.info(f"✅ Batch {batch_id} processed and checkpointed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Processing failed for batch {batch_id}: {e}")
        raise


# Usage in pipeline
def run_silver_transform():
    df = safe_read_delta("/mnt/bronze/transactions/")
    # ... transform logic
    df.write.format("delta").mode("append").save("/mnt/silver/transactions/")

process_with_checkpoint(
    process_fn=run_silver_transform,
    checkpoint_path="/mnt/checkpoints/silver_transform/",
    batch_id=batch_date
)
```

---

## 8.6 Job Clusters vs All-Purpose — Production Decision Matrix

```python
# ── cluster_config.py (reusable cluster definition) ──

# Job cluster for production ETL (cost-optimized)
JOB_CLUSTER_CONFIG = {
    "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS4_v2",
        "autoscale": {
            "min_workers": 2,
            "max_workers": 8
        },
        "spark_conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.databricks.delta.preview.enabled": "true",
        },
        "azure_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK_AZURE",  # Use spot instances (60-80% cheaper)
            "spot_bid_max_price": -1
        },
        "init_scripts": [
            {"dbfs": {"destination": "dbfs:/init_scripts/install_deps.sh"}}
        ]
    }
}
```

### Cost Comparison

```
Scenario: ETL job runs 2 hours daily

All-Purpose Cluster (keep running 24/7):
  VM cost:    Standard_DS4_v2 × 4 nodes = $0.80/hr × 4 = $3.20/hr
  Daily cost: $3.20 × 24 = $76.80/day
  Monthly:    ~$2,300

Job Cluster (spins up for 2 hours):
  VM cost:    Same $3.20/hr
  Daily cost: $3.20 × 2 = $6.40/day  ← only pay for 2 hours
  Monthly:    ~$192

SAVINGS: 92% cost reduction for production workloads
```

---

## 8.7 Dev → Test → Prod Workflow

```
┌────────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT PIPELINE                          │
│                                                                 │
│  Developer     Git Branch      CI/CD          Environment       │
│                                                                 │
│  Write code → feature/xyz → Push → PR → Merge → Deploy        │
│                                                                 │
│  1. Dev works in Databricks Repos (feature branch)            │
│  2. PR to main triggers GitHub Actions CI pipeline            │
│  3. CI runs: linting, unit tests (pytest), integration tests  │
│  4. Merge to main → auto-deploy to TEST environment           │
│  5. QA signs off → manual deploy to PROD                      │
└────────────────────────────────────────────────────────────────┘
```

```yaml
# .github/workflows/databricks-ci.yml

name: Databricks CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install pyspark==3.4.1 pytest delta-spark
      
      - name: Run unit tests
        run: |
          pytest tests/ -v --tb=short
      
      - name: Lint with flake8
        run: |
          pip install flake8
          flake8 src/ --max-line-length=120

  deploy-test:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to TEST
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_TEST_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TEST_TOKEN }}
        run: |
          pip install databricks-cli
          databricks repos update \
            --path /Repos/ci-cd/data-platform \
            --branch main
          
          # Trigger test job run
          databricks jobs run-now \
            --job-id ${{ secrets.TEST_JOB_ID }} \
            --notebook-params '{"environment": "test", "batch_date": "2024-01-15"}'
```

---

## 8.8 Monitoring Jobs — Key Metrics

```python
# ── monitoring.py — Send alerts on job completion ──

import requests
import json
from datetime import datetime

def send_teams_alert(
    webhook_url: str,
    pipeline_name: str,
    status: str,         # "SUCCESS" or "FAILED"
    duration_minutes: float,
    record_count: int,
    error_message: str = None
):
    """Send notification to Microsoft Teams channel."""
    
    color = "00FF00" if status == "SUCCESS" else "FF0000"
    
    payload = {
        "@type": "MessageCard",
        "themeColor": color,
        "title": f"Pipeline {status}: {pipeline_name}",
        "sections": [{
            "facts": [
                {"name": "Status",       "value": status},
                {"name": "Duration",     "value": f"{duration_minutes:.1f} minutes"},
                {"name": "Records",      "value": f"{record_count:,}"},
                {"name": "Completed At", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")},
                {"name": "Error",        "value": error_message or "None"}
            ]
        }]
    }
    
    requests.post(webhook_url, json=payload)


# Track pipeline execution
pipeline_start = datetime.utcnow()

try:
    # Your pipeline logic
    record_count = run_etl_pipeline()
    
    duration = (datetime.utcnow() - pipeline_start).total_seconds() / 60
    send_teams_alert(
        webhook_url=dbutils.secrets.get("kv-scope", "teams-webhook"),
        pipeline_name="daily-transaction-pipeline",
        status="SUCCESS",
        duration_minutes=duration,
        record_count=record_count
    )
    
except Exception as e:
    duration = (datetime.utcnow() - pipeline_start).total_seconds() / 60
    send_teams_alert(
        webhook_url=dbutils.secrets.get("kv-scope", "teams-webhook"),
        pipeline_name="daily-transaction-pipeline",
        status="FAILED",
        duration_minutes=duration,
        record_count=0,
        error_message=str(e)
    )
    raise
```

---

## 8.9 Interview Questions — Jobs & Pipelines

**Q1: Why use job clusters instead of all-purpose clusters for scheduled jobs?**
> Job clusters are ephemeral — created fresh for each run and terminated immediately after. They're 60-80% cheaper than keeping an all-purpose cluster running. They also prevent "cluster drift" (accumulated libraries, config changes from other users) and ensure reproducible execution environments.

**Q2: How do you pass data between tasks in a multi-task workflow?**
> Using `dbutils.jobs.taskValues.set(key, value)` in the upstream task and `dbutils.jobs.taskValues.get(taskKey, key)` in downstream tasks. Useful for passing record counts, status messages, or derived dates between pipeline stages.

**Q3: How do you make a Databricks notebook idempotent?**
> Three approaches: (1) Checkpoint files — write a marker file on success and check for it before processing. (2) Delta MERGE instead of append — naturally idempotent because duplicate records are handled. (3) `replaceWhere` for partition overwrite — overwrites only the target partition, safe to re-run.

**Q4: How do you implement a Dev → Prod workflow in Databricks?**
> Use Databricks Repos (Git integration) — developers work in feature branches. CI/CD via GitHub Actions: PR to main triggers tests (pytest), merge to main deploys to TEST, manual approval deploys to PROD. Separate workspaces or Unity Catalog schemas isolate environments. Job parameters handle environment-specific config.

---

*Next: [Module 09 — Unity Catalog & Data Governance](../09-Unity-Catalog/01-catalog-schema-table.md)*
