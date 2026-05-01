# Module 05 — Azure Integration
## Part 2: ADF Advanced Patterns — Triggers, Dynamic Pipelines & Error Handling

---

## 5.1 ADF Trigger Types

```
TRIGGER TYPES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Schedule Trigger    → Run on cron schedule (daily, hourly, etc.)
Tumbling Window     → Process time windows without overlap (backfill-friendly)
Event Trigger       → Run when file arrives in ADLS or Blob Storage
Manual Trigger      → Run on-demand via UI, API, or ADF SDK
```

### Tumbling Window Trigger — The ETL Standard

```
Why Tumbling Window over Schedule:
  ✅ Each window instance is independent — parallelism possible
  ✅ Built-in retry per window (re-run just the failed window)
  ✅ Dependency between windows (window N+1 waits for N to succeed)
  ✅ Pipeline gets window start/end times automatically
  ✅ If ADF is down for 2 days, it automatically queues missed runs

ANATOMY OF A TUMBLING WINDOW:
  Frequency: 1 Day
  Start time: 2024-01-01 02:00 UTC
  
  Window 1: 2024-01-01 02:00 → 2024-01-02 02:00
  Window 2: 2024-01-02 02:00 → 2024-01-03 02:00
  Window 3: 2024-01-03 02:00 → 2024-01-04 02:00
  (Each window processes data FOR that day)
```

```json
// Tumbling Window Trigger definition (ARM template)
{
    "name": "twt_daily_etl",
    "type": "Microsoft.DataFactory/factories/triggers",
    "properties": {
        "type": "TumblingWindowTrigger",
        "typeProperties": {
            "frequency": "Hour",
            "interval": 24,
            "startTime": "2024-01-01T02:00:00Z",
            "delay": "00:10:00",    // Wait 10 min after window end
            "maxConcurrency": 1,   // Only 1 window at a time
            "retryPolicy": {
                "count": 3,
                "intervalInSeconds": 300
            },
            "dependsOn": []
        },
        "pipeline": {
            "pipelineReference": {"name": "daily_etl_pipeline"}
        },
        "parameters": {
            "batch_date": {
                "type": "Expression",
                "value": "@formatDateTime(trigger().outputs.windowStartTime, 'yyyy-MM-dd')"
            },
            "window_start": "@trigger().outputs.windowStartTime",
            "window_end":   "@trigger().outputs.windowEndTime"
        }
    }
}
```

### Event Trigger — File Arrival

```json
// Run pipeline when file arrives in ADLS
{
    "name": "evt_file_arrival",
    "type": "BlobEventsTrigger",
    "properties": {
        "typeProperties": {
            "blobPathBeginsWith": "/bronze/transactions/",
            "blobPathEndsWith":   ".parquet",
            "events": ["Microsoft.Storage.BlobCreated"],
            "scope": "/subscriptions/.../storageAccounts/prodlake"
        },
        "pipeline": {
            "pipelineReference": {"name": "process_arrived_file"}
        },
        "parameters": {
            "file_path": "@triggerBody().folderPath",
            "file_name": "@triggerBody().fileName"
        }
    }
}
```

---

## 5.2 Dynamic ADF Pipelines — ForEach, IfCondition, Switch

```json
// ForEach: Process multiple tables in parallel

// Pipeline parameter: tables = ["transactions", "customers", "products"]

// Activity 1: Lookup — read table list from config
{
    "name": "GetTableList",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT table_name, partition_col FROM etl_config.pipeline_tables WHERE is_active = 1"
        },
        "firstRowOnly": false   // Return ALL rows
    }
}

// Activity 2: ForEach — iterate over table list
{
    "name": "ProcessEachTable",
    "type": "ForEach",
    "typeProperties": {
        "isSequential": false,          // Run in parallel
        "batchCount": 3,                // Max 3 parallel
        "items": "@activity('GetTableList').output.value",
        "activities": [
            {
                "name": "RunDatabricksNotebook",
                "type": "DatabricksNotebook",
                "typeProperties": {
                    "notebookPath": "/Repos/.../generic_pipeline",
                    "baseParameters": {
                        "table_name": "@item().table_name",
                        "partition_col": "@item().partition_col",
                        "batch_date": "@pipeline().parameters.batch_date"
                    }
                }
            }
        ]
    }
}
```

### Error Handling and Branching

```json
// IfCondition Activity — branch based on validation result
{
    "name": "CheckDataQuality",
    "type": "IfCondition",
    "typeProperties": {
        "expression": {
            "type": "Expression",
            "value": "@equals(activity('ValidateData').output.runOutput, 'PASSED')"
        },
        "ifTrueActivities": [
            {
                "name": "ProceedToSilver",
                "type": "DatabricksNotebook",
                "typeProperties": {
                    "notebookPath": "/notebooks/transform_silver"
                }
            }
        ],
        "ifFalseActivities": [
            {
                "name": "SendDQFailureAlert",
                "type": "WebActivity",
                "typeProperties": {
                    "url": "@pipeline().parameters.teams_webhook",
                    "method": "POST",
                    "body": {
                        "text": "@concat('❌ DQ Failed for batch: ', pipeline().parameters.batch_date)"
                    }
                }
            },
            {
                "name": "FailPipeline",
                "type": "Fail",
                "typeProperties": {
                    "message": "Data quality validation failed",
                    "errorCode": "DQ_FAILURE"
                }
            }
        ]
    }
}
```

---

## 5.3 ADF Parameters and Dynamic Content

```
ADF EXPRESSION LANGUAGE REFERENCE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Pipeline context:
  @pipeline().runId                → unique run ID
  @pipeline().parameters.Name     → pipeline parameter value
  @pipeline().globalParameters.Env → global parameter

Activity output:
  @activity('task_name').output.runOutput  → notebook exit value
  @activity('Lookup1').output.value        → lookup result array
  @activity('Lookup1').output.firstRow     → first row of lookup
  @activity('ForEach1').output.errors      → errors in foreach

Trigger context:
  @trigger().scheduledTime                → scheduled run time
  @trigger().outputs.windowStartTime      → tumbling window start
  @trigger().outputs.windowEndTime        → tumbling window end

Functions:
  @utcNow()                              → current UTC datetime
  @utcNow('yyyy-MM-dd')                  → formatted date string
  @formatDateTime(value, 'yyyy-MM-dd')   → format a datetime
  @addDays(utcNow(), -1)                 → yesterday
  @addDays(utcNow(), -1, 'yyyy-MM-dd')   → yesterday formatted
  @concat('prefix_', variables('env'))   → concatenation
  @if(equals(a, b), 'true_val', 'false') → ternary
  @length(activity('Lookup').output.value)→ array length
  @empty(activity('Lookup').output.value) → check if empty
  @string(variables('count'))            → type conversion
  @int(pipeline().parameters.Days)       → to integer
  @json('{"key":"value"}')               → parse JSON string
  @split(pipeline().parameters.tables, ',') → split string to array
```

---

## 5.4 ADF + Azure Functions — Custom Logic

```python
# Azure Function triggered by ADF (for custom Python logic)
# Use when: complex API calls, custom notifications, DB operations ADF can't do

import azure.functions as func
import json
import requests
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function called by ADF Web Activity.
    Performs custom validation and returns result to ADF.
    """
    req_body  = req.get_json()
    batch_date= req_body.get("batch_date")
    pipeline  = req_body.get("pipeline_name")
    
    # Custom logic: check if source files arrived
    storage_url = f"https://prodlake.dfs.core.windows.net/landing/transactions/date={batch_date}/"
    
    headers = {"Authorization": f"Bearer {get_managed_identity_token()}"}
    response = requests.get(storage_url, headers=headers)
    
    if response.status_code == 200:
        files = response.json().get("paths", [])
        file_count = len(files)
        
        return func.HttpResponse(
            json.dumps({
                "status":     "READY",
                "file_count": file_count,
                "message":    f"Found {file_count} files for {batch_date}"
            }),
            mimetype="application/json",
            status_code=200
        )
    else:
        return func.HttpResponse(
            json.dumps({
                "status":  "NOT_READY",
                "message": f"No files found for {batch_date}"
            }),
            mimetype="application/json",
            status_code=200   # Return 200 even on "not ready" — ADF will check status field
        )
```

```json
// ADF Web Activity calling Azure Function
{
    "name": "CheckSourceFiles",
    "type": "WebActivity",
    "typeProperties": {
        "url": "https://func-etl-prod.azurewebsites.net/api/check_source_files",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json",
            "x-functions-key": {
                "type": "AzureKeyVaultSecret",
                "store": {"referenceName": "kv_prod"},
                "secretName": "azure-function-key"
            }
        },
        "body": {
            "batch_date":    "@pipeline().parameters.batch_date",
            "pipeline_name": "@pipeline().RunId"
        }
    }
}

// Then check the result in IfCondition:
// @equals(activity('CheckSourceFiles').output.status, 'READY')
```

---

## 5.5 ADF Monitoring and Alerting

```python
# Azure Monitor integration for ADF pipeline alerts

# In Azure Portal:
# ADF → Monitor → Alerts → New Alert Rule
#
# Condition: "Pipeline runs Failed" > 0
# Severity:  Critical
# Action:    Email + Teams webhook
#
# Or via Python SDK:

from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.monitor.models import *

# Create alert rule for pipeline failures
alert_rule = MetricAlertResource(
    location="global",
    severity=0,  # Critical
    enabled=True,
    scopes=[f"/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{adf_name}"],
    evaluation_frequency="PT1M",  # Check every 1 minute
    window_size="PT5M",
    criteria=MetricAlertSingleResourceMultipleMetricCriteria(
        all_of=[
            MetricCriteria(
                name="PipelineFailedRuns",
                metric_name="PipelineFailedRuns",
                operator="GreaterThan",
                threshold=0,
                time_aggregation="Total"
            )
        ]
    ),
    actions=[
        MetricAlertAction(
            action_group_id=f"/subscriptions/{sub_id}/resourceGroups/{rg}/providers/microsoft.insights/actionGroups/de-team-alerts"
        )
    ]
)
```

---

## 5.6 ADF vs Databricks Workflows — When to Use Which

```
CHOOSE ADF ORCHESTRATION WHEN:
  ✅ Heterogeneous pipelines (Copy SQL→ADLS, run Python, call API, SFTP)
  ✅ Non-Databricks activities needed (Azure Functions, Logic Apps, etc.)
  ✅ Business users need to monitor pipelines (ADF UI is approachable)
  ✅ Tumbling window triggers for time-window processing
  ✅ Event-based triggers (file arrival in ADLS)
  ✅ Cross-service orchestration (ADF + Synapse + Databricks + SQL DB)

CHOOSE DATABRICKS WORKFLOWS WHEN:
  ✅ Pure Databricks workloads (all notebooks/jobs in Databricks)
  ✅ Need task-level parameters (taskValues between tasks)
  ✅ Want notebook-first development (run and schedule from same UI)
  ✅ Multi-task parallel execution with complex dependencies
  ✅ DLT pipeline management
  ✅ Simpler setup (no separate ADF service)

HYBRID PATTERN (most common in enterprise):
  ADF → orchestrates high-level pipeline flow
       → calls Databricks jobs for heavy compute
       → calls Azure Functions for custom logic
       → handles file movement, notifications
  Databricks → runs all ETL computation
             → manages its own task dependencies
             → returns status to ADF
```

---

## 5.7 Interview Questions — ADF Advanced

**Q: What is the difference between Schedule Trigger and Tumbling Window Trigger?**
> Schedule Trigger fires at a fixed time regardless of previous runs — if the pipeline was down for 2 days, it won't catch up. Tumbling Window Trigger creates one pipeline run per non-overlapping time window — if ADF was down for 2 days, it automatically queues all missed windows and processes them. Each window gets its start/end time as parameters, making it ideal for date-partitioned ETL. Tumbling windows also support run dependencies (window N+1 only starts when N succeeds) and per-window retries.

**Q: How does ADF pass a file path to a Databricks notebook when using an Event Trigger?**
> The Event Trigger (BlobEventsTrigger) captures the blob event metadata including `folderPath` and `fileName` in `@triggerBody()`. The pipeline defines parameters for these: `file_path: "@triggerBody().folderPath"` and `file_name: "@triggerBody().fileName"`. The Databricks Notebook Activity's Base Parameters then pass these to the notebook: `{"source_file": "@concat(pipeline().parameters.file_path, '/', pipeline().parameters.file_name)"}`. The notebook reads the parameter via `dbutils.widgets.get("source_file")`.

**Q: How do you handle a pipeline that processes 50 different tables and you want them to run in parallel but cap at 10 concurrent?**
> Use a Lookup activity to read the table list from a config table (SQL or metadata Delta table), pass the array to a ForEach activity with `isSequential=false` and `batchCount=10`. Each ForEach iteration calls a generic Databricks notebook with the table name as a parameter. The notebook uses a config-driven pattern to apply the right transformations per table. This way adding a new table only requires inserting a row into the config table — no pipeline code change.

---
*Next: [Module 13 — Real-World Patterns: Error Handling Cookbook](../13-Real-World-Patterns/02-error-handling-cookbook.md)*
