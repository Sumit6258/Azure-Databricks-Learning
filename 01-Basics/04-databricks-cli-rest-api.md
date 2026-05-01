# Module 01 — Basics
## Part 4: Databricks CLI & REST API — Complete Reference

> Every senior DE must know the CLI and API. ADF doesn't cover everything — sometimes you need to script cluster management, automate job creation, or debug via API.

---

## 4.1 Databricks CLI — Installation & Setup

```bash
# Install
pip install databricks-cli

# Configure (interactive)
databricks configure --token
# Enter: Host (https://adb-xxxxx.azuredatabricks.net)
# Enter: Token (dapi...)

# Configure multiple workspaces (profiles)
databricks configure --token --profile dev
databricks configure --token --profile prod

# Use a specific profile
databricks --profile prod jobs list

# Verify setup
databricks workspace ls /
```

---

## 4.2 CLI — Cluster Management

```bash
# List all clusters
databricks clusters list
databricks clusters list --output JSON | jq '.[] | {id:.cluster_id, name:.cluster_name, state:.state}'

# Create a cluster from JSON
databricks clusters create --json '{
  "cluster_name": "dev-cluster-cli",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autoscale": {"min_workers": 1, "max_workers": 4},
  "autotermination_minutes": 60,
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true"
  }
}'

# Start / stop / delete
databricks clusters start  --cluster-id 1234-567890-abc123
databricks clusters delete --cluster-id 1234-567890-abc123

# Get cluster details
databricks clusters get --cluster-id 1234-567890-abc123

# List cluster events (for debugging)
databricks clusters events --cluster-id 1234-567890-abc123

# List available Spark versions
databricks clusters spark-versions

# List available node types
databricks clusters node-types
```

---

## 4.3 CLI — Jobs Management

```bash
# List all jobs
databricks jobs list
databricks jobs list --output JSON

# Create a job from a JSON file
databricks jobs create --json @jobs/daily_etl_job.json

# Update an existing job (full reset)
databricks jobs reset --job-id 123 --json @jobs/daily_etl_job.json

# Run a job now (with parameters)
databricks jobs run-now \
  --job-id 123 \
  --notebook-params '{"batch_date":"2024-01-15","environment":"prod"}'

# List all runs for a job
databricks runs list --job-id 123 --limit 10

# Get run output
databricks runs get-output --run-id 456789

# Cancel a running job
databricks runs cancel --run-id 456789

# Delete a job
databricks jobs delete --job-id 123
```

---

## 4.4 CLI — Workspace & DBFS Operations

```bash
# List workspace contents
databricks workspace ls /Repos/
databricks workspace ls /Repos/Sumit6258/

# Export a notebook
databricks workspace export \
  /Repos/Sumit6258/Azure-Databricks-Learning/notebooks/ingestion/01_ingest_bronze \
  --format SOURCE \
  --output ./local_backup/01_ingest_bronze.py

# Import a notebook
databricks workspace import \
  ./local_backup/01_ingest_bronze.py \
  /Shared/ETL/01_ingest_bronze \
  --language PYTHON \
  --format SOURCE \
  --overwrite

# DBFS operations
databricks fs ls dbfs:/mnt/bronze/
databricks fs cp local_file.csv dbfs:/mnt/bronze/uploads/
databricks fs mv dbfs:/mnt/bronze/file.csv dbfs:/mnt/archive/file.csv
databricks fs rm dbfs:/mnt/temp/ --recursive

# Secrets management
databricks secrets list-scopes
databricks secrets list --scope kv-prod
databricks secrets put --scope kv-prod --key my-secret
databricks secrets delete --scope kv-prod --key my-secret
```

---

## 4.5 Databricks REST API — Python Client

```python
import requests
import json
import time
from typing import Dict, Any, Optional

class DatabricksAPI:
    """
    Production-grade Databricks REST API client.
    Wraps common operations used in automation scripts.
    """
    
    def __init__(self, host: str, token: str):
        self.host    = host.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json"
        }
    
    def _request(self, method: str, endpoint: str, payload: Dict = None) -> Dict:
        url = f"{self.host}/api/2.0/{endpoint}"
        response = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    # ── JOBS ──────────────────────────────────────────────
    
    def run_job(self, job_id: int, params: Dict = None) -> int:
        """Trigger a job run and return the run_id."""
        payload = {"job_id": job_id}
        if params:
            payload["notebook_params"] = params
        result = self._request("POST", "jobs/run-now", payload)
        return result["run_id"]
    
    def wait_for_run(self, run_id: int, poll_seconds: int = 30, timeout_minutes: int = 120) -> str:
        """Poll a run until completion. Returns result state."""
        deadline = time.time() + timeout_minutes * 60
        
        while time.time() < deadline:
            result   = self._request("GET", f"jobs/runs/get?run_id={run_id}")
            state    = result["state"]["life_cycle_state"]
            
            print(f"  Run {run_id}: {state}")
            
            if state == "TERMINATED":
                return result["state"]["result_state"]
            elif state in ("SKIPPED", "INTERNAL_ERROR"):
                raise RuntimeError(f"Run failed with state: {state}")
            
            time.sleep(poll_seconds)
        
        raise TimeoutError(f"Run {run_id} did not complete within {timeout_minutes} minutes")
    
    def run_job_and_wait(self, job_id: int, params: Dict = None) -> bool:
        """Run a job and wait for completion. Returns True on success."""
        run_id = self.run_job(job_id, params)
        print(f"  Started run: {run_id}")
        result = self.wait_for_run(run_id)
        print(f"  Run {run_id} completed: {result}")
        return result == "SUCCESS"
    
    def get_run_output(self, run_id: int) -> str:
        """Get notebook output from a run."""
        result = self._request("GET", f"jobs/runs/get-output?run_id={run_id}")
        return result.get("notebook_output", {}).get("result", "")
    
    # ── CLUSTERS ──────────────────────────────────────────
    
    def get_cluster(self, cluster_id: str) -> Dict:
        return self._request("GET", f"clusters/get?cluster_id={cluster_id}")
    
    def list_clusters(self) -> list:
        return self._request("GET", "clusters/list").get("clusters", [])
    
    def start_cluster(self, cluster_id: str):
        self._request("POST", "clusters/start", {"cluster_id": cluster_id})
        print(f"  Starting cluster {cluster_id}...")
        
        # Wait for running state
        while True:
            cluster = self.get_cluster(cluster_id)
            state   = cluster["state"]
            if state == "RUNNING":
                print(f"  Cluster {cluster_id} is RUNNING")
                return
            elif state in ("ERROR", "TERMINATED"):
                raise RuntimeError(f"Cluster failed to start: {state}")
            time.sleep(15)
    
    # ── RUNS API (for ad-hoc notebook runs) ───────────────
    
    def run_notebook(
        self,
        notebook_path: str,
        cluster_id: str,
        params: Dict = None,
        timeout_seconds: int = 3600
    ) -> str:
        """Run a notebook ad-hoc and return its output."""
        
        payload = {
            "run_name": f"adhoc_{notebook_path.split('/')[-1]}",
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": params or {}
            },
            "timeout_seconds": timeout_seconds
        }
        
        result = self._request("POST", "jobs/runs/submit", payload)
        run_id = result["run_id"]
        
        print(f"  Submitted notebook run: {run_id}")
        result_state = self.wait_for_run(run_id)
        
        if result_state != "SUCCESS":
            raise RuntimeError(f"Notebook run failed: {result_state}")
        
        return self.get_run_output(run_id)


# ── USAGE EXAMPLES ────────────────────────────────────────────────

host  = "https://adb-workspace.azuredatabricks.net"
token = dbutils.secrets.get("kv-scope", "databricks-pat")
api   = DatabricksAPI(host, token)

# Run a production job
success = api.run_job_and_wait(
    job_id=123,
    params={"batch_date": "2024-01-15", "environment": "prod"}
)

if success:
    print("✅ Pipeline completed successfully")
else:
    print("❌ Pipeline failed — check job logs")

# Run pipeline for last 7 days (backfill)
from datetime import datetime, timedelta

for i in range(7, 0, -1):
    date = (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d")
    print(f"\n📅 Processing {date}...")
    success = api.run_job_and_wait(
        job_id=123,
        params={"batch_date": date, "environment": "prod"}
    )
    if not success:
        print(f"❌ Failed for {date} — stopping backfill")
        break
```

---

## 4.6 REST API — Clusters & Permissions (2.1 API)

```python
# Unity Catalog Permissions API (newer 2.1 endpoints)

def grant_cluster_permission(api: DatabricksAPI, cluster_id: str, user: str, permission: str):
    """Grant a user permission on a cluster."""
    payload = {
        "access_control_list": [
            {
                "user_name": user,
                "permission_level": permission  # CAN_ATTACH_TO, CAN_RESTART, CAN_MANAGE
            }
        ]
    }
    return requests.patch(
        f"{api.host}/api/2.0/permissions/clusters/{cluster_id}",
        headers=api.headers,
        json=payload
    ).json()

# List job permissions
def get_job_permissions(api: DatabricksAPI, job_id: int):
    return requests.get(
        f"{api.host}/api/2.0/permissions/jobs/{job_id}",
        headers=api.headers
    ).json()
```

---

## 4.7 Automate Databricks Setup with Python Script

```python
#!/usr/bin/env python3
"""
setup_databricks.py
Complete workspace setup script — run once per environment.
"""

import json, sys
from databricks_api_client import DatabricksAPI  # our class above

def setup_workspace(host: str, token: str, env: str):
    api = DatabricksAPI(host, token)
    
    print(f"\n🔧 Setting up Databricks workspace: {env}")
    print("=" * 50)
    
    # 1. Create cluster policies (requires admin)
    cluster_policy = {
        "name": f"{env}-etl-policy",
        "definition": json.dumps({
            "node_type_id": {
                "type": "allowlist",
                "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
            },
            "autotermination_minutes": {
                "type": "range", "minValue": 30, "maxValue": 120,
                "defaultValue": 60
            },
            "spark_version": {
                "type": "regex",
                "pattern": "^13\\..*lts.*"
            }
        })
    }
    
    # 2. Create production ETL job
    etl_job = {
        "name": f"daily-transaction-pipeline-{env}",
        "tasks": [
            {
                "task_key": "ingest_bronze",
                "notebook_task": {
                    "notebook_path": "/Repos/ci-cd/Azure-Databricks-Learning/notebooks/ingestion/01_ingest_bronze",
                    "base_parameters": {"environment": env, "batch_date": ""}
                },
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "Standard_DS4_v2",
                    "autoscale": {"min_workers": 2, "max_workers": 8}
                },
                "max_retries": 2,
                "min_retry_interval_millis": 300000
            }
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED" if env != "prod" else "UNPAUSED"
        },
        "email_notifications": {
            "on_failure": [f"data-eng@company.com"]
        }
    }
    
    result = api._request("POST", "jobs/create", etl_job)
    print(f"✅ Job created: ID={result['job_id']}")
    
    print(f"\n✅ Workspace setup complete for {env}")

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    setup_workspace(
        host=f"https://adb-{env}.azuredatabricks.net",
        token=os.environ[f"DATABRICKS_{env.upper()}_TOKEN"],
        env=env
    )
```

---

## 4.8 Interview Questions — CLI & API

**Q: When would you use the Databricks REST API over the UI?**
> (1) CI/CD automation — GitHub Actions calls the API to deploy notebooks, trigger job runs, and check results. (2) Bulk operations — reset 50 jobs programmatically instead of clicking through UI. (3) Monitoring scripts — poll job statuses across environments and send alerts. (4) Infrastructure as Code — Terraform uses the API to create clusters, jobs, and permissions. (5) Cross-workspace orchestration — one workspace triggering jobs in another.

**Q: How do you securely store Databricks tokens for API access in GitHub Actions?**
> Store them as GitHub Encrypted Secrets (repo Settings → Secrets). In the workflow YAML, reference as `${{ secrets.DATABRICKS_PROD_TOKEN }}`. The token is injected as an environment variable during the run and is never written to logs (GitHub masks it). For enterprise setups: use Azure Key Vault as the single source of truth and have the GitHub Action fetch the token at runtime using OIDC authentication.

---
*Next: [Module 02 — PySpark: Advanced Data Ingestion Patterns](../02-PySpark/02-data-ingestion-patterns.md)*
