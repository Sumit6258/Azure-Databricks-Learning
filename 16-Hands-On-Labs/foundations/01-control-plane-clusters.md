# Topic 1 & 2 — Control Plane, Data Plane & Cluster Types
## Complete Guide: From Zero to Production Understanding

---

## PART 1: CONTROL PLANE & DATA PLANE

---

### 1.1 Why Does This Architecture Exist?

Imagine you rent a car from a company. The company manages the booking system, GPS tracking, and billing (that's their side). But you drive the car on your own roads with your own fuel (that's your side). The car company never touches your fuel or goes on your roads.

Databricks works exactly like this.

```
┌─────────────────────────────────────────────────────────────────┐
│                    THE BIG PICTURE                              │
│                                                                 │
│   DATABRICKS SIDE              YOUR AZURE SIDE                 │
│   (Control Plane)              (Data Plane)                    │
│                                                                 │
│   "We manage the brain"        "Your data stays with you"      │
│   "You never touch infra"      "We just run compute here"      │
│                                                                 │
│   ┌──────────────────┐         ┌──────────────────────────┐   │
│   │  Workspace UI    │         │  Your Azure Subscription │   │
│   │  REST API        │◄───────►│  Virtual Machines        │   │
│   │  Job Scheduler   │         │  ADLS Gen2 Storage       │   │
│   │  Cluster Manager │         │  Virtual Network         │   │
│   │  Notebook Server │         │  Network Security Groups │   │
│   │  Unity Catalog   │         │  Key Vault               │   │
│   └──────────────────┘         └──────────────────────────┘   │
│                                                                 │
│   Managed by: Databricks        Managed by: YOU (Microsoft)    │
│   Billed by:  Databricks        Billed by: Microsoft Azure     │
└─────────────────────────────────────────────────────────────────┘
```

---

### 1.2 CONTROL PLANE — The Brain of Databricks

**Definition:** The Control Plane is the set of services managed and hosted by Databricks. It is the "command center" that coordinates everything — but it never stores or processes your actual data.

**What lives in the Control Plane:**

```
CONTROL PLANE SERVICES (Hosted by Databricks, NOT in your Azure subscription)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. WORKSPACE UI
   The web interface you open in your browser
   URL: https://adb-xxxxx.azuredatabricks.net
   What it does: Shows you notebooks, clusters, jobs, files

2. REST API SERVER
   Accepts API calls to create clusters, run jobs, manage repos
   Used by: ADF, GitHub Actions, Terraform, Python scripts

3. CLUSTER MANAGER
   Decides when to start/stop/scale Virtual Machines in YOUR Azure
   It sends instructions to Azure to provision VMs — it doesn't run the VMs itself

4. JOB SCHEDULER
   Runs your scheduled jobs (daily 2AM pipelines etc.)
   Sends "start this notebook" instructions to your cluster VMs

5. NOTEBOOK SERVER
   Stores your notebook code (the text/code cells)
   NOTE: Notebook OUTPUT is temporary — code is saved, data is not

6. UNITY CATALOG METASTORE
   Stores metadata: table names, schemas, column definitions
   Does NOT store actual table data — data lives in your ADLS

7. SECRETS SERVICE (Databricks-managed scopes)
   Encrypted secret store
   For Key Vault-backed scopes: just a proxy — real secrets stay in your KV

8. BILLING & MONITORING
   Tracks DBU (Databricks Unit) consumption
   Sends usage to your Azure invoice
```

**Key Point about Control Plane:**
```
✅ Control Plane DOES:
   - Orchestrate (tell VMs what to do)
   - Store metadata (table definitions, notebook code)
   - Handle authentication (Azure AD integration)
   - Manage cluster lifecycle (start/stop/scale VMs)

❌ Control Plane NEVER:
   - Stores YOUR actual data (transactions, customers, etc.)
   - Runs your Spark jobs (that happens on YOUR VMs)
   - Accesses your ADLS files directly
   - Sees the content of your ADLS data
```

---

### 1.3 DATA PLANE — Where Your Data Actually Lives

**Definition:** The Data Plane is everything that runs inside YOUR Azure subscription. Your data never leaves your subscription. Databricks only orchestrates it — it never touches it.

**What lives in the Data Plane:**

```
DATA PLANE COMPONENTS (All inside YOUR Azure subscription)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. CLUSTER VIRTUAL MACHINES (Azure VMs)
   The actual computers running your Spark code
   Examples: Standard_DS4_v2, Standard_E8s_v3
   Provisioned by: Azure (based on Databricks instructions)
   Billed by: Microsoft Azure (VM cost on your Azure bill)

2. ADLS GEN2 STORAGE
   Where your actual data files live
   Bronze, Silver, Gold Delta tables
   Parquet, CSV, JSON files
   Never seen by Databricks — accessed directly by your VMs

3. VIRTUAL NETWORK (VNet)
   Network isolation for your cluster VMs
   Public subnet (for driver) + Private subnet (for workers)
   You control firewall rules (NSG)

4. AZURE KEY VAULT
   Your credentials, passwords, API keys
   Databricks reads from here — but your KV stays in your subscription

5. DATABRICKS FILE SYSTEM (DBFS)
   A virtualized path (/mnt/, /dbfs/) that maps to your ADLS
   The actual files are in your ADLS — DBFS is just an address system

6. CLUSTER LOGS & SPARK UI
   Spark execution logs stored temporarily on cluster VMs
   Long-term storage: can be configured to go to your ADLS
```

---

### 1.4 The Interaction — How They Work Together

Let's trace what happens when you click "Run" on a notebook cell:

```
STEP 1: You click Run in the browser
         ↓
         Your browser → Control Plane (Notebook Server)
         "Execute this code on cluster X"

STEP 2: Control Plane checks cluster status
         ↓
         Control Plane → Azure (in your subscription)
         "Is cluster X running?" → Yes → Continue
         "Is cluster X running?" → No  → Start it (provision VMs)

STEP 3: Code is sent to the cluster
         ↓
         Control Plane → Driver VM (in YOUR Azure subscription)
         "Execute: df = spark.read.parquet('/mnt/bronze/...')"

STEP 4: Cluster executes the code
         ↓
         Driver VM → Worker VMs (all in YOUR Azure subscription)
         Reads data directly from YOUR ADLS Gen2
         Processes in RAM of YOUR VMs
         YOUR data never goes to Databricks servers

STEP 5: Result is returned
         ↓
         Worker VMs → Driver VM → Control Plane → Your Browser
         Only the OUTPUT (summary, displayed rows) goes to Control Plane
         The actual 50GB DataFrame stays in YOUR VMs' memory
```

---

### 1.5 Security Implication — Why This Matters

```
ENTERPRISE SECURITY QUESTION:
"If we use Databricks, can Databricks employees see our transaction data?"

ANSWER: NO.

Here's why:
  1. Your data lives in YOUR ADLS Gen2 (in your Azure subscription)
  2. Your VMs run in YOUR Virtual Network (you control the firewall)
  3. Databricks Control Plane only sends CODE instructions — not data
  4. When your VM reads from ADLS, data travels:
     ADLS → Your VM's RAM → (processed) → back to ADLS
     It NEVER passes through Databricks servers

This is why large banks, hospitals, and government agencies use Databricks.
Data sovereignty is maintained — your data stays in your country/subscription.
```

---

## PART 2: TYPES OF CLUSTERS

---

### 2.1 What Is a Cluster?

A cluster is a **group of virtual machines** that work together to run your Spark code in parallel.

```
SINGLE MACHINE:
  Your laptop: 8 cores, 16GB RAM
  Can process: ~1GB of data comfortably

CLUSTER (10 machines):
  10 × 8 cores = 80 cores
  10 × 16GB = 160GB RAM
  Can process: 100GB+ of data by splitting work across all machines
```

**Anatomy of a Cluster:**

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLUSTER                                  │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                    DRIVER NODE                          │  │
│   │   The "manager" — runs YOUR Python/Scala code          │  │
│   │   Coordinates all worker nodes                         │  │
│   │   Collects results (show(), collect())                 │  │
│   │   Hosts the Spark UI (port 4040)                      │  │
│   │   RAM: usually larger than workers                    │  │
│   └─────────────────────────────────────────────────────────┘  │
│                              │                                  │
│              ┌───────────────┼───────────────┐                 │
│              ▼               ▼               ▼                 │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│   │  WORKER 1    │  │  WORKER 2    │  │  WORKER 3    │       │
│   │  Executes    │  │  Executes    │  │  Executes    │       │
│   │  tasks on    │  │  tasks on    │  │  tasks on    │       │
│   │  partitions  │  │  partitions  │  │  partitions  │       │
│   │  4 cores     │  │  4 cores     │  │  4 cores     │       │
│   │  16GB RAM    │  │  16GB RAM    │  │  16GB RAM    │       │
│   └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

---

### 2.2 TYPE 1: All-Purpose Clusters (Interactive)

**What it is:** A persistent cluster that stays running so you can run notebooks interactively.

**Think of it as:** Your personal workstation — always on, always ready.

```
CHARACTERISTICS:
  ✅ Persistent: stays running until you stop it (or auto-termination kicks in)
  ✅ Multi-user: multiple people can attach notebooks to same cluster
  ✅ Interactive: run code cells one at a time, see immediate results
  ✅ Flexible: can switch between Python, SQL, Scala mid-session
  ❌ Expensive: billed every hour it runs — even when nobody is using it
  ❌ Shared state: if one user crashes the cluster, everyone loses their session

WHEN TO USE:
  ✅ Development and exploration (90% of your time as a learner/developer)
  ✅ Ad-hoc analysis
  ✅ Debugging and testing
  ✅ Running notebooks interactively

WHEN NOT TO USE:
  ❌ Scheduled production pipelines (use Job Cluster instead)
  ❌ Running in CI/CD automation

COST EXAMPLE:
  Standard_DS4_v2 (28GB RAM, 8 cores) × 2 workers
  Running 24/7 for a month:
  VM cost:  $0.60/hr × 2 = $1.20/hr × 24 × 30 = $864/month
  Plus DBU: ~$288/month
  Total: ~$1,150/month for ONE always-on cluster!

  → That's why teams use Job Clusters for production
```

**Creating an All-Purpose Cluster (Step by Step):**

```
1. Left sidebar → Compute → Create Compute

2. Fill in:
   Cluster name:   my-dev-cluster
   Policy:         (leave as Personal Compute or Unrestricted for learning)
   Single node:    ✅ Check this for learning! (saves cost, 1 machine)
                   OR
   Multi node:     Min workers 1, Max workers 4 (for real data)

   Databricks Runtime: 14.3 LTS (Spark 3.5.0, Scala 2.12)
                        ← Always pick LTS for stability

   Node type:      Standard_DS3_v2 (14GB, 4 cores) ← Good for learning
   
   Auto termination: 30 minutes ← CRITICAL — stops billing when idle!

3. Advanced Options → Spark Config:
   spark.sql.adaptive.enabled true
   spark.sql.adaptive.coalescePartitions.enabled true

4. Click "Create Compute"
5. Wait for green circle (2-5 minutes)
```

---

### 2.3 TYPE 2: Job Clusters (Automated)

**What it is:** A cluster created fresh for each job run, terminated immediately after.

**Think of it as:** A taxi — you call it, it takes you where you need to go, you pay only for the ride, it disappears.

```
CHARACTERISTICS:
  ✅ Ephemeral: created at job start, destroyed at job end
  ✅ Clean: fresh environment every run (no state from previous runs)
  ✅ Cheap: billed ONLY for the duration of the job (not 24/7)
  ✅ Isolated: your job doesn't share resources with other users
  ❌ Startup time: 3-5 minutes to provision VMs before job begins
  ❌ Non-interactive: you can't run cells — only the notebook/script runs

WHEN TO USE:
  ✅ ALL scheduled production pipelines
  ✅ Daily/hourly ETL jobs
  ✅ CI/CD automated testing
  ✅ Any automated workload

COST COMPARISON:
  All-Purpose (24/7): $1,150/month (from above example)
  Job Cluster (2hr/day job): $1.20/hr × 2hr × 30 = $72/month

  SAVINGS: 94% cost reduction!
  This is the #1 cost optimization in enterprise Databricks.
```

**Job Cluster Config in a Job Definition:**

```json
{
  "new_cluster": {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS4_v2",
    "autoscale": {
      "min_workers": 2,
      "max_workers": 8
    },
    "spark_conf": {
      "spark.sql.adaptive.enabled": "true"
    },
    "azure_attributes": {
      "availability": "SPOT_WITH_FALLBACK_AZURE"
    }
  }
}
```

---

### 2.4 TYPE 3: Single Node Cluster

**What it is:** A cluster with ONLY a driver node — no workers.

**Think of it as:** A solo developer working alone.

```
CHARACTERISTICS:
  ✅ Cheapest: only 1 VM running
  ✅ Fast startup: 1 VM to provision
  ✅ Good for: small data, learning, ML model training
  ✅ Pandas-friendly: local pandas operations work normally
  ❌ No parallelism: can only use 1 machine's worth of resources
  ❌ Not for production ETL: can't process large distributed data

WHEN TO USE:
  ✅ Learning and tutorials (like right now!)
  ✅ Data exploration with small datasets
  ✅ ML model training (single node ML)
  ✅ Running notebooks with < 1GB of data
  ✅ Cost-sensitive development

CREATE IN UI:
  Compute → Create Compute
  → Check "Single node" radio button
  → Select Standard_DS3_v2 (14GB, 4 cores)
  → Auto termination: 30 min
  → Create
```

---

### 2.5 TYPE 4: SQL Warehouses (for SQL only)

**What it is:** Specialized compute designed specifically for SQL queries and BI tool connections.

```
CHARACTERISTICS:
  ✅ Optimized for SQL: ANSI SQL, BI queries, reporting
  ✅ ODBC/JDBC: Power BI, Tableau, Excel can connect directly
  ✅ Auto-start: starts when a query arrives
  ✅ Auto-stop: stops when idle (save cost)
  ✅ Serverless option: zero startup time
  ❌ SQL ONLY: cannot run PySpark, Scala, or Python notebooks
  ❌ No RDD: no low-level Spark API access

WHEN TO USE:
  ✅ Power BI / Tableau dashboards
  ✅ Analyst SQL queries
  ✅ dbt models
  ✅ Business reporting

SIZES (T-shirt sizing):
  2X-Small: 2 clusters (for light usage)
  X-Small:  4 clusters (good for BI dashboards)
  Small:    8 clusters
  Medium:   16 clusters  (for heavy concurrent analytics)
  Large:    32 clusters
  X-Large:  64 clusters

CREATE IN UI:
  Left sidebar → SQL Warehouses → Create SQL Warehouse
  Name: bi-reporting-warehouse
  Cluster Size: X-Small
  Auto Stop: 10 minutes
  ✅ Enable Serverless (if available)
  → Create
```

---

### 2.6 TYPE 5: Instance Pools (Accelerator for Clusters)

**What it is:** Not a cluster type itself — a pool of pre-warmed VMs that clusters can use to start faster.

```
WITHOUT INSTANCE POOL:
  Job starts → Azure provisions VMs from scratch → 3-5 minutes wait

WITH INSTANCE POOL:
  Job starts → Uses pre-warmed VMs from pool → 30 seconds wait

SETUP:
  Compute → Pools → Create Pool
  Name: etl-warm-pool
  Node Type: Standard_DS4_v2
  Min Idle: 4  ← Keep 4 VMs always warm
  Max Capacity: 20
  Idle Termination: 60 minutes

THEN: When creating a cluster, select "Instance Pool" and pick your pool
```

---

### 2.7 Cluster Comparison Table

```
┌─────────────────┬───────────────┬───────────────┬───────────────┬──────────────┐
│  Feature        │ All-Purpose   │ Job Cluster   │ Single Node   │ SQL Warehouse│
├─────────────────┼───────────────┼───────────────┼───────────────┼──────────────┤
│ Interactive     │     ✅        │     ❌        │     ✅        │     ✅ (SQL) │
│ Scheduled jobs  │     ✅        │     ✅        │     ✅        │     ❌       │
│ Multi-user      │     ✅        │     ❌        │     ✅        │     ✅       │
│ PySpark         │     ✅        │     ✅        │     ✅        │     ❌       │
│ SQL             │     ✅        │     ✅        │     ✅        │     ✅       │
│ BI Tools        │     ❌        │     ❌        │     ❌        │     ✅       │
│ Cost            │  💰💰💰💰    │    💰         │    💰         │    💰💰      │
│ Startup time    │  Already up   │  3-5 minutes  │  2-3 minutes  │  5-30 sec   │
│ Best for        │  Development  │  Production   │  Learning     │  Analytics  │
└─────────────────┴───────────────┴───────────────┴───────────────┴──────────────┘
```

---

### 2.8 Cluster Runtime Versions — What to Know

```
DATABRICKS RUNTIME NAMING:
  14.3 LTS (Spark 3.5.0, Scala 2.12)
   │    │
   │    └── LTS = Long Term Support (2+ years of patches)
   └─────── Version number

ALWAYS USE LTS IN PRODUCTION:
  LTS runtimes get security patches for 2+ years
  Non-LTS runtimes go end-of-life quickly
  Breaking changes happen in non-LTS between minor versions

RUNTIME FLAVORS:
  Standard:  14.3 LTS              ← ETL, data engineering
  ML:        14.3 LTS ML           ← Includes PyTorch, TensorFlow, scikit-learn
  GPU:       14.3 LTS GPU          ← GPU-accelerated ML
  Genomics:  14.3 LTS Genomics     ← Bioinformatics

FOR LEARNING: Always pick the latest Standard LTS runtime
```

---

### 2.9 Auto-Scaling Explained

```python
# When you set autoscale min=2, max=8:

# Spark monitors: how many tasks are PENDING (waiting for resources)?
# If many tasks pending → MORE workers needed → Azure adds VMs
# If few tasks pending → FEWER workers needed → Azure removes VMs

# SCALE UP trigger:
# Pending tasks exist for > 1 minute → add worker nodes (up to max)

# SCALE DOWN trigger:
# Worker idle for > 60 seconds → remove worker (down to min)

# PRACTICAL IMPACT:
# Your daily ETL job:
#   - Starts at 2AM with 2 workers (minimum)
#   - Data arrives → scales up to 8 workers (processes faster)
#   - Job completes → scales back to 2 (or terminates if job cluster)

# ENHANCED AUTOSCALING (Premium feature):
# Scales down more aggressively — better cost efficiency
# Enable in cluster config:
# "autoscale": {"min_workers": 2, "max_workers": 8, "mode": "ENHANCED"}
```

---

### 2.10 Interview Questions — Control Plane & Clusters

**Q: If Databricks is down, can I still access my data?**
> The Control Plane (Databricks) going down means you can't access the Workspace UI or create new clusters. However, your DATA is unaffected — it lives in your Azure ADLS Gen2 which is completely independent of Databricks. If you have other tools (Azure Synapse, Azure Data Factory), you can still access your data. Running jobs may fail because the Job Scheduler is in the Control Plane, but any data already written to Delta Lake is safe.

**Q: Why use a Job Cluster instead of always-on All-Purpose Cluster for production?**
> Cost: a 2-hour daily job costs $72/month on a Job Cluster vs $1,150/month if you keep the cluster running 24/7. Reliability: Job Clusters start fresh with a clean state every run — no accumulated garbage, leaked connections, or configuration drift. Isolation: if one job crashes the driver, it only affects that job run — not other notebooks running on a shared all-purpose cluster.

**Q: What is the difference between a SQL Warehouse and a Spark Cluster?**
> SQL Warehouse: SQL-only, optimized for concurrent BI queries, supports ODBC/JDBC for Power BI/Tableau, can be serverless, auto-start/stop. Spark Cluster: full Spark engine, supports PySpark, Scala, SQL, notebook execution, ETL workloads, streaming. They serve different audiences — analysts use SQL Warehouses, data engineers use Spark Clusters.
