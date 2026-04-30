# Module 01 — Azure Databricks Fundamentals
## Part 2: DBFS, Storage Mounts & ADLS Gen2

---

## 2.1 DBFS — Databricks File System

DBFS (Databricks File System) is an **abstraction layer** over cloud object storage (Azure Blob / ADLS Gen2). It makes storage look like a Unix-style file system.

```
DBFS paths use:     /dbfs/...     or     dbfs:/...
Equivalent to:      Azure Blob / ADLS storage under the hood
```

### What lives in DBFS?

```
dbfs:/
├── FileStore/          ← Files uploaded via UI (small files, CSV imports)
├── databricks/         ← Databricks internal data (logs, init scripts)
├── mnt/                ← YOUR MOUNTED STORAGE (this is where your data lives)
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── user/               ← User-specific data
```

### ⚠️ Important DBFS Limitation:

DBFS is **not** a real distributed file system. It's a virtualized path. In Unity Catalog (modern Databricks), the recommendation is to use **External Locations** instead of DBFS mounts. But you'll encounter DBFS mounts on virtually every existing enterprise project.

---

## 2.2 ADLS Gen2 — Azure Data Lake Storage

ADLS Gen2 is the enterprise standard for data lake storage on Azure. It combines:
- Blob Storage (cheap, scalable object storage)
- Hierarchical Namespace (real directories, not virtual — critical for ETL performance)
- Azure AD integration (fine-grained access control)

### ADLS Gen2 Structure for a Data Platform

```
Storage Account: stdatalakedev001
│
├── Container: bronze/          ← Raw, unprocessed data
│   ├── transactions/
│   │   ├── year=2024/
│   │   │   ├── month=01/
│   │   │   │   ├── day=01/
│   │   │   │   │   └── transactions_20240101.parquet
│
├── Container: silver/          ← Cleaned, validated, conformed data
│   └── transactions/
│       └── _delta_log/         ← Delta Lake transaction log
│
└── Container: gold/            ← Business-ready aggregated data
    └── daily_sales_summary/
```

---

## 2.3 Mounting ADLS Gen2 — Three Methods

### Method 1: Service Principal (Most Common in Production)

**Step 1:** Create a Service Principal in Azure AD

```
Azure Portal → Azure Active Directory → App Registrations
→ New Registration
   Name: sp-databricks-adls-dev
   Supported account types: Single tenant
→ Register
→ Copy: Application (client) ID
→ Certificates & Secrets → New Client Secret → Copy the VALUE
```

**Step 2:** Grant Service Principal access to ADLS

```
Storage Account → Access Control (IAM)
→ Add role assignment
→ Role: "Storage Blob Data Contributor"
→ Assign to: sp-databricks-adls-dev
→ Save
```

**Step 3:** Store secrets in Azure Key Vault (NEVER hardcode)

```
Key Vault → Secrets → Generate/Import
   sp-client-id:     <your-application-id>
   sp-client-secret: <your-client-secret-value>
   sp-tenant-id:     <your-tenant-id>
```

**Step 4:** Create Databricks Secret Scope backed by Key Vault

```
# In browser, navigate to:
https://<your-databricks-instance>#secrets/createScope

Fill in:
  Scope Name:         kv-scope
  Manage Principal:   All Users
  DNS Name:           https://<your-keyvault>.vault.azure.net/
  Resource ID:        /subscriptions/<sub-id>/resourceGroups/<rg>/providers/
                      Microsoft.KeyVault/vaults/<vault-name>
```

**Step 5:** Mount in Databricks Notebook

```python
# Mount ADLS Gen2 using Service Principal

storage_account = "stdatalakedev001"
container = "bronze"
mount_point = "/mnt/bronze"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": 
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": 
        dbutils.secrets.get(scope="kv-scope", key="sp-client-id"),
    "fs.azure.account.oauth2.client.secret": 
        dbutils.secrets.get(scope="kv-scope", key="sp-client-secret"),
    "fs.azure.account.oauth2.client.endpoint": 
        f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='kv-scope', key='sp-tenant-id')}/oauth2/token",
}

# Check if already mounted (idempotent pattern — production standard)
mount_exists = any(mount.mountPoint == mount_point 
                   for mount in dbutils.fs.mounts())

if not mount_exists:
    dbutils.fs.mount(
        source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"✅ Mounted {container} at {mount_point}")
else:
    print(f"ℹ️ {mount_point} already mounted — skipping")
```

### Method 2: Managed Identity (Modern — Recommended for New Projects)

Managed Identity eliminates secrets entirely. The cluster itself has an Azure identity.

```python
# With Managed Identity, no config needed in code — set at cluster level
# Cluster Advanced Options → Azure → Enable Managed Identity

# Then access storage directly:
df = spark.read.format("parquet") \
    .load("abfss://bronze@stdatalakedev001.dfs.core.windows.net/transactions/")

# OR via mount (configured once):
df = spark.read.format("parquet").load("/mnt/bronze/transactions/")
```

**Setup steps:**
```
1. Create a User-Assigned Managed Identity in Azure
2. Grant it "Storage Blob Data Contributor" on ADLS
3. In Databricks cluster → Advanced → Azure → Select the Managed Identity
4. Done — no secrets to manage
```

### Method 3: Access Key (Development Only — NEVER in Production)

```python
# ⚠️ ONLY for local testing / sandbox. NEVER use in production pipelines.
spark.conf.set(
    f"fs.azure.account.key.stdatalakedev001.dfs.core.windows.net",
    "<storage-account-access-key>"  # ← This is a hardcoded secret — NEVER DO THIS
)
```

---

## 2.4 dbutils — Complete Reference

`dbutils` is Databricks' utility library. Think of it as the Swiss Army knife of Databricks.

### dbutils.fs — File System Operations

```python
# List files
dbutils.fs.ls("/mnt/bronze/transactions/")
# Returns: [FileInfo(path=..., name=..., size=...)]

# Pretty display
display(dbutils.fs.ls("/mnt/bronze/transactions/"))

# Create directory
dbutils.fs.mkdirs("/mnt/bronze/new_folder/")

# Move/rename
dbutils.fs.mv(
    "/mnt/bronze/file.csv",
    "/mnt/bronze/archive/file.csv"
)

# Copy
dbutils.fs.cp(
    "/mnt/bronze/transactions/2024/",
    "/mnt/bronze/backup/2024/",
    recurse=True
)

# Delete
dbutils.fs.rm("/mnt/bronze/temp/", recurse=True)

# Read a file (small files only)
content = dbutils.fs.head("/mnt/bronze/config.json", maxBytes=65536)
print(content)
```

### dbutils.secrets — Secure Credential Retrieval

```python
# Get a secret (returns a masked string — won't print in notebook output)
password = dbutils.secrets.get(scope="kv-scope", key="db-password")

# List scopes
dbutils.secrets.listScopes()

# List keys in a scope
dbutils.secrets.list("kv-scope")

# Common usage pattern in production:
jdbc_url = "jdbc:sqlserver://server.database.windows.net:1433;database=mydb"
db_user  = dbutils.secrets.get(scope="kv-scope", key="sql-username")
db_pass  = dbutils.secrets.get(scope="kv-scope", key="sql-password")

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.transactions") \
    .option("user", db_user) \
    .option("password", db_pass) \
    .load()
```

### dbutils.widgets — Parameterized Notebooks

Widgets make notebooks work like functions — you pass parameters into them. Essential for production pipelines.

```python
# Define widgets (run this cell first)
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
dbutils.widgets.text("end_date",   "2024-01-31", "End Date")
dbutils.widgets.dropdown(
    "environment",
    "dev",
    ["dev", "test", "prod"],
    "Environment"
)
dbutils.widgets.combobox(
    "table_name",
    "transactions",
    ["transactions", "customers", "products"],
    "Source Table"
)

# Read widget values in subsequent cells
start_date  = dbutils.widgets.get("start_date")
end_date    = dbutils.widgets.get("end_date")
environment = dbutils.widgets.get("environment")
table_name  = dbutils.widgets.get("table_name")

print(f"Processing {table_name} from {start_date} to {end_date} in {environment}")

# Use in your pipeline logic
df = spark.read.format("delta") \
    .load(f"/mnt/{environment}/bronze/{table_name}") \
    .filter(f"transaction_date BETWEEN '{start_date}' AND '{end_date}'")
```

**How ADF passes parameters to widgets:**

When Azure Data Factory calls a Databricks notebook, it passes parameters as widget values:

```json
// ADF Databricks Notebook Activity → Base Parameters:
{
    "start_date": "@pipeline().parameters.StartDate",
    "end_date":   "@pipeline().parameters.EndDate",
    "environment": "prod"
}
```

### dbutils.notebook — Notebook Orchestration

```python
# Run another notebook and get its return value
result = dbutils.notebook.run(
    path="./utils/validate_schema",
    timeout_seconds=300,
    arguments={
        "table_name": "transactions",
        "expected_columns": "id,amount,date,customer_id"
    }
)

print(f"Validation result: {result}")

# In the called notebook — return a value:
# dbutils.notebook.exit("SUCCESS")
# dbutils.notebook.exit("FAILED: missing columns")
```

### dbutils.jobs — Job Run Context

```python
# Get current job run context (available when running as a job)
context = dbutils.jobs.taskValues

# Set a value to pass to downstream tasks
dbutils.jobs.taskValues.set(key="record_count", value=str(df.count()))

# Get a value set by an upstream task
upstream_count = dbutils.jobs.taskValues.get(
    taskKey="ingest_task",
    key="record_count",
    default="0"
)
```

---

## 2.5 Verify Your Setup — Complete Checklist

```python
# Run this notebook after initial setup to verify everything works

print("=" * 60)
print("DATABRICKS ENVIRONMENT VERIFICATION")
print("=" * 60)

# 1. Spark is working
print(f"\n✅ Spark Version: {spark.version}")

# 2. Check mounts
mounts = dbutils.fs.mounts()
print(f"\n📁 Active Mounts ({len(mounts)} total):")
for mount in mounts:
    if "/mnt/" in mount.mountPoint:
        print(f"   {mount.mountPoint} → {mount.source}")

# 3. Verify bronze mount is accessible
try:
    files = dbutils.fs.ls("/mnt/bronze/")
    print(f"\n✅ Bronze mount accessible — {len(files)} items")
except Exception as e:
    print(f"\n❌ Bronze mount error: {e}")

# 4. Verify secrets
try:
    secret = dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
    print(f"\n✅ Secrets accessible (value masked)")
except Exception as e:
    print(f"\n❌ Secrets error: {e}")

# 5. Test Delta write/read
try:
    test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
    test_df.write.format("delta").mode("overwrite").save("/mnt/bronze/_test/")
    read_back = spark.read.format("delta").load("/mnt/bronze/_test/")
    print(f"\n✅ Delta Lake read/write working")
    dbutils.fs.rm("/mnt/bronze/_test/", recurse=True)
except Exception as e:
    print(f"\n❌ Delta Lake error: {e}")

print("\n" + "=" * 60)
print("Verification complete")
```

---

## 2.6 Common Mistakes — Storage & DBFS

| Mistake | Consequence | Fix |
|---|---|---|
| Hardcoding storage keys | Security breach, key rotation breaks pipeline | Always use Key Vault + dbutils.secrets |
| Mounting without idempotency check | `MountException` on re-run | Check `dbutils.fs.mounts()` before mounting |
| Using DBFS root (not /mnt) | Data not persistent, security issues | Always use mounted ADLS Gen2 |
| Storing sensitive data in FileStore | Accessible to all workspace users | Use ADLS with proper ACLs |
| Not using Hierarchical Namespace on ADLS | Slow directory operations | Always enable HNS when creating storage |

---

## 2.7 Interview Questions — Storage & DBFS

**Q1: What is the difference between DBFS and ADLS Gen2?**
> DBFS is a Databricks abstraction layer — a virtualized file system path (`/mnt/...`) that maps to cloud storage underneath. ADLS Gen2 is the actual Azure storage service with a hierarchical namespace. In production, data lives in ADLS Gen2; DBFS mounts provide a convenient path to access it from notebooks.

**Q2: How do you securely connect Databricks to ADLS Gen2 without hardcoding credentials?**
> Three approaches: (1) **Service Principal** — store credentials in Azure Key Vault, create a Databricks secret scope, retrieve via `dbutils.secrets.get()`. (2) **Managed Identity** — assign an Azure identity to the cluster, grant it RBAC on storage, zero secrets to manage. (3) **Unity Catalog External Locations** — the modern approach using Unity Catalog's storage credentials. Managed Identity is preferred for new projects.

**Q3: What is an idempotent mount and why does it matter?**
> An idempotent mount checks if the mount point already exists before mounting — if it does, it skips the operation. This is critical because `dbutils.fs.mount()` throws an exception if the mount already exists. Without the idempotency check, notebooks fail on re-runs.

---

*Next: [Part 3 — Databricks Jobs & Production Pipeline Design](../08-Jobs-Pipelines/01-creating-jobs.md)*
