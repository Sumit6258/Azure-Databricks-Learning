# Module 09 — Unity Catalog & Data Governance
## Catalog → Schema → Table Structure, Access Control & Data Governance

> Unity Catalog is Databricks' answer to enterprise data governance. It's now the standard for all new Databricks deployments. If you're interviewing for a DE role at an MNC, you WILL be asked about this.

---

## 9.1 What is Unity Catalog?

Before Unity Catalog, every Databricks workspace had its own Hive metastore — completely isolated from other workspaces. If you had dev/test/prod workspaces, you had 3 separate metadata silos with no shared governance.

```
BEFORE Unity Catalog:
  Dev Workspace  → Hive Metastore (dev)   → ADLS Dev
  Test Workspace → Hive Metastore (test)  → ADLS Test
  Prod Workspace → Hive Metastore (prod)  → ADLS Prod
  
  Problems:
  ✗ No cross-workspace table sharing
  ✗ No centralized access control
  ✗ Data governance is per-workspace manual work
  ✗ No column-level security
  ✗ No data lineage tracking

AFTER Unity Catalog:
  Dev Workspace  ─┐
  Test Workspace ─┼→ Unity Catalog (account-level) → ADLS (all envs)
  Prod Workspace ─┘
  
  Benefits:
  ✅ One governance layer for ALL workspaces
  ✅ Centralized RBAC (Grant/Revoke)
  ✅ Column-level security and row filters
  ✅ Automated data lineage
  ✅ Cross-workspace table access
  ✅ PII tagging and masking
```

---

## 9.2 The Three-Level Namespace

```
Unity Catalog Hierarchy:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ACCOUNT LEVEL
└── Unity Catalog Metastore (one per region, shared by all workspaces)
    │
    ├── CATALOG: prod_catalog          ← Like a database server
    │   ├── SCHEMA: bronze             ← Like a database
    │   │   ├── TABLE: transactions    ← Like a table
    │   │   ├── TABLE: customers
    │   │   └── VIEW:  vw_recent_txns
    │   ├── SCHEMA: silver
    │   │   ├── TABLE: transactions_clean
    │   │   └── TABLE: customers_enriched
    │   └── SCHEMA: gold
    │       ├── TABLE: daily_revenue
    │       └── VIEW:  executive_dashboard
    │
    ├── CATALOG: dev_catalog
    │   └── ...
    │
    └── CATALOG: shared_catalog        ← Cross-team shared data
        └── SCHEMA: reference_data
            ├── TABLE: country_codes
            └── TABLE: currency_rates

Fully-qualified table name:
  prod_catalog.silver.transactions
  ↑ Catalog   ↑ Schema ↑ Table
```

### Addressing Tables

```python
# Old Hive way (2-level: database.table)
spark.sql("SELECT * FROM silver.transactions")

# Unity Catalog way (3-level: catalog.schema.table)
spark.sql("SELECT * FROM prod_catalog.silver.transactions")

# Set default catalog (so you don't need to prefix every query)
spark.sql("USE CATALOG prod_catalog")
spark.sql("USE SCHEMA silver")
spark.sql("SELECT * FROM transactions")  # Now works without full path
```

---

## 9.3 Setting Up Unity Catalog — Step by Step

### Admin Setup (Done Once by Platform Team)

```
STEP 1: Enable Unity Catalog for the Account
  accounts.azuredatabricks.com → Data (Unity Catalog)
  → Create Metastore
    Name: prod-metastore
    Region: East US (must match your workspace region!)
    ADLS Gen2 path: abfss://unity-catalog@metastore-storage.dfs.core.windows.net/
    Access Connector: /subscriptions/.../databricks-access-connector
  → Create → Assign to Workspace(s)

STEP 2: Create Catalogs
```

```sql
-- Create environment catalogs (run as account admin)
CREATE CATALOG IF NOT EXISTS prod_catalog
  COMMENT 'Production data catalog — all production tables';

CREATE CATALOG IF NOT EXISTS dev_catalog
  COMMENT 'Development data catalog — for engineering work';

CREATE CATALOG IF NOT EXISTS shared_catalog
  COMMENT 'Shared reference data — read by all teams';

-- Create schemas within catalog
USE CATALOG prod_catalog;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingested data — append-only, no transforms';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned, validated, business-rule-applied data';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Business-ready aggregations for BI consumption';

CREATE SCHEMA IF NOT EXISTS quarantine
  COMMENT 'Records failing data quality validation';
```

### Storage Credentials and External Locations

```sql
-- Create storage credential (for accessing ADLS from Unity Catalog)
CREATE STORAGE CREDENTIAL prod_adls_credential
  WITH AZURE_MANAGED_IDENTITY = (
    DIRECTORY_ID '<tenant-id>',
    APPLICATION_ID '<app-id>'
  )
  COMMENT 'Credential for production ADLS Gen2 access';

-- Create external locations
CREATE EXTERNAL LOCATION prod_bronze
  URL 'abfss://bronze@prodlake.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL prod_adls_credential)
  COMMENT 'Production bronze layer';

CREATE EXTERNAL LOCATION prod_silver
  URL 'abfss://silver@prodlake.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL prod_adls_credential);

CREATE EXTERNAL LOCATION prod_gold
  URL 'abfss://gold@prodlake.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL prod_adls_credential);
```

---

## 9.4 Access Control — GRANT & REVOKE

Unity Catalog uses standard SQL GRANT/REVOKE syntax for all permissions.

### Privilege Hierarchy

```
Object Level → Available Privileges
─────────────────────────────────────────────────────────────────────
ACCOUNT        → CREATE CATALOG, USE CATALOG
CATALOG        → USE CATALOG, CREATE SCHEMA, USE SCHEMA, CREATE TABLE
SCHEMA         → USE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION
TABLE          → SELECT, MODIFY, READ FILES, WRITE FILES
VIEW           → SELECT
FUNCTION       → EXECUTE
STORAGE CRED.  → CREATE EXTERNAL LOCATION, READ FILES, WRITE FILES
EXTERNAL LOC.  → READ FILES, WRITE FILES, CREATE EXTERNAL TABLE
```

### Setting Up Role-Based Access

```sql
-- ─────────────────────────────────────────────────────────
-- DATA ENGINEERING TEAM — Full write access to all layers
-- ─────────────────────────────────────────────────────────
GRANT USE CATALOG ON CATALOG prod_catalog TO `data-engineering-team`;
GRANT USE SCHEMA  ON SCHEMA prod_catalog.bronze TO `data-engineering-team`;
GRANT USE SCHEMA  ON SCHEMA prod_catalog.silver TO `data-engineering-team`;
GRANT USE SCHEMA  ON SCHEMA prod_catalog.gold   TO `data-engineering-team`;

-- Full CRUD on Silver and Gold
GRANT SELECT, MODIFY ON SCHEMA prod_catalog.silver TO `data-engineering-team`;
GRANT SELECT, MODIFY ON SCHEMA prod_catalog.gold   TO `data-engineering-team`;

-- Bronze is append-only even for DE team
GRANT SELECT, MODIFY ON SCHEMA prod_catalog.bronze TO `data-engineering-team`;

-- Storage access
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION prod_bronze TO `data-engineering-team`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION prod_silver TO `data-engineering-team`;

-- ─────────────────────────────────────────────────────────
-- DATA ANALYSTS — Read-only on Gold layer
-- ─────────────────────────────────────────────────────────
GRANT USE CATALOG ON CATALOG prod_catalog          TO `data-analysts`;
GRANT USE SCHEMA  ON SCHEMA prod_catalog.gold      TO `data-analysts`;
GRANT SELECT      ON SCHEMA prod_catalog.gold      TO `data-analysts`;
-- Note: Analysts can't see Bronze or Silver

-- ─────────────────────────────────────────────────────────
-- BI TEAM — Read-only on Gold via SQL Warehouse
-- ─────────────────────────────────────────────────────────
GRANT USE CATALOG ON CATALOG prod_catalog          TO `bi-team`;
GRANT USE SCHEMA  ON SCHEMA prod_catalog.gold      TO `bi-team`;
GRANT SELECT      ON SCHEMA prod_catalog.gold      TO `bi-team`;

-- SQL Warehouse access
GRANT USE ON SQL WAREHOUSE pbi-reporting-warehouse TO `bi-team`;

-- ─────────────────────────────────────────────────────────
-- SERVICE ACCOUNTS — For ADF, Airflow pipelines
-- ─────────────────────────────────────────────────────────
GRANT USE CATALOG ON CATALOG prod_catalog          TO `svc-adf-pipeline`;
GRANT USE SCHEMA  ON ALL SCHEMAS IN CATALOG prod_catalog TO `svc-adf-pipeline`;
GRANT SELECT, MODIFY ON ALL TABLES IN CATALOG prod_catalog TO `svc-adf-pipeline`;

-- ─────────────────────────────────────────────────────────
-- REVOKING ACCESS
-- ─────────────────────────────────────────────────────────
REVOKE MODIFY ON SCHEMA prod_catalog.gold FROM `data-analysts`;

-- Check current permissions
SHOW GRANTS ON TABLE prod_catalog.silver.transactions;
SHOW GRANTS TO `data-analysts`;
```

### Table-Level and Column-Level Security

```sql
-- ─────────────────────────────────────────────────────────
-- COLUMN MASKING — Hide PII from non-privileged users
-- ─────────────────────────────────────────────────────────

-- Create a masking function
CREATE OR REPLACE FUNCTION prod_catalog.security.mask_pan_card(pan_number STRING)
  RETURNS STRING
  RETURN CASE
    WHEN IS_MEMBER('data-compliance-team') THEN pan_number    -- Compliance sees full
    WHEN IS_MEMBER('data-engineering-team') THEN pan_number  -- DE sees full
    ELSE CONCAT('XXXXX', SUBSTR(pan_number, 6, 4), 'X')      -- Others see masked
  END;

-- Apply masking to column
ALTER TABLE prod_catalog.silver.customers
  ALTER COLUMN pan_card
  SET MASK prod_catalog.security.mask_pan_card;

-- Now when analysts query:
SELECT customer_id, pan_card FROM prod_catalog.silver.customers;
-- Analysts see:   XXXXX1234X  (masked)
-- Compliance sees: ABCDE1234F (full)

-- ─────────────────────────────────────────────────────────
-- ROW FILTERS — Restrict data by region/business unit
-- ─────────────────────────────────────────────────────────

-- Create a row filter function
CREATE OR REPLACE FUNCTION prod_catalog.security.filter_by_region(region STRING)
  RETURNS BOOLEAN
  RETURN CASE
    WHEN IS_MEMBER('global-data-team') THEN TRUE                  -- Global sees all
    WHEN IS_MEMBER('india-data-team') AND region = 'IN' THEN TRUE -- India sees IN only
    WHEN IS_MEMBER('us-data-team')    AND region = 'US' THEN TRUE -- US sees US only
    ELSE FALSE
  END;

-- Apply to table
ALTER TABLE prod_catalog.silver.transactions
  ADD ROW FILTER prod_catalog.security.filter_by_region ON (region);

-- India team queries: SELECT * FROM transactions
-- Gets: only rows where region = 'IN'
-- US team gets: only rows where region = 'US'
-- Global team gets: all rows

-- ─────────────────────────────────────────────────────────
-- DYNAMIC VIEWS — Alternative for complex masking (older approach)
-- ─────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW prod_catalog.gold.vw_customers_secure AS
SELECT
  customer_id,
  customer_name,
  -- Mask phone number for non-compliance users
  CASE
    WHEN IS_MEMBER('compliance-team') THEN phone_number
    ELSE CONCAT('XXXXXX', RIGHT(phone_number, 4))
  END AS phone_number,
  -- Mask email for non-compliance users  
  CASE
    WHEN IS_MEMBER('compliance-team') THEN email
    ELSE CONCAT(LEFT(email, 3), '****@****.com')
  END AS email,
  city,
  state,
  created_date
FROM prod_catalog.silver.customers;

-- Grant analysts access to the MASKED view, not the base table
GRANT SELECT ON VIEW prod_catalog.gold.vw_customers_secure TO `data-analysts`;
-- DO NOT grant access to prod_catalog.silver.customers directly
```

---

## 9.5 Data Tags and PII Classification

```sql
-- Tag columns with their data classification
ALTER TABLE prod_catalog.silver.customers
  ALTER COLUMN email
  SET TAGS ('pii' = 'true', 'pii_type' = 'email', 'gdpr_subject' = 'true');

ALTER TABLE prod_catalog.silver.customers
  ALTER COLUMN phone_number
  SET TAGS ('pii' = 'true', 'pii_type' = 'phone');

ALTER TABLE prod_catalog.silver.customers
  ALTER COLUMN pan_card
  SET TAGS ('pii' = 'true', 'pii_type' = 'financial_id', 'encryption_required' = 'true');

-- Find all PII columns across all tables (data catalog query)
SELECT
  table_catalog,
  table_schema,
  table_name,
  column_name,
  tag_name,
  tag_value
FROM information_schema.column_tags
WHERE tag_name = 'pii' AND tag_value = 'true'
ORDER BY table_catalog, table_schema, table_name;
```

---

## 9.6 Data Lineage

Unity Catalog automatically tracks data lineage — which tables read from which sources, which columns transform into which output columns.

```python
# Lineage is tracked automatically when you run SQL or use Spark
# To VIEW lineage:
# Unity Catalog → Data Explorer → Select a table → Lineage tab

# Example lineage Databricks tracks automatically:
# CSV Files (ADLS)
#   ↓ (Auto Loader)
# bronze.transactions
#   ↓ (transform_silver notebook)
# silver.transactions_clean
#   ↓ (build_gold notebook)
# gold.daily_revenue_summary
#   ↓ (Power BI)
# Revenue Dashboard

# Query lineage programmatically via API:
import requests

token = dbutils.secrets.get("kv-prod", "databricks-pat-token")
workspace = "https://adb-workspace.azuredatabricks.net"

response = requests.get(
    f"{workspace}/api/2.0/lineage-tracking/table-lineage",
    headers={"Authorization": f"Bearer {token}"},
    json={"table_name": "prod_catalog.silver.transactions_clean"}
)

lineage = response.json()
print(f"Upstream tables: {lineage.get('upstreams', [])}")
print(f"Downstream tables: {lineage.get('downstreams', [])}")
```

---

## 9.7 Practical Unity Catalog Setup Script

```python
# ── setup_unity_catalog.py ──
# Run this once per environment to set up the full UC structure

def setup_catalog(catalog_name: str, environment: str = "prod"):
    """Complete Unity Catalog setup for a data platform."""
    
    print(f"🏗️  Setting up Unity Catalog for: {catalog_name}")
    
    # Create catalog
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        COMMENT '{environment.upper()} data catalog'
    """)
    
    # Create schemas (Medallion Architecture)
    schemas = {
        "bronze":      "Raw ingested data — append-only",
        "silver":      "Cleaned and validated data",
        "gold":        "Business-ready aggregations",
        "quarantine":  "Failed data quality records",
        "metadata":    "Pipeline metadata and watermarks",
        "security":    "Security functions and policies",
    }
    
    for schema, comment in schemas.items():
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}
            COMMENT '{comment}'
        """)
        print(f"  ✅ Schema created: {catalog_name}.{schema}")
    
    # Set up default permissions
    team_permissions = {
        "data-engineering-team": {
            "schemas": ["bronze", "silver", "gold", "quarantine", "metadata"],
            "privilege": "SELECT, MODIFY, CREATE TABLE, CREATE VIEW"
        },
        "data-analysts": {
            "schemas": ["gold"],
            "privilege": "SELECT"
        },
        "bi-team": {
            "schemas": ["gold"],
            "privilege": "SELECT"
        },
    }
    
    for team, perms in team_permissions.items():
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{team}`")
        for schema in perms["schemas"]:
            spark.sql(f"""
                GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema} TO `{team}`
            """)
            spark.sql(f"""
                GRANT {perms['privilege']} 
                ON ALL TABLES IN SCHEMA {catalog_name}.{schema} 
                TO `{team}`
            """)
        print(f"  ✅ Permissions set for: {team}")
    
    print(f"\n✅ Unity Catalog setup complete for {catalog_name}")

# Run setup
setup_catalog("prod_catalog", "prod")
```

---

## 9.8 Interview Questions — Unity Catalog

**Q1: What is the three-level namespace in Unity Catalog?**
> `catalog.schema.table` — Catalog is the top-level container (equivalent to a database server), Schema is a logical grouping of tables (equivalent to a database), and Table is the actual data object. This adds a catalog level on top of the traditional two-level Hive metastore (`database.table`). Example: `prod_catalog.silver.transactions`.

**Q2: What is the difference between Column Masking and Row Filtering in Unity Catalog?**
> Column Masking applies a SQL function to a column at query time — users see masked values (e.g., `XXXXX1234X` instead of a PAN card) while the underlying data is untouched. Row Filtering restricts which rows a user can see based on their group membership — a regional analyst sees only their region's data. Both are implemented as SQL functions applied to the table definition, completely transparent to the querying application.

**Q3: How does Unity Catalog improve on the old per-workspace Hive metastore?**
> Unity Catalog is account-level (not workspace-level), so the same tables, permissions, and governance policies apply across all workspaces (dev/test/prod). With Hive metastore, each workspace had isolated metadata — sharing tables required manual duplication, and you had to manage access separately in each workspace. Unity Catalog adds column masking, row filtering, automated data lineage, PII tagging, and centralized RBAC — none of which existed in Hive metastore.

**Q4: How do you handle a GDPR right-to-erasure request in a Delta Lake + Unity Catalog setup?**
> (1) Use column tags to identify all PII columns for that customer across tables. (2) Use Delta Lake MERGE with DELETE to remove the customer's records from Bronze, Silver, and Gold tables. (3) Run VACUUM after deletion to permanently remove the data files (with appropriate retention set). (4) Use Delta's Change Data Feed to propagate the deletion to downstream systems. (5) Document using Unity Catalog's audit logs that the erasure occurred. The key challenge is Bronze (usually append-only) — you may need to specifically retain a "deletion record" log instead of actually deleting from Bronze.

---

*Next: [Module 10 — Monitoring, Spark UI & Debugging](../10-Monitoring-Debugging/01-monitoring-debugging-complete.md)*
