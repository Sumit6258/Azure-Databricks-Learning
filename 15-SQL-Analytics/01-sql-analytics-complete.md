# Module 15 — SQL Analytics & Databricks SQL
## SQL Warehouse, Advanced SQL Patterns & BI Integration

---

## 15.1 Databricks SQL — Overview

Databricks SQL is a dedicated SQL interface for analysts and BI engineers. It runs on SQL Warehouses (not Spark clusters) and supports ANSI SQL with Delta Lake extensions.

```
WHO USES DATABRICKS SQL:
  Data Analysts      → Ad-hoc queries, exploration
  BI Engineers       → Dashboard queries, Power BI source
  Analytics Engineers→ dbt models, data mart builds
  Data Scientists    → Quick data profiling

HOW IT DIFFERS FROM NOTEBOOK SQL:
  SQL Warehouse      → Optimized for concurrent BI queries
  Spark Cluster SQL  → Optimized for large batch ETL
  Serverless option  → Zero startup time for SQL Warehouse
  ODBC/JDBC          → Native connections for BI tools
```

---

## 15.2 Advanced SQL Patterns

### Window Functions in SQL

```sql
-- ── ROW NUMBER — deduplicate, top-N per group ──
SELECT *
FROM (
    SELECT
        customer_id, txn_date, amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY txn_date DESC
        ) AS rn
    FROM silver.transactions
)
WHERE rn = 1;  -- Latest transaction per customer

-- ── RUNNING TOTAL ──
SELECT
    txn_date,
    payment_mode,
    daily_revenue,
    SUM(daily_revenue) OVER (
        PARTITION BY payment_mode
        ORDER BY txn_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    AVG(daily_revenue) OVER (
        PARTITION BY payment_mode
        ORDER BY txn_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM gold.daily_revenue_summary;

-- ── LAG / LEAD — period-over-period comparison ──
SELECT
    txn_date,
    total_revenue,
    LAG(total_revenue, 1) OVER (ORDER BY txn_date) AS prev_day_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue, 1) OVER (ORDER BY txn_date))
        / LAG(total_revenue, 1) OVER (ORDER BY txn_date) * 100,
        2
    ) AS day_over_day_pct_change,
    LAG(total_revenue, 7) OVER (ORDER BY txn_date) AS same_day_last_week,
    ROUND(
        (total_revenue - LAG(total_revenue, 7) OVER (ORDER BY txn_date))
        / LAG(total_revenue, 7) OVER (ORDER BY txn_date) * 100,
        2
    ) AS week_over_week_pct
FROM (
    SELECT txn_date, SUM(total_revenue) AS total_revenue
    FROM gold.daily_revenue_summary
    GROUP BY txn_date
)
ORDER BY txn_date;

-- ── PERCENTILE AND DISTRIBUTION ──
SELECT
    payment_mode,
    COUNT(*)                                      AS txn_count,
    ROUND(AVG(amount), 2)                         AS avg_amount,
    PERCENTILE_APPROX(amount, 0.25)               AS p25,
    PERCENTILE_APPROX(amount, 0.50)               AS median,
    PERCENTILE_APPROX(amount, 0.75)               AS p75,
    PERCENTILE_APPROX(amount, 0.95)               AS p95,
    PERCENTILE_APPROX(amount, 0.99)               AS p99,
    MAX(amount)                                   AS max_amount,
    NTILE(4) OVER (ORDER BY AVG(amount) DESC)     AS spend_quartile
FROM silver.transactions
WHERE txn_date >= DATEADD(CURRENT_DATE(), -30)
GROUP BY payment_mode;
```

### CTEs and Complex Queries

```sql
-- ── MULTI-LEVEL CTEs ──
WITH
-- Step 1: Get 30-day transaction history
transaction_history AS (
    SELECT
        customer_id,
        txn_date,
        amount,
        payment_mode,
        COUNT(*) OVER (
            PARTITION BY customer_id
            ORDER BY txn_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS txns_last_30d,
        SUM(amount) OVER (
            PARTITION BY customer_id
            ORDER BY txn_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS spend_last_30d
    FROM silver.transactions
    WHERE txn_date >= DATEADD(CURRENT_DATE(), -60)
),

-- Step 2: Get latest state per customer
latest_per_customer AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY txn_date DESC
            ) AS rn
        FROM transaction_history
    )
    WHERE rn = 1
),

-- Step 3: Classify customers
customer_segments AS (
    SELECT
        customer_id,
        spend_last_30d,
        txns_last_30d,
        CASE
            WHEN spend_last_30d >= 50000 AND txns_last_30d >= 10 THEN 'CHAMPION'
            WHEN spend_last_30d >= 20000 AND txns_last_30d >= 5  THEN 'LOYAL'
            WHEN spend_last_30d >= 5000                          THEN 'POTENTIAL'
            WHEN spend_last_30d > 0                              THEN 'AT_RISK'
            ELSE                                                      'INACTIVE'
        END AS segment
    FROM latest_per_customer
),

-- Step 4: Join with customer master
enriched AS (
    SELECT
        cs.*,
        c.name,
        c.city,
        c.registration_date,
        DATEDIFF(CURRENT_DATE(), c.registration_date) AS customer_age_days
    FROM customer_segments cs
    LEFT JOIN silver.customers c USING (customer_id)
)

-- Final output
SELECT
    segment,
    city,
    COUNT(*)              AS customer_count,
    ROUND(AVG(spend_last_30d), 0) AS avg_spend,
    ROUND(AVG(txns_last_30d), 1)  AS avg_txn_count
FROM enriched
GROUP BY segment, city
ORDER BY segment, avg_spend DESC;
```

### Pivot and Unpivot in SQL

```sql
-- ── PIVOT — rows to columns ──
SELECT *
FROM (
    SELECT txn_date, payment_mode, amount
    FROM silver.transactions
    WHERE txn_date >= '2024-01-01'
)
PIVOT (
    SUM(amount)
    FOR payment_mode IN ('CARD', 'UPI', 'NETBANKING', 'NEFT', 'RTGS')
);

-- ── UNPIVOT — columns to rows ──
SELECT txn_date, payment_mode, revenue
FROM gold.monthly_pivot_data
UNPIVOT (
    revenue FOR payment_mode IN (CARD, UPI, NETBANKING, NEFT)
);

-- ── CONDITIONAL AGGREGATION (manual pivot) ──
SELECT
    txn_date,
    SUM(CASE WHEN payment_mode = 'CARD'       THEN amount END) AS card_revenue,
    SUM(CASE WHEN payment_mode = 'UPI'        THEN amount END) AS upi_revenue,
    SUM(CASE WHEN payment_mode = 'NETBANKING' THEN amount END) AS netbanking_revenue,
    COUNT(CASE WHEN fraud_risk_level = 'HIGH' THEN 1 END)      AS high_risk_count,
    COUNT(CASE WHEN status = 'FAILED'         THEN 1 END)      AS failed_count
FROM silver.transactions
GROUP BY txn_date
ORDER BY txn_date;
```

### Recursive CTEs

```sql
-- ── RECURSIVE CTE — traverse hierarchical data ──
-- Use case: org hierarchy, category trees, referral chains

WITH RECURSIVE org_hierarchy AS (
    -- Base case: top-level managers (no manager)
    SELECT
        employee_id,
        name,
        manager_id,
        1 AS level,
        CAST(name AS STRING) AS hierarchy_path
    FROM silver.employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case: employees who report to the above
    SELECT
        e.employee_id,
        e.name,
        e.manager_id,
        oh.level + 1,
        CONCAT(oh.hierarchy_path, ' > ', e.name)
    FROM silver.employees e
    JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
    WHERE oh.level < 10  -- Prevent infinite recursion
)

SELECT
    level,
    hierarchy_path,
    COUNT(*) AS headcount
FROM org_hierarchy
GROUP BY level, hierarchy_path
ORDER BY level, hierarchy_path;
```

### Semi-Structured Data Queries

```sql
-- ── QUERY JSON COLUMNS ──
SELECT
    order_id,
    customer:id           AS customer_id,         -- Struct/map access
    customer:name         AS customer_name,
    customer:address:city AS customer_city,        -- Nested access
    items[0]:sku          AS first_sku,            -- Array index
    ARRAY_SIZE(items)     AS item_count,
    
    -- Aggregate over array elements
    AGGREGATE(
        items,
        0.0,
        (acc, item) -> acc + (item:qty * item:price)
    ) AS order_total,
    
    -- Filter array elements
    FILTER(items, item -> item:price > 500) AS expensive_items,
    
    -- Transform array
    TRANSFORM(items, item -> item:sku) AS all_skus

FROM bronze.orders
WHERE customer:address:city = 'Mumbai'
  AND EXISTS(items, item -> item:qty > 5);

-- ── FROM_JSON — parse string column as JSON ──
SELECT
    event_id,
    from_json(payload, 'STRUCT<user_id:STRING, action:STRING, ts:LONG>').user_id AS user_id,
    from_json(payload, 'STRUCT<user_id:STRING, action:STRING, ts:LONG>').action AS action
FROM bronze.events;

-- ── EXPLODE ARRAY in SQL ──
SELECT
    order_id,
    item.sku,
    item.qty,
    item.price
FROM bronze.orders
LATERAL VIEW EXPLODE(items) AS item;
```

---

## 15.3 Data Profiling Queries

```sql
-- ── COMPLETE TABLE PROFILE ──
-- Run this on any new dataset to understand it immediately

-- 1. Basic statistics
SELECT
    COUNT(*)                                   AS total_rows,
    COUNT(DISTINCT customer_id)                AS unique_customers,
    MIN(txn_date)                              AS earliest_date,
    MAX(txn_date)                              AS latest_date,
    DATEDIFF(MAX(txn_date), MIN(txn_date))     AS date_range_days,
    ROUND(SUM(amount), 2)                      AS total_amount,
    ROUND(AVG(amount), 2)                      AS avg_amount,
    ROUND(STDDEV(amount), 2)                   AS std_amount,
    MIN(amount)                                AS min_amount,
    MAX(amount)                                AS max_amount,
    PERCENTILE_APPROX(amount, 0.5)             AS median_amount
FROM silver.transactions
WHERE txn_date >= '2024-01-01';

-- 2. Null counts per column
SELECT
    SUM(CASE WHEN txn_id       IS NULL THEN 1 ELSE 0 END) AS null_txn_id,
    SUM(CASE WHEN customer_id  IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN amount       IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN payment_mode IS NULL THEN 1 ELSE 0 END) AS null_payment_mode,
    SUM(CASE WHEN txn_date     IS NULL THEN 1 ELSE 0 END) AS null_txn_date,
    COUNT(*) AS total_rows,
    ROUND(SUM(CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS null_pct_txn_id
FROM silver.transactions;

-- 3. Distribution by category
SELECT
    payment_mode,
    COUNT(*)                                AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    ROUND(SUM(amount), 2)                   AS total_amount,
    ROUND(AVG(amount), 2)                   AS avg_amount
FROM silver.transactions
GROUP BY payment_mode
ORDER BY row_count DESC;

-- 4. Duplicate detection
SELECT
    txn_id,
    COUNT(*) AS occurrences
FROM silver.transactions
GROUP BY txn_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 5. Date gaps (missing days)
WITH date_spine AS (
    SELECT EXPLODE(SEQUENCE(
        DATE('2024-01-01'),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS dt
),
daily_counts AS (
    SELECT txn_date, COUNT(*) AS cnt
    FROM silver.transactions
    GROUP BY txn_date
)
SELECT ds.dt AS missing_date
FROM date_spine ds
LEFT JOIN daily_counts dc ON ds.dt = dc.txn_date
WHERE dc.txn_date IS NULL
ORDER BY ds.dt;
```

---

## 15.4 dbt with Databricks

dbt (data build tool) is widely used with Databricks for analytics engineering — transforming Silver data into Gold marts with version-controlled SQL.

```yaml
# profiles.yml — Connect dbt to Databricks SQL Warehouse
databricks_profile:
  target: prod
  outputs:
    dev:
      type: databricks
      host: adb-workspace.azuredatabricks.net
      http_path: /sql/1.0/warehouses/abc123
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      schema: dbt_dev_sumit      # Personal dev schema
      threads: 4

    prod:
      type: databricks
      host: adb-workspace.azuredatabricks.net
      http_path: /sql/1.0/warehouses/prod456
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: prod_catalog      # Unity Catalog
      schema: gold               # Target schema
      threads: 8
```

```sql
-- models/gold/daily_revenue_summary.sql
-- dbt model — runs as a SELECT, dbt handles CREATE TABLE/VIEW

{{
  config(
    materialized = 'incremental',     -- Only process new data
    unique_key   = ['txn_date', 'payment_mode'],
    partition_by = {'field': 'txn_date', 'data_type': 'date'},
    cluster_by   = ['payment_mode'],
    file_format  = 'delta',
    incremental_strategy = 'merge',
    on_schema_change = 'merge'
  )
}}

WITH base AS (
    SELECT
        txn_date,
        payment_mode,
        COUNT(txn_id)                AS transaction_count,
        SUM(amount)                  AS total_revenue,
        AVG(amount)                  AS avg_transaction,
        COUNT(DISTINCT customer_id)  AS unique_customers
    FROM {{ ref('silver_transactions') }}   -- References another dbt model
    
    {% if is_incremental() %}
    -- In incremental runs, only process new data
    WHERE txn_date > (SELECT MAX(txn_date) FROM {{ this }})
    {% endif %}
    
    GROUP BY txn_date, payment_mode
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS dbt_updated_at,
    '{{ invocation_id }}' AS dbt_run_id
FROM base
```

```yaml
# models/schema.yml — dbt data quality tests
models:
  - name: daily_revenue_summary
    description: "Daily transaction revenue by payment mode"
    columns:
      - name: txn_date
        tests:
          - not_null
          - dbt_utils.recency:
              datepart: day
              field: txn_date
              interval: 2        # Fail if no data in last 2 days

      - name: total_revenue
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0

      - name: payment_mode
        tests:
          - not_null
          - accepted_values:
              values: ['CARD','UPI','NETBANKING','NEFT','RTGS','IMPS']
```

```bash
# dbt commands
dbt run                           # Run all models
dbt run --select daily_revenue    # Run specific model
dbt run --select +daily_revenue   # Run model + all upstream
dbt test                          # Run all data quality tests
dbt docs generate && dbt docs serve  # Generate lineage docs
dbt compile                       # Compile SQL without running
```

---

## 15.5 Power BI Optimization for Databricks

```sql
-- ── CREATE OPTIMIZED VIEWS FOR POWER BI ──
-- Power BI works best with pre-aggregated, denormalized data
-- Minimize joins at query time — pre-join in Gold layer

CREATE OR REPLACE VIEW prod_catalog.gold.vw_executive_dashboard AS
SELECT
    t.txn_date                              AS Date,
    YEAR(t.txn_date)                        AS Year,
    MONTH(t.txn_date)                       AS Month,
    DATE_FORMAT(t.txn_date, 'MMMM')        AS MonthName,
    QUARTER(t.txn_date)                     AS Quarter,
    t.payment_mode                          AS PaymentMethod,
    c.city                                  AS CustomerCity,
    c.tier                                  AS CustomerTier,
    t.transaction_count                     AS Transactions,
    t.total_revenue                         AS Revenue,
    t.avg_transaction                       AS AvgTransactionValue,
    t.unique_customers                      AS UniqueCustomers,
    t.high_risk_count                       AS FraudAlerts,
    ROUND(t.high_risk_count * 100.0 / NULLIF(t.transaction_count, 0), 2) AS FraudRatePct,

    -- Pre-calculated period comparisons (avoid complex DAX)
    LAG(t.total_revenue) OVER (
        PARTITION BY t.payment_mode
        ORDER BY t.txn_date
    ) AS PrevDayRevenue,

    SUM(t.total_revenue) OVER (
        PARTITION BY YEAR(t.txn_date), MONTH(t.txn_date), t.payment_mode
    ) AS MonthlyRevenue

FROM prod_catalog.gold.daily_revenue_summary t
LEFT JOIN (
    SELECT customer_id, city, tier
    FROM prod_catalog.silver.customers
    WHERE is_current = true
) c ON t.customer_id = c.customer_id  -- If applicable

-- This view is what Power BI connects to
-- Apply OPTIMIZE + ZORDER on underlying tables, not this view

```

```python
# Power BI Connection Settings:
# Home → Get Data → Azure → Azure Databricks

# Server:    adb-workspace.azuredatabricks.net
# HTTP Path: /sql/1.0/warehouses/<warehouse-id>
# (Find in SQL Warehouses → Connection Details tab)

# Authentication: Azure Active Directory (recommended)
# OR: Personal Access Token (for service accounts)

# Import vs DirectQuery:
# Import:      Data copied to Power BI — stale after refresh window
# DirectQuery: Live queries to Databricks — always fresh, slower
# For dashboards: DirectQuery on Serverless SQL Warehouse (fast + fresh)
# For large exports: Import with scheduled refresh
```

---

## 15.6 Databricks SQL — Query Optimization

```sql
-- ── USE QUERY HISTORY TO FIND SLOW QUERIES ──
-- Databricks SQL → Query History → Filter by duration DESC

-- ── CHECK QUERY PLAN ──
EXPLAIN SELECT * FROM silver.transactions WHERE customer_id = 'C001';
-- Look for: PartitionFilters, DataFilters (data skipping)

-- ── FILE STATISTICS — how many files scanned? ──
-- Enable verbose EXPLAIN:
SET spark.sql.statistics.histogram.enabled = TRUE;
ANALYZE TABLE silver.transactions COMPUTE STATISTICS FOR ALL COLUMNS;

-- After ANALYZE, the optimizer has better statistics for:
-- - Choosing join strategies (broadcast vs sort-merge)
-- - Estimating cardinality
-- - Partition pruning decisions

-- ── RESULT CACHE — avoid re-running identical queries ──
-- Databricks SQL automatically caches results for 24 hours
-- Same query = instant result from cache
-- Invalidated when the underlying Delta table changes

-- ── QUERY HINTS ──
SELECT /*+ BROADCAST(c) */
    t.*,
    c.city
FROM silver.transactions t
JOIN silver.customers c ON t.customer_id = c.customer_id;

-- ── CACHE TABLE for repeated BI queries ──
CACHE TABLE gold.daily_revenue_summary;
-- Now all queries to this table read from in-memory cache
UNCACHE TABLE gold.daily_revenue_summary;
```

---

## 15.7 Interview Questions — SQL Analytics

**Q: How do you optimize a Power BI dashboard that runs slow queries on Databricks?**
> Four-layer approach: (1) **Gold layer pre-aggregation** — push aggregations into Delta tables so Power BI queries pre-computed data, not raw records. (2) **OPTIMIZE + ZORDER** — run on underlying Delta tables using columns Power BI filters on (date, region, category). (3) **Serverless SQL Warehouse** — zero startup time, instant scale for concurrent BI users. (4) **DirectQuery with Result Cache** — Databricks caches query results for 24 hours; identical Power BI refresh queries hit the cache, not Delta. For very large models, use Import mode with incremental refresh on the Power BI side.

**Q: What is the difference between window functions and GROUP BY in SQL?**
> GROUP BY collapses rows — for each group, you get exactly one output row with aggregated values. Window functions compute across rows related to the current row WITHOUT collapsing — every input row gets an output row with the computed window value alongside all original columns. Example: GROUP BY `customer_id` gives one row per customer with `SUM(amount)`. A window function gives every transaction row its customer's cumulative spend at that point in time.

**Q: How do you find the N-th highest value in a SQL column without using LIMIT?**
> Use DENSE_RANK(): `SELECT amount FROM (SELECT amount, DENSE_RANK() OVER (ORDER BY amount DESC) AS rnk FROM t) WHERE rnk = N`. DENSE_RANK handles ties correctly — if two rows have the same value, they share the same rank and the next rank is not skipped (unlike RANK()). ROW_NUMBER() would work but assigns arbitrary tiebreaking which may return inconsistent results.

---
*Next: [Module 14 — Cheatsheets: SQL Reference](../14-Cheatsheets/02-sql-cheatsheet.md)*
