# Module 02 — PySpark
## Part 4: Production Data Quality Framework

> Bad data costs enterprises millions. A robust DQ framework catches problems early — at ingestion, not when the CFO sees a wrong number in Power BI.

---

## 4.1 The Data Quality Pyramid

```
          🏆 BUSINESS RULES
         "Revenue > 0 for completed orders"
         "Customer tier must match spend"
        ─────────────────────────────────────
       📊 STATISTICAL CHECKS
       "Amount distribution unchanged from yesterday"
       "Record count within ±20% of 7-day average"
      ───────────────────────────────────────────────
     🔗 REFERENTIAL INTEGRITY
     "customer_id exists in customers table"
     "product_id exists in products catalog"
    ─────────────────────────────────────────────────
   ✅ DOMAIN VALIDITY
   "amount > 0", "date not in future"
   "payment_mode in allowed list"
  ───────────────────────────────────────────────────
 🔢 COMPLETENESS & FORMAT
 "txn_id not null", "amount not null"
 "date matches yyyy-MM-dd pattern"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 4.2 Production DQ Framework

```python
# ── data_quality.py ──
# Reusable data quality engine for all pipelines

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List, Dict, Tuple, Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json

@dataclass
class DQRule:
    """A single data quality rule definition."""
    rule_id:     str
    rule_name:   str
    description: str
    column:      Optional[str]           # None for table-level rules
    condition:   str                     # Spark SQL expression (returns bool)
    severity:    str = "ERROR"           # ERROR, WARNING, INFO
    threshold:   float = 0.0            # Max allowed failure % (0 = zero tolerance)
    tags:        List[str] = field(default_factory=list)

@dataclass
class DQResult:
    """Result of running a single DQ rule."""
    rule_id:        str
    rule_name:      str
    severity:       str
    total_records:  int
    passed_records: int
    failed_records: int
    failure_pct:    float
    threshold:      float
    passed:         bool
    sample_failures: List[Dict]

class DataQualityEngine:
    """
    Production-grade data quality engine.
    Runs rules, generates reports, routes bad records to quarantine.
    """
    
    def __init__(self, pipeline_name: str, batch_date: str):
        self.pipeline_name = pipeline_name
        self.batch_date    = batch_date
        self.results: List[DQResult] = []
    
    def run_checks(
        self,
        df: DataFrame,
        rules: List[DQRule],
        quarantine_path: Optional[str] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Run all DQ rules against a DataFrame.
        Returns: (df_passed, df_failed)
        """
        total = df.count()
        
        # Add a combined pass/fail column for each rule
        df_checked = df
        pass_conditions = []
        
        for rule in rules:
            col_name = f"_dq_{rule.rule_id}"
            df_checked = df_checked.withColumn(col_name, expr(rule.condition))
            
            # Count failures for this rule
            failed = df_checked.filter(~col(col_name)).count()
            passed = total - failed
            failure_pct = (failed / total * 100) if total > 0 else 0
            
            # Sample up to 5 failing rows for reporting
            sample = []
            if failed > 0:
                sample = df_checked.filter(~col(col_name)) \
                    .limit(5) \
                    .toPandas() \
                    .to_dict("records")
            
            is_passed = failure_pct <= rule.threshold
            
            result = DQResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                total_records=total,
                passed_records=passed,
                failed_records=failed,
                failure_pct=round(failure_pct, 4),
                threshold=rule.threshold,
                passed=is_passed,
                sample_failures=sample
            )
            self.results.append(result)
            
            # Only ERROR rules gate the pipeline
            if rule.severity == "ERROR":
                pass_conditions.append(col(col_name))
            
            status = "✅" if is_passed else "❌"
            print(f"  {status} [{rule.severity}] {rule.rule_name}: "
                  f"{failed:,}/{total:,} failures ({failure_pct:.2f}%) "
                  f"| threshold={rule.threshold}%")
        
        # Split into passed/failed DataFrames
        dq_columns = [f"_dq_{r.rule_id}" for r in rules]
        
        all_pass_expr = reduce(lambda a, b: a & b, pass_conditions) \
            if pass_conditions else lit(True)
        
        df_passed = df_checked.filter(all_pass_expr).drop(*dq_columns)
        df_failed  = df_checked.filter(~all_pass_expr)
        
        # Add failure reason column
        fail_reason_expr = None
        for rule in rules:
            if rule.severity == "ERROR":
                condition = when(
                    ~col(f"_dq_{rule.rule_id}"),
                    lit(f"{rule.rule_id}:{rule.rule_name}")
                )
                fail_reason_expr = condition if fail_reason_expr is None \
                    else fail_reason_expr.when(~col(f"_dq_{rule.rule_id}"), 
                                               lit(f"{rule.rule_id}:{rule.rule_name}"))
        
        if fail_reason_expr and df_failed.count() > 0:
            df_failed = df_failed \
                .withColumn("_dq_failure_reason", fail_reason_expr.otherwise(lit("UNKNOWN"))) \
                .withColumn("_dq_quarantined_at", current_timestamp()) \
                .withColumn("_dq_pipeline",        lit(self.pipeline_name)) \
                .withColumn("_dq_batch_date",      lit(self.batch_date)) \
                .drop(*dq_columns)
            
            # Write to quarantine
            if quarantine_path and df_failed.count() > 0:
                df_failed.write.format("delta").mode("append") \
                    .partitionBy("_dq_batch_date") \
                    .option("mergeSchema", "true") \
                    .save(quarantine_path)
                print(f"  📤 {df_failed.count():,} records quarantined → {quarantine_path}")
        
        return df_passed, df_failed
    
    def summary(self) -> Dict:
        """Generate DQ summary report."""
        total_rules    = len(self.results)
        failed_errors  = [r for r in self.results if not r.passed and r.severity == "ERROR"]
        failed_warnings= [r for r in self.results if not r.passed and r.severity == "WARNING"]
        
        summary = {
            "pipeline":       self.pipeline_name,
            "batch_date":     self.batch_date,
            "run_timestamp":  str(datetime.utcnow()),
            "total_rules":    total_rules,
            "passed_rules":   len([r for r in self.results if r.passed]),
            "failed_errors":  len(failed_errors),
            "failed_warnings":len(failed_warnings),
            "overall_status": "FAILED" if failed_errors else ("WARNING" if failed_warnings else "PASSED"),
            "rules": [
                {
                    "rule_id":       r.rule_id,
                    "rule_name":     r.rule_name,
                    "severity":      r.severity,
                    "failed_records":r.failed_records,
                    "failure_pct":   r.failure_pct,
                    "status":        "PASSED" if r.passed else "FAILED"
                }
                for r in self.results
            ]
        }
        
        print("\n" + "="*60)
        print(f"DQ REPORT: {self.pipeline_name} | {self.batch_date}")
        print(f"Overall: {summary['overall_status']}")
        print(f"Rules: {summary['passed_rules']}/{total_rules} passed")
        if failed_errors:
            print(f"❌ ERROR rules failed: {[r.rule_id for r in failed_errors]}")
        if failed_warnings:
            print(f"⚠️  WARNING rules failed: {[r.rule_id for r in failed_warnings]}")
        print("="*60)
        
        return summary
    
    def save_report(self, report_table: str):
        """Persist DQ report to a Delta table for monitoring dashboards."""
        summary = self.summary()
        report_df = spark.createDataFrame([{
            "pipeline_name":   summary["pipeline"],
            "batch_date":      summary["batch_date"],
            "run_timestamp":   summary["run_timestamp"],
            "overall_status":  summary["overall_status"],
            "total_rules":     summary["total_rules"],
            "passed_rules":    summary["passed_rules"],
            "failed_errors":   summary["failed_errors"],
            "failed_warnings": summary["failed_warnings"],
            "rules_detail":    json.dumps(summary["rules"])
        }])
        
        report_df.write.format("delta").mode("append").saveAsTable(report_table)
        return summary
```

---

## 4.3 Rule Definitions — Real Production Rules

```python
# ── rules/transaction_rules.py ──

TRANSACTION_DQ_RULES = [
    # ── COMPLETENESS ──
    DQRule(
        rule_id="TXN_001",
        rule_name="transaction_id_not_null",
        description="Every transaction must have an ID",
        column="txn_id",
        condition="txn_id IS NOT NULL",
        severity="ERROR",
        threshold=0.0   # Zero tolerance
    ),
    DQRule(
        rule_id="TXN_002",
        rule_name="amount_not_null",
        description="Transaction amount cannot be null",
        column="amount",
        condition="amount IS NOT NULL",
        severity="ERROR",
        threshold=0.0
    ),
    DQRule(
        rule_id="TXN_003",
        rule_name="customer_id_not_null",
        description="Every transaction must have a customer",
        column="customer_id",
        condition="customer_id IS NOT NULL",
        severity="ERROR",
        threshold=0.5   # Allow 0.5% null customer IDs (legacy records)
    ),
    
    # ── DOMAIN VALIDITY ──
    DQRule(
        rule_id="TXN_010",
        rule_name="amount_positive",
        description="Transaction amount must be greater than zero",
        column="amount",
        condition="amount > 0",
        severity="ERROR",
        threshold=0.0
    ),
    DQRule(
        rule_id="TXN_011",
        rule_name="amount_reasonable",
        description="Transaction amount must be less than 10 crore INR",
        column="amount",
        condition="amount < 100000000",
        severity="WARNING",
        threshold=0.1   # Flag but don't block — legitimate large transactions exist
    ),
    DQRule(
        rule_id="TXN_012",
        rule_name="payment_mode_valid",
        description="Payment mode must be from approved list",
        column="payment_mode",
        condition="payment_mode IN ('CARD','UPI','NETBANKING','NEFT','RTGS','IMPS','WALLET','CASH')",
        severity="ERROR",
        threshold=1.0   # Allow 1% — legacy systems may send old codes
    ),
    DQRule(
        rule_id="TXN_013",
        rule_name="date_not_future",
        description="Transaction date cannot be in the future",
        column="txn_date",
        condition="txn_date <= current_date()",
        severity="ERROR",
        threshold=0.0
    ),
    DQRule(
        rule_id="TXN_014",
        rule_name="date_not_too_old",
        description="Transaction date should not be more than 2 years old",
        column="txn_date",
        condition="txn_date >= add_months(current_date(), -24)",
        severity="WARNING",
        threshold=5.0
    ),
    
    # ── FORMAT VALIDATION ──
    DQRule(
        rule_id="TXN_020",
        rule_name="customer_id_format",
        description="Customer ID must match pattern CUSTXXXXXXX",
        column="customer_id",
        condition="customer_id RLIKE '^(CUST|C)[0-9]{3,10}$'",
        severity="ERROR",
        threshold=2.0
    ),
    
    # ── BUSINESS RULES ──
    DQRule(
        rule_id="TXN_030",
        rule_name="card_txn_has_merchant",
        description="Card transactions must have a merchant ID",
        column="merchant_id",
        condition="NOT (payment_mode = 'CARD' AND merchant_id IS NULL)",
        severity="WARNING",
        threshold=5.0
    ),
    DQRule(
        rule_id="TXN_031",
        rule_name="status_transition_valid",
        description="Cancelled transactions must have amount = 0 or negative",
        column="status",
        condition="NOT (status = 'CANCELLED' AND amount > 0)",
        severity="WARNING",
        threshold=0.0
    ),
]
```

---

## 4.4 Statistical / Volume Checks

```python
class StatisticalDQChecks:
    """
    Volume and distribution checks that compare today vs historical baseline.
    Catches issues like: data feed stopped, duplicates exploded, amounts shifted.
    """
    
    def __init__(self, history_table: str, lookback_days: int = 7):
        self.history_table = history_table
        self.lookback_days = lookback_days
    
    def check_volume_anomaly(
        self, 
        df_today: DataFrame, 
        batch_date: str,
        tolerance_pct: float = 30.0
    ) -> bool:
        """Alert if today's record count is ±30% from 7-day average."""
        
        today_count = df_today.count()
        
        historical_avg = spark.sql(f"""
            SELECT AVG(record_count) AS avg_count
            FROM {self.history_table}
            WHERE batch_date >= date_sub('{batch_date}', {self.lookback_days})
            AND batch_date < '{batch_date}'
        """).first()["avg_count"]
        
        if historical_avg is None:
            print("⚠️  No historical baseline — skipping volume check")
            return True
        
        deviation_pct = abs(today_count - historical_avg) / historical_avg * 100
        
        print(f"📊 Volume Check:")
        print(f"   Today:          {today_count:,}")
        print(f"   7-day average:  {historical_avg:,.0f}")
        print(f"   Deviation:      {deviation_pct:.1f}%")
        
        if deviation_pct > tolerance_pct:
            print(f"   ❌ ANOMALY: {deviation_pct:.1f}% deviation exceeds {tolerance_pct}% threshold!")
            return False
        
        print(f"   ✅ Within tolerance ({deviation_pct:.1f}% < {tolerance_pct}%)")
        return True
    
    def check_null_rate_anomaly(
        self,
        df_today: DataFrame,
        columns_to_check: List[str],
        max_null_rate_pct: float = 5.0
    ) -> Dict[str, bool]:
        """Check null rates haven't spiked vs yesterday."""
        
        total = df_today.count()
        results = {}
        
        for col_name in columns_to_check:
            null_count  = df_today.filter(col(col_name).isNull()).count()
            null_rate   = null_count / total * 100 if total > 0 else 0
            is_ok       = null_rate <= max_null_rate_pct
            
            status = "✅" if is_ok else "❌"
            print(f"  {status} {col_name}: {null_rate:.2f}% null ({null_count:,}/{total:,})")
            results[col_name] = is_ok
        
        return results
    
    def check_amount_distribution(
        self,
        df_today: DataFrame,
        batch_date: str
    ) -> bool:
        """Compare today's amount percentiles vs historical. Detects data corruption."""
        
        today_stats = df_today.select(
            percentile_approx("amount", 0.25).alias("p25"),
            percentile_approx("amount", 0.50).alias("p50"),
            percentile_approx("amount", 0.75).alias("p75"),
            percentile_approx("amount", 0.95).alias("p95"),
        ).first()
        
        print(f"📊 Amount Distribution (today):")
        print(f"   P25: ₹{today_stats['p25']:,.2f}")
        print(f"   P50: ₹{today_stats['p50']:,.2f}")
        print(f"   P75: ₹{today_stats['p75']:,.2f}")
        print(f"   P95: ₹{today_stats['p95']:,.2f}")
        
        # Compare with yesterday's stored stats
        # (In production: load from dq_stats_history table and compare)
        return True
```

---

## 4.5 Complete Pipeline with DQ Integration

```python
# ── 02_transform_silver_with_dq.py ──

%run ./utils/config
%run ./utils/data_quality     # Imports DataQualityEngine, DQRule classes

from pyspark.sql.functions import *

# 1. Read Bronze
df_bronze = spark.read.format("delta").load(BRONZE_PATH) \
    .filter(col("_batch_date") == BATCH_DATE)

# 2. Type casting
df_typed = df_bronze \
    .withColumn("txn_id",      col("txn_id").cast("bigint")) \
    .withColumn("txn_date",    to_date(col("txn_date"), "yyyy-MM-dd")) \
    .withColumn("amount",      regexp_replace(col("amount"), "[,$₹]","").cast("double")) \
    .withColumn("customer_id", upper(trim(col("customer_id"))))

# 3. Run DQ checks
dq = DataQualityEngine(
    pipeline_name="daily-transaction-pipeline",
    batch_date=BATCH_DATE
)

df_passed, df_failed = dq.run_checks(
    df=df_typed,
    rules=TRANSACTION_DQ_RULES,
    quarantine_path="/mnt/quarantine/transactions/"
)

# 4. Volume check
stat_checks = StatisticalDQChecks(history_table="metadata.dq_volume_history")
volume_ok   = stat_checks.check_volume_anomaly(df_passed, BATCH_DATE)

# 5. Save DQ report
dq_summary = dq.save_report("metadata.dq_run_reports")

# 6. Decide whether to proceed or halt
if dq_summary["overall_status"] == "FAILED" or not volume_ok:
    error_msg = f"DQ checks failed for {BATCH_DATE}. Pipeline halted."
    print(f"❌ {error_msg}")
    
    # Send alert
    send_teams_alert(
        webhook_url=dbutils.secrets.get("kv-prod", "teams-webhook"),
        title=f"⚠️ DQ Failure: {BATCH_DATE}",
        message=f"Pipeline halted.\nFailed rules: {dq_summary['failed_errors']}\n"
                f"Volume OK: {volume_ok}"
    )
    dbutils.notebook.exit(f"FAILED:DQ_CHECKS")
    raise ValueError(error_msg)

# 7. Continue with clean data
print(f"\n✅ DQ passed. Proceeding with {df_passed.count():,} clean records.")
print(f"   Quarantined: {df_failed.count():,} records")

# Write clean data to Silver
from delta.tables import DeltaTable
target = DeltaTable.forPath(spark, SILVER_PATH)

(target.alias("tgt")
 .merge(df_passed.alias("src"), "tgt.txn_id = src.txn_id AND tgt.txn_date = src.txn_date")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

---

## 4.6 DQ Monitoring Dashboard (SQL)

```sql
-- Query the DQ report table to build a monitoring view
-- In Databricks SQL or Power BI

-- Daily DQ pass rate trend
SELECT
    batch_date,
    overall_status,
    total_rules,
    passed_rules,
    failed_errors,
    ROUND(passed_rules * 100.0 / total_rules, 1) AS pass_rate_pct
FROM metadata.dq_run_reports
WHERE pipeline_name = 'daily-transaction-pipeline'
ORDER BY batch_date DESC
LIMIT 30;

-- Most frequently failing rules
SELECT
    json_extract_scalar(rule_detail, '$.rule_id')   AS rule_id,
    json_extract_scalar(rule_detail, '$.rule_name') AS rule_name,
    COUNT(*)  AS failure_count,
    AVG(CAST(json_extract_scalar(rule_detail,'$.failed_records') AS LONG)) AS avg_failures
FROM metadata.dq_run_reports
LATERAL VIEW explode(from_json(rules_detail, 'ARRAY<STRUCT<rule_id:STRING,rule_name:STRING,failed_records:LONG,status:STRING>>')) t AS rule_detail
WHERE json_extract_scalar(rule_detail, '$.status') = 'FAILED'
GROUP BY 1, 2
ORDER BY failure_count DESC;

-- Quarantine analysis — what types of bad records are we getting?
SELECT
    _dq_failure_reason,
    _dq_batch_date,
    COUNT(*) AS record_count,
    SUM(CAST(amount AS DOUBLE)) AS total_bad_amount
FROM quarantine.transactions
GROUP BY 1, 2
ORDER BY _dq_batch_date DESC, record_count DESC;
```

---

## 4.7 Interview Questions — Data Quality

**Q: How do you design a DQ framework that doesn't block the pipeline for every minor issue?**
> Three-tier severity model: (1) **ERROR** — zero tolerance, blocks pipeline, bad records to quarantine (e.g., null txn_id). (2) **WARNING** — threshold-based, pipeline continues but alerts team (e.g., 5% null merchant_id). (3) **INFO** — logged only, no action (e.g., round amounts). The threshold field per rule defines the allowed failure percentage. This way a few bad records from a legacy source don't halt a 500K-record daily job.

**Q: How do you detect if a data feed has silently stopped or reduced?**
> Volume anomaly detection: compare today's record count against the N-day rolling average. If deviation exceeds ±30% (configurable), trigger an alert. Also track source-file arrival time: if the landing zone doesn't have files by 1AM when they're expected at midnight, alert before even starting the pipeline. Both checks prevent the "we processed 0 records and wrote nothing to Gold" silent failure scenario.

---
*Next: [Module 03 — Delta Lake: Delta Live Tables](../03-Delta-Lake/02-delta-live-tables.md)*
