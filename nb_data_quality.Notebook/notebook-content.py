# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03ae72b6-ee4d-4949-b3ab-fcf5d4af47f1",
# META       "default_lakehouse_name": "lh_sa_energy",
# META       "default_lakehouse_workspace_id": "89b811b9-5622-4b8d-a823-0d9f0b269ef4",
# META       "known_lakehouses": [
# META         {
# META           "id": "03ae72b6-ee4d-4949-b3ab-fcf5d4af47f1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =============================================================================
# Data Quality Framework
# Runs configured checks, logs every result, quarantines failures.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DoubleType, BooleanType
)
from datetime import datetime, timezone
import uuid

DQ_RESULTS_TABLE = "dq_results"

DQ_SCHEMA = StructType([
    StructField("run_id",          StringType(),    False),
    StructField("check_name",      StringType(),    False),
    StructField("check_category",  StringType(),    True),
    StructField("target_table",    StringType(),    True),
    StructField("severity",        StringType(),    True),   # CRITICAL | WARNING
    StructField("passed",          BooleanType(),   True),
    StructField("observed_value",  DoubleType(),    True),
    StructField("expected_value",  StringType(),    True),
    StructField("failed_records",  LongType(),      True),
    StructField("message",         StringType(),    True),
    StructField("checked_at",      TimestampType(), True),
])

if not spark.catalog.tableExists(DQ_RESULTS_TABLE):
    (spark.createDataFrame([], DQ_SCHEMA)
        .write.format("delta").mode("overwrite").saveAsTable(DQ_RESULTS_TABLE))
    print(f"Created {DQ_RESULTS_TABLE}")
else:
    print(f"{DQ_RESULTS_TABLE} exists")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Check runner — executes a SQL check and logs the result
# -----------------------------------------------------------------------------

def run_check(run_id, name, category, table, severity, sql, expected, evaluate):
    """
    Execute one check.
      sql      -> must return a single row with a column named 'observed'
      expected -> human-readable description of what we wanted
      evaluate -> function(observed) -> True if the check passes
    """
    try:
        observed = float(spark.sql(sql).first()["observed"] or 0)
        passed   = evaluate(observed)
        message  = "OK" if passed else f"Expected {expected}, observed {observed:,.2f}"
    except Exception as e:
        observed, passed = None, False
        message = f"Check errored: {type(e).__name__}: {e}"

    row = [(run_id, name, category, table, severity, passed,
            observed, expected, None, message, datetime.now(timezone.utc))]

    (spark.createDataFrame(row, schema=DQ_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(DQ_RESULTS_TABLE))

    flag = "PASS" if passed else ("FAIL" if severity == "CRITICAL" else "WARN")
    print(f"  [{flag}] {name}: {message}")
    return passed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Check definitions
# -----------------------------------------------------------------------------

run_id = str(uuid.uuid4())
print(f"DQ run {run_id[:8]}\n")

results = []

# 1. Completeness — expect 288 intervals per complete day
results.append(run_check(run_id,
    "silver_interval_completeness", "completeness", "silver_dispatch_interval", "CRITICAL",
    """SELECT COUNT(*) AS observed FROM (
           SELECT settlement_date
           FROM silver_dispatch_interval
           WHERE region_id = 'SA1'
             AND settlement_date < (SELECT MAX(settlement_date) FROM silver_dispatch_interval)
           GROUP BY settlement_date
           HAVING COUNT(*) <> 288)""",
    "0 incomplete days", lambda o: o == 0))

# 2. Uniqueness — no duplicate region/interval pairs
results.append(run_check(run_id,
    "silver_no_duplicates", "uniqueness", "silver_dispatch_interval", "CRITICAL",
    """SELECT COUNT(*) AS observed FROM (
           SELECT settlement_ts_aest, region_id
           FROM silver_dispatch_interval
           GROUP BY settlement_ts_aest, region_id
           HAVING COUNT(*) > 1)""",
    "0 duplicates", lambda o: o == 0))

# 3. Validity — price within market floor and cap
results.append(run_check(run_id,
    "price_within_market_bounds", "validity", "silver_dispatch_interval", "WARNING",
    """SELECT COUNT(*) AS observed FROM silver_dispatch_interval
       WHERE rrp < -1000 OR rrp > 20000""",
    "0 out-of-bounds prices", lambda o: o == 0))

# 4. Null business keys
results.append(run_check(run_id,
    "no_null_keys", "completeness", "silver_dispatch_interval", "CRITICAL",
    """SELECT COUNT(*) AS observed FROM silver_dispatch_interval
       WHERE settlement_ts_aest IS NULL OR region_id IS NULL OR rrp IS NULL""",
    "0 null keys", lambda o: o == 0))

# 5. Referential integrity — every fact row resolves to a region
results.append(run_check(run_id,
    "fact_region_integrity", "referential", "fact_dispatch_interval", "CRITICAL",
    """SELECT COUNT(*) AS observed
       FROM fact_dispatch_interval f
       LEFT JOIN dim_region r ON f.region_key = r.region_key
       WHERE r.region_key IS NULL""",
    "0 orphaned facts", lambda o: o == 0))

# 6. Freshness — hours since the latest interval
results.append(run_check(run_id,
    "data_freshness_hours", "freshness", "silver_dispatch_interval", "WARNING",
    """SELECT (unix_timestamp(current_timestamp())
              - unix_timestamp(MAX(settlement_ts_utc))) / 3600.0 AS observed
       FROM silver_dispatch_interval""",
    "< 48 hours old", lambda o: o < 48))

print(f"\n{sum(results)}/{len(results)} checks passed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Which days are incomplete?
spark.sql("""
    SELECT settlement_date, COUNT(*) AS intervals, 288 - COUNT(*) AS missing
    FROM silver_dispatch_interval
    WHERE region_id = 'SA1'
    GROUP BY settlement_date
    HAVING COUNT(*) <> 288
    ORDER BY settlement_date
""").show(truncate=False)

# Which price breached the bounds?
spark.sql("""
    SELECT settlement_ts_local, region_id, rrp
    FROM silver_dispatch_interval
    WHERE rrp < -1000 OR rrp > 20000
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Quarantine — isolate rows that fail validation
# -----------------------------------------------------------------------------

QUARANTINE_TABLE = "quarantine_dispatch_interval"

def quarantine_rows(df, reason: str, run_id: str, severity: str = "WARNING"):
    """Write failing rows to quarantine with the reason they failed."""
    if df.isEmpty():
        print(f"  No rows to quarantine for: {reason}")
        return 0

    (df.withColumn("_quarantine_reason",   F.lit(reason))
       .withColumn("_quarantine_severity", F.lit(severity))
       .withColumn("_quarantine_run_id",   F.lit(run_id))
       .withColumn("_quarantined_at",      F.current_timestamp())
       .write.format("delta").mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(QUARANTINE_TABLE))

    n = df.count()
    print(f"  Quarantined {n} rows: {reason}")
    return n


silver = spark.table("silver_dispatch_interval")

# Rule 1 — prices outside market bounds
out_of_bounds = silver.filter((F.col("rrp") < -1000) | (F.col("rrp") > 20000))
quarantine_rows(out_of_bounds, "price_outside_market_bounds", run_id, "WARNING")

# Rule 2 — null business keys
null_keys = silver.filter(
    F.col("settlement_ts_aest").isNull() | F.col("region_id").isNull() | F.col("rrp").isNull()
)
quarantine_rows(null_keys, "null_business_key", run_id, "CRITICAL")

print(f"\nQuarantine total: {spark.table(QUARANTINE_TABLE).count() if spark.catalog.tableExists(QUARANTINE_TABLE) else 0} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
