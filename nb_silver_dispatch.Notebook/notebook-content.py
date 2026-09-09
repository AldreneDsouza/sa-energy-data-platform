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
# Silver — clean, type, deduplicate and conform dispatch data
# Reads:  bronze_dispatch_price, bronze_dispatch_regionsum
# Writes: silver_dispatch_interval
# =============================================================================

from pyspark.sql import functions as F, Window

SA_REGION      = "SA1"
MARKET_FLOOR   = -1000.0
MARKET_CAP     = 20000.0
SOURCE_TZ      = "Australia/Brisbane"   # AEST, no daylight saving
LOCAL_TZ       = "Australia/Adelaide"   # ACST/ACDT

print("Silver config loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Price: cast types, convert timezone, deduplicate
# -----------------------------------------------------------------------------

bronze_price = spark.table("bronze_dispatch_price")

price = (bronze_price
    .select(
        F.to_timestamp("SETTLEMENTDATE", "yyyy/MM/dd HH:mm:ss").alias("settlement_ts_aest"),
        F.col("REGIONID").alias("region_id"),
        F.col("DISPATCHINTERVAL").cast("string").alias("dispatch_interval"),
        F.col("RRP").cast("double").alias("rrp"),
        F.col("INTERVENTION").cast("int").alias("intervention"),
        F.to_timestamp("LASTCHANGED", "yyyy/MM/dd HH:mm:ss").alias("last_changed"),
        F.col("_source_file"),
        F.col("_ingest_timestamp"),
    )
    .filter(F.col("intervention") == 0)          # exclude intervention runs
)

print(f"After type cast and intervention filter: {price.count():,} rows")

# Deduplicate: one row per region per interval, keeping the latest revision
w = Window.partitionBy("settlement_ts_aest", "region_id").orderBy(F.col("last_changed").desc())

price_dedup = (price
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"After deduplication: {price_dedup.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Timezone: AEMO publishes AEST year-round; SA observes ACST/ACDT
# -----------------------------------------------------------------------------

price_tz = (price_dedup
    .withColumn("settlement_ts_utc",
        F.to_utc_timestamp(F.col("settlement_ts_aest"), SOURCE_TZ))
    .withColumn("settlement_ts_local",
        F.from_utc_timestamp(F.col("settlement_ts_utc"), LOCAL_TZ))
    .withColumn("settlement_date", F.to_date("settlement_ts_local"))
    .withColumn("settlement_hour", F.hour("settlement_ts_local"))
)

price_tz.select(
    "settlement_ts_aest", "settlement_ts_utc", "settlement_ts_local", "region_id", "rrp"
).filter(F.col("region_id") == SA_REGION).orderBy(F.col("settlement_ts_aest").desc()).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Regionsum: demand and available generation
# -----------------------------------------------------------------------------

bronze_regionsum = spark.table("bronze_dispatch_regionsum")

demand = (bronze_regionsum
    .select(
        F.to_timestamp("SETTLEMENTDATE", "yyyy/MM/dd HH:mm:ss").alias("settlement_ts_aest"),
        F.col("REGIONID").alias("region_id"),
        F.col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
        F.col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
        F.col("DEMANDFORECAST").cast("double").alias("demand_forecast_mw"),
        F.col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
        F.col("INTERVENTION").cast("int").alias("intervention"),
        F.to_timestamp("LASTCHANGED", "yyyy/MM/dd HH:mm:ss").alias("last_changed"),
    )
    .filter(F.col("intervention") == 0)
)

wd = Window.partitionBy("settlement_ts_aest", "region_id").orderBy(F.col("last_changed").desc())

demand_dedup = (demand
    .withColumn("_rn", F.row_number().over(wd))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "intervention", "last_changed")
)

print(f"Demand rows after dedup: {demand_dedup.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Join price and demand into one conformed fact, then write Silver
# -----------------------------------------------------------------------------

silver = (price_tz.alias("p")
    .join(
        demand_dedup.alias("d"),
        on=["settlement_ts_aest", "region_id"],
        how="inner"
    )
    .select(
        "settlement_ts_aest",
        "settlement_ts_utc",
        "settlement_ts_local",
        "settlement_date",
        "settlement_hour",
        "region_id",
        "dispatch_interval",
        "rrp",
        "total_demand_mw",
        "available_generation_mw",
        "demand_forecast_mw",
        "net_interchange_mw",
        F.col("p._source_file").alias("source_file"),
        F.col("p._ingest_timestamp").alias("ingest_timestamp"),
    )
    .withColumn("reserve_margin_mw",
        F.col("available_generation_mw") - F.col("total_demand_mw"))
    .withColumn("is_negative_price", F.col("rrp") < 0)
    .withColumn("is_price_spike",    F.col("rrp") > 300)
    .withColumn("is_importing",      F.col("net_interchange_mw") < 0)
    .withColumn("_silver_processed_at", F.current_timestamp())
)

print(f"Silver rows: {silver.count():,}")

(silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("settlement_date")
    .saveAsTable("silver_dispatch_interval"))

print("Written to silver_dispatch_interval")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    SELECT
        COUNT(*)                                        AS intervals,
        ROUND(AVG(rrp), 2)                              AS avg_price,
        ROUND(MIN(rrp), 2)                              AS min_price,
        ROUND(MAX(rrp), 2)                              AS max_price,
        SUM(CASE WHEN is_negative_price THEN 1 ELSE 0 END) AS negative_intervals,
        SUM(CASE WHEN is_price_spike    THEN 1 ELSE 0 END) AS spike_intervals,
        SUM(CASE WHEN is_importing      THEN 1 ELSE 0 END) AS importing_intervals,
        ROUND(AVG(total_demand_mw), 0)                  AS avg_demand_mw,
        ROUND(MAX(total_demand_mw), 0)                  AS peak_demand_mw
    FROM silver_dispatch_interval
    WHERE region_id = '{SA_REGION}'
""").show(truncate=False)

spark.sql(f"""
    SELECT settlement_hour,
           ROUND(AVG(rrp), 2) AS avg_price,
           SUM(CASE WHEN is_negative_price THEN 1 ELSE 0 END) AS negative_count
    FROM silver_dispatch_interval
    WHERE region_id = '{SA_REGION}'
    GROUP BY settlement_hour
    ORDER BY settlement_hour
""").show(24)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
