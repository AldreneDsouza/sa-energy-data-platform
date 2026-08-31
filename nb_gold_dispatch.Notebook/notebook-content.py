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
# Gold — dimensional model for dispatch analytics
# Reads:  silver_dispatch_interval
# Writes: dim_date, dim_region, fact_dispatch_interval
# =============================================================================

from pyspark.sql import functions as F, Window

silver = spark.table("silver_dispatch_interval")

print(f"Silver rows: {silver.count():,}")
print(f"Date range:  {silver.agg(F.min('settlement_date')).first()[0]} to {silver.agg(F.max('settlement_date')).first()[0]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# dim_date — one row per calendar date
# -----------------------------------------------------------------------------

date_bounds = silver.agg(
    F.min("settlement_date").alias("min_d"),
    F.max("settlement_date").alias("max_d")
).first()

dim_date = (spark.sql(f"""
    SELECT explode(sequence(
        to_date('{date_bounds['min_d']}'),
        to_date('{date_bounds['max_d']}'),
        interval 1 day
    )) AS full_date
""")
    .withColumn("date_key",      F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year",          F.year("full_date"))
    .withColumn("quarter",       F.quarter("full_date"))
    .withColumn("month",         F.month("full_date"))
    .withColumn("month_name",    F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month",  F.dayofmonth("full_date"))
    .withColumn("day_of_week",   F.dayofweek("full_date"))
    .withColumn("day_name",      F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend",    F.dayofweek("full_date").isin([1, 7]))
    .withColumn("season",
        F.when(F.month("full_date").isin([12, 1, 2]),  "Summer")
         .when(F.month("full_date").isin([3, 4, 5]),   "Autumn")
         .when(F.month("full_date").isin([6, 7, 8]),   "Winter")
         .otherwise("Spring"))
    .select("date_key", "full_date", "year", "quarter", "month", "month_name",
            "day_of_month", "day_of_week", "day_name", "is_weekend", "season")
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")

print(f"dim_date: {dim_date.count()} rows")
dim_date.orderBy("date_key").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Approximate state populations (~2024 ABS estimates, rounded).
# TODO: replace with sourced ABS data via the Azure SQL reference load.
# dim_region — the five NEM regions
# -----------------------------------------------------------------------------

region_meta = [
    ("SA1",  "South Australia",   "SA",  "Adelaide",  1_800_000),
    ("VIC1", "Victoria",          "VIC", "Melbourne", 6_800_000),
    ("NSW1", "New South Wales",   "NSW", "Sydney",    8_400_000),
    ("QLD1", "Queensland",        "QLD", "Brisbane",  5_500_000),
    ("TAS1", "Tasmania",          "TAS", "Hobart",      570_000),
]

dim_region = (spark.createDataFrame(
        region_meta,
        ["region_id", "region_name", "state_code", "capital_city", "population"])
    .withColumn("region_key", F.row_number().over(Window.orderBy("region_id")))
    .withColumn("is_focus_region", F.col("region_id") == "SA1")
    .select("region_key", "region_id", "region_name", "state_code",
            "capital_city", "population", "is_focus_region")
)

dim_region.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_region")

dim_region.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# fact_dispatch_interval — one row per region per five-minute interval
# -----------------------------------------------------------------------------

fact = (silver.alias("s")
    .join(F.broadcast(dim_region.alias("r")), F.col("s.region_id") == F.col("r.region_id"), "inner")
    .withColumn("date_key", F.date_format("s.settlement_date", "yyyyMMdd").cast("int"))
    .select(
        # keys
        "date_key",
        F.col("r.region_key").alias("region_key"),
        # degenerate dimension
        F.col("s.dispatch_interval").alias("dispatch_interval"),
        # timestamps
        F.col("s.settlement_ts_local").alias("settlement_ts"),
        F.col("s.settlement_ts_utc"),
        F.col("s.settlement_hour"),
        F.col("s.settlement_date"),
        # measures
        F.col("s.rrp").alias("spot_price"),
        F.col("s.total_demand_mw"),
        F.col("s.available_generation_mw"),
        F.col("s.demand_forecast_mw"),
        F.col("s.net_interchange_mw"),
        F.col("s.reserve_margin_mw"),
        # flags
        F.col("s.is_negative_price"),
        F.col("s.is_price_spike"),
        F.col("s.is_importing"),
    )
    .withColumn("forecast_error_mw", F.col("demand_forecast_mw") - F.col("total_demand_mw"))
    .withColumn("_gold_processed_at", F.current_timestamp())
)

(fact.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("settlement_date")
    .saveAsTable("fact_dispatch_interval"))

print(f"fact_dispatch_interval: {fact.count():,} rows")
fact.select("date_key", "region_key", "settlement_ts", "spot_price", "total_demand_mw").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        f.settlement_hour,
        ROUND(AVG(f.spot_price), 2)      AS avg_price,
        ROUND(AVG(f.total_demand_mw), 0) AS avg_demand_mw
    FROM fact_dispatch_interval f
    JOIN dim_region r ON f.region_key = r.region_key
    WHERE r.region_id = 'SA1'
    GROUP BY f.settlement_hour
    ORDER BY f.settlement_hour
""").show(24)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        CASE
            WHEN f.total_demand_mw >= 2000 THEN '1. Peak (2000+ MW)'
            WHEN f.total_demand_mw >= 1700 THEN '2. High (1700-2000)'
            WHEN f.total_demand_mw >= 1400 THEN '3. Normal (1400-1700)'
            ELSE                                '4. Low (under 1400)'
        END AS demand_band,
        COUNT(*)                                AS intervals,
        ROUND(AVG(f.spot_price), 2)             AS avg_price,
        ROUND(MAX(f.spot_price), 2)             AS max_price,
        ROUND(AVG(f.total_demand_mw), 0)        AS avg_demand_mw,
        ROUND(AVG(f.available_generation_mw), 0) AS avg_available_mw,
        ROUND(AVG(f.reserve_margin_mw), 0)      AS avg_reserve_mw,
        SUM(CASE WHEN f.reserve_margin_mw < 0 THEN 1 ELSE 0 END) AS shortfall_intervals
    FROM fact_dispatch_interval f
    JOIN dim_region r ON f.region_key = r.region_key
    WHERE r.region_id = 'SA1'
    GROUP BY 1
    ORDER BY 1
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# agg_daily_region — pre-aggregated daily summary for dashboard performance
# -----------------------------------------------------------------------------

agg_daily = (spark.table("fact_dispatch_interval")
    .groupBy("date_key", "region_key", "settlement_date")
    .agg(
        F.count("*").alias("interval_count"),
        F.round(F.avg("spot_price"), 2).alias("avg_price"),
        F.round(F.min("spot_price"), 2).alias("min_price"),
        F.round(F.max("spot_price"), 2).alias("max_price"),
        F.round(F.stddev("spot_price"), 2).alias("price_volatility"),
        F.round(F.avg("total_demand_mw"), 1).alias("avg_demand_mw"),
        F.round(F.max("total_demand_mw"), 1).alias("peak_demand_mw"),
        F.round(F.min("total_demand_mw"), 1).alias("min_demand_mw"),
        F.round(F.avg("reserve_margin_mw"), 1).alias("avg_reserve_mw"),
        F.sum(F.col("is_negative_price").cast("int")).alias("negative_intervals"),
        F.sum(F.col("is_price_spike").cast("int")).alias("spike_intervals"),
        F.sum(F.col("is_importing").cast("int")).alias("importing_intervals"),
        # energy served: MW averaged over a 5-min interval -> MWh
        F.round(F.sum(F.col("total_demand_mw") / 12.0), 1).alias("energy_mwh"),
    )
    .withColumn("negative_price_pct",
        F.round(F.col("negative_intervals") / F.col("interval_count") * 100, 1))
    .withColumn("import_dependency_pct",
        F.round(F.col("importing_intervals") / F.col("interval_count") * 100, 1))
)

(agg_daily.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("agg_daily_region"))

print(f"agg_daily_region: {agg_daily.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        d.day_name,
        d.is_weekend,
        ROUND(AVG(a.avg_price), 2)      AS avg_price,
        ROUND(AVG(a.peak_demand_mw), 0) AS avg_peak_demand,
        ROUND(AVG(a.negative_price_pct), 1) AS avg_negative_pct
    FROM agg_daily_region a
    JOIN dim_date   d ON a.date_key   = d.date_key
    JOIN dim_region r ON a.region_key = r.region_key
    WHERE r.region_id = 'SA1'
    GROUP BY d.day_name, d.day_of_week, d.is_weekend
    ORDER BY d.day_of_week
""").show(truncate=False)

spark.sql("""
    SELECT a.settlement_date,
           a.avg_price, a.max_price, a.price_volatility,
           a.peak_demand_mw, a.negative_price_pct
    FROM agg_daily_region a
    JOIN dim_region r ON a.region_key = r.region_key
    WHERE r.region_id = 'SA1'
    ORDER BY a.price_volatility DESC
    LIMIT 5
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
