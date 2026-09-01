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
# NEMWeb Ingestion — AEMO National Electricity Market dispatch data
# Source: https://nemweb.com.au
# Writes: bronze_dispatch_* Delta tables, ctl_ingested_files control table
# =============================================================================

import csv
import io
import os
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

# --- Source configuration ----------------------------------------------------
BASE_URL      = "https://nemweb.com.au"
CURRENT_PATH  = "/Reports/CURRENT/DispatchIS_Reports/"
ARCHIVE_PATH  = "/Reports/ARCHIVE/DispatchIS_Reports/"

# --- Lakehouse paths --------------------------------------------


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Parsing
# =============================================================================

def parse_nemweb_csv(content: str) -> dict:
    """
    Parse an AEMO NEMWeb CSV into {table_name: {version, columns, rows}}.

    Row types:
      C = comment (header/footer)
      I = schema declaration for the D rows that follow
      D = data row
      F = footer with record count

    One file interleaves several logical tables; each I row starts a new one.
    """
    tables = {}
    for parts in csv.reader(io.StringIO(content)):
        if not parts:
            continue

        if parts[0] == "I":
            name = f"{parts[1]}_{parts[2]}"
            tables[name] = {
                "version": int(parts[3]),
                "columns": parts[4:],
                "rows": [],
            }

        elif parts[0] == "D":
            name = f"{parts[1]}_{parts[2]}"
            if name in tables:                      # guard: D before its I
                tables[name]["rows"].append(parts[4:])

    return tables


def list_zip_links(path: str) -> list:
    """Return every .zip href from a NEMWeb directory listing."""
    resp = requests.get(BASE_URL + path, timeout=TIMEOUT_LISTING)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    return [
        a["href"] for a in soup.find_all("a", href=True)
        if a["href"].lower().endswith(".zip")
    ]


def absolute_url(href: str) -> str:
    """NEMWeb hrefs may be relative or absolute."""
    return href if href.startswith("http") else BASE_URL + href

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Control table — tracks which files have been ingested (the watermark)
# =============================================================================

def ensure_control_table():
    """Create the control table if absent. Safe to run repeatedly."""
    if not spark.catalog.tableExists(CONTROL_TABLE):
        (spark.createDataFrame([], CONTROL_SCHEMA)
            .write.format("delta").mode("overwrite").saveAsTable(CONTROL_TABLE))
        print(f"Created {CONTROL_TABLE}")
    else:
        print(f"{CONTROL_TABLE} exists")


def get_ingested(source_name: str) -> set:
    """Filenames already ingested successfully for this source."""
    return set(
        spark.table(CONTROL_TABLE)
            .filter((F.col("source_name") == source_name) & (F.col("status") == "SUCCESS"))
            .select("file_name")
            .toPandas()["file_name"]
            .tolist()
    )


def get_pending(source_name: str, available: list) -> list:
    """Available files minus those already ingested. This is the watermark."""
    done = get_ingested(source_name)
    pending = [f for f in available if os.path.basename(f) not in done]
    print(f"{source_name}: {len(available)} available, {len(done)} done, {len(pending)} pending")
    return pending


def log_ingestion(source_name: str, file_name: str, size: int, run_id: str, status: str):
    """Record the outcome. Written on both success and failure."""
    (spark.createDataFrame(
        [(source_name, file_name, size, run_id, datetime.now(timezone.utc), status)],
        schema=CONTROL_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(CONTROL_TABLE))


ensure_control_table()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("BASE_URL" in dir())
print(BASE_URL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RAW_ROOT      = "/lakehouse/default/Files/raw/nemweb/dispatchis"
CONTROL_TABLE = "ctl_ingested_files"
BRONZE_PREFIX = "bronze_"

TIMEOUT_LISTING = 30
TIMEOUT_FILE    = 300

CURRENT_PATH  = "/Reports/CURRENT/DispatchIS_Reports/"
ARCHIVE_PATH  = "/Reports/ARCHIVE/DispatchIS_Reports/"

CONTROL_SCHEMA = StructType([
    StructField("source_name",     StringType(),    True),
    StructField("file_name",       StringType(),    True),
    StructField("file_size_bytes", LongType(),      True),
    StructField("ingest_run_id",   StringType(),    True),
    StructField("ingested_at",     TimestampType(), True),
    StructField("status",          StringType(),    True),
])

print("Config complete:", CONTROL_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Archive ingestion — one daily bundle contains ~288 five-minute files.
# Parse all of them in memory, then write once per table.
# =============================================================================

SOURCE_ARCHIVE = "nemweb_dispatchis_archive"


def process_archive_day(href: str) -> dict:
    """Download one daily bundle and parse every inner file into memory."""
    file_name = os.path.basename(href)
    resp = requests.get(absolute_url(href), timeout=TIMEOUT_FILE)
    resp.raise_for_status()

    collected = defaultdict(lambda: {"columns": None, "version": None, "rows": []})

    with zipfile.ZipFile(io.BytesIO(resp.content)) as outer:
        for inner_name in outer.namelist():
            with zipfile.ZipFile(io.BytesIO(outer.read(inner_name))) as inner:
                body = inner.read(inner.namelist()[0]).decode("utf-8")

            for tname, t in parse_nemweb_csv(body).items():
                if not t["rows"]:
                    continue
                slot = collected[tname]
                if slot["columns"] is None:
                    slot["columns"] = t["columns"]
                    slot["version"] = t["version"]
                # skip files whose schema differs mid-bundle
                if len(t["columns"]) == len(slot["columns"]):
                    slot["rows"].extend(r + [inner_name] for r in t["rows"])

    return {"file_name": file_name, "size": len(resp.content), "tables": dict(collected)}


def write_day_to_bronze(day: dict, run_id: str) -> dict:
    """One write per table per day — not per file. This is the optimisation."""
    written = {}
    for tname, t in day["tables"].items():
        if not t["rows"]:
            continue
        cols   = t["columns"] + ["_source_file"]
        schema = StructType([StructField(c, StringType(), True) for c in cols])

        (spark.createDataFrame(t["rows"], schema=schema)
            .withColumn("_schema_version",   F.lit(t["version"]))
            .withColumn("_ingest_run_id",    F.lit(run_id))
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_archive_file",     F.lit(day["file_name"]))
            .write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(f"{BRONZE_PREFIX}{tname.lower()}"))

        written[tname] = len(t["rows"])
    return written


def ingest_archive_day(href: str) -> bool:
    """Download, parse, write, log. Errors are caught so one bad day can't stop a backfill."""
    file_name = os.path.basename(href)
    run_id    = str(uuid.uuid4())
    size      = None
    try:
        day  = process_archive_day(href)
        size = day["size"]
        write_day_to_bronze(day, run_id)
        status = "SUCCESS"
    except Exception as e:
        print(f"  FAILED {file_name}: {type(e).__name__}: {e}")
        status = "FAILED"

    log_ingestion(SOURCE_ARCHIVE, file_name, size, run_id, status)
    return status == "SUCCESS"


print("Archive functions defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Execution
# =============================================================================

archive_links = list_zip_links(ARCHIVE_PATH)
pending = get_pending(SOURCE_ARCHIVE, archive_links)

# Measure a single day before committing to the full backfill
if pending:
    target = pending[-1]
    print(f"\nIngesting {os.path.basename(target)}")

    t0 = time.time()
    ok = ingest_archive_day(target)
    elapsed = time.time() - t0

    print(f"{'SUCCESS' if ok else 'FAILED'} in {elapsed:.1f}s")
    print(f"288 files in {elapsed:.1f}s = {288/elapsed:.1f} files/s")
    print(f"Old approach: 0.1 files/s -> {(288/elapsed)/0.1:.0f}x faster")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BACKFILL_DAYS = 60

batch = get_pending(SOURCE_ARCHIVE, archive_links)[-BACKFILL_DAYS:]
print(f"\nBackfilling {len(batch)} days (~{len(batch)*36/60:.0f} min)\n")

start = time.time()
ok = fail = 0

for i, href in enumerate(batch, 1):
    if ingest_archive_day(href):
        ok += 1
    else:
        fail += 1

    if i % 10 == 0:
        el = time.time() - start
        print(f"{i}/{len(batch)}  ok={ok} fail={fail}  ~{(len(batch)-i)*(el/i)/60:.0f} min left")

print(f"\nDone in {(time.time()-start)/60:.1f} min. Success {ok}, failed {fail}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        MIN(SETTLEMENTDATE) AS first_interval,
        MAX(SETTLEMENTDATE) AS last_interval,
        COUNT(*)                      AS total_rows,
        COUNT(DISTINCT SETTLEMENTDATE) AS intervals
    FROM bronze_dispatch_price
""").show(truncate=False)

spark.sql("""
    SELECT
        REGIONID,
        COUNT(*)                          AS rows,
        ROUND(MIN(CAST(RRP AS DOUBLE)), 2) AS min_rrp,
        ROUND(AVG(CAST(RRP AS DOUBLE)), 2) AS avg_rrp,
        ROUND(MAX(CAST(RRP AS DOUBLE)), 2) AS max_rrp
    FROM bronze_dispatch_price
    GROUP BY REGIONID
    ORDER BY REGIONID
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
