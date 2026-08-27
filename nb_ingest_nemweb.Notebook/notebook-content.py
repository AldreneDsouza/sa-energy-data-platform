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
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "03ae72b6-ee4d-4949-b3ab-fcf5d4af47f1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://nemweb.com.au"
DIR_PATH = "/Reports/CURRENT/DispatchIS_Reports/"

# Fetch the directory listing
response = requests.get(BASE_URL + DIR_PATH, timeout=30)
response.raise_for_status()

# Parse out the .zip links
soup = BeautifulSoup(response.text, "html.parser")
zip_links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".zip")]

print(f"Found {len(zip_links)} zip files")
print(f"Earliest: {zip_links[0]}")
print(f"Latest:   {zip_links[-1]}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from datetime import datetime, timezone

latest = zip_links[-1]
filename = os.path.basename(latest)

# Build the full URL (hrefs may be relative or absolute)
file_url = latest if latest.startswith("http") else BASE_URL + latest

print(f"Downloading: {filename}")

resp = requests.get(file_url, timeout=60)
resp.raise_for_status()

# Partition the landing zone by ingest date
ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
target_dir = f"/lakehouse/default/Files/raw/nemweb/dispatchis/ingest_date={ingest_date}"
os.makedirs(target_dir, exist_ok=True)

target_path = f"{target_dir}/{filename}"
with open(target_path, "wb") as f:
    f.write(resp.content)

print(f"Saved to: {target_path}")
print(f"Size: {len(resp.content):,} bytes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import zipfile

with zipfile.ZipFile(target_path, "r") as z:
    print("Contents:", z.namelist())
    csv_name = z.namelist()[0]
    with z.open(csv_name) as f:
        content = f.read().decode("utf-8")

lines = content.splitlines()
print(f"\nTotal lines: {len(lines)}\n")

# Show every row that isn't a data row — these define the structure
for i, line in enumerate(lines):
    if not line.startswith("D,"):
        print(f"{i}: {line[:160]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find the PRICE schema declaration and its data rows
price_schema = None
price_rows = []

for line in lines:
    parts = line.split(",")
    if line.startswith("I,DISPATCH,PRICE"):
        price_schema = parts[4:]          # columns start after I,DISPATCH,PRICE,version
    elif line.startswith("D,DISPATCH,PRICE"):
        price_rows.append(parts[4:])

print(f"Columns ({len(price_schema)}): {price_schema[:12]}\n")

# Show SA1
for row in price_rows:
    record = dict(zip(price_schema, row))
    if record.get("REGIONID") == "SA1":
        for k in ["SETTLEMENTDATE", "REGIONID", "DISPATCHINTERVAL", "RRP", "LASTCHANGED"]:
            print(f"  {k:20} = {record.get(k)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
import io
from collections import defaultdict

def parse_nemweb_csv(content: str) -> dict:
    """
    Parse an AEMO NEMWeb CSV into {table_name: {"version": int, "columns": [...], "rows": [[...]]}}.

    Row types:
      C = comment/header/footer
      I = schema declaration for the D rows that follow
      D = data row
      F = footer with record count
    """
    tables = {}
    reader = csv.reader(io.StringIO(content))

    for parts in reader:
        if not parts:
            continue
        row_type = parts[0]

        if row_type == "I":
            table_name = f"{parts[1]}_{parts[2]}"
            tables[table_name] = {
                "version": int(parts[3]),
                "columns": parts[4:],
                "rows": []
            }

        elif row_type == "D":
            table_name = f"{parts[1]}_{parts[2]}"
            if table_name in tables:
                tables[table_name]["rows"].append(parts[4:])

    return tables


# Parse the file we downloaded
tables = parse_nemweb_csv(content)

for name, t in tables.items():
    print(f"{name:35} v{t['version']:<3} {len(t['columns']):>3} cols  {len(t['rows']):>5} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
import uuid

# Lineage metadata — every Bronze row carries where it came from
run_id = str(uuid.uuid4())
source_file = filename

def to_bronze_df(table_name: str, table: dict):
    """Build a Spark DataFrame with all columns as strings, plus lineage columns."""
    schema = StructType([StructField(c, StringType(), True) for c in table["columns"]])
    df = spark.createDataFrame(table["rows"], schema=schema)
    return (df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_schema_version", F.lit(table["version"]))
        .withColumn("_ingest_run_id", F.lit(run_id))
        .withColumn("_ingest_timestamp", F.current_timestamp())
    )

df_price = to_bronze_df("DISPATCH_PRICE", tables["DISPATCH_PRICE"])
df_price.select("SETTLEMENTDATE", "REGIONID", "RRP", "_source_file", "_schema_version").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name, table in tables.items():
    if not table["rows"]:
        print(f"{table_name:35} skipped (no rows)")
        continue

    df = to_bronze_df(table_name, table)
    delta_name = f"bronze_{table_name.lower()}"

    df.write.format("delta").mode("append").saveAsTable(delta_name)
    print(f"{table_name:35} -> {delta_name:40} {df.count():>5} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT REGIONID, SETTLEMENTDATE, RRP, _schema_version, _ingest_timestamp
# MAGIC FROM bronze_dispatch_price
# MAGIC ORDER BY REGIONID

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
