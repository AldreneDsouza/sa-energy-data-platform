# SA Energy Data Platform

**An end-to-end data platform for South Australian electricity market analytics, built on Microsoft Fabric.**

Ingests five-minute dispatch data from AEMO's National Electricity Market, transforms it through a
medallion architecture, models it into a star schema, validates it with an automated data quality
framework, and serves it to Power BI — orchestrated on a daily schedule.

---

## The problem

South Australia runs the most renewable-heavy grid in Australia's National Electricity Market. The
consequences show up in the wholesale price:

- **16.6%** of dispatch intervals had a **negative** spot price — generators paying to supply
- Prices ranged from **−$496.77** to **$20,300.00** per MWh across the period analysed
- South Australia relied on **imports from Victoria in 66.6%** of intervals

Anyone operating in that market — a network business planning capacity, a retailer managing hedge
exposure, an analyst forecasting demand — needs interval-level price and demand history joined with
regional context. AEMO publishes the underlying data publicly, but:

- It arrives every five minutes in a **proprietary row-typed format**, not standard CSV
- Timestamps are **AEST year-round**, while South Australia observes ACST/ACDT
- Data is **revised after publication**, so naive appends duplicate
- The `CURRENT` directory retains only about two days

There is no ready-made analytical dataset. Building one is a genuine data engineering problem.

---

## What this platform does

```
AEMO NEMWeb                Microsoft Fabric
(zipped CSV, 5 min)   ->   Data Factory pipeline
                             |
                             v
                           Lakehouse
                             Bronze  raw, parsed, as-landed
                             Silver  typed, deduplicated, timezone-corrected
                             Gold    star schema + daily aggregate
                             |
                             v
                           Power BI (Direct Lake)
```

Cross-cutting: data quality framework, control-table watermarking, Git integration, scheduled
orchestration with retry policies.

---

## Results

| Metric | Value |
|---|---|
| Source files ingested | 287 daily archives |
| Bronze rows (price table) | 186,565 |
| Silver rows after cleaning | 185,760 |
| Period covered | 26 Apr – 7 Sep 2026 |
| SA dispatch intervals analysed | 37,152 |
| Ingestion throughput after optimisation | **148x** faster than the naive implementation |
| Data quality checks | 6, run automatically on every load |

### Market findings

| Finding | Value |
|---|---|
| SA average spot price | $90.45/MWh |
| SA price range | −$496.77 to $20,300.00/MWh |
| Intervals with negative price | 6,172 (**16.6%**) |
| Intervals importing from Victoria | **66.6%** |
| Cheapest hour of day | ~12:00 (solar peak) |
| Most expensive hour of day | ~19:00 (post-sunset demand peak) |

The hourly price profile reproduces the **duck curve** directly from raw dispatch data: price
collapses through the middle of the day as rooftop and utility solar flood the market, then roughly
triples into the evening peak as solar output disappears and demand rises.

---

## Architecture

### Source

**AEMO NEMWeb** (`https://nemweb.com.au`) — public, no authentication, no licence restriction.

| Report | Cadence | Contents |
|---|---|---|
| `DispatchIS_Reports` | 5 min | Regional price, demand, interconnector flows, constraints |
| `ARCHIVE/DispatchIS_Reports` | Daily | Bundle of ~288 five-minute files |

AEMO migrated NEMWeb to HTTPS with case-sensitive paths in May 2026. Base URLs are held in
configuration rather than hardcoded, so a future migration is a one-line change.

### The file format

NEMWeb CSVs are not conventional CSVs. The first column is a row-type marker:

| Marker | Meaning |
|---|---|
| `C` | Comment — header and footer metadata |
| `I` | **Schema declaration** for the `D` rows that follow |
| `D` | Data row |
| `F` | Footer with record count |

A single file interleaves **seven logical tables** — price, regional demand, interconnector flows,
network constraints, and others — each introduced by its own `I` row with a different column set.

The parser reads sequentially, treats each `I` row as a schema declaration, and splits the file into
separately typed datasets. Because the schema is read from the file rather than hardcoded, **column
additions in future AEMO releases are absorbed automatically**.

`csv.reader` is used rather than `split(",")` because timestamp fields are quoted and constraint
identifiers can contain commas.

### Medallion layers

| Layer | Rule | Contents |
|---|---|---|
| `raw/` | Byte-for-byte as received | Original ZIP archives, partitioned by ingest date |
| **Bronze** | Parsed to tabular form, **nothing else** | All columns as strings, plus four lineage columns |
| **Silver** | One clean row per business event | Typed, deduplicated, timezone-converted, joined |
| **Gold** | Modelled for consumption | Star schema, surrogate keys, pre-aggregates |

Bronze holds every value as a string deliberately. Bronze's job is faithful capture, not correctness:
if AEMO publishes a malformed value, casting at Bronze would either fail the load or silently null
it. Type conversion is a transformation, and transformations belong in Silver.

Every Bronze row carries `_source_file`, `_schema_version`, `_ingest_run_id` and `_ingest_timestamp`,
so any value can be traced back to the archive it came from and the run that loaded it.

---

## Engineering decisions

### Incremental ingestion via a control table

A `ctl_ingested_files` Delta table records every file attempted, with its size, run ID, timestamp and
status. Each run computes *available minus already-successful* and processes only the difference.

Filenames are tracked rather than a high-water timestamp. A timestamp watermark silently skips files
that publish out of order; a filename set is immune to ordering.

Files that fail are logged as `FAILED` rather than omitted, so they remain pending and retry
automatically on the next run. Error recovery falls out of the design rather than needing separate
machinery.

### Idempotency

Every stage is safe to re-run:

- Bronze appends with a run identifier on every row
- Silver deduplicates via a window function partitioned by `(settlement_ts, region_id)` ordered by
  `LASTCHANGED` descending — the latest published revision wins
- Silver and Gold rebuild fully rather than accumulating

Re-running any window produces an identical result. This is the property that makes a failed pipeline
safe to restart.

### Performance: a 148x improvement

The first implementation processed one file at a time — download, then write seven Delta tables.
Measured throughput: **0.1 files/second**. A full backfill would have taken twelve days.

Profiling showed the bottleneck was **per-write fixed overhead**, not data volume. Each Delta write
costs roughly half a second in planning, Parquet serialisation and transaction log commit, regardless
of whether it writes five rows or five million. The loop performed ~4,000 writes to move 12 MB.

The fix was to change the *shape* of the work, not the size of the cluster:

- Switched from the `CURRENT` directory (one file per interval) to `ARCHIVE` (one bundle per day) —
  288 HTTP requests became one
- Parsed all 288 inner files into memory, then wrote **once per table per day** — ~2,000 writes
  became 7

Measured result: **14.8 files/second, a 148x improvement**, on identical capacity.

Notably, testing confirmed that adding capacity would *not* have solved this. The loop was
latency-bound on sequential fixed overhead, not throughput-bound. Scaling up would have delivered a
small improvement at significant cost.

### Timezone handling

AEMO publishes `SETTLEMENTDATE` in AEST year-round with no daylight saving. South Australia observes
ACST (UTC+9:30) and ACDT (UTC+10:30).

Conversion anchors to UTC as the canonical form:

```
AEST  ->  UTC  ->  Australia/Adelaide
```

`Australia/Brisbane` represents the source zone, because Queensland never observes daylight saving
and is therefore permanently UTC+10 — matching AEMO's market time. Using `Australia/Sydney` would
apply DST and shift every summer timestamp by an hour.

Silver retains all three timestamps: AEST for reconciliation against AEMO, UTC as the canonical
store, and Adelaide local for anything user-facing.

Without this conversion every chart would be shifted by 30 minutes in winter and 90 in summer — a
silent failure that produces no error and wrong answers.

---

## Data model

Star schema in the Gold layer.

**Facts**

- `fact_dispatch_interval` — one row per region per five-minute interval. Measures: spot price,
  total demand, available generation, net interchange, reserve margin, forecast error.
- `agg_daily_region` — pre-aggregated daily summary, so dashboard visuals read ~600 rows rather than
  186,000.

**Dimensions**

- `dim_date` — calendar attributes including southern-hemisphere seasons
- `dim_region` — the five NEM regions

**Design decisions**

- **Surrogate keys** on dimensions. Business keys (`REGIONID`) are stable today but not guaranteed
  to be, and surrogate keys are what make Type 2 history possible later.
- **Star, not snowflake.** Query simplicity and Power BI engine performance outweigh the storage
  saving at this scale.
- **Degenerate dimension** — `dispatch_interval` sits on the fact with no dimension table, because
  there is nothing to describe about it beyond itself.
- **Partitioned on `settlement_date`** — the near-universal query filter, enabling predicate
  pushdown.

---

## Data quality framework

Six checks run automatically as the final stage of every pipeline execution. Each is defined as a SQL
query plus an evaluation rule, so new checks are added as configuration rather than code.

| Check | Category | Severity | Rule |
|---|---|---|---|
| Interval completeness | Completeness | CRITICAL | 288 intervals per complete day |
| Duplicate detection | Uniqueness | CRITICAL | No repeated `(interval, region)` |
| Price bounds | Validity | WARNING | Within market floor and cap |
| Null business keys | Completeness | CRITICAL | No nulls in key columns |
| Referential integrity | Referential | CRITICAL | Every fact resolves to a region |
| Freshness | Freshness | WARNING | Latest interval within threshold |

Every result is logged to `dq_results` — passes as well as failures — because a check that passes
today and fails tomorrow is only meaningful with history.

**Severity is enforced, not decorative.** CRITICAL failures stop the pipeline; downstream tables
built on broken data are worse than no update at all. WARNING failures log, quarantine and continue.

Rows failing validation are copied to `quarantine_dispatch_interval` with the reason, severity and
run ID attached.

### Defects found

The framework identified real issues on its first run, none of them synthetic:

- **620 duplicate rows** where the `ARCHIVE` and `CURRENT` ingestion windows overlapped
- **Incomplete days** at the boundaries of the loaded range, including a timezone-conversion boundary
  artefact
- **A price above the market cap** in Tasmania, flagged for investigation rather than silently
  clipped

The last is a deliberate design choice: correcting data you do not yet understand is worse than
surfacing it.

**Negative prices are treated as valid**, not as errors. In South Australia they occur in 16.6% of
intervals. Validation rules encode market knowledge rather than naive assumptions — a check
rejecting all sub-zero prices would discard the most interesting sixth of the dataset.

---

## Orchestration

`pl_daily_dispatch_load` chains four notebook activities, each conditional on the previous
succeeding:

```
Ingest NEMWeb -> Transform Silver -> Build Gold -> Run Data Quality
```

- **Retry policy:** 2 attempts with increasing delay (exponential backoff). NEMWeb is an external
  service; a transient failure should not require human intervention, and backoff avoids adding load
  to a struggling server.
- **Timeout:** 1 hour per activity. Retries catch loud failures; timeouts catch silent ones.
- **Success dependencies:** without them, all four notebooks would execute concurrently and Silver
  would read Bronze mid-write — producing partial data with no error raised.
- **Schedule:** daily at 06:00 Adelaide time, after AEMO's previous-day archive publishes and before
  business hours.
- **Failure notification** by email.

The daily run pulls a two-day window rather than the full backfill. Overlap covers late publication;
the watermark skips anything already ingested.

---

## Technology choices

| Technology | Why | Alternative considered |
|---|---|---|
| Microsoft Fabric | Orchestration, Spark, warehouse and BI on one storage layer | Azure PaaS stack (ADF + Databricks + Synapse) — more services, higher cost, no unified trial |
| OneLake | ADLS Gen2 underneath, auto-provisioned, no keys to manage | Standalone storage account — manual provisioning, no benefit |
| Fabric Data Factory | Control flow, dependencies, retries, scheduling | Notebook scheduling alone — no dependency management |
| PySpark notebooks | MERGE, window functions, deduplication at scale | Dataflows Gen2 — low-code, no transferable Spark skill |
| Delta Lake | ACID, schema evolution, safe re-runs | Plain Parquet — no MERGE, no schema evolution |
| Direct Lake | Import-mode performance with no refresh step | Import mode — adds refresh dependency and staleness |
| Git integration | Pipelines and notebooks version-controlled as code | Manual export — repository drifts from reality |

**Deliberately excluded:** Event Hubs and Stream Analytics. The source publishes on a fixed
five-minute batch cadence, so a streaming layer would add infrastructure without solving a problem
this data has.

---

## Repository structure

```
├── README.md
├── docs/
│   ├── project-charter.md      Design decisions, made before implementation
│   ├── architecture.md
│   ├── data-model.md
│   └── data-quality.md
├── fabric/                     Fabric items, synced from the workspace
│   ├── nb_ingest_nemweb.Notebook
│   ├── nb_silver_dispatch.Notebook
│   ├── nb_gold_dispatch.Notebook
│   ├── nb_data_quality.Notebook
│   ├── pl_daily_dispatch_load.DataPipeline
│   ├── lh_sa_energy.Lakehouse
│   └── sm_sa_energy.SemanticModel
└── docs/images/                Architecture diagram, pipeline, dashboard
```

Fabric commits workspace items to the `fabric-dev` branch, which merges to `main` via pull request.
Fabric commits directly without review, so pointing it at a protected branch would allow unreviewed
changes to land on `main`.

---

## Challenges

**A proprietary format with no library support.** No maintained parser exists for AEMO's C/I/D/F
format, and community packages predate the 2026 NEMWeb migration. Writing a schema-driven parser was
required — and turned out to be the right decision anyway, since reading the schema from the file
means column additions need no code change.

**A 148x performance problem that capacity would not have fixed.** The instinct on a slow pipeline is
to scale up. Profiling showed the bottleneck was sequential per-write overhead, which more executors
do not address. The fix was fewer, larger operations.

**Timezone handling that fails silently.** AEST-to-Adelaide conversion produces no error when omitted
— just consistently wrong answers. Anchoring to UTC and testing explicitly was the only way to be
confident.

**Notebook execution order.** The first orchestrated run failed with a `NameError`: the notebook
worked interactively because of the order cells had been run in, but not top-to-bottom from a cold
session. Restructuring into imports, configuration, functions and execution fixed it — and the
pipeline failure was what surfaced the problem.

---

## What I would do next

- **Additional sources** — SCADA generation output per unit, and BOM weather observations, to explain
  *why* prices move rather than only recording that they did
- **SCD Type 2** on a generator dimension, once registration data is ingested
- **Automated tests** — pytest against the parser, the highest-value target given schema drift risk
- **CI/CD** — GitHub Actions running tests on pull request, Fabric deployment pipelines for
  DEV → PROD promotion
- **Monitoring** — pipeline telemetry to Azure Monitor with KQL alert rules
- **Infrastructure as code** — Bicep for the Azure resources

---

## Running this yourself

Requires a Microsoft Fabric workspace on trial or paid capacity.

1. Create a Lakehouse named `lh_sa_energy`
2. Import the notebooks from `fabric/`
3. Run `nb_ingest_nemweb` — set `BACKFILL_DAYS` for the desired history
4. Run `nb_silver_dispatch`, then `nb_gold_dispatch`, then `nb_data_quality`
5. Import `pl_daily_dispatch_load` and configure the schedule

Data volumes are modest — a full year of dispatch archives is roughly 2 GB of downloads.

---

## Data source and licence

Data is published by the Australian Energy Market Operator at
[nemweb.com.au](https://nemweb.com.au) and is publicly available. This project is not affiliated with
or endorsed by AEMO.
