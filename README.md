# Falcon ad network: a small data warehouse in PySpark

A daily batch pipeline over the 62 delivered day folders (2026-08-24 to 2026-10-25, `dt=2026-10-03`
was not delivered; its events came in the 10-04 folder). It loads each folder into a staging layer, rebuilds the parts of the warehouse that the
folder can change, and writes an ML training table and a data quality report.

Everything runs as Delta tables in one schema (default `workspace.falcon`), partitioned by date.

## How to run (from a laptop, against a Databricks workspace)

This is how the pipeline was developed, tested and fully run. The Python code runs on the laptop; the Spark
work runs on the Databricks workspace through `databricks-connect` (serverless compute).

**You need:** Python **3.12** (required by `databricks-connect` 16.4), git, the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html),
a Databricks workspace with serverless compute and Unity Catalog, and permission to create a schema and a
volume in one catalog.

**1. Log in to the workspace** (once; the login is stored in `~/.databrickscfg` on your machine, never in the repo)

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

**2. Upload the dataset** (the folder that contains `manifest.json` and the `dt=...` folders)

```bash
databricks schemas create falcon <catalog>
databricks volumes create <catalog> falcon raw MANAGED
databricks fs cp -r <local>/dataset dbfs:/Volumes/<catalog>/falcon/raw/dataset
```

**3. Install**

```bash
git clone https://github.com/raphaelvgbrito/falcon-dwh.git && cd falcon-dwh
python -m venv .venv
source .venv/bin/activate            # Windows (Git Bash): source .venv/Scripts/activate
pip install -r requirements-dev.txt
pip install -e .
```

**4. Point the config at your workspace** – edit [`config/config.yaml`](config/config.yaml), nothing else:
- `input.base_path`: `/Volumes/<catalog>/falcon/raw/dataset`
- `warehouse.catalog` / `warehouse.schema`: where the tables are written (e.g. `<catalog>` / `falcon`)
- `spark.databricks_profile`: the CLI profile name if not `DEFAULT`; to use a cluster instead of serverless,
  set `serverless: false` and `cluster_id`.

**5. Run**

```bash
bash scripts/full_run.sh python      # the hand-in run: see warning below
```

`full_run.sh` **drops every table in `warehouse.schema`**, runs all days in order (2026-08-24 → 2026-10-25,
~1.5 min per day, ~1.5 h in total), then writes `reports/row_counts.md`, `reports/rerun_proof_2026-09-15.md`,
`reports/dq_summary.md` and `reports/dq_report_by_day.csv`. The log goes to `logs/full_run.log`.

Single steps:

```bash
python -m falcon_dwh.run --date 2026-08-24                      # one day (what a scheduler calls)
python -m falcon_dwh.run --start 2026-08-24 --end 2026-09-30     # several days, in order
python -m falcon_dwh.report rerun --date 2026-09-15              # re-run proof -> reports/
python -m falcon_dwh.report counts                               # row counts -> reports/
python -m falcon_dwh.report dq                                   # DQ summary + reconciliation -> reports/
python -m pytest                                                 # tests, ~11 min (see below)
```

The tests use the same connection but their own schema, `<catalog>.falcon_test` (created if missing, and
emptied by the integration test), so they never touch the warehouse schema. Another config file:
`--config path` or `FALCON_CONFIG=path`.

### Running on your cluster

What was actually run, and how:

| Way | Status |
|---|---|
| Laptop → Databricks via `databricks-connect` (`config/config.yaml`) | used for development, the tests and the full run in `logs/` |
| **Databricks job** on serverless, code checked out from this GitHub repo (`jobs/job.json`, `config/config.databricks_job.yaml`, `spark.mode: cluster`) | ran one day end to end (all layers) on Databricks Free Edition: run `469415988902347`, 216 s, SUCCESS |
| Plain Spark (EMR, Dataproc, local) | **not tested**; needs Delta Lake (`delta-spark`) and a metastore, see below |

As a Databricks job:
1. Upload the dataset to a volume (or any path the cluster reads); set `input.base_path`,
   `warehouse.catalog`, `warehouse.schema` and `output.reports_dir` in `config/config.databricks_job.yaml`.
2. Create the job: `databricks jobs create --json @jobs/job.json` (edit `git_url` if you fork). It runs
   `jobs/run_job.py run --config config/config.databricks_job.yaml --schema {{schema}} --date {{date}}` with
   `pyyaml`, `requests`, `holidays` installed by the job environment.
3. One day: `databricks jobs run-now --json '{"job_id": <id>, "job_parameters": {"date": "2026-08-24"}}'`.
   A scheduler passes the date; a backfill passes `--start/--end` instead of `--date`.
4. Reports: `jobs/run_job.py report --config config/config.databricks_job.yaml counts|dq|rerun --date ...`
   write to `output.reports_dir` (a volume, because the job's checkout is temporary).

No internet on the cluster? Databricks Free Edition serverless has none. Exchange rates are fetched from
frankfurter by code; every successful fetch also saves the raw API response (with URL and fetch time) to
`reference/frankfurter_fx_response.json`, and a run that cannot reach the API uses that saved response
with a warning (`ref_fx_rates.source` says which). Holidays come from the `holidays` package (offline).
To refresh the saved response: `python -m falcon_dwh.external`.

Plain Spark: start Spark with Delta (`--packages io.delta:delta-spark_2.12:3.2.0` plus the two Delta
`spark.sql.extensions` / `spark.sql.catalog.spark_catalog` settings), set `spark.mode: cluster`,
`warehouse.catalog: spark_catalog`, and run `spark-submit jobs/run_job.py run --config ... --date ...`.
Parquet is not supported: the idempotent partition replacement relies on Delta's `replaceWhere`.

## What it builds

| Layer | Table | Grain (one row per ...) | Key | Partition |
|---|---|---|---|---|
| staging | `stg_<source>` (15 tables) | row as delivered in folder D | source key + `load_date` | `load_date` |
| dimension | `dim_placement` | placement version (placement → site → publisher → platform, + site category) | `placement_key` (= hash(placement_id, valid_from)) | – |
| dimension | `dim_banner` | banner version (banner → campaign → advertiser, + campaign category) | `banner_key` | – |
| dimension | `dim_date` | calendar day of the run range | `date` | – |
| dimension | `dim_time` | minute of the day (1,440 rows) | `time_key` = HHMM | – |
| fact | `fact_request` | request (`request_id`) | `request_id` | `request_date` |
| fact | `fact_impression` | shown slot (`fid`), with click + conversion outcomes | `fid` | `impression_date` |
| fact | `fact_postback` | conversion (`postback_id`, de-duplicated), also without impression | `postback_id` | `postback_date` |
| fact | `rejected_impressions` | staged impression row that cannot be used, with `reject_reason` | `impression_id` | `load_date` |
| mart | `ml_training` | shown slot (`fid`) | `fid` | `impression_date` |
| reference | `ref_fx_rates` | (calendar day, currency) | `rate_date, currency` | – |
| reference | `ref_holidays` | (country, holiday date) | `country_code, holiday_date` | – |
| quality | `dq_report` | (run day, check, table): rows in vs manifest, rows in → out per table, and every problem check | `load_date, check, table_name` | `load_date` |
| quality | `recon_impressions` | run day | `load_date` | `load_date` |

Facts join dimensions on id + event date between `valid_from` and `valid_to` (the key of the version is
stored in the fact), `<event>_date` → `dim_date.date`, `<event>_time_key` → `dim_time.time_key`. All times UTC.

### The daily run (`run.py`)

`run_day(D)` = staging → dims → facts → mart → quality:

1. **Staging** (`staging.py`, `schemas.py`): every file read with an explicit schema, nothing fixed. CSV
   booleans (`t`/`f`) stay strings, `requests.data` stays raw JSON text, rows that do not parse land in
   `_corrupt_record` instead of being nulled. Each source writes `replaceWhere load_date = D`.
2. **Dims** (`dims.py`): SCD2 rebuilt from all staged snapshots. A new version starts when any attribute of the
   flattened row (including the parent site / publisher / campaign / advertiser) changes, or when the id was
   missing from a snapshot. "Snapshot of day D is what we knew on D; a later one wins from its day on." The
   dims are tiny, so a full rebuild is simpler and safer than a merge.
3. **Facts** (`facts.py`): the folder of day D can change older days (a postback for a day-2 impression comes on
   day 10). The run computes which partitions D touches (dates of the requests in D, impression dates of the fids
   that have impressions / clicks / postbacks in D, impressions of requests that arrived in D) and rebuilds
   exactly those partitions from all staged data.
4. **Mart** (`mart.py`, `history.py`, `external.py`): rebuilt for the same partitions as `fact_impression`.
5. **Quality** (`dq.py`): the DQ checks of day D and the impression reconciliation.

**Why re-running is safe:** every write replaces a defined slice (a partition or a small full table) and never
appends; every slice is computed from staging only, and staging of day D is the folder of day D. So the
warehouse is a function of the staged folders: running any day again, also an old one after later days, gives
the same tables. Proof: `reports/rerun_proof_*.md` (row counts, sums and a checksum of every table before /
after) and `tests/test_incremental.py`.

### ML training table

One row per `fid`. Decision time = request time. Labels `is_click`, `is_conversion` (as known so far).
Features, all known at request time:
- request context: country/state/city, offer count, GDPR flag, template, language, cart size, has customer id,
  `order_amount` and **`order_amount_usd`** (ECB rate of the request day via frankfurter, weekend = last
  published rate, never a later one);
- `device_type`, `os`, `os_major`, `browser` parsed from `user_agent` with built-in functions (no UDF);
- placement and banner attributes from the dimension version valid that day;
- site and campaign enrichment: the newest fetch with `created_at <= request time`;
- time: `hour_utc`, `day_of_week`, `is_weekend`; `is_public_holiday` in the shopper's country (`holidays` package);
- history, by **arrival date**: publisher and campaign impressions / CTR / CVR over the previous 7 and 28 days.
  For a request on day d only folders `d-N .. d-1` count; a conversion that arrives on day 10 for a day-2
  impression is usable from day 11. Unit test: `test_history_uses_only_previous_days`.

Caveat (from the spec): the dimension and enrichment snapshots carry today's attribute values, so this table
does **not** support a leakage-free historical backtest. Event-based features do respect arrival times.
Labels of recent days are not mature (conversions arrive up to ~45 days later); filter on
`as_of - impression_date` when training. Preview traffic is kept with `is_preview = true`.

## Data problems found and what I did

Full list with counts and examples per day: `reports/dq_summary.md`, `reports/dq_report_by_day.csv`.

| Problem | Size | Decision |
|---|---|---|
| Folder `dt=2026-10-03` missing (spec says 63 days, manifest has 62); its events arrive in the `dt=2026-10-04` folder | 24k requests, 31k impressions, 14k orders, 1.7k clicks, 54 postbacks | dated by `created_at`, so the 10-03 fact partitions are filled; `dim_date.source_folder_loaded=false`; no 10-03 dimension snapshot, so SCD2 versions span the gap |
| 20 request lines truncated mid-JSON (2026-09-25) | 20 rows | kept in `_corrupt_record`; id / placement / time recovered by regex, `is_truncated_record` |
| Other events in a later day's folder | 4.7k impressions (09-17, 09-18), 50 postbacks spread over 30 days | dated by `created_at`, earlier partitions rebuilt |
| Re-renders: several impression rows per slot, up to 11 days apart | 429k rows | one fact row per fid (first render), `render_count` |
| Postbacks delivered twice (same id, next day) | 50 | de-duplicated by `postback_id`; revenue counted once |
| One slot with 147,291 clicks over 51 days (~61% of all clicks) | 1 fid | kept; `is_click` counts once per slot; `click_flood` check |
| One impression with hundreds of postbacks ~43 days later | 1 fid | kept; `long_lag` check (fraud / attribution review) |
| Placement `3d2c6824fe8d1a8a` twice in every snapshot from 09-12 (template 15 vs 99) | 1 id x 43 days | keep the row with the latest `updated_at` |
| 3 banner ids never in any banner snapshot (+ null banner ids) | ~24k slots (1.1%) | kept, `banner_found=false` |
| 2 placement ids never in any placement snapshot | 465 slots | kept, `placement_found=false` |
| Impression placement differs from the request placement (`d3e49cdc…` vs `367a285a…`) | 8,077 slots (0.4%) | kept; impression placement used for the dimension; flag + check |
| Postbacks whose fid matches no impression | 59 | kept in `fact_postback` with `impression_found=false` (revenue not lost) |
| Order amount missing / currency missing; currencies without an ECB rate (SAR, AED, QAR, ...) | ~31% / ~28% of orders / ~760 requests | `order_amount_usd = null`, never guessed; checks |
| `weight` written as `1.00` in campaign / banner | 1 row each per day | typed as double, not int |
| `orderid` / `orderId` (/ `orderID`, `order_value` documented but never seen) | – | coalesced |

Publisher 003's rev share changed 60 → 70 on 2026-09-27; this is the only dimension change in the window
(1,662 placements got a 2nd version), banners never change.

## Decisions and trade-offs

- **Delta + `replaceWhere`** instead of append + merge: idempotent by construction, and Delta refuses to write a
  row outside the declared partitions.
- **Rebuild touched partitions from all staging** instead of patching rows: simple and always correct; the cost
  is re-reading staging every day (fine at 2.5M impressions, see below for 100x).
- **SCD2 on the flattened hierarchy**: one join per fact instead of four, and a publisher change creates a new
  placement version. Cost: the dimension grows with every parent change.
- **History by arrival date**, not by event date: slightly less fresh, but no leakage from late events.
- **Flag, don't drop**: every fact row is kept; missing dimension rows are flags plus DQ checks. The only rows
  that leave the fact are unusable impression rows, and they go to `rejected_impressions` with a reason.
- **Reference data cached as tables**: the FX API is called once, re-runs do not depend on it.
- UTC everywhere; local-time features (shopper's hour) would need a time zone per country/state.

## With 100x more data

- Stop re-reading all of staging every day: keep an index table `fid → impression_date` (and request →
  impression) maintained incrementally, and read only the partitions the new folder touches.
- `MERGE` into the facts on the touched keys instead of rewriting whole partitions; Z-order / liquid clustering
  on `fid`, `request_id`.
- SCD2 by merge on changed ids only (hash compare against the current version) instead of a full rebuild.
- History features as a daily aggregate table updated incrementally (one day added, one day dropped).
- Parallel ingestion of sources (already threaded), Auto Loader / file notifications instead of listing folders.
- DQ checks as a library (e.g. expectations) running on the touched slices, alerts on thresholds.

## Tests

`python -m pytest` (runs on the configured Spark, writes to schema `falcon_test`):
- `test_transforms.py`: fid decoding (real value, url alphabet, missing padding, garbage → null), `t`/`f`,
  order JSON spellings.
- `test_dedup.py`: re-renders collapse to the first render per fid (ties deterministic, render counts add up to
  the input rows), postbacks delivered twice count once (revenue too), repeated clicks make one clicked slot,
  duplicate requests keep the first arrival and truncated request lines are recovered.
- `test_dims.py`: SCD2 (change, missing folder, deleted id), snapshot de-duplication, `dim_time`, time key.
- `test_ml.py`: user agent parsing (12 real shapes), FX fill-forward never uses a later rate, USD conversion,
  FX falls back to the saved API response (and fails loudly without one), holidays, history features use
  only previous days.
- `test_staging.py`: explicit schema, re-run idempotent, only its own partition, missing folder.
- `test_incremental.py`: from empty tables, run 08-24..08-26: a postback arriving 08-26 flips the 08-24
  impression to converted; re-running 08-26 and then 08-24 leaves all tables identical.

How the AI-written code was checked: staging counts against `manifest.json` (all 15 tables x 62 days), fact
totals against staging (fids, render counts, revenue to the cent), per-day reconciliation, distributions
(devices, currencies, FX ratios), and the DQ examples looked at by hand. Checks that caught real problems on
the way: `_corrupt_record` counts (the `1.00` weight), SCD2 overlap check (the duplicated placement), a
fingerprint that must differ between two different days (so the re-run test cannot pass trivially), and the
time key test (a client-side time zone shift).

## Known gaps / not done

- Holidays use the UTC date and the shopper's IP country. `ref_holidays` covers every country the `holidays`
  package supports (155 with holidays in 2026); requests from other countries are counted in the DQ check
  `country_without_holiday_calendar` and get `is_public_holiday = false`.
- iPadOS Safari reports itself as a Mac, so those iPads are counted as desktop.
- A conversion counts with or without a click on the slot (every postback in the data has one anyway); no
  attribution window is applied. To confirm with the team.
- Orchestration is a single Databricks job definition (`jobs/job.json`, one task per day); no Airflow DAG,
  retries/alerts or schedule are configured. A daily trigger would pass yesterday's date.
- Tests run against Spark through the configured connection (Databricks), not a local SparkSession: the
  Windows laptop could not run local Spark without unofficial Hadoop binaries.
