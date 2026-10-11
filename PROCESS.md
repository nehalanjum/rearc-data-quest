# PROCESS.md: Rearc Data Quest (Databricks Edition)

## 1. Architecture

### Ingestion (notebook `01_ingest_raw`)
- Lists the BLS directory (`/pub/time.series/pr/`) on every run, so no filenames are hardcoded. New files are picked up automatically.
- Every request sends a `User-Agent` header with a contact email, as the BLS data access policy requires. This resolves the 403 Forbidden.
- Each file is downloaded to a temp location and its SHA-256 hash is compared with a manifest stored in the Volume.
  - Unchanged content: skipped, nothing is written (re-running ingestion is a no-op).
  - New or changed content: landed as a new immutable snapshot at `bls/pr/<file>/<timestamp>/<file>`.
  - File removed at the source: moved to `_archive/` and logged; raw history is never silently deleted.
- The population API is handled the same way (hash of the JSON payload, snapshot only if changed).
- I kept all 12 files in the BLS folder (including `pr.txt` and `pr.contacts`), but only the data and mapping files feed the pipeline.

### Pipeline (Spark Declarative Pipeline `rearc_quest`)
- **Bronze** (streaming tables, Auto Loader): raw rows, every column kept as a string, header names trimmed (BLS pads them with spaces), plus `source_path`. Auto Loader tracks which files it has already read, so nothing is reprocessed. Expectation: no rescued/unparseable data.
- **Silver** (materialized views): trimmed and typed columns.
  - Only the newest snapshot of each source file is used.
  - `pr.data.0.Current` and `pr.data.1.AllData` overlap heavily, so rows are de-duplicated on `(series_id, year, period)`, preferring `Current` because it carries BLS's latest revisions.
  - Expectations: null `series_id`, invalid year, null period -> row dropped. Bad period format and non-numeric value -> recorded as warnings.
  - `silver_series_labeled` joins `pr.series` with the sector, class, measure, duration and seasonal lookup files to produce a readable `series_label` (e.g. "Hours worked | Manufacturing | All workers | Index (2017=100) | Seasonal Adjusted").
- **Gold** (materialized views): one table per question, each implemented in both Spark SQL and PySpark. A verification notebook compares each primary table with its alternative using `exceptAll` in both directions. All three pairs match.

| Question | Primary (feeds Gold) | Alternative (documented) |
|---|---|---|
| Q1 population mean/std dev | Spark SQL | PySpark |
| Q2 best year per series | PySpark | Spark SQL |
| Q3 PRS30006032 / Q01 + population | Spark SQL | PySpark |

**Why this split:** Q1 and Q3 are a plain aggregation and a plain join, which read most clearly in SQL. Q2 has several stages (quarterly sums, a fallback for annual-only series, a window ranking, then a label join), which is easier to build and test step by step as DataFrame transformations in PySpark.

### Modeling decisions worth knowing
- **Period Q05 is the annual average**, not a quarter. Q2 sums only Q01-Q04. Summing Q05 as well would double count every year.
- Some series have only annual data (Q05 only, e.g. `PRS30006081`). For those, the best year uses the annual value. The `basis` column says `quarterly_sum` or `annual_value`, so this is visible rather than hidden.
- Ties for best year are broken by choosing the later year (deterministic).
- Incomplete years (the current year has only Q01-Q02) appear naturally lower; `periods_counted` is included so a reader can see this.
- Many series are percent changes or indexes, where summing is not a meaningful quantity. The task asks for the sum, so I do that, and the label includes the measure type so readers are not misled.
- Q1 provides both sample and population standard deviation, since the task does not say which one it wants.
- Q3 uses a LEFT JOIN: population is NULL for years the API does not cover.

### Results
- Q1 (2013-2018, 6 years): mean 322,069,808; sample standard deviation 4,158,441.04. The population standard deviation is in the same table.
- Q2: 282 series, each with a best year and label.
- Q3: yearly values for `PRS30006032` / `Q01` from 1987 onward, with population where the API has data (2013 onward, with gaps).

## 2. Trade-offs: what I would do differently for a real client
- **Schema drift:** Auto Loader's rescued-data column and my expectation would flag unexpected columns. Silver selects columns by name, so a renamed column fails loudly instead of silently producing nulls. For production I would add a contract test on the file headers and alert on it.
- **Removed source files:** removal is archived and logged, but Silver currently still contains the last data from a removed file. For a client I would keep the manifest as a table and flag or remove rows from files that no longer exist.
- **Data volume and cost:** this dataset is about 5 MB, so a full download each run is cheap. For large sources I would use ETag/Last-Modified checks or an incremental API, file-notification mode in Auto Loader, and partitioning. Pipelines would run triggered on a schedule (BLS updates quarterly), not continuously.
- **Access control:** I would create a read-only analyst group with SELECT on the Gold tables only, keep Bronze/Silver and the Volume restricted to the pipeline's service principal, and avoid personal accounts running production jobs.
- **Monitoring:** alert on pipeline failure; track expectation pass/fail counts over time; add a freshness check (latest BLS period vs today); keep the ingestion log and manifest as an audit trail.
- **Reliability:** make sure two ingestion runs cannot overlap (job concurrency of 1); add unit tests for the parsing logic; deploy with Databricks Asset Bundles and CI instead of clicking through the UI.
- **Politeness to the source:** the ingestion sleeps between requests and identifies itself; at larger scale I would honor ETags to avoid downloading unchanged files.

## 3. Retrospective: what was hardest
1. **Understanding the data, not the code.** Reading `pr.txt` mattered: Q05 means annual average, and `pr.data.0.Current` is not just "current year" but a large overlap with `AllData`. Without that, Q2 would have been quietly wrong.
2. **Messy file formatting.** BLS pads headers and values with spaces, so every column is trimmed before typing.
3. **JSON schema surprises.** The population JSON failed twice: first because nested JSON was read as plain text (so `explode` failed), then because a field named `Nation ID` contains a space, which Delta does not allow as a column name. I fixed it by reading with type inference on and keeping only the three fields I need.
4. **Idempotency.** Deciding what "re-run safely" means for changed, new and removed files, and keeping an immutable raw history while Silver still shows only current data.

## 4. AI usage
I used Claude (Anthropic) as a reference and for drafting the ingestion notebook, the pipeline scaffold and this document. I ran everything in my own Databricks workspace, debugged the pipeline failures described above, and checked the SQL and PySpark outputs against each other. I can explain every part of the code.
