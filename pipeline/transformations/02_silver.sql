-- Small lookup tables: always use the newest snapshot of each file
CREATE OR REFRESH MATERIALIZED VIEW silver_sector AS
SELECT trim(sector_code) AS sector_code, trim(sector_name) AS sector_name
FROM bronze_bls_sector
QUALIFY regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)
      = MAX(regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)) OVER ();

CREATE OR REFRESH MATERIALIZED VIEW silver_class AS
SELECT trim(class_code) AS class_code, trim(class_text) AS class_text
FROM bronze_bls_class
QUALIFY regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)
      = MAX(regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)) OVER ();

CREATE OR REFRESH MATERIALIZED VIEW silver_measure AS
SELECT trim(measure_code) AS measure_code, trim(measure_text) AS measure_text
FROM bronze_bls_measure
QUALIFY regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)
      = MAX(regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)) OVER ();

CREATE OR REFRESH MATERIALIZED VIEW silver_duration AS
SELECT trim(duration_code) AS duration_code, trim(duration_text) AS duration_text
FROM bronze_bls_duration
QUALIFY regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)
      = MAX(regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)) OVER ();

CREATE OR REFRESH MATERIALIZED VIEW silver_seasonal AS
SELECT trim(seasonal_code) AS seasonal_code, trim(seasonal_text) AS seasonal_text
FROM bronze_bls_seasonal
QUALIFY regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)
      = MAX(regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)) OVER ();

CREATE OR REFRESH MATERIALIZED VIEW silver_series (
  CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL AND series_id <> '') ON VIOLATION DROP ROW
) AS
SELECT trim(series_id) AS series_id,
       trim(sector_code) AS sector_code,
       trim(class_code) AS class_code,
       trim(measure_code) AS measure_code,
       trim(duration_code) AS duration_code,
       trim(seasonal) AS seasonal_code,
       NULLIF(trim(base_year), '-') AS base_year,
       TRY_CAST(trim(begin_year) AS INT) AS begin_year,
       trim(begin_period) AS begin_period,
       TRY_CAST(trim(end_year) AS INT) AS end_year,
       trim(end_period) AS end_period
FROM bronze_bls_series
QUALIFY regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)
      = MAX(regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1)) OVER ();

-- Human-readable label per series, built from the lookup files
CREATE OR REFRESH MATERIALIZED VIEW silver_series_labeled (
  CONSTRAINT has_measure_label EXPECT (measure_text IS NOT NULL),
  CONSTRAINT has_sector_label  EXPECT (sector_name IS NOT NULL)
) AS
SELECT s.series_id,
       concat_ws(' | ', m.measure_text, sec.sector_name, cl.class_text, d.duration_text, se.seasonal_text) AS series_label,
       sec.sector_name, cl.class_text, m.measure_text, d.duration_text, se.seasonal_text,
       s.sector_code, s.class_code, s.measure_code, s.duration_code, s.seasonal_code,
       s.base_year, s.begin_year, s.begin_period, s.end_year, s.end_period
FROM silver_series s
LEFT JOIN silver_sector   sec ON s.sector_code   = sec.sector_code
LEFT JOIN silver_class    cl  ON s.class_code    = cl.class_code
LEFT JOIN silver_measure  m   ON s.measure_code  = m.measure_code
LEFT JOIN silver_duration d   ON s.duration_code = d.duration_code
LEFT JOIN silver_seasonal se  ON s.seasonal_code = se.seasonal_code;

-- The numbers: trimmed, typed, newest snapshot per file, Current preferred over AllData on overlap
CREATE OR REFRESH MATERIALIZED VIEW silver_bls_data (
  CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL AND series_id <> '') ON VIOLATION DROP ROW,
  CONSTRAINT year_valid         EXPECT (year BETWEEN 1900 AND 2100) ON VIOLATION DROP ROW,
  CONSTRAINT period_not_null    EXPECT (period IS NOT NULL AND period <> '') ON VIOLATION DROP ROW,
  CONSTRAINT period_format_ok   EXPECT (period RLIKE '^Q0[1-5]$'),
  CONSTRAINT value_is_numeric   EXPECT (value IS NOT NULL)
) AS
WITH parsed AS (
  SELECT trim(series_id) AS series_id,
         TRY_CAST(trim(year) AS INT) AS year,
         trim(period) AS period,
         TRY_CAST(trim(value) AS DOUBLE) AS value,
         NULLIF(trim(footnote_codes), '') AS footnote_codes,
         regexp_extract(source_path, '/bls/pr/([^/]+)/', 1) AS source_file,
         regexp_extract(source_path, '/bls/pr/[^/]+/([^/]+)/', 1) AS snapshot_id
  FROM bronze_bls_data
),
latest AS (
  SELECT *, MAX(snapshot_id) OVER (PARTITION BY source_file) AS max_snapshot
  FROM parsed
)
SELECT series_id, year, period, value, footnote_codes, source_file, snapshot_id
FROM latest
WHERE snapshot_id = max_snapshot
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY series_id, year, period
  ORDER BY CASE WHEN source_file = 'pr.data.0.Current' THEN 0 ELSE 1 END
) = 1;

-- Population: newest snapshot only, one row per year
CREATE OR REFRESH MATERIALIZED VIEW silver_population (
  CONSTRAINT year_not_null   EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT population_gt_0 EXPECT (population > 0)   ON VIOLATION DROP ROW
) AS
WITH exploded AS (
  SELECT explode(data) AS rec,
         regexp_extract(source_path, '/datausa/population/([^/]+)/', 1) AS snapshot_id
  FROM bronze_population
),
latest AS (
  SELECT * FROM exploded WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM exploded)
)
SELECT DISTINCT CAST(rec.Year AS INT) AS year,
       rec.Nation AS nation,
       CAST(rec.Population AS BIGINT) AS population
FROM latest;
