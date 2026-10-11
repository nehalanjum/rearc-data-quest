-- Q1 (PRIMARY, SQL): mean and standard deviation of US population, 2013-2018 inclusive
CREATE OR REFRESH MATERIALIZED VIEW gold_q1_population_stats
COMMENT 'Q1 primary (Spark SQL). Both sample and population std dev are provided.' AS
SELECT 2013 AS start_year,
       2018 AS end_year,
       COUNT(*) AS years_counted,
       AVG(population) AS mean_population,
       STDDEV_SAMP(population) AS stddev_population_sample,
       STDDEV_POP(population)  AS stddev_population_pop
FROM silver_population
WHERE year BETWEEN 2013 AND 2018;

-- Q2 (ALTERNATIVE, SQL): best year per series
CREATE OR REFRESH MATERIALIZED VIEW gold_q2_best_year_per_series_sql_alt
COMMENT 'Q2 alternative (Spark SQL). Compared with the PySpark primary in the verification notebook.' AS
WITH quarterly AS (
  SELECT series_id, year, SUM(value) AS sum_value, COUNT(*) AS periods_counted, 'quarterly_sum' AS basis
  FROM silver_bls_data
  WHERE period IN ('Q01','Q02','Q03','Q04')
  GROUP BY series_id, year
),
annual_only AS (
  SELECT series_id, year, SUM(value) AS sum_value, COUNT(*) AS periods_counted, 'annual_value' AS basis
  FROM silver_bls_data
  WHERE period = 'Q05'
    AND series_id NOT IN (SELECT DISTINCT series_id FROM quarterly)
  GROUP BY series_id, year
),
yearly AS (
  SELECT * FROM quarterly UNION ALL SELECT * FROM annual_only
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY sum_value DESC, year DESC) AS rn
  FROM yearly
)
SELECT r.series_id, l.series_label, l.sector_name, l.class_text, l.measure_text, l.duration_text, l.seasonal_text,
       r.year AS best_year, r.sum_value, r.periods_counted, r.basis
FROM ranked r
LEFT JOIN silver_series_labeled l ON r.series_id = l.series_id
WHERE r.rn = 1;

-- Q3 (PRIMARY, SQL): PRS30006032, Q01, value per year joined with population
CREATE OR REFRESH MATERIALIZED VIEW gold_q3_series_value_with_population
COMMENT 'Q3 primary (Spark SQL). LEFT JOIN: population is NULL for years the API does not cover.' AS
SELECT b.series_id, b.year, b.period, b.value, p.population
FROM silver_bls_data b
LEFT JOIN silver_population p ON b.year = p.year
WHERE b.series_id = 'PRS30006032' AND b.period = 'Q01'
ORDER BY b.year;
