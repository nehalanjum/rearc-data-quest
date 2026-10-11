# Rearc Data Quest: Databricks Edition

BLS productivity data (PR) plus US population, ingested into a Databricks Volume
and modeled as a Spark Declarative Pipeline (Bronze, Silver, Gold).

## Layout
- notebooks/01_ingest_raw.py: idempotent ingestion of the BLS folder and the population API into a Volume
- pipeline/transformations/: the pipeline (01_bronze.py, 02_silver.sql, 03_gold_sql.sql, 04_gold_pyspark.py)
- notebooks/02_verify_results.py: shows the three answers and checks the SQL and PySpark versions match
- PROCESS.md: architecture, trade-offs, retrospective
- screenshots/: pipeline, tables and results in Databricks

## Run order
1. Run notebooks/01_ingest_raw.py
2. Run the pipeline
3. Run notebooks/02_verify_results.py
