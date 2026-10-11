# Databricks notebook source
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA rearc")

# The three answers
display(spark.table("gold_q1_population_stats"))
display(spark.table("gold_q2_best_year_per_series").orderBy("series_id"))
display(spark.table("gold_q3_series_value_with_population").orderBy("year"))

# COMMAND ----------

display(spark.table("gold_q1_population_stats").select("mean_population", "stddev_population_sample", "stddev_population_pop"))
display(spark.table("gold_q2_best_year_per_series").filter("series_id = 'PRS30006011'").select("series_id", "best_year", "sum_value", "periods_counted", "basis"))

# COMMAND ----------

pairs = [
    ("gold_q1_population_stats", "gold_q1_population_stats_pyspark_alt"),
    ("gold_q2_best_year_per_series", "gold_q2_best_year_per_series_sql_alt"),
    ("gold_q3_series_value_with_population", "gold_q3_series_value_with_population_pyspark_alt"),
]
for a, b in pairs:
    ta, tb = spark.table(a), spark.table(b)
    diff = ta.exceptAll(tb).count() + tb.exceptAll(ta).count()
    print(f"{a} vs {b}: {'MATCH' if diff == 0 else str(diff) + ' differing rows'}")

# README example check: PRS30006011 and a quick look at annual-only series
display(spark.table("gold_q2_best_year_per_series").filter("series_id = 'PRS30006011'"))
display(spark.table("gold_q2_best_year_per_series").filter("basis = 'annual_value'").select("series_id", "series_label", "best_year", "sum_value"))