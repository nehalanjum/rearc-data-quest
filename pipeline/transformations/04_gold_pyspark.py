from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Q1 (ALTERNATIVE, PySpark)
@dp.materialized_view(name="gold_q1_population_stats_pyspark_alt",
                      comment="Q1 alternative (PySpark DataFrame API)")
def q1_alt():
    pop = spark.read.table("silver_population").filter(F.col("year").between(2013, 2018))
    return pop.agg(
        F.lit(2013).alias("start_year"),
        F.lit(2018).alias("end_year"),
        F.count("*").alias("years_counted"),
        F.avg("population").alias("mean_population"),
        F.stddev_samp("population").alias("stddev_population_sample"),
        F.stddev_pop("population").alias("stddev_population_pop"),
    )

# Q2 (PRIMARY, PySpark): best year per series
@dp.materialized_view(name="gold_q2_best_year_per_series",
                      comment="Q2 primary (PySpark). Sum of Q01-Q04 per year; annual-only series use Q05.")
def q2_primary():
    data = spark.read.table("silver_bls_data")

    quarterly = (data.filter(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
                 .groupBy("series_id", "year")
                 .agg(F.sum("value").alias("sum_value"),
                      F.count("*").alias("periods_counted"))
                 .withColumn("basis", F.lit("quarterly_sum")))

    series_with_quarters = quarterly.select("series_id").distinct()
    annual_only = (data.filter(F.col("period") == "Q05")
                   .join(series_with_quarters, "series_id", "left_anti")
                   .groupBy("series_id", "year")
                   .agg(F.sum("value").alias("sum_value"),
                        F.count("*").alias("periods_counted"))
                   .withColumn("basis", F.lit("annual_value")))

    yearly = quarterly.unionByName(annual_only)
    w = Window.partitionBy("series_id").orderBy(F.col("sum_value").desc(), F.col("year").desc())
    best = (yearly.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .withColumnRenamed("year", "best_year"))

    labels = spark.read.table("silver_series_labeled").select(
        "series_id", "series_label", "sector_name", "class_text",
        "measure_text", "duration_text", "seasonal_text")

    return (best.join(labels, "series_id", "left")
            .select("series_id", "series_label", "sector_name", "class_text",
                    "measure_text", "duration_text", "seasonal_text",
                    "best_year", "sum_value", "periods_counted", "basis"))

# Q3 (ALTERNATIVE, PySpark)
@dp.materialized_view(name="gold_q3_series_value_with_population_pyspark_alt",
                      comment="Q3 alternative (PySpark DataFrame API)")
def q3_alt():
    bls = (spark.read.table("silver_bls_data")
           .filter((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01")))
    pop = spark.read.table("silver_population").select("year", "population")
    return (bls.join(pop, "year", "left")
            .select("series_id", "year", "period", "value", "population")
            .orderBy("year"))
