from pyspark import pipelines as dp
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/rearc/raw"

BLS_FILES = {
    "bronze_bls_data":     "pr.data.*",      # matches both pr.data.0.Current and pr.data.1.AllData
    "bronze_bls_series":   "pr.series",
    "bronze_bls_measure":  "pr.measure",
    "bronze_bls_sector":   "pr.sector",
    "bronze_bls_class":    "pr.class",
    "bronze_bls_duration": "pr.duration",
    "bronze_bls_seasonal": "pr.seasonal",
    "bronze_bls_period":   "pr.period",
    "bronze_bls_footnote": "pr.footnote",
}

def make_bls_bronze(table_name, folder_glob):
    @dp.table(name=table_name, comment=f"Raw BLS PR file(s) '{folder_glob}', all columns as strings")
    @dp.expect_all({
        "no_schema_surprises": "_rescued_data IS NULL",   # warns if a row doesn't fit the expected columns
        "has_source_path": "source_path IS NOT NULL",
    })
    def _bronze():
        df = (spark.readStream.format("cloudFiles")       # Auto Loader: processes each new file exactly once
              .option("cloudFiles.format", "csv")
              .option("header", "true")
              .option("sep", "\t")
              .option("cloudFiles.inferColumnTypes", "false")
              .load(f"{BASE}/bls/pr/{folder_glob}/*/*")
              .select("*", F.col("_metadata.file_path").alias("source_path")))
        # BLS pads header names with spaces ("value     "): trim them
        return df.select([F.col(f"`{c}`").alias(c.strip()) for c in df.columns])

for _name, _glob in BLS_FILES.items():
    make_bls_bronze(_name, _glob)

@dp.table(name="bronze_population", comment="Raw DataUSA population JSON snapshots")
@dp.expect("has_source_path", "source_path IS NOT NULL")
def bronze_population():
    raw = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("multiLine", "true")
           .option("cloudFiles.inferColumnTypes", "true")
           .load(f"{BASE}/datausa/population/*/*")
           .select("*", F.col("_metadata.file_path").alias("source_path")))
    # Keep only the fields we need. This drops fields like "Nation ID" whose names contain spaces.
    return raw.select(
        F.transform("data", lambda r: F.struct(
            r["Year"].alias("Year"),
            r["Nation"].alias("Nation"),
            r["Population"].alias("Population"),
        )).alias("data"),
        "source_path",
    )
