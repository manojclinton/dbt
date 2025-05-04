#!/usr/bin/env python3
#
# pySpark_to_bq.py
#
# Incrementally load all new JSON files from GCS into
#   data-management-2-manoj.cricket_raw.cricket_match_raw
# with columns (file_name STRING, content JSON, file_upload_timestamp TIMESTAMP).
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace, current_timestamp, col

def main():
    # --- CONFIG ---
    project_id = "data-management-2-manoj"
    bucket     = "cricket_analytics_src"
    dataset    = "cricket_raw"
    table      = "cricket_match_raw"
    # ----------------

    gcs_pattern = f"gs://{bucket}/*.json"
    bq_table    = f"{project_id}.{dataset}.{table}"

    spark = (
        SparkSession.builder
        .appName("GCS-root-to-BigQuery-Incremental-Load")
        .getOrCreate()
    )

    # 1) Already-loaded file names in BigQuery
    already = (
        spark.read
             .format("bigquery")
             .option("table", bq_table)
             .load()
             .select("file_name")
    )

    # 2) Read each JSON as raw text, capture its full path
    raw = (
        spark.read
             .text(gcs_pattern)
             .withColumn("full_path", input_file_name())
    )

    # 3) Derive just the JSON file name, filter out already-loaded
    new_files = (
        raw
        .withColumn(
            "file_name",
            regexp_replace(col("full_path"), f"^gs://{bucket}/", "")
        )
        .join(already, on="file_name", how="left_anti")
    )

    # 4) Build the final DataFrame with JSON metadata on `content`
    to_write = (
        new_files
        # annotate the column so the connector knows it's JSON :contentReference[oaicite:0]{index=0}
        .withColumn(
            "content",
            col("value")
              .alias("content", metadata={"sqlType": "JSON"})
        )
        .withColumn("file_upload_timestamp", current_timestamp())
        .select("file_name", "content", "file_upload_timestamp")
    )

    # 5) Append into BigQuery via the connector (indirect + Avro) :contentReference[oaicite:1]{index=1}
    (
        to_write.write
                .format("bigquery")
                .mode("append")
                .option("table",              bq_table)
                .option("temporaryGcsBucket", bucket)
                .option("writeDisposition",   "WRITE_APPEND")
                .option("writeMethod",        "indirect")
                .option("intermediateFormat", "avro")
                .save()
    )

    spark.stop()

if __name__ == "__main__":
    main()
