"""
aws-jobs-pipeline - Stage 1: Ingest (BRONZE)
============================================

WHAT: Read the raw JSON-lines job postings and land them in Spark, unchanged, as
the BRONZE layer. Bronze = raw as-is, no cleaning. We only add ingestion metadata
(when it was ingested) and persist it in a columnar format (Parquet).

CLOUD EQUIVALENT: an AWS Glue job reading s3://.../raw/postings.jsonl and writing
Parquet to s3://.../bronze/. Same code, different paths.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from spark_setup import get_spark, RAW_PATH, BRONZE_PATH


def main() -> None:
    spark = get_spark("jobs-pipeline-stage1-ingest")
    print(f"Spark {spark.version} up. Reading raw postings from:\n  {RAW_PATH}")

    # Read JSON lines. Spark infers the schema from the data. Each line = one row.
    bronze = spark.read.json(RAW_PATH)

    # Add an ingestion timestamp. Bronze is append-only in production, so you can
    # always trace WHEN a row entered the lake. current_timestamp() is evaluated
    # by Spark at run time.
    bronze = bronze.withColumn("ingested_at", F.current_timestamp())

    print("\n--- Bronze schema (raw, uncleaned) ---")
    bronze.printSchema()

    total = bronze.count()  # ACTION: forces execution (Spark is lazy until now)
    print(f"--- Ingested {total} raw rows (duplicates + dirty rows still present) ---")
    bronze.show(5, truncate=60)

    # Write Parquet. coalesce(1) = write a single file (fine for small data; in
    # the cloud you'd let Spark write many part-files partitioned by date).
    bronze.coalesce(1).write.mode("overwrite").parquet(BRONZE_PATH)
    print(f"\nStage 1 complete. Bronze written to:\n  {BRONZE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
    import os as _os
    _os._exit(0)
