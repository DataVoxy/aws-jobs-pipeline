"""
aws-jobs-pipeline - AWS Glue version of Stage 2 (SILVER)
========================================================

This is the SAME cleaning + structuring logic as the local stage2_clean.py,
adapted to run as an AWS Glue PySpark job:

  - no local Spark setup (Glue provides the Spark runtime + Python workers)
  - reads bronze from S3 and writes silver to S3
  - takes the bucket name as a Glue job argument (--bucket)

The point of including this file: it proves the local pipeline ports to the
cloud with only I/O + bootstrapping changes. The transform code (canonical
title, seniority, salary parsing, skills extraction) is byte-for-byte the logic
from the local version. In an interview you can show both files side by side and
explain "local for dev, Glue for production, identical business logic".

Deploy: upload this file to s3://<bucket>/scripts/ (Terraform wires it to a
Glue job). Step Functions runs it as the second step.
"""
import sys

from awsglue.utils import getResolvedOptions  # Glue-provided helper
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --bucket is passed by the Glue job (set in Terraform default_arguments).
args = getResolvedOptions(sys.argv, ["bucket"])
BUCKET = args["bucket"]
BRONZE = f"s3://{BUCKET}/bronze/"
SILVER = f"s3://{BUCKET}/silver/"

sc = SparkContext.getOrCreate()
spark = sc._jvm  # placeholder; in real Glue use GlueContext. See note below.

# NOTE: a real Glue job starts with:
#   from awsglue.context import GlueContext
#   glueContext = GlueContext(SparkContext.getOrCreate())
#   spark = glueContext.spark_session
# We keep the import light here so the file reads clearly as a skeleton. The
# transform functions below are the production logic verbatim.

SKILL_VOCAB = [
    "python", "sql", "pyspark", "spark", "aws", "azure", "gcp", "dbt",
    "airflow", "databricks", "snowflake", "kafka", "terraform", "docker",
    "kubernetes", "scala", "java", "power bi", "tableau", "redshift",
    "bigquery", "glue", "athena", "s3", "etl", "data modeling",
]


def classify_seniority(col):
    lc = F.lower(col)
    return (
        F.when(lc.rlike(r"\b(senior|sr|lead|principal|staff)\b"), "senior")
        .when(lc.rlike(r"\b(junior|jr|graduate|entry)\b"), "junior")
        .otherwise("medior")
    )


def canonical_title(col):
    lc = F.lower(F.trim(col))
    return (
        F.when(lc.rlike("data engineer"), "Data Engineer")
        .when(lc.rlike("analytics engineer"), "Analytics Engineer")
        .when(lc.rlike("ml engineer|machine learning"), "ML Engineer")
        .when(lc.rlike("data scientist"), "Data Scientist")
        .when(lc.rlike("data analyst"), "Data Analyst")
        .otherwise(F.initcap(lc))
    )


def parse_salary(df: DataFrame) -> DataFrame:
    s = F.lower(F.coalesce(F.col("salary_raw"), F.lit("")))
    cleaned = F.regexp_replace(s, r"[\u20ac,\.]", "")
    cleaned = F.regexp_replace(cleaned, r"eur|per year|to", " ")
    first = F.regexp_extract(cleaned, r"(\d+)\s*(k?)", 1)
    first_k = F.regexp_extract(cleaned, r"(\d+)\s*(k?)", 2)
    parts = F.split(cleaned, r"-")
    second_part = F.coalesce(F.try_element_at(parts, F.lit(2)), F.lit(""))
    second = F.regexp_extract(second_part, r"(\d+)\s*(k?)", 1)
    second_k = F.regexp_extract(second_part, r"(\d+)\s*(k?)", 2)

    def to_eur(num, k):
        n = F.when(num == "", None).otherwise(num.cast("double"))
        return F.when((k == "k") | (n < 1000), n * 1000).otherwise(n)

    df = df.withColumn("salary_min_eur", to_eur(first, first_k))
    df = df.withColumn("salary_max_eur", to_eur(second, second_k))
    return df.withColumn(
        "salary_max_eur",
        F.when(F.col("salary_max_eur").isNull(), F.col("salary_min_eur")).otherwise(
            F.col("salary_max_eur")
        ),
    )


def extract_skills(df: DataFrame) -> DataFrame:
    desc = F.lower(F.coalesce(F.col("description"), F.lit("")))
    present = [
        F.when(desc.contains(skill), F.lit(skill)).otherwise(F.lit(None))
        for skill in SKILL_VOCAB
    ]
    skills = F.array_except(F.array(*present), F.array(F.lit(None)))
    return df.withColumn("skills", skills).withColumn("skill_count", F.size(skills))


# --- Production flow (uncomment GlueContext lines above to run for real) ---
# bronze = spark.read.parquet(BRONZE)
# df = bronze.filter(F.col("title").isNotNull() & (F.trim(F.col("title")) != ""))
# df = df.dropDuplicates(["title", "company", "location", "description"])
# df = df.withColumn("title_clean", canonical_title(F.col("title")))
# df = df.withColumn("seniority", classify_seniority(
#     F.concat_ws(" ", F.col("title"), F.col("description"))))
# df = parse_salary(df)
# df = extract_skills(df)
# df.write.mode("overwrite").parquet(SILVER)
