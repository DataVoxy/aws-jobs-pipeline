"""
aws-jobs-pipeline - Stage 2: Clean + Structure (SILVER)
=======================================================

THIS IS THE INTERESTING STAGE: turning messy scraped text into clean structure.
It is the exact "messy text -> JSON/columns" problem this project is built around.

What it does:
  1. Drop dirty rows (missing/blank title) and exact duplicates.
  2. Normalise the title -> a canonical job title + a seniority level.
  3. Parse the five different messy salary formats into numeric min/max EUR.
  4. Normalise location -> city + a remote/hybrid/onsite flag.
  5. Extract a skills[] array from the free-text description via keyword matching
     (a simple, explainable classifier - the regex/keyword approach done at scale).
  6. Write clean, typed Parquet as the SILVER layer.

CLOUD EQUIVALENT: an AWS Glue PySpark job, bronze (S3) -> silver (S3).
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from spark_setup import get_spark, BRONZE_PATH, SILVER_PATH

# The skill vocabulary we look for in free text. In production this would live in
# a config table; the matching logic (lowercase contains) stays the same.
SKILL_VOCAB = [
    "python", "sql", "pyspark", "spark", "aws", "azure", "gcp", "dbt",
    "airflow", "databricks", "snowflake", "kafka", "terraform", "docker",
    "kubernetes", "scala", "java", "power bi", "tableau", "redshift",
    "bigquery", "glue", "athena", "s3", "etl", "data modeling",
]


def classify_seniority(col):
    """Map raw title/keywords -> junior | medior | senior. Explainable rules."""
    lc = F.lower(col)
    return (
        F.when(lc.rlike(r"\b(senior|sr|lead|principal|staff)\b"), "senior")
        .when(lc.rlike(r"\b(junior|jr|graduate|entry)\b"), "junior")
        .otherwise("medior")
    )


def canonical_title(col):
    """Collapse title variants (DATA ENGINEER / data engineer) into clean roles."""
    lc = F.lower(F.trim(col))
    return (
        F.when(lc.rlike("data engineer"), "Data Engineer")
        .when(lc.rlike("analytics engineer"), "Analytics Engineer")
        .when(lc.rlike("ml engineer|machine learning"), "ML Engineer")
        .when(lc.rlike("data scientist"), "Data Scientist")
        .when(lc.rlike("data analyst"), "Data Analyst")
        .when(lc.rlike("bi developer"), "BI Developer")
        .when(lc.rlike("etl developer"), "ETL Developer")
        .when(lc.rlike("data architect"), "Data Architect")
        .when(lc.rlike("platform engineer"), "Platform Engineer")
        .otherwise(F.initcap(lc))
    )


def parse_salary(df: DataFrame) -> DataFrame:
    """Parse messy salary strings into numeric min/max EUR (annual).

    Handles: '€55.000 - €70.000', '55k-70k EUR', '€55,000 to €70,000',
             '55,000 - 70,000 per year', '', 'competitive'.
    Strategy: strip currency + separators, pull the two numbers, scale 'k'.
    """
    s = F.lower(F.coalesce(F.col("salary_raw"), F.lit("")))

    # Remove euro signs, the word 'eur', dots/commas used as thousands separators,
    # and 'per year'. Keep digits, 'k', spaces and the dash.
    cleaned = F.regexp_replace(s, r"[\u20ac,\.]", "")
    cleaned = F.regexp_replace(cleaned, r"eur|per year|to", " ")

    # Extract up to two number(+optional k) tokens.
    first = F.regexp_extract(cleaned, r"(\d+)\s*(k?)", 1)
    first_k = F.regexp_extract(cleaned, r"(\d+)\s*(k?)", 2)
    # For the second number, match the second occurrence by splitting on the dash.
    # try_element_at returns NULL (not an error) when there is no second part,
    # e.g. salaries with a single value, 'competitive', or empty. coalesce to ""
    # so the downstream regexp_extract has a string to work on.
    parts = F.split(cleaned, r"-")
    second_part = F.coalesce(F.try_element_at(parts, F.lit(2)), F.lit(""))
    second = F.regexp_extract(second_part, r"(\d+)\s*(k?)", 1)
    second_k = F.regexp_extract(second_part, r"(\d+)\s*(k?)", 2)

    def to_eur(num, k):
        n = F.when(num == "", None).otherwise(num.cast("double"))
        # If 'k' present OR the number is small (<1000), treat as thousands.
        scaled = F.when((k == "k") | (n < 1000), n * 1000).otherwise(n)
        return scaled

    df = df.withColumn("salary_min_eur", to_eur(first, first_k))
    df = df.withColumn("salary_max_eur", to_eur(second, second_k))
    # If only one number was found, max = min.
    df = df.withColumn(
        "salary_max_eur",
        F.when(F.col("salary_max_eur").isNull(), F.col("salary_min_eur")).otherwise(
            F.col("salary_max_eur")
        ),
    )
    return df


def normalise_location(df: DataFrame) -> DataFrame:
    loc = F.lower(F.trim(F.coalesce(F.col("location"), F.lit(""))))
    work_mode = (
        F.when(loc.rlike("remote"), "remote")
        .when(loc.rlike("hybrid"), "hybrid")
        .otherwise("onsite")
    )
    # City = text before a comma or dash, title-cased; map 'the hague' -> 'Den Haag'.
    city = F.initcap(F.trim(F.regexp_extract(loc, r"^([a-z\s]+)", 1)))
    city = F.when(F.lower(city).rlike("the hague"), "Den Haag").otherwise(city)
    city = F.when(F.trim(city) == "", None).otherwise(city)
    return df.withColumn("work_mode", work_mode).withColumn("city", city)


def extract_skills(df: DataFrame) -> DataFrame:
    """Build a skills array by testing each vocab term against the description."""
    desc = F.lower(F.coalesce(F.col("description"), F.lit("")))
    # For each skill, produce the skill name if present, else null; then filter nulls.
    present = [
        F.when(desc.contains(skill), F.lit(skill)).otherwise(F.lit(None))
        for skill in SKILL_VOCAB
    ]
    skills = F.array_except(F.array(*present), F.array(F.lit(None)))
    return df.withColumn("skills", skills).withColumn("skill_count", F.size(skills))


def main() -> None:
    spark = get_spark("jobs-pipeline-stage2-clean")
    print("Reading bronze...")
    bronze = spark.read.parquet(BRONZE_PATH)
    start = bronze.count()

    # 1. DROP dirty rows: null/blank title.
    df = bronze.filter(
        F.col("title").isNotNull() & (F.trim(F.col("title")) != "")
    )
    after_dirty = df.count()

    # 2. DEDUPLICATE on the business key (title+company+location+description).
    df = df.dropDuplicates(["title", "company", "location", "description"])
    after_dedup = df.count()

    # 3-5. Enrich: canonical title, seniority, salary, location, skills.
    df = df.withColumn("title_clean", canonical_title(F.col("title")))
    df = df.withColumn(
        "seniority",
        classify_seniority(F.concat_ws(" ", F.col("title"), F.col("description"))),
    )
    df = parse_salary(df)
    df = normalise_location(df)
    df = extract_skills(df)

    silver = df.select(
        "id",
        "title_clean",
        "seniority",
        "company",
        "city",
        "work_mode",
        "contract_type",
        "salary_min_eur",
        "salary_max_eur",
        "skills",
        "skill_count",
        "posted_days_ago",
        "ingested_at",
    )

    print(f"\n--- Cleaning summary ---")
    print(f"  bronze rows:            {start}")
    print(f"  after dropping dirty:   {after_dirty}  (removed {start - after_dirty})")
    print(f"  after dedup:            {after_dedup}  (removed {after_dirty - after_dedup})")
    print("\n--- Silver schema (clean, typed) ---")
    silver.printSchema()
    silver.show(6, truncate=50)

    silver.coalesce(1).write.mode("overwrite").parquet(SILVER_PATH)
    print(f"\nStage 2 complete. Silver written to:\n  {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
    import os as _os
    _os._exit(0)
