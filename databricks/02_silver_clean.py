# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 of 3 - SILVER clean
# MAGIC
# MAGIC Second task in the Job. Reads `bronze_postings` from the catalog, cleans +
# MAGIC structures it (drop dirty, dedupe, canonical title, seniority, parse messy
# MAGIC salary, normalise location, extract skills[]), writes `silver_postings`.
# MAGIC
# MAGIC In the Job DAG this task **depends on** the bronze task, so Databricks will
# MAGIC only start it after bronze succeeds. If bronze fails, this never runs.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE jobs_pipeline")

SKILL_VOCAB = ["python", "sql", "pyspark", "spark", "aws", "azure", "dbt", "airflow",
               "databricks", "snowflake", "kafka", "terraform", "scala", "power bi",
               "redshift", "bigquery", "glue", "athena", "s3", "etl", "data modeling"]

def classify_seniority(col):
    lc = F.lower(col)
    return (F.when(lc.rlike(r"\b(senior|sr|lead|principal|staff)\b"), "senior")
             .when(lc.rlike(r"\b(junior|jr|graduate|entry)\b"), "junior")
             .otherwise("medior"))

def canonical_title(col):
    lc = F.lower(F.trim(col))
    return (F.when(lc.rlike("data engineer"), "Data Engineer")
             .when(lc.rlike("analytics engineer"), "Analytics Engineer")
             .when(lc.rlike("ml engineer|machine learning"), "ML Engineer")
             .when(lc.rlike("data scientist"), "Data Scientist")
             .when(lc.rlike("data analyst"), "Data Analyst")
             .when(lc.rlike("etl developer"), "ETL Developer")
             .otherwise(F.initcap(lc)))

df = spark.table("bronze_postings")
start = df.count()
df = df.filter(F.col("title").isNotNull() & (F.trim(F.col("title")) != ""))
df = df.dropDuplicates(["title", "company", "location", "description"])
df = df.withColumn("title_clean", canonical_title(F.col("title")))
df = df.withColumn("seniority", classify_seniority(F.concat_ws(" ", F.col("title"), F.col("description"))))

s = F.lower(F.coalesce(F.col("salary_raw"), F.lit("")))
cleaned = F.regexp_replace(s, r"[\u20ac,\.]", "")
cleaned = F.regexp_replace(cleaned, r"eur|per year|to", " ")
first = F.regexp_extract(cleaned, r"(\d+)\s*(k?)", 1); first_k = F.regexp_extract(cleaned, r"(\d+)\s*(k?)", 2)
parts = F.split(cleaned, r"-"); second_part = F.coalesce(F.try_element_at(parts, F.lit(2)), F.lit(""))
second = F.regexp_extract(second_part, r"(\d+)\s*(k?)", 1); second_k = F.regexp_extract(second_part, r"(\d+)\s*(k?)", 2)
def to_eur(num, k):
    n = F.when(num == "", None).otherwise(num.cast("double"))
    return F.when((k == "k") | (n < 1000), n * 1000).otherwise(n)
df = df.withColumn("salary_min_eur", to_eur(first, first_k)).withColumn("salary_max_eur", to_eur(second, second_k))
df = df.withColumn("salary_max_eur", F.when(F.col("salary_max_eur").isNull(), F.col("salary_min_eur")).otherwise(F.col("salary_max_eur")))

loc = F.lower(F.trim(F.coalesce(F.col("location"), F.lit(""))))
df = df.withColumn("work_mode", F.when(loc.rlike("remote"), "remote").when(loc.rlike("hybrid"), "hybrid").otherwise("onsite"))
df = df.withColumn("city", F.initcap(F.trim(F.regexp_extract(loc, r"^([a-z\s]+)", 1))))

desc = F.lower(F.coalesce(F.col("description"), F.lit("")))
present = [F.when(desc.contains(sk), F.lit(sk)).otherwise(F.lit(None)) for sk in SKILL_VOCAB]
skills = F.array_except(F.array(*present), F.array(F.lit(None)))
df = df.withColumn("skills", skills).withColumn("skill_count", F.size(skills))

silver = df.select("id", "title_clean", "seniority", "company", "city", "work_mode",
                   "contract_type", "salary_min_eur", "salary_max_eur", "skills",
                   "skill_count", "posted_days_ago", "ingested_at")
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_postings")
end = silver.count()
print(f"SILVER done: {start} -> {end} clean rows -> jobs_pipeline.silver_postings")
