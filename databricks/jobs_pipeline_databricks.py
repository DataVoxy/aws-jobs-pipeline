# Databricks notebook source
# MAGIC %md
# MAGIC # aws-jobs-pipeline on Databricks (Delta Lake lakehouse)
# MAGIC
# MAGIC The same medallion pipeline (messy job postings -> bronze -> silver -> gold),
# MAGIC ported to **Databricks Free Edition** using **Delta Lake** instead of plain
# MAGIC Parquet. This is the modern-lakehouse version of the local pipeline.
# MAGIC
# MAGIC **What changes vs. the local version, and why it matters in an interview:**
# MAGIC - **Delta Lake** tables instead of Parquet: ACID transactions, schema
# MAGIC   enforcement, time-travel/versioning, `MERGE` upserts.
# MAGIC - **Managed catalog** (`jobs_pipeline` schema) so the gold tables are
# MAGIC   queryable with plain SQL from any notebook or SQL warehouse.
# MAGIC - **Databricks Jobs** (not shown in-notebook) schedule + orchestrate this
# MAGIC   with retries and alerting, the production replacement for run_pipeline.py.
# MAGIC
# MAGIC The transform LOGIC (clean, dedupe, salary parsing, skills extraction,
# MAGIC window-function ranking) is identical to the local stage2/stage3 scripts.
# MAGIC
# MAGIC **How to run:** paste this into a Databricks notebook (or import the .py as a
# MAGIC notebook) and Run All. Spark is already provided by the Databricks runtime,
# MAGIC so there is no local setup.

# COMMAND ----------

# MAGIC %md ## 0. Setup: catalog + schema

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Free Edition gives you a workspace catalog. Create a schema for this project.
spark.sql("CREATE SCHEMA IF NOT EXISTS jobs_pipeline")
spark.sql("USE jobs_pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate synthetic messy postings (BRONZE source)
# MAGIC Same generator idea as local `generate_data.py`, inline so the notebook is
# MAGIC self-contained. In a real deployment this data would land in cloud storage
# MAGIC and bronze would read it from there.

# COMMAND ----------

import random
random.seed(42)

TITLES = ["Data Engineer", "data engineer", "Senior Data Engineer", "DATA ENGINEER",
          "Analytics Engineer", "ML Engineer", "Machine Learning Engineer",
          "Data Analyst", "Senior Data Analyst", "Data Scientist", "ETL Developer"]
COMPANIES = ["Northwind Analytics", "Datafell BV", "Blue Harbor Tech", "Meridian Data",
             "Helix AI", "Polder Pay", "Tulip Retail", "Stroom Energy"]
CITIES = ["Amsterdam", "amsterdam", "Rotterdam", "Utrecht", "Berlin", "Remote",
          "remote", "Amsterdam, NL", "Hybrid - Utrecht", "The Hague"]
SKILLS = ["Python", "SQL", "PySpark", "Spark", "AWS", "Azure", "dbt", "Airflow",
          "Databricks", "Snowflake", "Kafka", "Terraform", "Scala", "Power BI",
          "Redshift", "BigQuery", "Glue", "Athena", "S3", "ETL", "data modeling"]
SENIORITY = {"junior": ["junior", "jr", "graduate"], "medior": ["medior", "experienced"],
             "senior": ["senior", "sr", "lead", "principal", "staff"]}
CONTRACTS = ["Permanent", "permanent", "Contract", "Freelance", "", "Fulltime"]

def messy_salary():
    s = random.random(); lo = random.choice([45,50,55,60,65,70,80]); hi = lo + random.choice([10,15,20,25])
    if s < 0.20: return ""
    if s < 0.45: return f"\u20ac{lo}.000 - \u20ac{hi}.000"
    if s < 0.65: return f"{lo}k-{hi}k EUR"
    if s < 0.80: return f"\u20ac{lo*1000:,} to \u20ac{hi*1000:,}"
    if s < 0.92: return f"{lo},000 - {hi},000 per year"
    return "competitive"

rows = []
for i in range(600):
    lvl = random.choice(list(SENIORITY)); word = random.choice(SENIORITY[lvl])
    sk = random.sample(SKILLS, k=random.randint(3, 8))
    desc = f"We seek a {word} professional. You will work with {', '.join(sk)}. Tech stack: {', '.join(sk)}."
    rows.append({"id": f"job-{i:04d}", "title": random.choice(TITLES),
                 "company": random.choice(COMPANIES), "location": random.choice(CITIES),
                 "contract_type": random.choice(CONTRACTS), "salary_raw": messy_salary(),
                 "posted_days_ago": random.randint(0, 90), "description": desc})
# inject dupes + dirty rows
rows += random.sample(rows, k=30)
for _ in range(8):
    bad = dict(random.choice(rows)); bad["title"] = ""; bad["id"] = f"bad-{random.randint(1000,9999)}"; rows.append(bad)
random.shuffle(rows)

bronze = spark.createDataFrame(rows).withColumn("ingested_at", F.current_timestamp())
bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_postings")
print(f"Bronze: {bronze.count()} raw rows written to Delta table jobs_pipeline.bronze_postings")

# COMMAND ----------

# MAGIC %md ## 2. SILVER: clean, dedupe, parse, extract skills (same logic as local)

# COMMAND ----------

SKILL_VOCAB = [s.lower() for s in SKILLS]

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
df = df.filter(F.col("title").isNotNull() & (F.trim(F.col("title")) != ""))
df = df.dropDuplicates(["title", "company", "location", "description"])
df = df.withColumn("title_clean", canonical_title(F.col("title")))
df = df.withColumn("seniority", classify_seniority(F.concat_ws(" ", F.col("title"), F.col("description"))))

# messy salary -> numeric min/max (try_element_at tolerates single-value salaries)
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

# location -> city + work_mode
loc = F.lower(F.trim(F.coalesce(F.col("location"), F.lit(""))))
df = df.withColumn("work_mode", F.when(loc.rlike("remote"), "remote").when(loc.rlike("hybrid"), "hybrid").otherwise("onsite"))
df = df.withColumn("city", F.initcap(F.trim(F.regexp_extract(loc, r"^([a-z\s]+)", 1))))

# skills[] from free text
desc = F.lower(F.coalesce(F.col("description"), F.lit("")))
present = [F.when(desc.contains(sk), F.lit(sk)).otherwise(F.lit(None)) for sk in SKILL_VOCAB]
skills = F.array_except(F.array(*present), F.array(F.lit(None)))
df = df.withColumn("skills", skills).withColumn("skill_count", F.size(skills))

silver = df.select("id", "title_clean", "seniority", "company", "city", "work_mode",
                   "contract_type", "salary_min_eur", "salary_max_eur", "skills",
                   "skill_count", "posted_days_ago", "ingested_at")
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_postings")
print(f"Silver: {silver.count()} clean rows written to Delta table jobs_pipeline.silver_postings")

# COMMAND ----------

# MAGIC %md ## 3. GOLD: skill demand (window fn), salary bands, market summary

# COMMAND ----------

silver = spark.table("silver_postings")
salary_mid = (F.col("salary_min_eur") + F.col("salary_max_eur")) / 2

exploded = silver.withColumn("skill", F.explode("skills")).withColumn("salary_mid", salary_mid)
skill_demand = (exploded.groupBy("skill")
    .agg(F.count("*").alias("postings"), F.round(F.avg("salary_mid")).alias("avg_salary_eur")))
skill_demand = skill_demand.withColumn("demand_rank", F.rank().over(Window.orderBy(F.desc("postings")))).orderBy("demand_rank")
skill_demand.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_skill_demand")

role_salary = (silver.withColumn("salary_mid", salary_mid).filter(F.col("salary_mid").isNotNull())
    .groupBy("title_clean", "seniority")
    .agg(F.count("*").alias("postings"), F.round(F.avg("salary_min_eur")).alias("avg_min_eur"),
         F.round(F.avg("salary_max_eur")).alias("avg_max_eur")).orderBy(F.desc("postings")))
role_salary.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_role_salary")

market = silver.agg(
    F.count("*").alias("total_postings"),
    F.round(F.avg(F.when(F.col("work_mode") == "remote", 1).otherwise(0)) * 100, 1).alias("remote_pct"),
    F.round(F.avg("skill_count"), 1).alias("avg_skills_per_posting"),
    F.round(F.avg(salary_mid)).alias("avg_salary_eur"),
    F.countDistinct("company").alias("distinct_companies"))
market.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_market_summary")

print("Gold Delta tables written: gold_skill_demand, gold_role_salary, gold_market_summary")

# COMMAND ----------

# MAGIC %md ## 4. Query the gold layer with SQL (the payoff)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT skill, postings, avg_salary_eur, demand_rank
# MAGIC FROM jobs_pipeline.gold_skill_demand
# MAGIC ORDER BY demand_rank
# MAGIC LIMIT 12;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta Lake feature: time travel
# MAGIC Because these are Delta tables (not plain Parquet), every write is a
# MAGIC versioned transaction. You can query a table AS OF an earlier version,
# MAGIC something Parquet cannot do. This is a concrete talking point for interviews.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY jobs_pipeline.silver_postings;
