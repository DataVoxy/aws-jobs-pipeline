# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 of 3 - BRONZE ingest
# MAGIC
# MAGIC First task in the `jobs_pipeline` Databricks Job (Workflow). Generates the
# MAGIC synthetic messy postings and writes them to a Delta table. Downstream tasks
# MAGIC (silver, gold) read from the catalog, so tasks communicate through Delta
# MAGIC tables, not in-memory, which is how real multi-task Jobs pass data.

# COMMAND ----------

from pyspark.sql import functions as F
import random

spark.sql("CREATE SCHEMA IF NOT EXISTS jobs_pipeline")
spark.sql("USE jobs_pipeline")

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
rows += random.sample(rows, k=30)
for _ in range(8):
    bad = dict(random.choice(rows)); bad["title"] = ""; bad["id"] = f"bad-{random.randint(1000,9999)}"; rows.append(bad)
random.shuffle(rows)

bronze = spark.createDataFrame(rows).withColumn("ingested_at", F.current_timestamp())
bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_postings")
print(f"BRONZE done: {bronze.count()} raw rows -> jobs_pipeline.bronze_postings")
