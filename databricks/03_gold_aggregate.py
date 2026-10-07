# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3 of 3 - GOLD aggregate
# MAGIC
# MAGIC Final task. Reads `silver_postings`, builds the three business-ready GOLD
# MAGIC Delta tables: skill demand (with a rank WINDOW function), salary bands per
# MAGIC role x seniority, and a one-row market summary. Depends on the silver task.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE jobs_pipeline")

silver = spark.table("silver_postings")
salary_mid = (F.col("salary_min_eur") + F.col("salary_max_eur")) / 2

# GOLD 1: skill demand + rank window function
exploded = silver.withColumn("skill", F.explode("skills")).withColumn("salary_mid", salary_mid)
skill_demand = (exploded.groupBy("skill")
    .agg(F.count("*").alias("postings"), F.round(F.avg("salary_mid")).alias("avg_salary_eur")))
skill_demand = skill_demand.withColumn("demand_rank", F.rank().over(Window.orderBy(F.desc("postings")))).orderBy("demand_rank")
skill_demand.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_skill_demand")

# GOLD 2: salary bands per role x seniority
role_salary = (silver.withColumn("salary_mid", salary_mid).filter(F.col("salary_mid").isNotNull())
    .groupBy("title_clean", "seniority")
    .agg(F.count("*").alias("postings"), F.round(F.avg("salary_min_eur")).alias("avg_min_eur"),
         F.round(F.avg("salary_max_eur")).alias("avg_max_eur")).orderBy(F.desc("postings")))
role_salary.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_role_salary")

# GOLD 3: one-row market summary
market = silver.agg(
    F.count("*").alias("total_postings"),
    F.round(F.avg(F.when(F.col("work_mode") == "remote", 1).otherwise(0)) * 100, 1).alias("remote_pct"),
    F.round(F.avg("skill_count"), 1).alias("avg_skills_per_posting"),
    F.round(F.avg(salary_mid)).alias("avg_salary_eur"),
    F.countDistinct("company").alias("distinct_companies"))
market.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_market_summary")

print("GOLD done: gold_skill_demand, gold_role_salary, gold_market_summary")

# COMMAND ----------

# MAGIC %md ## Verify: top skills by demand

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT skill, postings, avg_salary_eur, demand_rank
# MAGIC FROM jobs_pipeline.gold_skill_demand
# MAGIC ORDER BY demand_rank LIMIT 12;
