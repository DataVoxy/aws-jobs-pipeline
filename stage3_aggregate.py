"""
aws-jobs-pipeline - Stage 3: Aggregate (GOLD)
=============================================

Turn the clean SILVER rows into business-ready GOLD tables that answer real
questions a job-market dashboard would ask:

  1. skill_demand   : how often each skill appears, + its average salary
                      (uses explode + groupBy, and a WINDOW FUNCTION to rank).
  2. role_salary    : average salary band per canonical role x seniority.
  3. market_summary : one-row headline KPIs (total roles, remote share, etc.).

CLOUD EQUIVALENT: a Glue job silver (S3) -> gold (S3), then exposed to Athena /
QuickSight for querying + dashboards.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_setup import get_spark, SILVER_PATH, GOLD_PATH


def main() -> None:
    spark = get_spark("jobs-pipeline-stage3-aggregate")
    silver = spark.read.parquet(SILVER_PATH)
    print(f"Read {silver.count()} clean rows from silver.")

    salary_mid = ((F.col("salary_min_eur") + F.col("salary_max_eur")) / 2)

    # --- GOLD 1: skill demand + ranking (WINDOW FUNCTION) ---
    # explode() turns the skills array into one row per skill, so we can count
    # demand per skill across all postings.
    exploded = silver.withColumn("skill", F.explode("skills")).withColumn(
        "salary_mid", salary_mid
    )
    skill_demand = (
        exploded.groupBy("skill")
        .agg(
            F.count("*").alias("postings"),
            F.round(F.avg("salary_mid")).alias("avg_salary_eur"),
        )
    )
    # WINDOW FUNCTION: rank skills by demand (most in-demand = rank 1). A window
    # with no partition ranks across the whole result set.
    rank_w = Window.orderBy(F.desc("postings"))
    skill_demand = skill_demand.withColumn("demand_rank", F.rank().over(rank_w))
    skill_demand = skill_demand.orderBy("demand_rank")

    print("\n--- GOLD 1: top skills by demand ---")
    skill_demand.show(12, truncate=False)

    # --- GOLD 2: salary band per role x seniority ---
    role_salary = (
        silver.withColumn("salary_mid", salary_mid)
        .filter(F.col("salary_mid").isNotNull())
        .groupBy("title_clean", "seniority")
        .agg(
            F.count("*").alias("postings"),
            F.round(F.avg("salary_min_eur")).alias("avg_min_eur"),
            F.round(F.avg("salary_max_eur")).alias("avg_max_eur"),
        )
        .orderBy(F.desc("postings"))
    )
    print("--- GOLD 2: salary by role x seniority ---")
    role_salary.show(12, truncate=False)

    # --- GOLD 3: one-row market summary (headline KPIs) ---
    market_summary = silver.agg(
        F.count("*").alias("total_postings"),
        F.round(F.avg(F.when(F.col("work_mode") == "remote", 1).otherwise(0)) * 100, 1).alias(
            "remote_pct"
        ),
        F.round(F.avg("skill_count"), 1).alias("avg_skills_per_posting"),
        F.round(F.avg(salary_mid)).alias("avg_salary_eur"),
        F.countDistinct("company").alias("distinct_companies"),
    )
    print("--- GOLD 3: market summary ---")
    market_summary.show(truncate=False)

    # Write all three gold tables as Parquet (Athena/BI would read these).
    os.makedirs(GOLD_PATH, exist_ok=True)
    skill_demand.coalesce(1).write.mode("overwrite").parquet(
        os.path.join(GOLD_PATH, "skill_demand")
    )
    role_salary.coalesce(1).write.mode("overwrite").parquet(
        os.path.join(GOLD_PATH, "role_salary")
    )
    market_summary.coalesce(1).write.mode("overwrite").parquet(
        os.path.join(GOLD_PATH, "market_summary")
    )
    print(f"\nStage 3 complete. Gold tables written under:\n  {GOLD_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
    import os as _os
    _os._exit(0)
