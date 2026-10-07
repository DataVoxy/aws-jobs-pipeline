"""
aws-jobs-pipeline - data generator  (synthetic, public-style job postings)
==========================================================================

WHY SYNTHETIC DATA?
A portfolio project must be reproducible by anyone, offline, with no API keys
and no proprietary data. So instead of scraping a live job board (fragile, and
legally messy), we generate realistic *messy* job postings that look like real
scraped data: inconsistent casing, extra whitespace, salary written five
different ways, missing fields, duplicate rows. This is EXACTLY the kind of
"messy text -> clean structure" problem the pipeline then solves.

The output (data/raw/postings.jsonl) stands in for what, in the cloud version,
would land in an S3 "raw" bucket.
"""
from __future__ import annotations

import json
import os
import random

random.seed(42)  # deterministic: same data every run, so results are stable

TITLES = [
    "Data Engineer", "data engineer", "Senior Data Engineer", "DATA ENGINEER",
    "Analytics Engineer", "ML Engineer", "Machine Learning Engineer",
    "Data Analyst", "Senior Data Analyst", "BI Developer",
    "Data Scientist", "Jr Data Scientist", "Platform Engineer",
    "Data Platform Engineer", "Cloud Data Engineer", "ETL Developer",
    "Big Data Engineer", "Software Engineer - Data", "Data Architect",
]

COMPANIES = [
    "Northwind Analytics", "Datafell BV", "Blue Harbor Tech", "Meridian Data",
    "Lowlands Logistics", "Helix AI", "Orange Grove Media", "Kade & Co",
    "Verve Mobility", "Stroom Energy", "Polder Pay", "Tulip Retail",
]

CITIES = [
    "Amsterdam", "amsterdam", "Rotterdam", "Utrecht", "Eindhoven",
    "Den Haag", "The Hague", "Berlin", "Antwerp", "Remote", "remote",
    "Amsterdam, NL", "Hybrid - Utrecht",
]

SKILLS_POOL = [
    "Python", "SQL", "PySpark", "Spark", "AWS", "Azure", "GCP", "dbt",
    "Airflow", "Databricks", "Snowflake", "Kafka", "Terraform", "Docker",
    "Kubernetes", "Scala", "Java", "Power BI", "Tableau", "Redshift",
    "BigQuery", "Glue", "Athena", "S3", "ETL", "data modeling",
]

SENIORITY_HINTS = {
    "junior": ["junior", "jr", "graduate", "entry level", "medior/junior"],
    "medior": ["medior", "mid-level", "experienced"],
    "senior": ["senior", "sr", "lead", "principal", "staff"],
}

CONTRACTS = ["Permanent", "permanent", "Contract", "contract", "Freelance", "", "Fulltime"]


def messy_salary() -> str:
    """Return a salary string in one of several messy real-world formats (or empty)."""
    style = random.random()
    lo = random.choice([45, 50, 55, 60, 65, 70, 80])
    hi = lo + random.choice([10, 15, 20, 25])
    if style < 0.20:
        return ""  # many postings hide salary
    if style < 0.45:
        return f"\u20ac{lo}.000 - \u20ac{hi}.000"           # euro, dotted thousands
    if style < 0.65:
        return f"{lo}k-{hi}k EUR"                           # k shorthand
    if style < 0.80:
        return f"\u20ac{lo*1000:,} to \u20ac{hi*1000:,}"    # comma thousands
    if style < 0.92:
        return f"{lo},000 - {hi},000 per year"
    return "competitive"                                   # useless but common


def build_description(skills: list[str], seniority_word: str) -> str:
    """Assemble a noisy free-text description that embeds the skills + seniority."""
    joined = ", ".join(skills)
    blurbs = [
        f"We are looking for a {seniority_word} professional to join our team. ",
        f"You will work with {joined}. ",
        f"  Experience with {random.choice(skills)} is a strong plus.  ",
        "Responsibilities include building and maintaining data pipelines. ",
        f"Tech stack: {joined}.",
    ]
    random.shuffle(blurbs)
    # Inject messy whitespace and casing like real scraped HTML-to-text.
    return "   ".join(blurbs).replace(". ", ".  ")


def main() -> None:
    out_dir = os.path.join(os.path.dirname(__file__), "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "postings.jsonl")

    rows = []
    n = 600
    for i in range(n):
        title = random.choice(TITLES)
        seniority_level = random.choice(["junior", "medior", "senior"])
        seniority_word = random.choice(SENIORITY_HINTS[seniority_level])
        skills = random.sample(SKILLS_POOL, k=random.randint(3, 8))

        row = {
            "id": f"job-{i:04d}",
            "title": title,
            "company": random.choice(COMPANIES),
            "location": random.choice(CITIES),
            "contract_type": random.choice(CONTRACTS),
            "salary_raw": messy_salary(),
            "posted_days_ago": random.randint(0, 90),
            "description": build_description(skills, seniority_word),
        }
        rows.append(row)

    # Inject ~5% exact-duplicate rows (real scrapes double-collect postings).
    dupes = random.sample(rows, k=int(n * 0.05))
    rows.extend(dupes)
    # And a handful of rows with a missing title (dirty data to clean out).
    for _ in range(8):
        bad = dict(random.choice(rows))
        bad["title"] = random.choice(["", "   ", None])
        bad["id"] = f"job-bad-{random.randint(1000,9999)}"
        rows.append(bad)

    random.shuffle(rows)

    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Wrote {len(rows)} raw job postings to {path}")
    print("  (includes intentional duplicates + dirty rows for the pipeline to clean)")


if __name__ == "__main__":
    main()
