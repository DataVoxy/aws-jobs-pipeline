# aws-jobs-pipeline — a cloud-ready medallion pipeline for messy job-posting text

A data engineering pipeline that turns **messy, scraped-style job postings** into
clean, queryable analytics, built with PySpark on the industry-standard
**bronze / silver / gold** (medallion) architecture, and designed to deploy to
**AWS (S3 + Glue + Step Functions + Athena)**.

It runs locally for development and the identical transform logic runs as AWS
Glue jobs in the cloud. The project focuses on the problem type I'm strongest at:
**turning unstructured, inconsistent text into reliable structure**.

> Companion to [`medallion-pipeline`](https://github.com/DataVoxy/medallion-pipeline)
> (gold/dollar market data). This project applies the same medallion discipline
> to a text-heavy NLP-flavoured problem and adds a real cloud deployment design.

---

## What problem it solves

Scraped job postings are a mess: titles in five casings (`DATA ENGINEER`,
`data engineer`), salaries in five formats (`€55.000 - €70.000`, `55k-70k EUR`,
`competitive`, blank), locations that mix city and work-mode (`Hybrid - Utrecht`,
`Remote`), duplicates, and skills buried in free text. This pipeline cleans all
of that into typed columns plus an extracted `skills[]` array, then aggregates it
into a job-market view (skill demand, salary bands, headline KPIs).

The dataset is **synthetic and generated locally** (`generate_data.py`), so the
project is fully reproducible, offline, with no API keys and no proprietary data.

---

## Architecture

```
                 generate_data.py  (synthetic messy postings, JSON lines)
                                   │
                                   ▼                      LOCAL          CLOUD
          ┌───────────────────────────────────────┐   data/raw/     s3://.../raw/
  STAGE 1 │  BRONZE   raw ingest, as-is + ingest ts │   data/bronze/  s3://.../bronze/
          └───────────────────────────────────────┘
                                   │
                                   ▼
          ┌───────────────────────────────────────┐
  STAGE 2 │  SILVER   drop dirty rows · dedupe ·    │   data/silver/  s3://.../silver/
          │           canonical title · seniority · │
          │           parse messy salary → min/max ·│
          │           normalise location · extract  │
          │           skills[] from free text       │
          └───────────────────────────────────────┘
                                   │
                                   ▼
          ┌───────────────────────────────────────┐
  STAGE 3 │  GOLD     skill_demand (+ rank WINDOW)  │   data/gold/    s3://.../gold/
          │           role_salary bands             │
          │           market_summary KPIs           │
          └───────────────────────────────────────┘
                                   │
                 LOCAL  ─▶ run_pipeline.py (sequential, abort-on-fail)
                 CLOUD  ─▶ Step Functions state machine (Glue jobs) ─▶ Athena / BI
```

---

## Concepts demonstrated

- **Messy text → structure:** regex/keyword extraction, canonicalisation,
  multi-format salary parsing — the "unstructured to JSON/columns" problem at the
  core of the project.
- **Spark fundamentals:** partitions, shuffles, lazy evaluation, actions vs
  transformations.
- **Window functions:** ranking skills by demand with `Window` + `rank()`.
- **Aggregations:** `groupBy` + `agg`, `explode` of an array column, `countDistinct`.
- **Data quality:** dropping dirty rows, deduplication on a business key, typed
  schema.
- **Medallion layering:** bronze (raw) → silver (clean) → gold (business-ready).
- **Orchestration:** local sequential runner, plus a production Step Functions
  design with Glue jobs.
- **Infrastructure as code:** a Terraform skeleton provisioning the whole AWS stack.

---

## Run it locally

```bash
pip install -r requirements.txt
python run_pipeline.py
```

Requires a JDK (17 recommended). On Windows the scripts set `JAVA_HOME`,
`PYSPARK_PYTHON`, and `HADOOP_HOME` automatically (winutils is bundled in
`hadoop/`). The run generates data, then executes all three stages, aborting if
any stage fails.

### Verified result (local run)

```
Generate data            OK   (638 raw postings, with duplicates + dirty rows)
Stage 1 - Ingest (bronze) OK   638 rows ingested
Stage 2 - Clean (silver)  OK   638 → 630 (dropped 8 dirty) → 600 (dropped 30 dupes)
Stage 3 - Aggregate (gold)OK   skill_demand · role_salary · market_summary written
PIPELINE COMPLETE in ~35s. Gold tables in data/gold/ (Parquet).
```

---

## Files

| File | Purpose |
|------|---------|
| `generate_data.py`        | Create synthetic messy job postings (raw layer) |
| `spark_setup.py`          | Local Spark session + Windows env fixes + paths |
| `stage1_ingest.py`        | Bronze: raw ingest into Parquet |
| `stage2_clean.py`         | Silver: clean, dedupe, parse, extract skills |
| `stage3_aggregate.py`     | Gold: skill demand (window), salary bands, KPIs |
| `run_pipeline.py`         | Local orchestrator (sequential, abort-on-fail) |
| `aws_glue/`               | Glue-adapted stage scripts (same logic, S3 I/O) |
| `infra/main.tf`           | Terraform: S3 lake, Glue jobs + crawler, Step Functions, IAM |

---

## Deploy to AWS (step by step)

> This runs locally today. The steps below deploy the same pipeline to AWS. They
> require an AWS account (free tier covers most of this) and the AWS CLI
> configured. Glue and crawler runs incur small charges, so destroy when done.

1. **Prereqs:** create an AWS account, install the AWS CLI, run `aws configure`
   with an IAM user that has admin (for setup) or the needed Glue/S3/Step
   Functions/IAM permissions. Install Terraform.

2. **Set your bucket name** in `infra/main.tf` (`bucket_name`, must be globally
   unique) and region if not `eu-central-1`.

3. **Provision infra:**
   ```bash
   cd infra
   terraform init
   terraform apply        # creates the S3 bucket, Glue DB, jobs, crawler, Step Functions
   ```

4. **Upload the Glue scripts** to the bucket:
   ```bash
   aws s3 cp ../aws_glue/ s3://<your-bucket>/scripts/ --recursive
   ```

5. **Seed raw data** (generate locally, push to S3 raw):
   ```bash
   cd .. && python generate_data.py
   aws s3 cp data/raw/postings.jsonl s3://<your-bucket>/raw/postings.jsonl
   ```

6. **Run the pipeline** via Step Functions:
   ```bash
   aws stepfunctions start-execution --state-machine-arn <state_machine_arn from terraform output>
   ```
   Step Functions runs ingest → clean → aggregate → crawler, stopping on any
   failure. Watch it in the Step Functions console (visual DAG, per-step logs).

7. **Query the gold layer in Athena:**
   ```sql
   SELECT skill, postings, avg_salary_eur, demand_rank
   FROM jobs_pipeline.skill_demand
   ORDER BY demand_rank
   LIMIT 10;
   ```

8. **Tear down** to stop any charges:
   ```bash
   cd infra && terraform destroy
   ```

### One-shot scripts

Steps 3-7 are wrapped into a single script, and the teardown into another:

```bash
./deploy/deploy.ps1      # Windows PowerShell   (or ./deploy/deploy.sh on bash)
./deploy/teardown.ps1    # destroy everything    (or ./deploy/teardown.sh)
```

The deploy script runs `aws sts get-caller-identity` first and stops early if
credentials are invalid, so it fails loudly rather than halfway through.

### What the cloud layer adds over `run_pipeline.py`

`run_pipeline.py` enforces order and aborts on failure — the core idea, but
deliberately minimal. **AWS Step Functions** adds what production needs:
scheduling (EventBridge trigger), automatic retries with backoff, per-step
monitoring and alerting, a visual DAG, and parallelism where steps are
independent. **AWS Glue** replaces the local Spark setup with a managed,
autoscaling Spark runtime. **Athena + Glue Data Catalog** make the gold layer
queryable with plain SQL and connectable to QuickSight/BI.

---

## Honest status

- Local pipeline: **built and verified end to end** (output above).
- AWS deployment: **designed and codified** (Terraform + Glue scripts + steps).
  It is ready to apply; I run it on a real AWS account as the next step so I can
  speak to it first-hand in interviews, not just from the design.

---
Built by [DataVoxy](https://datavoxy.com).
