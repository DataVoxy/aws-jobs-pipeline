# Databricks (Delta Lake lakehouse) version

The same medallion pipeline as the local project, running on **Databricks Free
Edition** with **Delta Lake** instead of plain Parquet. This is the modern
lakehouse version and the hands-on, actually-deployed proof of the pipeline.

## Why this exists

The local pipeline proves the transform logic. This proves it on a real managed
lakehouse platform, closing the "modern cloud lakehouse + orchestration" gap. The
AWS Terraform/Glue code in `../infra` and `../aws_glue` remains as the design of
the AWS equivalent.

## Run it (free, no credit card)

1. Sign up for **Databricks Free Edition** (not the paid trial):
   https://docs.databricks.com/aws/en/getting-started/free-edition
   Free Edition replaced the retired Community Edition. No card required.
2. In the workspace, create a notebook (or import `jobs_pipeline_databricks.py`
   as a notebook: Workspace -> Import -> File).
3. Attach it to the provided compute and **Run All**.
4. The notebook creates the `jobs_pipeline` schema and writes Delta tables:
   `bronze_postings`, `silver_postings`, `gold_skill_demand`,
   `gold_role_salary`, `gold_market_summary`.
5. The SQL cells query the gold layer and show Delta time-travel history.

## What it demonstrates beyond the local version

- **Delta Lake:** ACID transactions, schema enforcement, time travel
  (`DESCRIBE HISTORY`, query `AS OF` a version), `MERGE` upserts.
- **Managed catalog:** gold tables queryable with plain SQL from any notebook or
  SQL warehouse.
- **Production path:** wrap the three sections as tasks in a **Databricks Job**
  (Workflows) for scheduling, retries, and alerting, the managed replacement for
  the local `run_pipeline.py` orchestrator.

## The orchestrated Job (3-task DAG) — the orchestration proof

The pipeline is split into three task notebooks so it can run as a real
dependency-aware Databricks Job, not one manual notebook:

| Notebook | Task | Reads | Writes |
|----------|------|-------|--------|
| `01_bronze_ingest.py`   | bronze | (generates) | `bronze_postings` |
| `02_silver_clean.py`    | silver | `bronze_postings` | `silver_postings` |
| `03_gold_aggregate.py`  | gold   | `silver_postings` | `gold_*` |

Tasks pass data through **Delta tables in the catalog**, not in memory, which is
how production multi-task Jobs actually communicate.

### Build the Job (Databricks UI, ~5 min)

1. Import all three `0X_*.py` files as notebooks (Workspace -> Import -> File).
2. **Workflows -> Create job**. Name it `jobs_pipeline`.
3. Add task **bronze**: type Notebook -> `01_bronze_ingest`. No dependencies.
4. Add task **silver**: Notebook -> `02_silver_clean`. **Depends on:** bronze.
5. Add task **gold**: Notebook -> `03_gold_aggregate`. **Depends on:** silver.
6. In job settings add a **retry policy** (e.g. 2 retries, 1-min backoff) and a
   **schedule** (e.g. daily 06:00).
7. **Run now.** The run view shows the DAG bronze -> silver -> gold, per-task
   logs, timings, and status. If silver fails, gold never starts, dependency
   enforcement in action.

### What to screenshot for the portfolio
- The Job run graph (the 3-task DAG, all green).
- The gold SQL output (top skills by demand).
- `DESCRIBE HISTORY jobs_pipeline.silver_postings` (Delta time travel).

### Interview talking point
"I built it as a 3-task Databricks Job with a bronze -> silver -> gold
dependency graph, retry policy, and a daily schedule. Tasks hand off through
Delta tables, so each stage is independently retryable and the DAG won't run a
downstream stage if an upstream one fails." That is the production-orchestration
answer to the gap the Verdo interview flagged.
