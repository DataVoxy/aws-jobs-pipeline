"""
aws-jobs-pipeline - orchestrator
================================

Runs the three stages in order (ingest -> clean -> aggregate) and ABORTS the
whole run if any stage fails, because each stage depends on the previous one's
output. This is the minimal, hand-coded version of what Airflow / Step Functions
/ Glue Workflows do in production.

In the cloud, this file is replaced by AWS Step Functions (a visual state machine
with the three Glue jobs as steps) which adds: scheduling, automatic retries with
backoff, per-step monitoring/alerting, and parallelism where steps are
independent. See infra/ and README for that design.
"""
from __future__ import annotations

import subprocess
import sys
import time

STAGES = [
    ("Generate data", "generate_data.py"),
    ("Stage 1 - Ingest (bronze)", "stage1_ingest.py"),
    ("Stage 2 - Clean (silver)", "stage2_clean.py"),
    ("Stage 3 - Aggregate (gold)", "stage3_aggregate.py"),
]


def main() -> None:
    print("=" * 60)
    print("  aws-jobs-pipeline  |  medallion  bronze -> silver -> gold")
    print("=" * 60)
    overall_start = time.time()

    for label, script in STAGES:
        print(f"\n>>> {label}  ({script})")
        start = time.time()
        result = subprocess.run([sys.executable, script])
        elapsed = time.time() - start
        if result.returncode != 0:
            print(f"\n!!! {label} FAILED (exit {result.returncode}). Aborting pipeline.")
            print("    Downstream stages depend on this output, so we stop here.")
            sys.exit(1)
        print(f"<<< {label} OK in {elapsed:.1f}s")

    total = time.time() - overall_start
    print("\n" + "=" * 60)
    print(f"  PIPELINE COMPLETE in {total:.1f}s. Gold tables are in data/gold/.")
    print("=" * 60)


if __name__ == "__main__":
    main()
