#!/usr/bin/env bash
# aws-jobs-pipeline - one-shot deploy (bash / Linux / macOS / WSL)
# ================================================================
# Mirror of deploy.ps1 for non-Windows shells. Provisions the AWS stack with
# Terraform, uploads Glue scripts, seeds raw data, and starts the pipeline.
#
# PREREQS: AWS CLI v2 (`aws configure` done), Terraform, bucket_name set in
# infra/main.tf. Usage: ./deploy/deploy.sh   Teardown: ./deploy/teardown.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> 0. Sanity check: who am I on AWS?"
aws sts get-caller-identity >/dev/null || {
  echo "AWS credentials invalid. Run 'aws configure' with a valid key first." >&2
  exit 1
}

echo "==> 1. Provision infrastructure (Terraform)"
pushd "$ROOT/infra" >/dev/null
terraform init -input=false
terraform apply -auto-approve
BUCKET="$(terraform output -raw bucket)"
SFN_ARN="$(terraform output -raw state_machine_arn)"
popd >/dev/null
echo "    bucket=$BUCKET  state_machine=$SFN_ARN"

echo "==> 2. Upload Glue scripts to s3://$BUCKET/scripts/"
aws s3 cp "$ROOT/aws_glue" "s3://$BUCKET/scripts/" --recursive

echo "==> 3. Generate + seed raw data to s3://$BUCKET/raw/"
python "$ROOT/generate_data.py"
aws s3 cp "$ROOT/data/raw/postings.jsonl" "s3://$BUCKET/raw/postings.jsonl"

echo "==> 4. Start the pipeline (Step Functions)"
EXEC="$(aws stepfunctions start-execution --state-machine-arn "$SFN_ARN" \
        --query executionArn --output text)"
echo "    execution: $EXEC"
echo "    status: aws stepfunctions describe-execution --execution-arn $EXEC --query status"

echo "==> Done. On SUCCESS, query gold in Athena:"
echo "    SELECT skill, postings, avg_salary_eur, demand_rank"
echo "    FROM jobs_pipeline.skill_demand ORDER BY demand_rank LIMIT 10;"
