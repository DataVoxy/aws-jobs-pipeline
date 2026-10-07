# aws-jobs-pipeline - one-shot deploy (PowerShell)
# =================================================
# Provisions the AWS stack with Terraform, uploads the Glue scripts, seeds the
# raw data to S3, and kicks off the Step Functions pipeline.
#
# PREREQS (run once, outside this script):
#   - AWS CLI v2 installed and `aws configure` done with a valid key
#   - Terraform installed
#   - bucket_name set in infra/main.tf (globally unique)
#
# This script is idempotent-ish: re-running re-applies Terraform (no-op if
# nothing changed) and re-uploads scripts/data.
#
# Usage:   ./deploy/deploy.ps1
# Teardown: ./deploy/teardown.ps1   (destroys everything to stop charges)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot   # repo root (deploy/ is one level down)

Write-Host "==> 0. Sanity check: who am I on AWS?" -ForegroundColor Cyan
aws sts get-caller-identity
if ($LASTEXITCODE -ne 0) {
    Write-Error "AWS credentials invalid. Run 'aws configure' with a valid access key first."
    exit 1
}

Write-Host "`n==> 1. Provision infrastructure (Terraform)" -ForegroundColor Cyan
Push-Location (Join-Path $root "infra")
terraform init -input=false
terraform apply -auto-approve
# Capture outputs we need for the next steps.
$bucket = (terraform output -raw bucket)
$sfnArn = (terraform output -raw state_machine_arn)
Pop-Location
Write-Host "    bucket           = $bucket"
Write-Host "    state_machine    = $sfnArn"

Write-Host "`n==> 2. Upload Glue scripts to s3://$bucket/scripts/" -ForegroundColor Cyan
aws s3 cp (Join-Path $root "aws_glue") "s3://$bucket/scripts/" --recursive

Write-Host "`n==> 3. Generate raw data locally and seed it to s3://$bucket/raw/" -ForegroundColor Cyan
python (Join-Path $root "generate_data.py")
aws s3 cp (Join-Path $root "data/raw/postings.jsonl") "s3://$bucket/raw/postings.jsonl"

Write-Host "`n==> 4. Start the pipeline (Step Functions)" -ForegroundColor Cyan
$exec = aws stepfunctions start-execution --state-machine-arn $sfnArn --query executionArn --output text
Write-Host "    execution: $exec"
Write-Host "    Watch it: Step Functions console, or:"
Write-Host "    aws stepfunctions describe-execution --execution-arn $exec --query status"

Write-Host "`n==> Done. When the execution SUCCEEDS, query the gold layer in Athena:" -ForegroundColor Green
Write-Host "    SELECT skill, postings, avg_salary_eur, demand_rank"
Write-Host "    FROM jobs_pipeline.skill_demand ORDER BY demand_rank LIMIT 10;"
