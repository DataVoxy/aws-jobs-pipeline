# aws-jobs-pipeline - teardown (PowerShell)
# =========================================
# Destroys everything deploy.ps1 created, so you stop paying for it. Empties the
# S3 bucket first (Terraform can't delete a non-empty bucket), then destroys.
#
# Usage: ./deploy/teardown.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

Push-Location (Join-Path $root "infra")
$bucket = (terraform output -raw bucket 2>$null)
Pop-Location

if ($bucket) {
    Write-Host "==> Emptying s3://$bucket (required before bucket delete)" -ForegroundColor Yellow
    aws s3 rm "s3://$bucket" --recursive
}

Write-Host "==> Destroying all infrastructure" -ForegroundColor Yellow
Push-Location (Join-Path $root "infra")
terraform destroy -auto-approve
Pop-Location

Write-Host "==> Teardown complete. No further AWS charges from this project." -ForegroundColor Green
