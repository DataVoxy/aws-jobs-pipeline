#!/usr/bin/env bash
# aws-jobs-pipeline - teardown (bash)
# ===================================
# Destroys everything deploy.sh created. Empties the S3 bucket first (Terraform
# cannot delete a non-empty bucket), then terraform destroy.
# Usage: ./deploy/teardown.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pushd "$ROOT/infra" >/dev/null
BUCKET="$(terraform output -raw bucket 2>/dev/null || true)"
popd >/dev/null

if [ -n "${BUCKET:-}" ]; then
  echo "==> Emptying s3://$BUCKET (required before bucket delete)"
  aws s3 rm "s3://$BUCKET" --recursive
fi

echo "==> Destroying all infrastructure"
pushd "$ROOT/infra" >/dev/null
terraform destroy -auto-approve
popd >/dev/null

echo "==> Teardown complete. No further AWS charges from this project."
