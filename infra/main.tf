# aws-jobs-pipeline - infrastructure as code (Terraform)
# =======================================================
# This defines the AWS resources the pipeline needs IN THE CLOUD:
#   - an S3 bucket with raw/bronze/silver/gold prefixes (the data lake)
#   - a Glue database + crawler (so Athena can see the gold tables)
#   - three Glue PySpark jobs (one per stage)
#   - a Step Functions state machine that orchestrates the three jobs in order
#   - the IAM role Glue + Step Functions assume
#
# This is a SKELETON: it is complete enough to read and understand, and to apply
# with `terraform apply` once you have an AWS account + credentials configured.
# Values marked CHANGE_ME must be set before applying. Nothing here runs until
# you run terraform, so it is safe to keep in the repo.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "eu-central-1" # Frankfurt - closest to NL
}

variable "bucket_name" {
  type    = string
  default = "CHANGE_ME-jobs-pipeline-datalake" # must be globally unique
}

# ---------------------------------------------------------------------------
# S3 data lake: one bucket, four prefixes mirroring the medallion layers.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

# Block all public access - a data lake is never public.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Where Glue scripts live.
resource "aws_s3_object" "scripts_prefix" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/"
}

# ---------------------------------------------------------------------------
# IAM role that Glue jobs assume (read/write the lake + run Glue).
# ---------------------------------------------------------------------------
resource "aws_iam_role" "glue" {
  name = "jobs-pipeline-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Least-privilege access to just this bucket.
resource "aws_iam_role_policy" "glue_s3" {
  name = "jobs-pipeline-glue-s3"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
    }]
  })
}

# ---------------------------------------------------------------------------
# Glue Data Catalog database (Athena queries the gold tables through this).
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "jobs" {
  name = "jobs_pipeline"
}

# A crawler that reads the gold prefix and registers table schemas so Athena
# can query them with SQL. Run it after the gold job finishes.
resource "aws_glue_crawler" "gold" {
  name          = "jobs-pipeline-gold-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.jobs.name
  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/gold/"
  }
}

# ---------------------------------------------------------------------------
# Three Glue PySpark jobs - one per stage. Each runs the matching script that
# you upload to s3://<bucket>/scripts/. The scripts are the aws_glue/*.py files.
# ---------------------------------------------------------------------------
locals {
  stages = {
    stage1 = "aws_glue_stage1_ingest.py"
    stage2 = "aws_glue_stage2_clean.py"
    stage3 = "aws_glue_stage3_aggregate.py"
  }
}

resource "aws_glue_job" "stage" {
  for_each = local.stages

  name     = "jobs-pipeline-${each.key}"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/scripts/${each.value}"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2 # smallest viable; raise for real volume

  default_arguments = {
    "--bucket"             = aws_s3_bucket.lake.bucket
    "--job-language"       = "python"
    "--enable-metrics"     = "true"
    "--TempDir"            = "s3://${aws_s3_bucket.lake.bucket}/tmp/"
  }
}

# ---------------------------------------------------------------------------
# Step Functions: orchestrate stage1 -> stage2 -> stage3 -> crawler, in order,
# stopping on any failure. This is the production replacement for run_pipeline.py.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "sfn" {
  name = "jobs-pipeline-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_glue" {
  name = "jobs-pipeline-sfn-glue"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun", "glue:StartCrawler"]
      Resource = "*"
    }]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "jobs-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = jsonencode({
    Comment = "Medallion job pipeline: ingest -> clean -> aggregate -> crawl"
    StartAt = "Ingest"
    States = {
      Ingest = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.stage["stage1"].name }
        Next     = "Clean"
      }
      Clean = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.stage["stage2"].name }
        Next     = "Aggregate"
      }
      Aggregate = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.stage["stage3"].name }
        Next     = "Crawl"
      }
      Crawl = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = { Name = aws_glue_crawler.gold.name }
        End      = true
      }
    }
  })
}

output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}
