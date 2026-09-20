terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "stage" {
  type    = string
  default = "prod"
}

locals {
  lambda_root = "${path.module}/../../lambdas"

  common_environment = {
    LOG_LEVEL = "INFO"
    STAGE     = var.stage
  }

  data_platform_functions = {
    glue_partition_registrar   = { memory = 512, description = "Register Hive partitions from S3 keys" }
    athena_query_poller        = { memory = 512, description = "Run and page an Athena query" }
    parquet_compaction_planner = { memory = 1024, description = "Plan small-file compaction groups" }
    schema_drift_detector      = { memory = 512, description = "Diff Glue schemas against snapshots" }
    data_quality_rule_engine   = { memory = 512, description = "Evaluate declarative DQ rules" }
    redshift_copy_orchestrator = { memory = 512, description = "Submit and poll Redshift COPY" }
    s3_inventory_diff          = { memory = 1024, description = "Reconcile bucket inventories" }
    cdc_change_applier         = { memory = 512, description = "Apply CDC records to the serving store" }
    dataset_lineage_builder    = { memory = 1024, description = "Traverse the lineage graph" }
    cost_attribution_tagger    = { memory = 512, description = "Backfill cost allocation tags" }
  }

  analytics_functions = {
    funnel_conversion_aggregator = { memory = 1024, description = "Sessionise events and score the funnel" }
    cohort_retention_builder     = { memory = 1024, description = "Build the cohort retention matrix" }
    session_stitcher             = { memory = 512, description = "Resolve the identity graph" }
    ab_test_significance         = { memory = 512, description = "Score experiment arms" }
    realtime_kpi_updater         = { memory = 512, description = "Update sliding-window KPIs" }
    attribution_model_runner     = { memory = 1024, description = "Run multi-touch attribution" }
    churn_propensity_scorer      = { memory = 1024, description = "Score churn propensity" }
    anomaly_alert_dispatcher     = { memory = 512, description = "Dispatch metric anomaly alerts" }
    event_schema_validator       = { memory = 512, description = "Validate the ingest event batch" }
    usage_metering_rollup        = { memory = 1024, description = "Roll usage into billable units" }
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "data_platform" {
  name               = "data-platform-lambda-${var.stage}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "data_platform_basic" {
  role       = aws_iam_role.data_platform.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role" "analytics" {
  name               = "analytics-lambda-${var.stage}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "analytics_basic" {
  role       = aws_iam_role.analytics.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "archive_file" "data_platform" {
  type        = "zip"
  source_dir  = "${local.lambda_root}/data_platform"
  output_path = "${path.module}/build/data_platform.zip"
}

data "archive_file" "analytics" {
  type        = "zip"
  source_dir  = "${local.lambda_root}/analytics"
  output_path = "${path.module}/build/analytics.zip"
}

resource "aws_lambda_function" "data_platform" {
  for_each = local.data_platform_functions

  function_name    = "${replace(each.key, "_", "-")}-${var.stage}"
  description      = each.value.description
  role             = aws_iam_role.data_platform.arn
  runtime          = "python3.11"
  handler          = "${each.key}.lambda_handler"
  filename         = data.archive_file.data_platform.output_path
  source_code_hash = data.archive_file.data_platform.output_base64sha256
  memory_size      = each.value.memory

  environment {
    variables = local.common_environment
  }
}

resource "aws_lambda_function" "analytics" {
  for_each = local.analytics_functions

  function_name    = "${replace(each.key, "_", "-")}-${var.stage}"
  description      = each.value.description
  role             = aws_iam_role.analytics.arn
  runtime          = "python3.11"
  handler          = "${each.key}.lambda_handler"
  filename         = data.archive_file.analytics.output_path
  source_code_hash = data.archive_file.analytics.output_base64sha256
  memory_size      = each.value.memory

  environment {
    variables = local.common_environment
  }
}

resource "aws_cloudwatch_event_rule" "scheduled" {
  for_each = {
    parquet_compaction_planner   = "rate(1 hour)"
    schema_drift_detector        = "rate(30 minutes)"
    s3_inventory_diff            = "cron(0 2 * * ? *)"
    cost_attribution_tagger      = "cron(0 5 * * ? *)"
    funnel_conversion_aggregator = "rate(15 minutes)"
    cohort_retention_builder     = "cron(0 6 ? * MON *)"
    churn_propensity_scorer      = "cron(0 3 * * ? *)"
    anomaly_alert_dispatcher     = "rate(5 minutes)"
    usage_metering_rollup        = "cron(15 * * * ? *)"
  }

  name                = "${replace(each.key, "_", "-")}-schedule-${var.stage}"
  schedule_expression = each.value
}

resource "aws_cloudwatch_event_target" "scheduled" {
  for_each = aws_cloudwatch_event_rule.scheduled

  rule = each.value.name
  arn = try(
    aws_lambda_function.data_platform[each.key].arn,
    aws_lambda_function.analytics[each.key].arn,
  )
}

resource "aws_sqs_queue" "redshift_copy_manifests" {
  name                       = "redshift-copy-manifests-${var.stage}"
  visibility_timeout_seconds = 300
}

resource "aws_sqs_queue" "analytics_ingest" {
  name                       = "analytics-ingest-queue-${var.stage}"
  visibility_timeout_seconds = 300
}

resource "aws_lambda_event_source_mapping" "redshift_copy" {
  event_source_arn = aws_sqs_queue.redshift_copy_manifests.arn
  function_name    = aws_lambda_function.data_platform["redshift_copy_orchestrator"].arn
  batch_size       = 10
}

resource "aws_lambda_event_source_mapping" "event_schema_validator" {
  event_source_arn                   = aws_sqs_queue.analytics_ingest.arn
  function_name                      = aws_lambda_function.analytics["event_schema_validator"].arn
  batch_size                         = 10
  function_response_types            = ["ReportBatchItemFailures"]
  maximum_batching_window_in_seconds = 5
}

output "data_platform_function_names" {
  value = [for fn in aws_lambda_function.data_platform : fn.function_name]
}

output "analytics_function_names" {
  value = [for fn in aws_lambda_function.analytics : fn.function_name]
}
