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
  default = "us-east-1"
}

variable "stage" {
  type    = string
  default = "prod"
}

locals {
  lambda_root = "${path.module}/../../functions"

  common_environment = {
    LOG_LEVEL = "WARNING"
    STAGE     = var.stage
  }

  telemetry_guard_vars = {
    MAX_PAYLOAD_SIZE_BYTES = "262144"
    MAX_LOOP_ITERATIONS    = "1000"
    MAX_RETRIES            = "3"
    MAX_BACKOFF_SECONDS    = "10.0"
    MAX_PAGINATION_PAGES   = "100"
    MIN_REMAINING_MS       = "5000"
    MAX_INVOCATION_DEPTH   = "3"
  }

  reporting_guard_vars = {
    MAX_PAYLOAD_SIZE_BYTES = "262144"
    MAX_PAGINATION_PAGES   = "100"
    MIN_REMAINING_MS       = "5000"
    SOURCE_PREFIX          = "settlements/"
    OUTPUT_PREFIX          = "reconciliation/"
  }

  telemetry_functions = {
    sensor_ingest = {
      handler = "sensor_ingest.lambda_handler"
      memory  = 512
      timeout = 60
    }
    anomaly_scorer = {
      handler = "anomaly_scorer.lambda_handler"
      memory  = 1024
      timeout = 60
    }
    device_shadow_sync = {
      handler = "device_shadow_sync.lambda_handler"
      memory  = 512
      timeout = 60
    }
    fleet_command_fanout = {
      handler = "fleet_command_fanout.lambda_handler"
      memory  = 512
      timeout = 60
    }
    telemetry_rollup = {
      handler = "telemetry_rollup.lambda_handler"
      memory  = 1769
      timeout = 120
    }
  }

  reporting_functions = {
    revenue_reconciliation = {
      handler = "revenue_reconciliation.lambda_handler"
      memory  = 1024
      timeout = 120
    }
    usage_metering = {
      handler = "usage_metering.lambda_handler"
      memory  = 1024
      timeout = 120
    }
    dashboard_snapshot = {
      handler = "dashboard_snapshot.lambda_handler"
      memory  = 512
      timeout = 25
    }
    cohort_export = {
      handler = "cohort_export.lambda_handler"
      memory  = 2048
      timeout = 120
    }
  }
}

# -----------------------------------------------------------------------------
# Packaging
# -----------------------------------------------------------------------------

data "archive_file" "telemetry" {
  type        = "zip"
  source_dir  = "${local.lambda_root}/telemetry"
  output_path = "${path.module}/build/telemetry.zip"
}

data "archive_file" "reporting" {
  type        = "zip"
  source_dir  = "${local.lambda_root}/reporting"
  output_path = "${path.module}/build/reporting.zip"
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda" {
  name = "telemetry-reporting-lambda-${var.stage}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# -----------------------------------------------------------------------------
# DLQs
# -----------------------------------------------------------------------------

resource "aws_sqs_queue" "telemetry_dlq" {
  name                      = "telemetry-dlq-${var.stage}"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "reporting_dlq" {
  name                      = "reporting-dlq-${var.stage}"
  message_retention_seconds = 1209600
}

# -----------------------------------------------------------------------------
# telemetry/  -- packaging root is ../../functions/telemetry
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "telemetry" {
  for_each = local.telemetry_functions

  function_name    = "telemetry-${each.key}-${var.stage}"
  role             = aws_iam_role.lambda.arn
  handler          = each.value.handler
  runtime          = "python3.12"
  filename         = data.archive_file.telemetry.output_path
  source_code_hash = data.archive_file.telemetry.output_base64sha256
  memory_size      = each.value.memory
  timeout          = each.value.timeout

  environment {
    variables = merge(local.common_environment, local.telemetry_guard_vars, {
      READING_TABLE = aws_dynamodb_table.sensor_readings.name
      STATE_TABLE   = aws_dynamodb_table.anomaly_state.name
    })
  }
}

resource "aws_lambda_function" "reporting" {
  for_each = local.reporting_functions

  function_name    = "reporting-${each.key}-${var.stage}"
  role             = aws_iam_role.lambda.arn
  handler          = each.value.handler
  runtime          = "python3.12"
  filename         = data.archive_file.reporting.output_path
  source_code_hash = data.archive_file.reporting.output_base64sha256
  memory_size      = each.value.memory
  timeout          = each.value.timeout

  environment {
    variables = merge(local.common_environment, local.reporting_guard_vars, {
      CAPTURE_TABLE = aws_dynamodb_table.captures_mirror.name
      REPORT_BUCKET = aws_s3_bucket.reports.id
    })
  }
}

# -----------------------------------------------------------------------------
# Log groups
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "telemetry" {
  for_each = local.telemetry_functions

  name              = "/aws/lambda/telemetry-${each.key}-${var.stage}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "reporting" {
  for_each = local.reporting_functions

  name              = "/aws/lambda/reporting-${each.key}-${var.stage}"
  retention_in_days = 30
}

# -----------------------------------------------------------------------------
# Event sources
# -----------------------------------------------------------------------------

resource "aws_kinesis_stream" "telemetry" {
  name        = "telemetry-${var.stage}"
  shard_count = 4
}

resource "aws_lambda_event_source_mapping" "sensor_ingest" {
  event_source_arn               = aws_kinesis_stream.telemetry.arn
  function_name                  = aws_lambda_function.telemetry["sensor_ingest"].arn
  starting_position              = "LATEST"
  batch_size                     = 500
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 600
  bisect_batch_on_function_error = true
  function_response_types        = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.telemetry_dlq.arn
    }
  }
}

resource "aws_lambda_event_source_mapping" "anomaly_scorer" {
  event_source_arn               = aws_kinesis_stream.telemetry.arn
  function_name                  = aws_lambda_function.telemetry["anomaly_scorer"].arn
  starting_position              = "LATEST"
  batch_size                     = 200
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 600
  bisect_batch_on_function_error = true
  function_response_types        = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.telemetry_dlq.arn
    }
  }
}

resource "aws_sns_topic" "fleet_commands" {
  name = "fleet-commands-${var.stage}"
}

resource "aws_sns_topic_subscription" "fleet_command_fanout" {
  topic_arn = aws_sns_topic.fleet_commands.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.telemetry["fleet_command_fanout"].arn
}

resource "aws_cloudwatch_event_rule" "telemetry_rollup" {
  name                = "telemetry-rollup-${var.stage}"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "telemetry_rollup" {
  rule = aws_cloudwatch_event_rule.telemetry_rollup.name
  arn  = aws_lambda_function.telemetry["telemetry_rollup"].arn
}

resource "aws_cloudwatch_event_rule" "usage_metering" {
  name                = "usage-metering-${var.stage}"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "usage_metering" {
  rule = aws_cloudwatch_event_rule.usage_metering.name
  arn  = aws_lambda_function.reporting["usage_metering"].arn
}

resource "aws_s3_bucket" "settlement_landing" {
  bucket = "settlement-landing-${var.stage}"
}

resource "aws_s3_bucket_notification" "settlement" {
  bucket = aws_s3_bucket.settlement_landing.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.reporting["revenue_reconciliation"].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "settlements/"
  }
}

resource "aws_s3_bucket" "reports" {
  bucket = "reconciliation-reports-${var.stage}"
}

# -----------------------------------------------------------------------------
# Async retry bounds and DLQ wiring
# -----------------------------------------------------------------------------

resource "aws_lambda_function_event_invoke_config" "fleet_command_fanout" {
  function_name          = aws_lambda_function.telemetry["fleet_command_fanout"].function_name
  maximum_retry_attempts = 1
  maximum_event_age_in_seconds = 300

  destination_config {
    on_failure {
      destination = aws_sqs_queue.telemetry_dlq.arn
    }
  }
}

resource "aws_lambda_function_event_invoke_config" "telemetry_rollup" {
  function_name          = aws_lambda_function.telemetry["telemetry_rollup"].function_name
  maximum_retry_attempts = 1
  maximum_event_age_in_seconds = 300

  destination_config {
    on_failure {
      destination = aws_sqs_queue.telemetry_dlq.arn
    }
  }
}

resource "aws_lambda_function_event_invoke_config" "revenue_reconciliation" {
  function_name          = aws_lambda_function.reporting["revenue_reconciliation"].function_name
  maximum_retry_attempts = 1
  maximum_event_age_in_seconds = 300

  destination_config {
    on_failure {
      destination = aws_sqs_queue.reporting_dlq.arn
    }
  }
}

resource "aws_lambda_function_event_invoke_config" "usage_metering" {
  function_name          = aws_lambda_function.reporting["usage_metering"].function_name
  maximum_retry_attempts = 1
  maximum_event_age_in_seconds = 300

  destination_config {
    on_failure {
      destination = aws_sqs_queue.reporting_dlq.arn
    }
  }
}

# -----------------------------------------------------------------------------
# Data stores
# -----------------------------------------------------------------------------

resource "aws_dynamodb_table" "sensor_readings" {
  name         = "sensor-readings-${var.stage}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "device_id"
  range_key    = "metric_observed_at"

  attribute {
    name = "device_id"
    type = "S"
  }

  attribute {
    name = "metric_observed_at"
    type = "S"
  }
}

resource "aws_dynamodb_table" "anomaly_state" {
  name         = "anomaly-state-${var.stage}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "series_key"

  attribute {
    name = "series_key"
    type = "S"
  }
}

resource "aws_dynamodb_table" "captures_mirror" {
  name         = "captures-mirror-${var.stage}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "capture_id"

  attribute {
    name = "capture_id"
    type = "S"
  }
}
