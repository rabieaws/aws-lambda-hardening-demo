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
    LOG_LEVEL = "INFO"
    STAGE     = var.stage
  }

  telemetry_functions = {
    sensor_ingest = {
      handler = "sensor_ingest.lambda_handler"
      memory  = 512
    }
    anomaly_scorer = {
      handler = "anomaly_scorer.lambda_handler"
      memory  = 1024
    }
    device_shadow_sync = {
      handler = "device_shadow_sync.lambda_handler"
      memory  = 512
    }
    fleet_command_fanout = {
      handler = "fleet_command_fanout.lambda_handler"
      memory  = 512
    }
    telemetry_rollup = {
      handler = "telemetry_rollup.lambda_handler"
      memory  = 1769
    }
  }

  reporting_functions = {
    revenue_reconciliation = {
      handler = "revenue_reconciliation.lambda_handler"
      memory  = 1024
    }
    usage_metering = {
      handler = "usage_metering.lambda_handler"
      memory  = 1024
    }
    dashboard_snapshot = {
      handler = "dashboard_snapshot.lambda_handler"
      memory  = 512
    }
    cohort_export = {
      handler = "cohort_export.lambda_handler"
      memory  = 2048
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

  environment {
    variables = merge(local.common_environment, {
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
  timeout          = 120

  environment {
    variables = merge(local.common_environment, {
      CAPTURE_TABLE = aws_dynamodb_table.captures_mirror.name
      REPORT_BUCKET = aws_s3_bucket.reports.id
    })
  }
}

# -----------------------------------------------------------------------------
# Event sources
# -----------------------------------------------------------------------------

resource "aws_kinesis_stream" "telemetry" {
  name        = "telemetry-${var.stage}"
  shard_count = 4
}

resource "aws_lambda_event_source_mapping" "sensor_ingest" {
  event_source_arn  = aws_kinesis_stream.telemetry.arn
  function_name     = aws_lambda_function.telemetry["sensor_ingest"].arn
  starting_position = "LATEST"
  batch_size        = 500
}

resource "aws_lambda_event_source_mapping" "anomaly_scorer" {
  event_source_arn  = aws_kinesis_stream.telemetry.arn
  function_name     = aws_lambda_function.telemetry["anomaly_scorer"].arn
  starting_position = "LATEST"
  batch_size        = 200
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
  }
}

resource "aws_s3_bucket" "reports" {
  bucket = "reconciliation-reports-${var.stage}"
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
