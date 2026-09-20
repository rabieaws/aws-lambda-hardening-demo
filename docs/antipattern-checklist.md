# Anti-pattern checklist (generation spec)

This repo is a **deliberately unhardened** corpus of Python AWS Lambda handlers. It exists to
demo the `lambda-security-hardening` AWS Transform custom transformation definition. Every
handler contains realistic production-grade business logic *plus* one or more of the weaknesses
below, so the transformation has something concrete to fix in every phase.

> Nothing here is deployment-ready. The weaknesses are the point.

## Weakness IDs

| ID | Weakness | Transformation phase that fixes it |
|----|----------|------------------------------------|
| W1 | Handler consumes `event` with no payload size measurement or early rejection | Phase 2 - payload size validation |
| W2 | `while True` / `while next_token` loop over an external paginated source, no iteration cap | Phase 4 - unbounded loop protection |
| W3 | `for` loop over attacker-influenced collection (event records, API results) with no cap | Phase 4 - unbounded loop protection |
| W4 | Retry loop with no max attempt count, or uncapped exponential backoff (`2 ** attempt`) | Phase 4 - retry protection |
| W5 | `boto3` paginator fully drained with no page-count limit | Phase 4 - pagination cap |
| W6 | S3-triggered handler writes derived output back into the trigger bucket with no source-prefix validation (self-recursion) | Phase 3 - recursive invocation detection |
| W7 | SQS/SNS handler re-publishes to its own queue/topic with no invocation-depth tracking | Phase 3 - invocation depth guard |
| W8 | No `context.get_remaining_time_in_millis()` execution-budget check, so the function times out and is auto-retried | Phase 3 / Phase 5 - remaining time check |
| W9 | API Gateway handler reads unlimited query params, headers, or body with no count/size limits and no 400/413/431 responses | Phase 6 - event-source guards |
| W10 | Self re-invocation via `lambda_client.invoke` for continuation, with no depth guard | Phase 3 - recursive invocation detection |
| W11 | SQS batch consumed with no batch-size or per-message size validation | Phase 6 - SQS guards |
| W12 | All thresholds hardcoded as magic numbers instead of environment variables | Phase 7 - configurable thresholds |

W1, W8 and W12 are present in effectively every handler by construction. W2-W7, W9-W11 are
distributed across domains so each guard the transformation injects has a home.

## Authoring rules for handlers

1. Standard signature: `def lambda_handler(event, context):`. One handler per file.
2. Imports limited to the Python standard library plus `boto3` / `botocore`.
3. Logic must be genuinely non-trivial: state machines, `Decimal` money math, idempotency keys,
   DynamoDB conditional writes, weighted scoring, EWMA smoothing, SLA computation, dedup,
   tiered pricing, geospatial bucketing, cohort math, and similar.
4. Module docstring names the event source and what the function does.
5. Code must read as plausible production code. Do **not** annotate the weaknesses inline - the
   transformation is supposed to find them.
6. Structured logging via `logging`, real error handling, type hints on helpers.

## Layout

```
lambdas/
  ecommerce/        API Gateway + DynamoDB Streams
  payments/         API Gateway + SQS
  media_pipeline/   S3 object-created triggers (W6 concentration)
  iot_telemetry/    Kinesis + IoT Core
  data_platform/    S3 / Glue / Athena paginators (W5 concentration)
  messaging/        SQS + SNS fan-out (W7 concentration)
  identity/         API Gateway authorizers + Cognito triggers (W9 concentration)
  logistics/        EventBridge + external HTTP retries (W4 concentration)
  analytics/        DynamoDB scans + paginators (W2/W5)
  ops_automation/   Scheduled EventBridge maintenance jobs
infra/              SAM, Serverless, CDK, Terraform - all missing Timeout/concurrency/DLQ
```
