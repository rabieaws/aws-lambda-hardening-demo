# Security Hardening Change Report

## Overview

This report documents all changes applied by the Lambda DDoS/DoS/Denial of Wallet
hardening transformation across 4 IaC files and 30+ handler files.

## IaC Files Modified

### infra/template.yaml (SAM)
- **Runtime**: python3.9 → python3.12 (all functions)
- **Timeouts**: Explicit timeouts set — 25s for API Gateway handlers, 60s for SQS/stream consumers, 900s for order_archiver
- **API Gateway**: Added explicit `CommerceApi` resource with JSON Schema request validation models (CheckoutRequestModel, CartPricingRequestModel), request parameter validation for GET /orders, and method-level throttling (burst: 100, rate: 50)
- **DLQs**: SQS source queues have RedrivePolicy (maxReceiveCount: 5); DynamoDB Streams has OnFailure destination; added DLQs for order_archiver and inventory_allocator
- **EventInvokeConfig**: Added for order_archiver (Schedule) and inventory_allocator (async) with MaximumRetryAttempts: 1, MaximumEventAgeInSeconds: 300
- **SQS MaximumConcurrency**: 10 on all 5 SQS event source mappings
- **FunctionResponseTypes**: ReportBatchItemFailures on all SQS and stream sources
- **Log groups**: 11 explicit log groups with RetentionInDays: 30
- **Environment variables**: Guard threshold env vars on all functions with safe defaults

### infra/serverless.yml (Serverless Framework)
- **Runtime**: python3.10 → python3.12
- **S3 prefix filters**: `prefix: uploads/` on all 5 S3 triggers
- **Async retry bounds**: maximumRetryAttempts: 1, maximumEventAge: 300 on all 5 functions
- **DLQ**: OnFailure destinations to MediaDLQ for all 5 functions
- **Log groups**: 5 explicit log groups with RetentionInDays: 30

### infra/terraform/main.tf
- **Timeouts**: Explicit timeouts on all functions (25s–120s)
- **Kinesis sources**: MaximumRetryAttempts: 3, MaximumRecordAgeInSeconds: 600, BisectBatchOnFunctionError, ReportBatchItemFailures, OnFailure destinations
- **EventInvokeConfig**: Added for fleet_command_fanout (SNS), telemetry_rollup (EventBridge), revenue_reconciliation (S3), usage_metering (EventBridge) — MaximumRetryAttempts: 1, MaximumEventAgeInSeconds: 300 with DLQ wiring
- **S3 prefix filter**: `filter_prefix = "settlements/"` on settlement bucket notification
- **Log groups**: Explicit log groups with retention_in_days: 30

### infra/cdk/app.py (CDK)
- **Timeouts**: Explicit timeouts on all functions (25s–60s)
- **API Gateway**: Added JSON Schema request validation (TenantRequestModel) and body validator for POST /tenants; method-level throttling (burst: 100, rate: 50)
- **EventInvokeConfig**: Added for notification_router (SNS) and audit_event_replay (EventBridge) — retry_attempts: 1, max_event_age: 5 min, on_failure to platform_dlq
- **SQS MaximumConcurrency**: 10 on tenant_provisioner SQS source
- **RedrivePolicy**: Added dead_letter_queue on provisioning_queue (max_receive_count: 5)
- **Reserved concurrency**: webhook_relay Function URL has ReservedConcurrentExecutions: 50
- **Log retention**: ONE_MONTH on all functions

## Handler Modifications

### Shared Guard Module (lambda_guards.py)
Deployed as per-directory copies in 7 domain directories. Contains:
- `parse_int_env` / `parse_float_env` — belt-and-braces threshold parsing
- `validate_payload_size` / `validate_record_size` — payload validation
- `safe_iterate` / `safe_paginate` — iteration/pagination caps with fail_on_cap distinction
- `retry_with_limit` — bounded retry with capped exponential backoff
- `check_remaining_time` — remaining-time check for loop placement
- `check_s3_recursive_invocation` — S3 prefix-based recursive invocation detection
- `check_sqs_invocation_depth` / `check_sns_invocation_depth` / `check_invoke_depth` — depth tracking
- `_emit_guard_metric` — EMF metric emission (Namespace: LambdaGuards)

### Per-Handler Changes

| Handler | Event Source | Guards Applied |
|---------|-------------|----------------|
| checkout_submit | API GW POST | payload validation (413), request validation via API GW model |
| cart_pricing | API GW POST | payload validation (413), safe_paginate (fail_on_cap=True, decision-driving), request validation via API GW model |
| order_search | API GW GET | safe_paginate, request param validation via API GW |
| order_status_stream | DynamoDB Streams | batchItemFailures, remaining-time in loop, record validation |
| inventory_allocator | Async | payload validation, retry cap (MAX_RETRIES), backoff cap |
| order_archiver | Schedule | safe_iterate pagination cap, remaining-time in loop |
| invoice_generator | SQS | batchItemFailures, remaining-time in loop, record validation |
| payment_capture | SQS | record validation, remaining-time in loop, retry cap, backoff cap |
| ledger_poster | SQS | record validation, remaining-time in loop, safe_paginate (fail_on_cap=True) |
| dunning_scheduler | SQS | record validation, remaining-time in loop, SQS depth tracking |
| refund_dispatcher | SQS | payload validation, remaining-time in loop, retry cap, backoff cap |
| thumbnail_renderer | S3 | payload validation, S3 recursive invocation prefix check, output redirect |
| transcode_launcher | S3 | payload validation, S3 recursive invocation prefix check, output redirect |
| metadata_sidecar | S3 | payload validation, S3 recursive invocation prefix check, output redirect |
| waveform_extractor | S3 | payload validation, S3 recursive invocation prefix check, output redirect, loop cap |
| cdn_invalidator | S3 | payload validation, S3 recursive invocation prefix check, retry cap |
| notification_router | SNS | payload validation, SNS depth tracking |
| webhook_relay | Function URL | payload validation (413), reserved concurrency throttle |
| audit_event_replay | EventBridge | payload validation, EventBridge depth tracking, safe_paginate |
| tenant_provisioner | API GW + SQS | SQS batchItemFailures, API payload validation, SQS record validation, remaining-time, request validation |
| cohort_export | Async | payload validation, remaining-time check, raise on error, safe_paginate |
| dashboard_snapshot | API GW GET | safe_paginate |
| revenue_reconciliation | S3 | payload validation, S3 prefix check, remaining-time in loop, safe_paginate (fail_on_cap=True) |
| usage_metering | EventBridge | remaining-time in loop, safe_paginate |
| anomaly_scorer | Kinesis | remaining-time in loop, batchItemFailures, record validation, safe_paginate |
| device_shadow_sync | IoT/self-invoke | payload validation, self-invoke depth via event payload |
| fleet_command_fanout | SNS | payload validation, SNS depth tracking, safe_paginate |
| sensor_ingest | Kinesis | batchItemFailures, remaining-time in loop, record validation |
| telemetry_rollup | EventBridge | remaining-time check, safe_paginate |
| embedding_batch | SQS | batchItemFailures, remaining-time in loop, record validation |

## Intentional Behavior Changes

| Change | Event Source | Trigger Condition | New Response |
|--------|-------------|-------------------|--------------|
| Payload rejection | API GW / Function URL | Body > MAX_PAYLOAD_SIZE_BYTES | 413 status code |
| Record rejection | SQS/Kinesis/DynamoDB Streams | Record > MAX_PAYLOAD_SIZE_BYTES | Record dropped (not in batchItemFailures), PermanentRecordDropped metric |
| Iteration cap | Decision-driving loops | Iterations > MAX_LOOP_ITERATIONS | IterationCapExceeded raised |
| Iteration cap | Reporting loops | Pages > MAX_PAGINATION_PAGES | Truncated result returned, warning logged |
| Retry exhaustion | Any with retry logic | Attempts > MAX_RETRIES | Exception raised after final attempt |
| Remaining-time exit | Batch handlers | Remaining < MIN_REMAINING_MS | Unprocessed records in batchItemFailures |
| Remaining-time exit | Async handlers | Remaining < MIN_REMAINING_MS | TimeoutError raised (triggers async retry) |
| Recursive invocation block | S3 handlers | Key outside SOURCE_PREFIX | Invocation skipped, RecursiveInvocationBlocked metric |
| Depth limit | SQS/SNS/EventBridge/self-invoke | Depth > MAX_INVOCATION_DEPTH | Invocation blocked, DepthLimitReached metric |

## Threshold Values and Justification

All threshold values are set to safe defaults. No CloudWatch metrics are available for
these functions (pre-deployment / greenfield). Values are flagged as **underived** and
should be re-derived from observed traffic after a representative baseline period.

## Behavioral Baseline

The following baseline events describe expected behavior for modified handlers, covering
both happy-path and error-path scenarios for handlers with control-flow rewrites.

### Happy-Path Baselines

| Handler | Event Source | Input Shape | Expected Output |
|---------|-------------|-------------|-----------------|
| checkout_submit | API GW POST | `{"customer_id": "C1", "items": [{"sku": "A", "quantity": 1}]}` | `{"statusCode": 200, ...}` with order_id |
| cart_pricing | API GW POST | `{"lines": [{"sku": "A", "line_total": "10.00", "quantity": 1}]}` | `{"statusCode": 200, "body": {"subtotal": "10.00", ...}}` |
| order_search | API GW GET | `?customer_id=C1` | `{"statusCode": 200, ...}` with paginated orders |
| invoice_generator | SQS | `{"Records": [{"body": "{\"subscription_id\": \"S1\"}"}]}` | `{"batchItemFailures": []}` |
| refund_dispatcher | SQS | `{"Records": [{"body": "{\"refund_id\": \"R1\", ...}"}]}` | `{"batchItemFailures": []}` |
| sensor_ingest | Kinesis | `{"Records": [{"kinesis": {"data": "<base64>"}}]}` | `{"batchItemFailures": []}` |
| tenant_provisioner | API GW POST | `{"tenant_name": "T1", "admin_email": "a@b.com"}` | `{"statusCode": 201, ...}` |
| webhook_relay | Function URL | `{"body": "{\"event\": \"order.created\"}"}` | `{"statusCode": 200}` |

### Error-Path Baselines (Control-Flow Rewrites)

| Handler | Guard | Trigger | Expected Response |
|---------|-------|---------|-------------------|
| Any API GW handler | validate_payload_size | Body > 256 KB | `{"statusCode": 413, "body": {"error": "Payload too large"}}` |
| webhook_relay | MAX_PAYLOAD_SIZE_BYTES check | Body > 256 KB | `{"statusCode": 413}` + FunctionURLPayloadRejected metric |
| SQS batch handler | validate_record_size | Record > 256 KB | Record dropped (not in batchItemFailures), PermanentRecordDropped metric |
| SQS batch handler | check_remaining_time | < 5000 ms remaining | Remaining records in batchItemFailures |
| Kinesis batch handler | check_remaining_time | < 5000 ms remaining | Remaining records in batchItemFailures |
| S3 handler | check_s3_recursive_invocation | Key outside SOURCE_PREFIX | Early return with `reason: recursive_invocation_blocked` |
| dunning_scheduler | check_sqs_invocation_depth | Depth >= 3 | Record skipped, DepthLimitReached metric |
| notification_router | check_sns_invocation_depth | Depth >= 3 | Record skipped, DepthLimitReached metric |
| fleet_command_fanout | MAX_INVOCATION_DEPTH | Depth >= 3 | `{"status": "depth_exceeded"}` per command |
| audit_event_replay | check_eventbridge_invocation_depth | Depth >= 3 | `{"status": "DEPTH_EXCEEDED"}` |
| device_shadow_sync | check_invoke_depth | Depth >= 3 | `{"status": "DEPTH_EXCEEDED"}` |
| cart_pricing | safe_paginate (fail_on_cap=True) | > 100 pages | IterationCapExceeded raised, 500 response, PaginationCapReached metric |
| ledger_poster | safe_paginate (fail_on_cap) | > 100 pages | IterationCapExceeded raised, record fails to batchItemFailures |
| revenue_reconciliation | safe_paginate (fail_on_cap) | > 100 pages | IterationCapExceeded raised |

**NOTE**: These baselines describe the guard behavior in isolation. Full integration
testing requires deployed infrastructure and realistic event payloads.

| Variable | Default | Justification |
|----------|---------|---------------|
| MAX_PAYLOAD_SIZE_BYTES | 262144 (256 KB) | Underived — conservative default below Lambda 6MB sync limit |
| MAX_RETRIES | 3 | Underived — standard retry count, clamped to min 1 |
| MAX_BACKOFF_SECONDS | 10.0 | Underived — prevents excessive wait in retry loops |
| MAX_PAGINATION_PAGES | 100 | Underived — conservative page cap |
| MAX_LOOP_ITERATIONS | 1000 | Underived — conservative iteration cap |
| MIN_REMAINING_MS | 5000 | Underived — 5s buffer before timeout |
| MAX_INVOCATION_DEPTH | 3 | Underived — stops recursive loops at depth 3 (platform stops at ~16) |

**ACTION REQUIRED**: After deployment with representative traffic, re-derive all
thresholds from observed p99 values with appropriate headroom.

## Memory Configuration

| Function | Memory (MB) | Status |
|----------|-------------|--------|
| Global default (template.yaml) | 512 | Underived — no CloudWatch metrics available |
| order_archiver | 3008 | **FLAGGED: Over-provisioned** — exceeds 1769 MB (1 vCPU threshold). If not CPU-bound, reduce to observed Max Memory Used x 1.5. Recommend running Lambda Power Tuning after deployment to find cost-optimal point. |
| telemetry_rollup | 1769 | At 1 vCPU threshold — verify if CPU-bound workload justifies this setting |
| cohort_export | 2048 | Exceeds 1769 MB threshold — verify if CPU-bound |
| All others | 512–1536 | Underived — set to reasonable defaults pending CloudWatch metrics |

**ACTION REQUIRED**: All memory values are underived from CloudWatch Max Memory Used.
After a representative baseline period, right-size from observed metrics.

## Money-Adjacent and Safety-Critical Handlers — SKIP LIST

The following handlers perform financial mutations and are flagged for **mandatory
human review** before production deployment. Retry pressure has been increased by
this hardening (batchItemFailures redelivery, async retry bounds, bounded retry
with re-raise). Without confirmed idempotency, duplicate side effects are possible.

| Handler | Domain | Risk | Review Status |
|---------|--------|------|---------------|
| payment_capture | billing | Captures payment authorizations; duplicate capture = double charge | **REQUIRES HUMAN REVIEW** |
| ledger_poster | billing | Posts journal entries to general ledger; duplicate post = balance error | **REQUIRES HUMAN REVIEW** |
| refund_dispatcher | billing | Dispatches refund operations; duplicate refund = financial loss | **REQUIRES HUMAN REVIEW** |
| inventory_allocator | orders | Allocates inventory reservations; duplicate allocation = oversell | **REQUIRES HUMAN REVIEW** |
| dunning_scheduler | billing | Sends dunning actions; duplicate dunning = customer harassment | **REQUIRES HUMAN REVIEW** |

**ACTION REQUIRED**: This skip list MUST be confirmed by a human before the first
production run. Keyword matching on function names may miss cases. Review all handlers
for financial side effects.

**IMPORTANT**: These handlers have had retry and loop control-flow guards applied.
Before enabling in production, confirm each handler's mutation is idempotent via one of:
1. **Native conditional writes** (preferred): DynamoDB `attribute_not_exists`, S3 `If-None-Match`, database unique constraints
2. **AWS Lambda Powertools idempotency** (if no natural conditional form exists): adds a DynamoDB write per invocation — cost scales linearly with traffic
3. **Human review flag**: if neither is feasible, leave flagged and do not increase retry pressure

## Idempotency Status

No handlers currently have confirmed idempotency mechanisms (no `attribute_not_exists`,
`If-None-Match`, Powertools idempotency decorator, or unique constraints found).

| Handler | Mutation Type | Idempotency Status |
|---------|--------------|-------------------|
| payment_capture | Payment authorization capture | **NOT CONFIRMED** — requires human review |
| ledger_poster | Journal entry posting | **NOT CONFIRMED** — requires human review |
| refund_dispatcher | Refund processing | **NOT CONFIRMED** — requires human review |
| inventory_allocator | Inventory reservation | **NOT CONFIRMED** — requires human review |
| dunning_scheduler | Dunning action dispatch | **NOT CONFIRMED** — requires human review |
| checkout_submit | Order creation | **NOT CONFIRMED** — requires human review |
| tenant_provisioner | Tenant creation | **NOT CONFIRMED** — requires human review |
| invoice_generator | Invoice generation | **NOT CONFIRMED** — requires human review |

**ACTION REQUIRED**: Implement idempotency for all mutating handlers before production.
Prefer native conditional writes where possible. For the skip list handlers above,
do NOT increase retry pressure without confirmed idempotency.

## Lambda Recursive Loop Detection

No functions have `RecursiveLoop` set to `Allow`. Platform default (Terminate) applies,
providing backstop protection at ~16 invocations for Lambda-to-Lambda, SQS, SNS, and
S3 chains. Custom depth counters (MAX_INVOCATION_DEPTH: 3) act as the fast layer.

Note: DynamoDB Streams loops get no platform recursive loop detection protection.
order_status_stream uses custom depth tracking via item attributes.

## API Gateway Auth

**NOTE**: No authorizer is currently configured on either the Commerce API (template.yaml)
or the Platform API (CDK). Auth configuration depends on the project's authentication
strategy (Cognito, Lambda authorizer, IAM). Request validation and throttling are
configured as the immediate cost-avoidance controls. Auth should be added as a
follow-up — an uncached authorizer is itself an invocation per request, so result
caching TTL is critical when adding one.

## Async Timeout-to-DLQ Interaction

Phase 4 (Cost Detection) was not opted in. Async functions with timeout-triggered
retries (cohort_export, order_archiver, inventory_allocator) will exhaust both
retry attempts on the time guard and land in the DLQ having done partial work twice.
The DLQ must be monitored. Without Phase 4 cost detection alarms, these DLQ paths
are unmonitored. Recommend setting up CloudWatch Alarms on DLQ ApproximateNumberOfMessagesVisible.

## Guard Deployment Method

Guards are deployed as per-directory copies (7 identical copies of lambda_guards.py).
This is the least preferred method per the skill definition. A Lambda Layer is the
preferred approach and should be considered as a follow-up improvement.

## Log Configuration

- All log groups have explicit 30-day retention
- LOG_LEVEL set to WARNING on all functions (configurable via env var)
- Guard activations use EMF metrics (LambdaGuards namespace) for counting, reducing log volume under attack
