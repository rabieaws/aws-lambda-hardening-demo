# Lambda Security Hardening — demo corpus

100 realistic Python AWS Lambda handlers, deliberately left unhardened, for demoing the
**`lambda-security-hardening`** AWS Transform custom transformation definition.

Every handler carries genuine production-grade business logic (Decimal money math, state
machines, Welford variance, haversine + ray casting, union-find identity resolution, 2-opt
routing, bin-packing, t-digest percentiles, double-entry accounting, SCIM patch semantics,
hand-rolled JWS verification) *and* at least two of the weaknesses the transformation fixes.

> **This code is not deployment-ready.** The DDoS / DoS / Denial-of-Wallet exposure is the
> point of the exercise. Do not ship it.

## At a glance

| | |
|---|---|
| Handlers | 100 (10 domains × 10) |
| Lines of handler code | ~21,600 (186–244 per file) |
| Dependencies | Python stdlib + `boto3` only |
| Event sources | API Gateway, S3, SQS, SNS, Kinesis, DynamoDB Streams, IoT Core, EventBridge, direct invoke, Cognito trigger |
| Handlers with a payload-size guard | 0 |
| Handlers with a remaining-time guard | 0 |
| Thresholds exposed as env vars | 0 (all hardcoded) |

All 100 files compile cleanly:

```powershell
python -m compileall -q lambdas
```

## Layout

```
lambdas/
  ecommerce/        API Gateway + DynamoDB Streams        -> infra/template.yaml   (SAM)
  payments/         API Gateway + SQS + S3                -> infra/template.yaml   (SAM)
  media_pipeline/   S3 ObjectCreated                      -> infra/serverless.yml
  messaging/        SQS + SNS fan-out                     -> infra/serverless.yml
  identity/         API Gateway + Cognito trigger         -> infra/cdk/app.py
  iot_telemetry/    Kinesis + IoT Core                    -> infra/cdk/app.py
  data_platform/    S3 / Glue / Athena / Redshift         -> infra/terraform/main.tf
  analytics/        Kinesis + scheduled rollups           -> infra/terraform/main.tf
  logistics/        EventBridge + external HTTP           -> (no IaC, on purpose)
  ops_automation/   Scheduled maintenance jobs            -> (no IaC, on purpose)
docs/
  antipattern-checklist.md    weakness IDs W1-W12 and the authoring rules
infra/
  template.yaml               SAM       - 20 functions
  serverless.yml              Serverless - 20 functions
  cdk/app.py, cdk/cdk.json    CDK       - 20 functions
  terraform/main.tf           Terraform - 20 functions
```

Four IaC flavours prove the transformation is framework agnostic. `logistics` and
`ops_automation` have **no** IaC so you can also demo the Phase 5 fallback that writes a
recommended-configuration comment block into the handler file instead.

## Weakness catalogue

Full definitions live in [`docs/antipattern-checklist.md`](docs/antipattern-checklist.md).

| ID | Weakness | Fixed by |
|----|----------|----------|
| W1 | No payload size measurement or early rejection | Phase 2 |
| W2 | `while True` / token loop over an external source, no iteration cap | Phase 4 |
| W3 | `for` loop over an attacker-influenced collection, no cap | Phase 4 |
| W4 | Retry loop with no max attempts or uncapped `2 ** attempt` backoff | Phase 4 |
| W5 | `boto3` paginator drained with no page limit | Phase 4 |
| W6 | S3 handler writes derived output back into its trigger bucket, no prefix check | Phase 3 |
| W7 | SQS/SNS handler re-publishes to its own queue/topic, no depth counter | Phase 3 |
| W8 | No `context.get_remaining_time_in_millis()` budget check | Phase 3 / 5 |
| W9 | API Gateway params, headers and body read with no count/size limits | Phase 6 |
| W10 | Self re-invocation via `lambda_client.invoke`, no depth guard | Phase 3 |
| W11 | SQS batch consumed with no batch-size or per-message size check | Phase 6 |
| W12 | Thresholds hardcoded instead of environment-driven | Phase 7 |

Measured spread across the corpus: 45 files with unbounded `while` loops, 28 with uncapped
retry backoff, 27 draining paginators, 32 that re-publish or re-invoke themselves, 13 API
Gateway entry points, 37 SQS batch consumers.

## Phase-by-phase demo script

### Phase 2 — payload size validation
Any handler works; the API Gateway ones show the 413 path best.

- `identity/user_provisioning_scim.py` — applies an unbounded SCIM operation list
- `ecommerce/checkout_order_submit.py` — parses an arbitrarily large cart body
- `analytics/event_schema_validator.py` — non-HTTP source, so expect an exception rather than a 413

### Phase 3 — recursive invocation detection
`media_pipeline/` is the concentration point. Eight of its ten handlers read
`record["s3"]["bucket"]["name"]` and then `put_object` straight back to that same bucket.

- **S3 self-recursion (W6):** `image_thumbnail_generator`, `exif_metadata_extractor`,
  `audio_waveform_peaks`, `watermark_applier`, `media_manifest_builder`,
  `subtitle_burn_in`, `video_transcode_orchestrator`, `content_moderation_screener`,
  plus `payments/settlement_reconciliation` and `logistics/proof_of_delivery_validator`
- **Queue/topic depth (W7):** `messaging/webhook_retry_scheduler`,
  `messaging/event_ordering_buffer`, `messaging/campaign_throttle_dispatcher`,
  `messaging/sqs_dlq_redriver`, `messaging/notification_fanout_router`,
  `payments/payout_batch_dispatcher`
- **Self-invoke (W10):** `iot_telemetry/fleet_command_dispatcher`,
  `ecommerce/abandoned_cart_sweeper`, `messaging/chat_message_broadcaster`

### Phase 4 — unbounded loops, retries and pagination

- **Unbounded polling (W2):** `data_platform/athena_query_poller` (`while True` on
  `get_query_execution`), `data_platform/redshift_copy_orchestrator`,
  `ops_automation/backup_verification_runner`, `iot_telemetry/telemetry_backfill_replayer`
- **Unbounded graph/optimiser loops (W2/W3):** `data_platform/dataset_lineage_builder`
  (unbounded BFS frontier), `identity/permission_expander`,
  `logistics/shipment_route_optimizer` (2-opt until no improvement),
  `logistics/warehouse_slotting_optimizer`
- **Uncapped retry (W4):** `logistics/carrier_rate_shopper` and
  `logistics/reverse_logistics_router` are the clearest — `while True` with
  `time.sleep(2 ** attempt)`, no ceiling. Also `payments/payment_authorization`,
  `payments/currency_rate_sync`, `media_pipeline/content_moderation_screener`
- **Paginator drains (W5):** `data_platform/` has six (`get_partitions`,
  `get_query_results`, `list_objects_v2` ×2, `get_tables`, `get_resources`);
  `ops_automation/` has seven more across EC2, CloudWatch Logs, EBS snapshots, security
  groups, ACM, SSM patch compliance and autoscaling

### Phase 5 — timeout, concurrency and DLQ configuration
All four IaC files are missing reserved concurrency and DLQ configuration entirely:

```powershell
Get-ChildItem infra -Recurse -File |
  Select-String -Pattern 'ReservedConcurrent|DeadLetter|dead_letter|onFailure'
# no matches
```

Timeouts are wrong in both directions, so the transformation has two cases to handle:

- **SAM and Serverless** declare `Timeout: 900` on the long-running functions — 30× the
  recommended ceiling, and the main Denial-of-Wallet lever in the repo
- **CDK and Terraform** declare no timeout at all, so those 40 functions silently inherit the
  3-second default rather than an explicit, reviewed value

Then show the no-IaC fallback on `logistics/` or `ops_automation/`.

### Phase 6 — event-source-specific guards

- **API Gateway (W9):** `identity/` is the concentration point. `jwt_custom_authorizer`
  iterates every header and multi-value query parameter hunting for a bearer token;
  `consent_preference_center` and `device_trust_evaluator` build their working set from
  arbitrary `purpose.*` / `fp.*` / `x-device-*` keys with no cap
- **SQS (W11):** `payments/payment_capture_worker`, `payments/refund_processor`,
  `logistics/tracking_event_normalizer`, `messaging/email_digest_batcher`,
  `messaging/sms_rate_governor`, `analytics/event_schema_validator`

### Phase 7 — integration, shared module, configurability
Every domain folder is a multi-handler project, so all ten should collapse their injected
guards into a shared `lambda_guards.py`. `messaging/` (10 handlers, 4 distinct guard needs) and
`data_platform/` (10 handlers, heavy paginator use) make the clearest before/after.

Nothing in the corpus reads a threshold from the environment today:

```powershell
Select-String -Path lambdas\*\*.py -Pattern 'int\(os\.environ|float\(os\.environ'
# no matches
```

After the transformation, expect `MAX_PAYLOAD_SIZE_BYTES`, `MAX_INVOCATION_DEPTH`,
`MAX_LOOP_ITERATIONS`, `MAX_RETRIES`, `MAX_PAGINATION_PAGES` and friends to be honoured.

## Suggested short demo path

If you only have a few minutes, these five files cover every phase:

1. `identity/jwt_custom_authorizer.py` — W1, W3, W9 (Phases 2, 4, 6)
2. `media_pipeline/image_thumbnail_generator.py` — W1, W3, W6 (Phases 2, 3, 4)
3. `logistics/carrier_rate_shopper.py` — W1, W4, W9 (Phase 4 retry, no IaC fallback)
4. `data_platform/athena_query_poller.py` — W1, W2, W5, W8 (Phase 4 loops + pagination)
5. `messaging/webhook_retry_scheduler.py` — W1, W4, W7, W11 (Phase 3 depth + Phase 6 SQS)

## Verifying the transformation preserved behaviour

The corpus has no test suite by design — the transformation's contract is that existing
behaviour is preserved and only abnormal input is blocked. The cheapest regression check is
that the tree still compiles and every entry point survives:

```powershell
python -m compileall -q lambdas
Select-String -Path lambdas\*\*.py -Pattern 'def lambda_handler\(event, context\)' | Measure-Object
# expect 100
```

After hardening, these should flip from 0 to non-zero:

```powershell
Select-String -Path lambdas\*\*.py -Pattern 'get_remaining_time_in_millis' -List | Measure-Object
Select-String -Path lambdas\*\*.py -Pattern 'MAX_PAYLOAD_SIZE_BYTES' -List | Measure-Object
```
