# Coverage matrix

Do not give this file to the transformation. It names every planted weakness.

Step numbers refer to the `lambda-security-hardening` definition.

## Functions

### orders/ — SAM, packaging root `functions/orders/`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 1 | `checkout_submit` | API Gateway POST | Inline JWT signature verification in handler; no body size check; no request model in IaC | Move auth to an authorizer with caching (step 16); generate a request model + `RequestValidator` (step 17); keep `len(body)` as defence-in-depth only (step 28) |
| 2 | `cart_pricing` | API Gateway POST | Unbounded loop over promotion rules; output drives money math | Cap the loop and **raise** on breach — decision-driving (step 32) |
| 3 | `order_search` | API Gateway GET | Unbounded DynamoDB paginator; output returned to UI | Cap and **truncate with an explicit indicator** — reporting (step 32) |
| 4 | `order_status_stream` | DynamoDB Streams | Returns `{"statusCode": 200}`; no `batchItemFailures`; `BatchSize: 1000` in IaC | Remove the statusCode dict (return-shape rule); add `batchItemFailures` (step 35); add `FunctionResponseTypes` (step 12) |
| 5 | `inventory_allocator` | Direct invoke (Step Functions) | `while True` retry on conditional update; non-idempotent decrement; inventory-adjacent | **Skip list** (step 4) — flag for human review, do not rewrite retry. Flag non-idempotent (step 38). On guard exit must `raise`, not return a dict |
| 6 | `order_archiver` | EventBridge schedule | Unbounded paginator; `Timeout: 900`; `MemorySize: 3008` | Right-size memory (step 6); reduce timeout (step 7); cap the scan |

### billing/ — SAM, packaging root `functions/billing/`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 7 | `invoice_generator` | SQS | No `batchItemFailures`; invoice totals are money | Add partial failure reporting (step 35); skip list |
| 8 | `payment_capture` | SQS | `while True` retry against PSP; capture is non-idempotent | Skip list (step 4); flag non-idempotent (step 38); do not increase retry pressure |
| 9 | `ledger_poster` | SQS | Decision-driving loop **inside** per-record processing | Cap must raise, **and** the raise must be caught inside the record loop so it cannot fail the batch (step 32 batch interaction) |
| 10 | `dunning_scheduler` | SQS self-requeue | Re-sends to its own queue with no depth attribute | Add SQS depth read **and** write `invocation_depth` on the outbound `send_message` (step 31) |
| 11 | `refund_dispatcher` | SQS | Its DLQ in IaC **is its own trigger queue** | Detect and reject the self-referential DLQ (step 11 HARD RULE) |

### media/ — Serverless, per-file packaging, `python3.10`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 12 | `thumbnail_renderer` | S3 ObjectCreated | Writes 4 renditions back under the source prefix — exponential fan-out | Redirect output to `OUTPUT_PREFIX` (step 31, mandatory); add S3 notification prefix filter (step 13) |
| 13 | `transcode_launcher` | S3 ObjectCreated | MediaConvert destination is the same bucket/prefix — recursion via an intermediary service, which native loop detection cannot see | Redirect the MediaConvert destination, not just handler writes |
| 14 | `metadata_sidecar` | S3 ObjectCreated | Writes `.meta.json` beside source — linear recursion | Redirect output prefix |
| 15 | `waveform_extractor` | S3 ObjectCreated | `while True` chunked range-read; `timeout: 900` | Bound the loop; reduce timeout |
| 16 | `cdn_invalidator` | S3 ObjectCreated | Retry with uncapped `2 ** attempt` backoff | Cap backoff (step 34); note this function is the cost multiplier for any unclosed loop above |

### telemetry/ — Terraform, packaging root `functions/telemetry/`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 17 | `sensor_ingest` | Kinesis | Bare success return; no `batchItemFailures`; `eventID` unused | Add partial failure reporting keyed on `eventID` (step 35) |
| 18 | `anomaly_scorer` | Kinesis | `get_remaining_time_in_millis()` checked **as the first statement**; decision-driving scoring | Move the check inside the record loop (step 33 placement rule); cap must raise |
| 19 | `device_shadow_sync` | IoT Core | Self-invoke continuation via `lambda_client.invoke`, no depth | Depth must travel in the `Payload`, read from `event` not `os.environ` (step 31) |
| 20 | `fleet_command_fanout` | SNS | Publishes to its own topic; reads attributes via the **SQS** path on an SNS event | Fix to `Sns.MessageAttributes[...]["Value"]` (step 31 warning); add depth on outbound `publish` |
| 21 | `telemetry_rollup` | EventBridge schedule | **No timeout set at all** in Terraform | Set an explicit timeout (step 7); greenfield fallback applies |

### reporting/ — Terraform, packaging root `functions/reporting/`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 22 | `revenue_reconciliation` | S3 ObjectCreated | Paginator over internal captures; truncation **invents** reconciliation breaks | Cap must **raise** (step 32); skip list. This is the exact failure mode from the earlier 100-function run |
| 23 | `usage_metering` | EventBridge | Paginator drives billable usage totals | Cap must raise; skip list |
| 24 | `dashboard_snapshot` | API Gateway GET | Paginator feeds a display payload | Cap must truncate with indicator |
| 25 | `cohort_export` | Direct invoke | Reporting paginator; returns `{"statusCode": 200}` from a direct-invoke handler | Truncate with indicator; remove the statusCode dict — a direct-invoke `{"statusCode": 503}` reads as success to the caller |

### platform/ — CDK, packaging root `functions/platform/`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 26 | `notification_router` | SNS | Republishes to its own topic on **any** dispatch failure, no delay, no depth — a guaranteed live loop | Depth read on the SNS path + depth write on `publish` (step 31) |
| 27 | `webhook_relay` | Function URL | No API Gateway in front; unbounded body read | Function URL profile (steps 19, 30): payload check is the *primary* layer; reserved concurrency is the only throttle; recommend CloudFront + WAF |
| 28 | `audit_event_replay` | EventBridge | Re-puts to the same bus it consumes | Depth via `detail` attributes on `put_events` (step 31) |
| 29 | `tenant_provisioner` | **API Gateway + SQS** | Single handler serving two sources; non-idempotent tenant creation | Source detection and per-source dispatch — API Gateway path returns a dict, SQS path returns `batchItemFailures`. Flag non-idempotent |

### ml/ — container image, `functions/ml/`

| # | Function | Event source | Planted weakness | Expected action |
|---|---|---|---|---|
| 30 | `embedding_batch` | SQS | Container image packaging; no `batchItemFailures` | Packaging root must be discovered by tracing the Dockerfile `COPY` (step 25); guard module placed inside the copied source dir |

## Rule coverage

Every rule in the definition and the function(s) that exercise it.

| Definition rule | Test cases |
|---|---|
| Entry 3 — runtime gate | `checkout_submit`, `cart_pricing` on `python3.9` (block); `media/*` on `python3.10` (migrate-now) |
| Entry 5 — version control | Whole corpus; verify the run requires a clean tree |
| 1.2 — auth-in-handler detection | 1 |
| 1.2 — self-invoke detection | 19 |
| 1.2 — non-idempotent detection | 5, 8, 29 |
| 1.2 — decision vs reporting classification | 2, 9, 18, 22, 23 (decision) vs 3, 24, 25 (reporting) |
| 1.4 — skip list | 5, 7, 8, 9, 11, 22, 23 |
| 1.5 — baseline capture | Whole corpus |
| 2.6 — memory right-sizing | 6 (3008 MB), 15 (2048 MB) |
| 2.6 / 2.7 — greenfield fallback | Whole corpus (no CloudWatch history anywhere) |
| 2.7 — timeout | 6, 15 (900s), 21 (unset) |
| 2.8 — reserved concurrency | None set anywhere |
| 2.9 — SQS `MaximumConcurrency` | 7–11, 29, 30 |
| 2.10 — platform retry bounds | None set anywhere |
| 2.11 — DLQ per invocation model | None set; 11 has the self-referential DLQ |
| 2.12 — `FunctionResponseTypes` | 4, 7–11, 17, 18, 29, 30 |
| 2.13 — S3 prefix filter | 12–16, 22 |
| 2.14 — log retention | None set anywhere |
| 2.15 — threshold env vars | None set anywhere |
| 3.16 — auth to authorizer | 1 |
| 3.17 — request validation | 1, 2, 3, 24 |
| 3.18 — method throttling | All API Gateway methods |
| 3.19 / 6.30 — Function URL profile | 27 |
| 5.25 — packaging roots | 7 roots, 5 mechanisms |
| 6 — return-shape rule | 4 (stream), 25 (direct invoke) — both plant a statusCode dict |
| 6 — no uniform prologue | Batch (7–11, 17, 18, 30) vs single-event (1–3, 5, 6, 12–16) |
| 6.28 — payload validation per source | 1, 27 (single-event); 7, 17, 30 (per-record) |
| 6.31 — S3 recursion | 12, 13, 14 |
| 6.31 — SQS depth | 10 |
| 6.31 — SNS depth + attribute path | 20, 26 |
| 6.31 — EventBridge depth | 28 |
| 6.31 — self-invoke depth | 19 |
| 6.32 — cap raises (decision) | 2, 9, 18, 22, 23 |
| 6.32 — cap truncates (reporting) | 3, 24, 25 |
| 6.32 — cap raise must not escape batch | 9 |
| 6.33 — remaining-time placement | 18 (planted at handler entry) |
| 6.34 — bounded retry | 5, 8, 15, 16 |
| 6.35 — `batchItemFailures` | 4, 7, 17, 30 |
| 7.38 — idempotency | 5, 8, 29 |
| Multi-source dispatch | 29 |

## Verified inventory

Counts confirmed by grep against the corpus, not from the design intent. Use these
as the denominators when scoring.

| Plant | Count | Sites |
|---|---|---|
| `while True` unbounded loops | 6 | `payment_capture:109`, `inventory_allocator:53`, `waveform_extractor:87`, `cdn_invalidator:78`, `cart_pricing:41`, `fleet_command_fanout:72` |
| Uncapped boto3 paginator drains | 11 | `ledger_poster:59`, `order_archiver:59`, `order_search:42`, `audit_event_replay:81`, `cohort_export:50` and `:64`, `dashboard_snapshot:53`, `revenue_reconciliation:79`, `usage_metering:81`, `anomaly_scorer:106`, `telemetry_rollup:54` |
| Batch handlers with no `batchItemFailures` | 5 | `order_status_stream`, `invoice_generator`, `sensor_ingest`, `embedding_batch`, `anomaly_scorer` |
| S3 outputs derived from the source key stem | 4 files | `thumbnail_renderer:132`, `transcode_launcher:175` and `:249`, `metadata_sidecar:157`, `waveform_extractor:141` |
| `statusCode` dict on a non-API handler | 2 handlers | `order_status_stream:201`, `cohort_export:180/190/197` |
| Self-republish / re-put to own trigger | 4 | `notification_router:165` (SNS), `fleet_command_fanout:153` (SNS), `audit_event_replay:152` (EventBridge), `dunning_scheduler:155` (SQS) |
| Self-invoke via `lambda_client.invoke` | 1 | `device_shadow_sync:151` |
| Remaining-time check at handler entry | 1 | `anomaly_scorer:246` |
| SQS attribute path on an SNS handler | 1 | `fleet_command_fanout:41` |

Two counts are higher than the per-function table above implies, because some
functions carry a second instance that emerged from writing realistic logic:

- `cart_pricing` and `fleet_command_fanout` each contain a `while True` pagination
  loop in addition to their headline weakness.
- `waveform_extractor` writes a stem-derived S3 output, so the media recursion group
  is four functions rather than three. Its chain terminates by accident (the peaks
  JSON fails `parse_riff_header` on re-entry) rather than by design — a correct run
  should still redirect the output prefix, and a run that argues the loop is already
  safe has missed that the safety is incidental.
- `anomaly_scorer` is both a decision-driving cap case and a batch handler with no
  partial-failure reporting.

## Baseline state

Confirmed before any transformation runs:

- 30 handlers, 7 packaging roots, all compile under Python 3.12
- All 30 import successfully with the packaging root as `sys.path[0]`, so the corpus
  is deployable as-is. Any import failure after the run is caused by the run.
- No cross-imports between packaging roots. Every handler depends only on the standard
  library plus `boto3`/`botocore`.
- IaC confirmed to contain none of: reserved concurrency, `FunctionResponseTypes`,
  DLQ or on-failure destinations, `MaximumRetryAttempts`, method throttling, request
  validators or models, log retention, threshold environment variables,
  `MaximumConcurrency`, or S3 notification prefix filters.

## Known gaps in this corpus

Stated so you do not read absence of coverage as a pass.

- **`RecursiveLoop` config (step 3)** cannot be expressed in any of these IaC files;
  it is an API-level function property. Verify manually that the run reports recursion
  detection status per function.
- **Phase 4** (cost detection) is opt-in and account-scoped. Nothing here forces it.
  Run once with the flag and once without, and check the change report notes the
  unmonitored async timeout-to-DLQ path when it is off.
- **`MAX_BACKOFF_SECONDS` / `parse_float_env`** is only exercised through 16.
- **Quarantine queue routing** for permanently invalid records (step 35) has no
  pre-existing queue to route to; check whether the run creates one or flags the gap.
