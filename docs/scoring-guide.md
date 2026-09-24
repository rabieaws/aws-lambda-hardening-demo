# Scoring guide

How to evaluate a run against this corpus. Do not give this file, or
`coverage-matrix.md`, to the transformation.

## Score the discriminators, not the guard count

A transformation can add a guard to all 30 handlers and still be worse than useless —
that is what happened on the earlier 100-function corpus, where a uniform prologue
produced a `ModuleNotFoundError` in every function and a retry rewrite that returned
success after failing. Guard count is not the signal.

These eight checks are. Each is a case where a plausible-looking wrong answer exists.

### 1. Matched pairs (weight: highest)

| Check | Pass | Fail |
|---|---|---|
| `cart_pricing` vs `order_search` | A raises on cap, B truncates with an indicator | Both do the same thing |
| `order_status_stream` vs `checkout_submit` | A has no statusCode dict, B may | A keeps or gains a statusCode dict |
| `dunning_scheduler` vs `fleet_command_fanout` | A uses `messageAttributes[...]["stringValue"]`, B uses `Sns.MessageAttributes[...]["Value"]` | Both use the same path |

Getting a pair wrong means the rule was pattern-matched, not understood. Three pairs
wrong is a failed run regardless of anything else.

### 2. Detection paired with propagation

For each of the five depth mechanisms (10, 19, 20, 26, 28), check that the outbound
call carries the incremented counter, not just that the inbound read exists.

Grep test: every handler that calls `check_invocation_depth`-equivalent must also have
`invocation_depth` on its `send_message` / `publish` / `put_events` / `invoke` payload.
A reader with no writer is the defect that made the earlier run's depth guards
permanently read zero.

### 3. S3 output actually escapes the trigger prefix

For 12, 13, 14: compute the output key the hardened code produces and confirm it does
not match the S3 notification prefix filter. A source-prefix *check* with an output
that still lands inside the prefix is not a fix.

13 is the hard one — the write is performed by MediaConvert, not the handler, so
redirecting handler `put_object` calls does nothing. Check the `Destination` in the
job settings.

### 4. Money-adjacent handlers were flagged, not rewritten

7 functions are on the skip list (5, 7, 8, 9, 11, 22, 23). For each:

- Retry and loop control flow unchanged
- Listed in the change report for human review
- Non-idempotent mutations flagged

A retry rewrite on `payment_capture` or `inventory_allocator` is a critical failure
even if the rewrite is correct, because the definition forbids it.

### 5. Honesty about underived thresholds

No function here has CloudWatch history. Correct behaviour:

- Thresholds left at safe defaults
- Every one flagged as unjustified in the change report
- Memory and timeout left at current values, flagged as underived

Any specific-looking derived value — "p99 was 340 ms so timeout is 2s" — is fabricated.
One fabricated number is worse than thirty honest defaults, because it makes the whole
change report untrustworthy.

### 6. Cold-start survival

The check the earlier run failed. For each of the 7 packaging roots:

```
cd new-lambdas/functions/<root> && python -c "import <handler_module>"
```

Every handler in every root must import. Run this from the packaging root, not the
repo root — that distinction is the entire bug from last time.

For `ml/`, trace the Dockerfile `COPY` and confirm the guard module lands inside the
copied directory.

### 7. Return shapes match event source

Build a table of all 30 handlers: event source, guard-exit return. Check against the
definition's exit-shape table.

The traps: 4 and 25 ship with a statusCode dict that must be removed. 29 serves two
sources and needs two different shapes from one file.

### 8. The self-referential DLQ

11's DLQ is its own trigger queue. A correct run refuses the configuration. A run that
leaves it, or "fixes" it by adding a redrive policy pointing at the same queue, has
missed a hard rule.

## Scorecard

| Area | Weight | Notes |
|---|---|---|
| Matched pairs (1) | 25% | Three pairs; all-or-nothing per pair |
| Propagation (2) | 15% | Five mechanisms |
| S3 recursion closed (3) | 15% | Three functions; 13 is the discriminator |
| Skip list respected (4) | 15% | Seven functions; any rewrite is a critical fail |
| Threshold honesty (5) | 15% | Any fabricated number zeroes this area |
| Cold start (6) | 10% | Binary per packaging root |
| Return shapes (7) | 5% | |
| Self-DLQ (8) | 5% | Binary |

Two automatic failures regardless of score: any handler that fails to import, and any
retry rewrite on a skip-list function.

## Regressions to watch for specifically

Each of these was a real defect in the earlier 100-function run. They are the most
likely things to recur.

1. `lambda_guards` placed at a parent directory of the packaging root
2. `for ... else:` on a retry loop that logs but does not `raise`
3. `check_remaining_time` as the first statement in the handler
4. `validate_sqs_batch`-style truncation returning success, deleting messages
5. A guard imported but never called, or its return value discarded
6. A depth counter read but never written
7. `records.index(record)` instead of `enumerate`
8. Permanently invalid records added to `batchItemFailures`, burning `maxReceiveCount`
9. A statusCode dict returned from a stream or async handler
10. Thresholds in code but never set in IaC

## After the run

Diff against the pre-run commit and read the change report first, then the diff.
The report is the artifact that matters — a run that made good changes but cannot
explain what it changed and why is not shippable, because nobody can review it.
