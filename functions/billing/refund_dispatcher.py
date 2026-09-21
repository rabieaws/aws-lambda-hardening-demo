"""Refund dispatch worker.

Event source: SQS queue fed by the returns and disputes services.
Validates refund eligibility against the original capture, sends the refund to the PSP,
and writes the refund record plus its reversing ledger entry request.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from lambda_guards import (
    MAX_RETRIES,
    MAX_BACKOFF_SECONDS,
    MAX_LOOP_ITERATIONS,
    _emit_guard_metric,
    check_remaining_time,
    validate_record_size,
    PermanentError,
    IterationCapExceeded,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

REFUND_TABLE = os.environ.get("REFUND_TABLE", "refunds")
CAPTURE_TABLE = os.environ.get("CAPTURE_TABLE", "captures")
LEDGER_QUEUE_URL = os.environ.get("LEDGER_QUEUE_URL", "")
PSP_ENDPOINT = os.environ.get("PSP_ENDPOINT", "https://psp.internal")
PSP_API_KEY = os.environ.get("PSP_API_KEY", "")
HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "8"))

CENTS = Decimal("0.01")
SECONDS_PER_DAY = 86400
REFUND_WINDOW_DAYS = int(os.environ.get("REFUND_WINDOW_DAYS", "180"))

REASON_CODES = {
    "customer_return": Decimal("1.00"),
    "damaged_in_transit": Decimal("1.00"),
    "never_arrived": Decimal("1.00"),
    "price_adjustment": Decimal("0.50"),
    "goodwill": Decimal("0.25"),
}
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class RefundRejected(Exception):
    """Raised when a refund can never be dispatched."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def load_capture(capture_id: str) -> Optional[Dict[str, Any]]:
    table = dynamodb.Table(CAPTURE_TABLE)
    response = table.get_item(Key={"capture_id": capture_id})
    return response.get("Item")


def already_refunded(capture_id: str) -> Decimal:
    """Sum of refunds already issued against a capture.

    This is decision-driving (determines refund eligibility), so iteration is
    capped with fail_on_cap behaviour -- an incomplete total would approve
    refunds that exceed the capture amount.
    """
    from lambda_guards import safe_iterate

    table = dynamodb.Table(REFUND_TABLE)
    response = table.query(
        IndexName=os.environ.get("REFUND_CAPTURE_INDEX", "by-capture"),
        KeyConditionExpression="capture_id = :cid",
        ExpressionAttributeValues={":cid": capture_id},
    )
    total = Decimal("0.00")
    for item in safe_iterate(response.get("Items", []), max_items=MAX_LOOP_ITERATIONS, fail_on_cap=True):
        if str(item.get("status", "")).upper() in {"REFUNDED", "PENDING"}:
            total += _money(item.get("amount", "0"))
    return total


def validate_refund(payload: Dict[str, Any], capture: Dict[str, Any]) -> Decimal:
    """Validate eligibility and return the refundable amount."""
    reason = str(payload.get("reason", "")).lower()
    if reason not in REASON_CODES:
        raise RefundRejected("unknown reason code %s" % reason)

    if str(capture.get("status", "")).upper() != "CAPTURED":
        raise RefundRejected("capture status %s is not refundable" % capture.get("status"))

    captured_at = int(capture.get("captured_at", 0))
    age_days = (int(time.time()) - captured_at) // SECONDS_PER_DAY
    if age_days > REFUND_WINDOW_DAYS:
        raise RefundRejected("capture is %s days old, outside the refund window" % age_days)

    captured_amount = _money(capture.get("amount", "0"))
    requested = _money(payload.get("amount", captured_amount))
    if requested <= 0:
        raise RefundRejected("refund amount must be positive")

    prior = already_refunded(str(capture["capture_id"]))
    remaining = captured_amount - prior
    if requested > remaining:
        raise RefundRejected("refund %s exceeds remaining %s" % (requested, remaining))

    max_fraction = REASON_CODES[reason]
    ceiling = _money(captured_amount * max_fraction)
    if prior + requested > ceiling:
        raise RefundRejected(
            "reason %s permits at most %s, already refunded %s" % (reason, ceiling, prior)
        )

    return requested


def submit_to_psp(refund_id: str, capture: Dict[str, Any], amount: Decimal) -> Dict[str, Any]:
    """Send the refund to the PSP with a bounded number of transient retries."""
    body = {
        "refund_id": refund_id,
        "psp_capture_reference": str(capture.get("psp_reference", "")),
        "amount": str(amount),
        "currency": str(capture.get("currency", "USD")),
    }
    request = urllib.request.Request(
        "{0}/v1/refunds".format(PSP_ENDPOINT.rstrip("/")),
        data=json.dumps(body, default=str).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(PSP_API_KEY),
        },
        method="POST",
    )

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as handle:
                return json.loads(handle.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS:
                raise RefundRejected("psp rejected with status %s" % exc.code)
            logger.info("psp_refund_transient status=%s attempt=%s", exc.code, attempt)
            _emit_guard_metric("RetryAttempt", 1)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            logger.info("psp_refund_unreachable attempt=%s error=%s", attempt, exc)
            _emit_guard_metric("RetryAttempt", 1)
        time.sleep(min(0.25 * (2 ** attempt), MAX_BACKOFF_SECONDS))

    _emit_guard_metric("RetryExhausted", 1)
    raise ConnectionError("psp unreachable after %d attempts for refund %s" % (MAX_RETRIES, refund_id))


def record_refund(
    refund_id: str, capture_id: str, amount: Decimal, reason: str, psp_reference: str
) -> None:
    dynamodb.Table(REFUND_TABLE).put_item(
        Item={
            "refund_id": refund_id,
            "capture_id": capture_id,
            "amount": str(amount),
            "reason": reason,
            "psp_reference": psp_reference,
            "status": "REFUNDED",
            "refunded_at": int(time.time()),
        }
    )


def request_reversing_entry(refund_id: str, capture: Dict[str, Any], amount: Decimal) -> None:
    """Ask the ledger to post the reversing journal entry."""
    if not LEDGER_QUEUE_URL:
        logger.warning("ledger_queue_unconfigured refund=%s", refund_id)
        return
    sqs.send_message(
        QueueUrl=LEDGER_QUEUE_URL,
        MessageBody=json.dumps(
            {
                "entry_id": "rev_{0}".format(refund_id),
                "lines": [
                    {"account_code": "REVENUE", "side": "DEBIT", "amount": str(amount)},
                    {"account_code": "CASH", "side": "CREDIT", "amount": str(amount)},
                ],
            }
        ),
    )


def process_record(record: Dict[str, Any]) -> str:
    """Dispatch one refund. Returns the refund id."""
    payload = json.loads(record.get("body") or "{}")
    capture_id = str(payload.get("capture_id", "")).strip()
    if not capture_id:
        raise RefundRejected("capture_id is required")

    capture = load_capture(capture_id)
    if capture is None:
        raise RefundRejected("capture %s not found" % capture_id)

    amount = validate_refund(payload, capture)
    reason = str(payload.get("reason", "")).lower()
    refund_id = str(payload.get("refund_id") or "ref_{0}_{1}".format(capture_id, int(time.time())))

    response = submit_to_psp(refund_id, capture, amount)
    psp_reference = str(response.get("reference", ""))

    record_refund(refund_id, capture_id, amount, reason, psp_reference)
    request_reversing_entry(refund_id, capture, amount)

    logger.info(
        "refund_dispatched refund=%s capture=%s amount=%s reason=%s",
        refund_id, capture_id, amount, reason,
    )
    return refund_id


def lambda_handler(event, context):
    refunded: List[str] = []
    rejected = 0
    failures: List[Dict[str, str]] = []

    records = event.get("Records", [])
    for i, record in enumerate(records):
        # Remaining-time check inside the loop, not at handler entry
        if not check_remaining_time(context):
            failures.extend(
                {"itemIdentifier": r.get("messageId", "unknown")}
                for r in records[i:]
            )
            break

        message_id = record.get("messageId", "unknown")
        try:
            validate_record_size(record)
            refunded.append(process_record(record))
        except PermanentError:
            # Permanently invalid: log, metric, do NOT add to batchItemFailures
            rejected += 1
            _emit_guard_metric("PermanentRecordDropped", 1)
            logger.warning("refund_record_oversized message_id=%s", message_id)
        except RefundRejected as exc:
            # Permanent business rejection -- do NOT add to batchItemFailures
            rejected += 1
            _emit_guard_metric("RefundRejected", 1)
            logger.warning("refund_rejected message_id=%s reason=%s", message_id, exc)
        except json.JSONDecodeError:
            # Permanent parse failure -- do NOT add to batchItemFailures
            rejected += 1
            _emit_guard_metric("RefundBodyNotJson", 1)
            logger.error("refund_body_not_json message_id=%s", message_id)
        except (ClientError, ConnectionError) as exc:
            # Transient failure -- add to batchItemFailures for retry
            logger.exception("refund_dispatch_failed message_id=%s error=%s", message_id, exc)
            _emit_guard_metric("RefundDispatchFailed", 1)
            failures.append({"itemIdentifier": message_id})
        except IterationCapExceeded as exc:
            # Transient -- iteration cap on decision-driving query, retry may succeed with less data
            logger.warning("refund_iteration_cap_exceeded message_id=%s error=%s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "refund_batch_complete refunded=%s rejected=%s failed=%s",
        len(refunded), rejected, len(failures),
    )
    return {"batchItemFailures": failures, "refunded": len(refunded), "rejected": rejected}
