"""Payment capture worker.

Event source: SQS queue fed by the authorization service once an order is ready to
ship. Captures a previously authorized payment against the PSP and records the
settlement reference.
"""

import json
import logging
import os
import random
import time
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

CAPTURE_TABLE = os.environ.get("CAPTURE_TABLE", "captures")
AUTHORIZATION_TABLE = os.environ.get("AUTHORIZATION_TABLE", "authorizations")
PSP_ENDPOINT = os.environ.get("PSP_ENDPOINT", "https://psp.internal")
PSP_API_KEY = os.environ.get("PSP_API_KEY", "")
HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "8"))

CENTS = Decimal("0.01")
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
PERMANENT_DECLINE_CODES = {
    "card_expired",
    "card_revoked",
    "authorization_expired",
    "amount_exceeds_authorization",
    "fraud_suspected",
}
BASE_BACKOFF_SECONDS = 0.25


class CaptureRejected(Exception):
    """Raised when a capture request can never succeed."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def load_authorization(authorization_id: str) -> Optional[Dict[str, Any]]:
    table = dynamodb.Table(AUTHORIZATION_TABLE)
    response = table.get_item(Key={"authorization_id": authorization_id})
    return response.get("Item")


def _validate(payload: Dict[str, Any], authorization: Dict[str, Any]) -> Decimal:
    """Validate the capture against its authorization and return the capture amount."""
    requested = _money(payload.get("amount", "0"))
    if requested <= 0:
        raise CaptureRejected("capture amount must be positive")

    authorized = _money(authorization.get("amount", "0"))
    already_captured = _money(authorization.get("captured_amount", "0"))
    remaining = authorized - already_captured

    if requested > remaining:
        raise CaptureRejected(
            "capture %s exceeds remaining authorization %s" % (requested, remaining)
        )

    expires_at = int(authorization.get("expires_at", 0))
    if expires_at and expires_at < int(time.time()):
        raise CaptureRejected("authorization expired")

    if str(authorization.get("status", "")).upper() not in {"AUTHORIZED", "PARTIALLY_CAPTURED"}:
        raise CaptureRejected(
            "authorization status %s is not capturable" % authorization.get("status")
        )

    return requested


def _call_psp(body: Dict[str, Any]) -> Dict[str, Any]:
    request = urllib.request.Request(
        "{0}/v1/captures".format(PSP_ENDPOINT.rstrip("/")),
        data=json.dumps(body, default=str).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(PSP_API_KEY),
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as handle:
        return json.loads(handle.read().decode("utf-8"))


def capture_at_psp(authorization: Dict[str, Any], amount: Decimal, capture_id: str) -> Dict[str, Any]:
    """Send the capture to the PSP, retrying while the failure is transient."""
    from lambda_guards import MAX_RETRIES, MAX_BACKOFF_SECONDS, _emit_guard_metric
    body = {
        "capture_id": capture_id,
        "psp_authorization_reference": str(authorization.get("psp_reference", "")),
        "amount": str(amount),
        "currency": str(authorization.get("currency", "USD")),
    }

    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            response = _call_psp(body)
            code = str(response.get("decline_code", "")).lower()
            if code and code in PERMANENT_DECLINE_CODES:
                raise CaptureRejected("psp declined: %s" % code)
            return response
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS:
                raise CaptureRejected("psp rejected with status %s" % exc.code)
            last_exception = exc
            logger.info("psp_transient status=%s attempt=%s", exc.code, attempt)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exception = exc
            logger.info("psp_unreachable attempt=%s error=%s", attempt, exc)

        delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 0.2), MAX_BACKOFF_SECONDS)
        _emit_guard_metric("RetryAttempt", 1)
        time.sleep(delay)

    _emit_guard_metric("RetryExhausted", 1)
    raise last_exception


def record_capture(
    capture_id: str, authorization_id: str, amount: Decimal, psp_reference: str
) -> None:
    """Persist the capture and advance the authorization's captured total."""
    dynamodb.Table(CAPTURE_TABLE).put_item(
        Item={
            "capture_id": capture_id,
            "authorization_id": authorization_id,
            "amount": str(amount),
            "psp_reference": psp_reference,
            "status": "CAPTURED",
            "captured_at": int(time.time()),
        }
    )

    dynamodb.Table(AUTHORIZATION_TABLE).update_item(
        Key={"authorization_id": authorization_id},
        UpdateExpression=(
            "SET captured_amount = captured_amount + :amount, "
            "#st = :status, updated_at = :now"
        ),
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":amount": amount,
            ":status": "CAPTURED",
            ":now": int(time.time()),
        },
    )


def process_record(record: Dict[str, Any]) -> str:
    """Capture one payment. Returns the capture id."""
    payload = json.loads(record.get("body") or "{}")
    authorization_id = str(payload.get("authorization_id", "")).strip()
    if not authorization_id:
        raise CaptureRejected("authorization_id is required")

    authorization = load_authorization(authorization_id)
    if authorization is None:
        raise CaptureRejected("authorization %s not found" % authorization_id)

    amount = _validate(payload, authorization)
    capture_id = "cap_{0}_{1}".format(authorization_id, int(time.time() * 1000))

    response = capture_at_psp(authorization, amount, capture_id)
    psp_reference = str(response.get("reference", ""))

    record_capture(capture_id, authorization_id, amount, psp_reference)

    logger.info(
        "payment_captured capture=%s authorization=%s amount=%s psp_ref=%s",
        capture_id, authorization_id, amount, psp_reference,
    )
    return capture_id


def lambda_handler(event, context):
    from lambda_guards import validate_record_size, check_remaining_time, PermanentError, _emit_guard_metric

    captured: List[str] = []
    rejected = 0
    failures: List[Dict[str, str]] = []

    records = event.get("Records", [])
    for idx, record in enumerate(records):
        message_id = record.get("messageId", "unknown")
        if not check_remaining_time(context):
            failures.extend({"itemIdentifier": r.get("messageId", "unknown")} for r in records[idx:])
            break
        try:
            validate_record_size(record)
            captured.append(process_record(record))
        except PermanentError as exc:
            _emit_guard_metric("PermanentRecordDropped", 1)
            logger.warning("capture_record_oversized message_id=%s error=%s", message_id, exc)
        except CaptureRejected as exc:
            rejected += 1
            logger.warning("capture_rejected message_id=%s reason=%s", message_id, exc)
        except json.JSONDecodeError:
            rejected += 1
            logger.error("capture_body_not_json message_id=%s", message_id)
        except ClientError as exc:
            logger.exception("capture_store_failed message_id=%s error=%s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "capture_batch_complete captured=%s rejected=%s failed=%s",
        len(captured), rejected, len(failures),
    )
    return {"batchItemFailures": failures, "captured": len(captured), "rejected": rejected}
