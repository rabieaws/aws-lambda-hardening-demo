"""Authorization capture worker.

Event source: SQS queue ``payment-capture-requests``.

Each message asks for a partial or full capture against an existing authorization.
The worker enforces the network capture window, tracks cumulative captured amounts
against the authorized total, and isolates per-record failures so only the failing
messages are returned to SQS through ``batchItemFailures``.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_sqs_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

AUTH_TABLE = os.environ.get("AUTH_TABLE", "payment-authorizations")
CAPTURE_TABLE = os.environ.get("CAPTURE_TABLE", "payment-captures")
CAPTURE_EVENT_TOPIC = os.environ.get("CAPTURE_EVENT_TOPIC", "")

CAPTURE_WINDOW_SECONDS = {
    "visa": 604800,
    "mastercard": 604800,
    "amex": 2592000,
    "discover": 1209600,
}
DEFAULT_CAPTURE_WINDOW_SECONDS = 604800
OVERCAPTURE_TOLERANCE = Decimal("0.10")
MONEY_QUANTUM = Decimal("0.01")


class CaptureRejected(Exception):
    """Raised for permanent, non-retryable capture failures."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)


def _load_authorization(auth_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(AUTH_TABLE)
    response = table.get_item(Key={"idempotency_key": auth_id})
    item = response.get("Item")
    if not item:
        raise CaptureRejected("authorization_not_found:%s" % auth_id)
    if item.get("status") != "approved":
        raise CaptureRejected("authorization_not_capturable:%s" % item.get("status"))
    return item


def _window_expired(authorization: Dict[str, Any], now: int) -> bool:
    network = str(authorization.get("network", "visa")).lower()
    window = CAPTURE_WINDOW_SECONDS.get(network, DEFAULT_CAPTURE_WINDOW_SECONDS)
    created_at = int(authorization.get("created_at", now))
    return (now - created_at) > window


def _captured_to_date(auth_id: str) -> Decimal:
    table = dynamodb.Table(CAPTURE_TABLE)
    total = Decimal("0.00")
    response = table.query(
        KeyConditionExpression=Key("authorization_id").eq(auth_id)
    )
    for row in response.get("Items", []):
        if row.get("status") == "captured":
            total += _money(row.get("amount", "0"))
    return total.quantize(MONEY_QUANTUM)


def _remaining_capacity(authorized: Decimal, captured: Decimal) -> Decimal:
    ceiling = (authorized * (Decimal("1") + OVERCAPTURE_TOLERANCE)).quantize(MONEY_QUANTUM)
    remaining = ceiling - captured
    if remaining < 0:
        return Decimal("0.00")
    return remaining


def _write_capture(record: Dict[str, Any]) -> None:
    table = dynamodb.Table(CAPTURE_TABLE)
    try:
        table.put_item(
            Item=record,
            ConditionExpression="attribute_not_exists(capture_id)",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        logger.info("capture_duplicate_ignored capture_id=%s", record["capture_id"])


def _publish_event(record: Dict[str, Any]) -> None:
    if not CAPTURE_EVENT_TOPIC:
        return
    try:
        sns.publish(
            TopicArn=CAPTURE_EVENT_TOPIC,
            Subject="capture.settled",
            Message=json.dumps(record, default=str),
            MessageAttributes={
                "authorization_id": {
                    "DataType": "String",
                    "StringValue": record["authorization_id"],
                }
            },
        )
    except ClientError as exc:
        logger.warning("capture_event_publish_failed error=%s", exc)


def _process_capture(payload: Dict[str, Any], now: int) -> Dict[str, Any]:
    auth_id = payload.get("authorization_id")
    capture_id = payload.get("capture_id")
    if not auth_id or not capture_id:
        raise CaptureRejected("missing_identifiers")

    authorization = _load_authorization(auth_id)
    if _window_expired(authorization, now):
        raise CaptureRejected("capture_window_expired:%s" % auth_id)

    authorized = _money(authorization.get("amount", "0"))
    captured = _captured_to_date(auth_id)
    remaining = _remaining_capacity(authorized, captured)

    if payload.get("full_capture"):
        requested = authorized - captured
    else:
        requested = _money(payload.get("amount", "0"))

    if requested <= 0:
        raise CaptureRejected("nothing_to_capture:%s" % auth_id)
    if requested > remaining:
        raise CaptureRejected(
            "capture_exceeds_authorization:%s>%s" % (requested, remaining)
        )

    is_final = (captured + requested) >= authorized
    record = {
        "authorization_id": auth_id,
        "capture_id": capture_id,
        "merchant_id": authorization.get("merchant_id"),
        "currency": authorization.get("currency", "USD"),
        "amount": requested,
        "captured_to_date": captured + requested,
        "capture_type": "full" if is_final else "partial",
        "status": "captured",
        "captured_at": now,
    }
    _write_capture(record)
    _publish_event(record)
    logger.info(
        "capture_recorded auth=%s capture=%s amount=%s type=%s",
        auth_id,
        capture_id,
        requested,
        record["capture_type"],
    )
    return record


def _parse_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.error("capture_body_not_json message_id=%s", record.get("messageId"))
        return None


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records", [])
    now = int(time.time())
    failures: List[Dict[str, str]] = []
    settled = 0

    for record in records:
        message_id = record.get("messageId", "unknown")
        payload = _parse_record(record)
        if payload is None:
            continue

        try:
            _process_capture(payload, now)
            settled += 1
        except CaptureRejected as exc:
            logger.warning("capture_rejected message_id=%s reason=%s", message_id, exc)
        except ClientError as exc:
            logger.error(
                "capture_aws_error message_id=%s code=%s",
                message_id,
                exc.response.get("Error", {}).get("Code"),
            )
            failures.append({"itemIdentifier": message_id})
        except Exception:  # noqa: BLE001 - unexpected errors are retried by SQS
            logger.exception("capture_unhandled message_id=%s", message_id)
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "capture_batch_done received=%s settled=%s failed=%s",
        len(records),
        settled,
        len(failures),
    )
    return {"batchItemFailures": failures}
