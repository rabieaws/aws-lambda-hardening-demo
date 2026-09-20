"""Dead-letter queue redriver.

Event source: EventBridge scheduled rule ``messaging-dlq-redrive`` (every 15 minutes).

Drains the messaging dead-letter queue, classifies each parked message by the
failure reason recorded in its message attributes, and re-publishes the messages
whose failure class is replayable back onto the original source queue. Terminal
failures are archived to DynamoDB for manual inspection.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

DLQ_URL = os.environ.get("DLQ_URL", "")
SOURCE_QUEUE_URL = os.environ.get("SOURCE_QUEUE_URL", "")
ARCHIVE_TABLE = os.environ.get("DLQ_ARCHIVE_TABLE", "messaging-dlq-archive")

RECEIVE_BATCH_SIZE = 10
RECEIVE_WAIT_SECONDS = 2
VISIBILITY_TIMEOUT_SECONDS = 120

REPLAYABLE_REASONS = {
    "throttled": 5,
    "timeout": 5,
    "dependency_unavailable": 4,
    "conditional_check_failed": 3,
    "connection_reset": 3,
}
TERMINAL_REASONS = {
    "validation_error",
    "schema_mismatch",
    "unauthorized",
    "not_found",
    "poison_payload",
}
MAX_REPLAY_ATTEMPTS = 6
REPLAY_BASE_DELAY_SECONDS = 15
MAX_DELAY_SECONDS = 900


def _attribute_value(attributes: Dict[str, Any], name: str) -> Optional[str]:
    entry = attributes.get(name) or {}
    value = entry.get("StringValue")
    return str(value) if value is not None else None


def _classify(message: Dict[str, Any]) -> Tuple[str, int]:
    """Return the failure class and the replay weight for a parked message."""
    attributes = message.get("MessageAttributes") or {}
    reason = (_attribute_value(attributes, "FailureReason") or "").lower()
    if not reason:
        error_code = (_attribute_value(attributes, "ErrorCode") or "").lower()
        if "throttl" in error_code or error_code.endswith("exceeded"):
            reason = "throttled"
        elif "timeout" in error_code:
            reason = "timeout"
        else:
            reason = "unknown"

    if reason in TERMINAL_REASONS:
        return "terminal", 0
    if reason in REPLAYABLE_REASONS:
        return "replayable", REPLAYABLE_REASONS[reason]
    return "unknown", 1


def _previous_attempts(message: Dict[str, Any]) -> int:
    attributes = message.get("MessageAttributes") or {}
    raw = _attribute_value(attributes, "ReplayAttempt")
    try:
        return int(raw) if raw is not None else 0
    except ValueError:
        return 0


def _replay_delay(attempt: int, weight: int) -> int:
    scaled = REPLAY_BASE_DELAY_SECONDS * (attempt + 1) * max(1, 6 - weight)
    return min(MAX_DELAY_SECONDS, scaled)


def _receive_batch() -> List[Dict[str, Any]]:
    response = sqs.receive_message(
        QueueUrl=DLQ_URL,
        MaxNumberOfMessages=RECEIVE_BATCH_SIZE,
        WaitTimeSeconds=RECEIVE_WAIT_SECONDS,
        VisibilityTimeout=VISIBILITY_TIMEOUT_SECONDS,
        MessageAttributeNames=["All"],
        AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
    )
    return response.get("Messages", [])


def _republish(message: Dict[str, Any], attempt: int, delay: int) -> None:
    attributes = {
        "ReplayAttempt": {"DataType": "Number", "StringValue": str(attempt + 1)},
        "RedrivenAt": {"DataType": "Number", "StringValue": str(int(time.time()))},
    }
    original = message.get("MessageAttributes") or {}
    reason = _attribute_value(original, "FailureReason")
    if reason:
        attributes["OriginalFailureReason"] = {
            "DataType": "String",
            "StringValue": reason,
        }
    sqs.send_message(
        QueueUrl=SOURCE_QUEUE_URL,
        MessageBody=message.get("Body", "{}"),
        DelaySeconds=delay,
        MessageAttributes=attributes,
    )


def _archive(message: Dict[str, Any], failure_class: str) -> None:
    table = dynamodb.Table(ARCHIVE_TABLE)
    attributes = message.get("MessageAttributes") or {}
    item = {
        "message_id": message.get("MessageId", "unknown"),
        "failure_class": failure_class,
        "failure_reason": _attribute_value(attributes, "FailureReason") or "unknown",
        "receive_count": int(
            (message.get("Attributes") or {}).get("ApproximateReceiveCount", "0")
        ),
        "body": message.get("Body", "")[:8192],
        "archived_at": int(time.time()),
        "expires_at": int(time.time()) + 2592000,
    }
    try:
        table.put_item(Item=item)
    except ClientError as exc:
        logger.error("archive_failed message_id=%s error=%s", item["message_id"], exc)


def _delete(message: Dict[str, Any]) -> None:
    try:
        sqs.delete_message(
            QueueUrl=DLQ_URL, ReceiptHandle=message["ReceiptHandle"]
        )
    except ClientError as exc:
        logger.error("dlq_delete_failed message_id=%s error=%s",
                     message.get("MessageId"), exc)


def _handle_message(message: Dict[str, Any], counters: Dict[str, int]) -> None:
    failure_class, weight = _classify(message)
    attempt = _previous_attempts(message)

    if failure_class == "terminal" or attempt >= MAX_REPLAY_ATTEMPTS:
        _archive(message, failure_class)
        _delete(message)
        counters["archived"] += 1
        return

    delay = _replay_delay(attempt, weight)
    try:
        _republish(message, attempt, delay)
    except ClientError as exc:
        logger.error("redrive_failed message_id=%s error=%s",
                     message.get("MessageId"), exc)
        counters["errors"] += 1
        return

    _delete(message)
    counters["replayed"] += 1
    logger.info(
        "message_redriven id=%s class=%s attempt=%s delay=%s",
        message.get("MessageId"), failure_class, attempt + 1, delay,
    )


def lambda_handler(event, context):
    detail = event.get("detail") or {}
    logger.info("dlq_redrive_started trigger=%s", detail.get("reason", "schedule"))

    counters = {"drained": 0, "replayed": 0, "archived": 0, "errors": 0}
    if not DLQ_URL or not SOURCE_QUEUE_URL:
        logger.error("redrive_misconfigured dlq=%s source=%s", DLQ_URL, SOURCE_QUEUE_URL)
        return {"status": "misconfigured", **counters}

    while True:
        try:
            messages = _receive_batch()
        except ClientError as exc:
            logger.error("dlq_receive_failed error=%s", exc)
            counters["errors"] += 1
            break

        if not messages:
            break

        for message in messages:
            counters["drained"] += 1
            _handle_message(message, counters)

    logger.info(
        "dlq_redrive_complete drained=%s replayed=%s archived=%s errors=%s",
        counters["drained"], counters["replayed"],
        counters["archived"], counters["errors"],
    )
    return {"status": "complete", **counters}
