"""Webhook retry scheduler.

Event source: SQS queue ``webhook-retry-scheduler`` (the same queue this function
re-enqueues attempts onto).

Reads a failed webhook delivery attempt, replays it against the subscriber
endpoint, and when the attempt fails again computes the next retry delay from the
attempt history using decorrelated jitter before re-enqueueing the attempt onto
its own queue with ``DelaySeconds``.
"""

import json
import logging
import os
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_sqs_batch,
    check_invocation_depth,
    get_invocation_depth,
    increment_invocation_depth,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

RETRY_QUEUE_URL = os.environ.get("RETRY_QUEUE_URL", "")
ATTEMPT_TABLE = os.environ.get("WEBHOOK_ATTEMPT_TABLE", "webhook-attempts")

BASE_DELAY_SECONDS = 4
MAX_SQS_DELAY_SECONDS = 900
JITTER_RATIO = 0.35
HTTP_TIMEOUT_SECONDS = 6
RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
PERMANENT_STATUS = {400, 401, 403, 404, 410, 422}
CIRCUIT_TRIP_CONSECUTIVE_FAILURES = 12
CIRCUIT_COOLDOWN_SECONDS = 1800


def _parse_attempt(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.warning("undecodable_attempt message_id=%s", record.get("messageId"))
        return None
    if not body.get("delivery_id") or not body.get("target_url"):
        return None
    body.setdefault("attempt_history", [])
    body["_message_id"] = record.get("messageId")
    return body


def _history_delays(history: List[Dict[str, Any]]) -> List[float]:
    return [float(entry.get("delay_seconds", 0.0)) for entry in history]


def _next_delay(history: List[Dict[str, Any]]) -> int:
    """Decorrelated jitter: grow from the previous delay, never exceed the SQS cap."""
    delays = _history_delays(history)
    previous = delays[-1] if delays else 0.0
    ceiling = max(BASE_DELAY_SECONDS, previous * 3.0)
    candidate = random.uniform(BASE_DELAY_SECONDS, ceiling)
    jitter = candidate * JITTER_RATIO * (random.random() - 0.5) * 2.0
    return int(max(1.0, min(float(MAX_SQS_DELAY_SECONDS), candidate + jitter)))


def _consecutive_failures(history: List[Dict[str, Any]]) -> int:
    count = 0
    for entry in reversed(history):
        if entry.get("outcome") == "success":
            break
        count += 1
    return count


def _circuit_open(subscriber_id: str, now: int) -> bool:
    table = dynamodb.Table(ATTEMPT_TABLE)
    try:
        response = table.get_item(Key={"delivery_id": "circuit#" + subscriber_id})
    except ClientError as exc:
        logger.error("circuit_lookup_failed subscriber=%s error=%s", subscriber_id, exc)
        return False
    item = response.get("Item") or {}
    opened_at = int(item.get("opened_at", 0))
    return bool(opened_at) and now - opened_at < CIRCUIT_COOLDOWN_SECONDS


def _trip_circuit(subscriber_id: str, now: int) -> None:
    table = dynamodb.Table(ATTEMPT_TABLE)
    try:
        table.put_item(Item={
            "delivery_id": "circuit#" + subscriber_id,
            "opened_at": now,
            "expires_at": now + CIRCUIT_COOLDOWN_SECONDS * 2,
        })
        logger.warning("circuit_tripped subscriber=%s", subscriber_id)
    except ClientError as exc:
        logger.error("circuit_trip_failed subscriber=%s error=%s", subscriber_id, exc)


def _deliver(attempt: Dict[str, Any]) -> Tuple[int, str]:
    payload = json.dumps(attempt.get("payload") or {}).encode("utf-8")
    request = urllib.request.Request(
        attempt["target_url"],
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json",
                 "X-Delivery-Id": str(attempt["delivery_id"]),
                 "X-Attempt-Number": str(len(attempt["attempt_history"]) + 1)},
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            return int(response.status), "ok"
    except urllib.error.HTTPError as exc:
        return int(exc.code), "http_error"
    except urllib.error.URLError as exc:
        return 0, "transport_error:" + str(exc.reason)
    except TimeoutError:
        return 0, "timeout"


def _record_attempt(attempt: Dict[str, Any], status: int, detail: str, delay: int) -> None:
    table = dynamodb.Table(ATTEMPT_TABLE)
    now = int(time.time())
    try:
        table.put_item(Item={
            "delivery_id": str(attempt["delivery_id"]),
            "attempt_number": len(attempt["attempt_history"]) + 1,
            "subscriber_id": str(attempt.get("subscriber_id", "unknown")),
            "status_code": status,
            "detail": detail[:512],
            "next_delay_seconds": delay,
            "recorded_at": now,
            "expires_at": now + 604800,
        })
    except ClientError as exc:
        logger.error("attempt_record_failed delivery=%s error=%s",
                     attempt["delivery_id"], exc)


def _requeue(attempt: Dict[str, Any], delay: int, status: int, detail: str) -> None:
    history = list(attempt["attempt_history"])
    history.append({
        "attempted_at": int(time.time()),
        "status_code": status,
        "detail": detail[:256],
        "delay_seconds": delay,
        "outcome": "failure",
    })
    body = dict(attempt)
    body.pop("_message_id", None)
    body["attempt_history"] = history
    sqs.send_message(
        QueueUrl=RETRY_QUEUE_URL,
        MessageBody=json.dumps(body),
        DelaySeconds=delay,
        MessageAttributes={"subscriber_id": {
            "DataType": "String",
            "StringValue": str(attempt.get("subscriber_id", "unknown")),
        }},
    )


def _process(attempt: Dict[str, Any], now: int) -> str:
    subscriber_id = str(attempt.get("subscriber_id", "unknown"))
    if _circuit_open(subscriber_id, now):
        delay = MAX_SQS_DELAY_SECONDS
        _requeue(attempt, delay, 0, "circuit_open")
        return "circuit_deferred"

    status, detail = _deliver(attempt)
    if 200 <= status < 300:
        _record_attempt(attempt, status, detail, 0)
        return "delivered"

    if status in PERMANENT_STATUS or (status and status not in RETRYABLE_STATUS):
        _record_attempt(attempt, status, detail, 0)
        logger.info("attempt_abandoned delivery=%s status=%s",
                    attempt["delivery_id"], status)
        return "abandoned"

    failures = _consecutive_failures(attempt["attempt_history"]) + 1
    if failures >= CIRCUIT_TRIP_CONSECUTIVE_FAILURES:
        _trip_circuit(subscriber_id, now)

    delay = _next_delay(attempt["attempt_history"])
    _record_attempt(attempt, status, detail, delay)
    _requeue(attempt, delay, status, detail)
    logger.info(
        "attempt_rescheduled delivery=%s status=%s failures=%s delay=%s",
        attempt["delivery_id"], status, failures, delay,
    )
    return "rescheduled"


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_invocation_depth(event):
        return {"statusCode": 200, "body": "Skipped: max invocation depth reached"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = int(time.time())
    outcomes: Dict[str, int] = {}

    for record in records:
        attempt = _parse_attempt(record)
        if attempt is None:
            outcomes["invalid"] = outcomes.get("invalid", 0) + 1
            continue
        try:
            outcome = _process(attempt, now)
        except ClientError as exc:
            outcome = "error"
            logger.exception("attempt_processing_failed delivery=%s error=%s",
                             attempt.get("delivery_id"), exc)
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

    logger.info("retry_scheduler_complete records=%s outcomes=%s", len(records), outcomes)
    return {"records": len(records), "outcomes": outcomes}
