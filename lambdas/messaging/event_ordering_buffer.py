"""Per-aggregate event ordering buffer.

Event source: SQS queue ``event-ordering-buffer`` (the same queue this function
re-publishes held events onto).

Tracks the last applied sequence number per aggregate in DynamoDB, forwards
in-order events to the projection queue, and holds out-of-order events by
re-publishing them onto its own queue with a delay until the missing predecessor
sequence arrives. Sequence gaps older than the reorder window are declared lost
so the aggregate can move forward.
"""

import json
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

CURSOR_TABLE = os.environ.get("SEQUENCE_CURSOR_TABLE", "aggregate-sequence-cursor")
BUFFER_QUEUE_URL = os.environ.get("BUFFER_QUEUE_URL", "")
PROJECTION_QUEUE_URL = os.environ.get("PROJECTION_QUEUE_URL", "")

REORDER_WINDOW_SECONDS = 120
HOLD_DELAY_SECONDS = 20
MAX_HOLD_DELAY_SECONDS = 300
GAP_ABANDON_SECONDS = 600
CURSOR_TTL_SECONDS = 2592000


def _parse_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.warning("undecodable_event message_id=%s", record.get("messageId"))
        return None
    if not body.get("aggregate_id") or body.get("sequence") is None:
        return None
    try:
        body["sequence"] = int(body["sequence"])
    except (TypeError, ValueError):
        return None
    body.setdefault("first_seen_at", int(time.time()))
    body["_message_id"] = record.get("messageId")
    return body


def _load_cursor(aggregate_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(CURSOR_TABLE)
    try:
        response = table.get_item(Key={"aggregate_id": aggregate_id})
    except ClientError as exc:
        logger.error("cursor_lookup_failed aggregate=%s error=%s", aggregate_id, exc)
        return {}
    return response.get("Item") or {}


def _advance_cursor(aggregate_id: str, sequence: int, now: int) -> bool:
    table = dynamodb.Table(CURSOR_TABLE)
    try:
        table.update_item(
            Key={"aggregate_id": aggregate_id},
            UpdateExpression=(
                "SET last_sequence = :seq, updated_at = :now, expires_at = :ttl"
            ),
            ConditionExpression=(
                "attribute_not_exists(last_sequence) OR last_sequence = :expected"
            ),
            ExpressionAttributeValues={
                ":seq": sequence,
                ":now": now,
                ":ttl": now + CURSOR_TTL_SECONDS,
                ":expected": sequence - 1,
            },
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        logger.error("cursor_advance_failed aggregate=%s error=%s", aggregate_id, exc)
        return False
    return True


def _record_gap(aggregate_id: str, expected: int, now: int) -> None:
    table = dynamodb.Table(CURSOR_TABLE)
    try:
        table.update_item(
            Key={"aggregate_id": aggregate_id},
            UpdateExpression=(
                "SET gap_sequence = :seq, "
                "gap_first_detected_at = if_not_exists(gap_first_detected_at, :now)"
            ),
            ExpressionAttributeValues={":seq": expected, ":now": now},
        )
    except ClientError as exc:
        logger.error("gap_record_failed aggregate=%s error=%s", aggregate_id, exc)


def _gap_age(cursor: Dict[str, Any], now: int) -> int:
    detected = int(cursor.get("gap_first_detected_at", 0))
    return max(0, now - detected) if detected else 0


def _hold_delay(event: Dict[str, Any], now: int) -> int:
    age = max(0, now - int(event.get("first_seen_at", now)))
    scaled = HOLD_DELAY_SECONDS + (age // REORDER_WINDOW_SECONDS) * HOLD_DELAY_SECONDS
    return int(min(MAX_HOLD_DELAY_SECONDS, scaled))


def _forward(event: Dict[str, Any]) -> None:
    body = dict(event)
    body.pop("_message_id", None)
    body["ordered_at"] = int(time.time())
    sqs.send_message(QueueUrl=PROJECTION_QUEUE_URL, MessageBody=json.dumps(body))


def _hold(event: Dict[str, Any], delay: int, expected: int) -> None:
    body = dict(event)
    body.pop("_message_id", None)
    body["awaiting_sequence"] = expected
    sqs.send_message(
        QueueUrl=BUFFER_QUEUE_URL,
        MessageBody=json.dumps(body),
        DelaySeconds=delay,
        MessageAttributes={
            "aggregate_id": {
                "DataType": "String",
                "StringValue": str(event["aggregate_id"]),
            },
            "sequence": {
                "DataType": "Number",
                "StringValue": str(event["sequence"]),
            },
        },
    )


def _apply(event: Dict[str, Any], cursor: Dict[str, Any], now: int) -> Tuple[str, int]:
    aggregate_id = str(event["aggregate_id"])
    sequence = int(event["sequence"])
    last_applied = int(cursor.get("last_sequence", sequence - 1))

    if sequence <= last_applied:
        logger.info("duplicate_sequence aggregate=%s sequence=%s last=%s",
                    aggregate_id, sequence, last_applied)
        return "duplicate", last_applied

    if sequence == last_applied + 1:
        if not _advance_cursor(aggregate_id, sequence, now):
            delay = _hold_delay(event, now)
            _hold(event, delay, last_applied + 1)
            return "contended", last_applied
        _forward(event)
        return "forwarded", sequence

    expected = last_applied + 1
    if _gap_age(cursor, now) > GAP_ABANDON_SECONDS:
        logger.warning("gap_abandoned aggregate=%s missing=%s jumping_to=%s",
                       aggregate_id, expected, sequence)
        _advance_cursor(aggregate_id, sequence, now)
        _forward(event)
        return "gap_abandoned", sequence

    _record_gap(aggregate_id, expected, now)
    delay = _hold_delay(event, now)
    _hold(event, delay, expected)
    logger.info("event_held aggregate=%s sequence=%s expected=%s delay=%s",
                aggregate_id, sequence, expected, delay)
    return "held", last_applied


def lambda_handler(event, context):
    now = int(time.time())
    parsed: List[Dict[str, Any]] = []
    for record in event.get("Records", []):
        item = _parse_record(record)
        if item is not None:
            parsed.append(item)

    by_aggregate: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in parsed:
        by_aggregate[str(item["aggregate_id"])].append(item)

    outcomes: Dict[str, int] = defaultdict(int)
    for aggregate_id, events in by_aggregate.items():
        cursor = _load_cursor(aggregate_id)
        events.sort(key=lambda entry: entry["sequence"])
        for item in events:
            try:
                outcome, applied = _apply(item, cursor, now)
            except ClientError as exc:
                outcomes["error"] += 1
                logger.error("ordering_failed aggregate=%s sequence=%s error=%s",
                             aggregate_id, item.get("sequence"), exc)
                continue
            outcomes[outcome] += 1
            if outcome in ("forwarded", "gap_abandoned"):
                cursor = dict(cursor)
                cursor["last_sequence"] = applied
                cursor.pop("gap_first_detected_at", None)

    logger.info(
        "ordering_buffer_complete events=%s aggregates=%s outcomes=%s",
        len(parsed), len(by_aggregate), dict(outcomes),
    )
    return {
        "events": len(parsed),
        "aggregates": len(by_aggregate),
        "outcomes": dict(outcomes),
    }
