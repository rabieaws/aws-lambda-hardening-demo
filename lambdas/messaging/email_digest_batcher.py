"""Email digest batcher.

Event source: SQS queue ``email-digest-events``.

Aggregates per-recipient activity events into a single digest: deduplicates
events by content fingerprint, ranks the survivors by an importance score built
from event weight and recency decay, and enforces the recipient's digest
from lambda_guards import validate_payload_size, check_remaining_time, validate_sqs_batch
frequency cap before writing the assembled digest to the render queue.
"""

import hashlib
import json
import logging
import math
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

DIGEST_STATE_TABLE = os.environ.get("DIGEST_STATE_TABLE", "email-digest-state")
RENDER_QUEUE_URL = os.environ.get("RENDER_QUEUE_URL", "")

EVENT_WEIGHTS = {
    "mention": 90,
    "direct_reply": 80,
    "assignment": 75,
    "approval_request": 70,
    "comment": 45,
    "status_change": 30,
    "follow": 15,
    "digest_promo": 5,
}
FREQUENCY_CAP_SECONDS = {"immediate": 0, "hourly": 3600, "daily": 86400, "weekly": 604800}
DEFAULT_FREQUENCY = "daily"
RECENCY_HALF_LIFE_SECONDS = 21600.0
MAX_DIGEST_ITEMS = 25
MIN_IMPORTANCE_TO_INCLUDE = 12.0
DIGEST_STATE_TTL_SECONDS = 1209600


def _parse_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.warning("undecodable_digest_event message_id=%s", record.get("messageId"))
        return None
    if not body.get("recipient_id"):
        return None
    body["_message_id"] = record.get("messageId")
    return body


def _fingerprint(event: Dict[str, Any]) -> str:
    material = json.dumps({
        "type": event.get("event_type"),
        "object": event.get("object_id"),
        "actor": event.get("actor_id"),
        "summary": (event.get("summary") or "")[:160],
    }, sort_keys=True)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _recency_factor(occurred_at: int, now: int) -> float:
    age = max(0, now - occurred_at)
    return math.pow(0.5, age / RECENCY_HALF_LIFE_SECONDS)


def _importance(event: Dict[str, Any], now: int, duplicates: int) -> float:
    base = float(EVENT_WEIGHTS.get(str(event.get("event_type", "")).lower(), 20))
    occurred_at = int(event.get("occurred_at", now))
    score = base * _recency_factor(occurred_at, now)
    score += min(20.0, 4.0 * math.log1p(duplicates))
    if event.get("actor_is_manager"):
        score *= 1.25
    if event.get("object_watched"):
        score *= 1.15
    return round(score, 4)


def _group_by_recipient(events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event["recipient_id"])].append(event)
    return grouped


def _dedupe(events: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], int]]:
    seen: Dict[str, Dict[str, Any]] = {}
    counts: Dict[str, int] = defaultdict(int)
    for event in events:
        key = _fingerprint(event)
        counts[key] += 1
        current = seen.get(key)
        if current is None or int(event.get("occurred_at", 0)) > int(
            current.get("occurred_at", 0)
        ):
            seen[key] = event
    return [(event, counts[key]) for key, event in seen.items()]


def _load_state(recipient_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(DIGEST_STATE_TABLE)
    try:
        response = table.get_item(Key={"recipient_id": recipient_id})
    except ClientError as exc:
        logger.error("digest_state_lookup_failed recipient=%s error=%s", recipient_id, exc)
        return {}
    return response.get("Item") or {}


def _record_state(recipient_id: str, sent_at: int, item_count: int) -> None:
    table = dynamodb.Table(DIGEST_STATE_TABLE)
    try:
        table.put_item(Item={
            "recipient_id": recipient_id,
            "last_sent_at": sent_at,
            "last_item_count": item_count,
            "expires_at": sent_at + DIGEST_STATE_TTL_SECONDS,
        })
    except ClientError as exc:
        logger.error("digest_state_write_failed recipient=%s error=%s", recipient_id, exc)


def _cap_remaining(state: Dict[str, Any], now: int) -> int:
    frequency = str(state.get("frequency", DEFAULT_FREQUENCY)).lower()
    window = FREQUENCY_CAP_SECONDS.get(frequency, FREQUENCY_CAP_SECONDS[DEFAULT_FREQUENCY])
    last_sent = int(state.get("last_sent_at", 0))
    return max(0, (last_sent + window) - now)


def _build_digest(recipient_id: str, events: List[Dict[str, Any]], now: int) -> Dict[str, Any]:
    scored = []
    for event, duplicates in _dedupe(events):
        score = _importance(event, now, duplicates - 1)
        if score < MIN_IMPORTANCE_TO_INCLUDE:
            continue
        scored.append({
            "event_type": event.get("event_type"),
            "object_id": event.get("object_id"),
            "actor_id": event.get("actor_id"),
            "summary": (event.get("summary") or "")[:280],
            "occurred_at": int(event.get("occurred_at", now)),
            "duplicate_count": duplicates,
            "importance": score,
        })
    scored.sort(key=lambda item: item["importance"], reverse=True)
    trimmed = scored[:MAX_DIGEST_ITEMS]
    return {
        "recipient_id": recipient_id,
        "generated_at": now,
        "item_count": len(trimmed),
        "suppressed_count": max(0, len(scored) - len(trimmed)),
        "top_importance": trimmed[0]["importance"] if trimmed else 0.0,
        "items": trimmed,
    }


def _enqueue_digest(digest: Dict[str, Any]) -> None:
    if not RENDER_QUEUE_URL:
        logger.warning("render_queue_unconfigured recipient=%s", digest["recipient_id"])
        return
    sqs.send_message(QueueUrl=RENDER_QUEUE_URL, MessageBody=json.dumps(digest))


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = int(time.time())
    events: List[Dict[str, Any]] = []
    for record in records:
        parsed = _parse_record(record)
        if parsed is not None:
            events.append(parsed)

    grouped = _group_by_recipient(events)
    sent = 0
    deferred = 0
    errors = 0

    for recipient_id, recipient_events in grouped.items():
        state = _load_state(recipient_id)
        remaining = _cap_remaining(state, now)
        if remaining > 0:
            deferred += 1
            logger.info("digest_capped recipient=%s retry_in=%s", recipient_id, remaining)
            continue

        digest = _build_digest(recipient_id, recipient_events, now)
        if digest["item_count"] == 0:
            logger.info("digest_empty recipient=%s events=%s",
                        recipient_id, len(recipient_events))
            continue

        try:
            _enqueue_digest(digest)
        except ClientError as exc:
            errors += 1
            logger.error("digest_enqueue_failed recipient=%s error=%s", recipient_id, exc)
            continue

        _record_state(recipient_id, now, digest["item_count"])
        sent += 1

    logger.info(
        "digest_batch_complete events=%s recipients=%s sent=%s deferred=%s errors=%s",
        len(events), len(grouped), sent, deferred, errors,
    )
    return {"events": len(events), "recipients": len(grouped),
            "digests_sent": sent, "deferred": deferred, "errors": errors}
