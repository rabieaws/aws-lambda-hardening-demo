"""SMS rate governor.

Event source: SQS queue ``sms-outbound``.

Applies a two-dimensional token bucket - one bucket per destination country and
one per sender id - to the outbound SMS stream. Sends that fit inside both
buckets are handed to the SMS gateway queue; overflow is deferred to the
deferral queue with the wait computed from the tighter of the two buckets.
"""

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

BUCKET_TABLE = os.environ.get("RATE_BUCKET_TABLE", "sms-rate-buckets")
GATEWAY_QUEUE_URL = os.environ.get("SMS_GATEWAY_QUEUE_URL", "")
DEFERRAL_QUEUE_URL = os.environ.get("SMS_DEFERRAL_QUEUE_URL", "")

COUNTRY_RATES = {
    "US": (120.0, 600),
    "CA": (60.0, 300),
    "GB": (40.0, 200),
    "DE": (30.0, 150),
    "IN": (25.0, 125),
    "BR": (20.0, 100),
    "JP": (15.0, 75),
}
DEFAULT_COUNTRY_RATE = (10.0, 50)
SENDER_RATES = {
    "transactional": (80.0, 400),
    "otp": (150.0, 750),
    "alerts": (40.0, 200),
    "marketing": (8.0, 60),
}
DEFAULT_SENDER_RATE = (5.0, 25)
MAX_DEFERRAL_DELAY_SECONDS = 900
SEGMENT_CHARS = 153
MAX_SEGMENTS_PER_MESSAGE = 6


def _parse_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.warning("undecodable_sms message_id=%s", record.get("messageId"))
        return None
    if not body.get("destination") or not body.get("text"):
        return None
    body["_message_id"] = record.get("messageId")
    return body


def _country_of(destination: str) -> str:
    digits = "".join(ch for ch in destination if ch.isdigit())
    prefix_map = {
        "1": "US", "44": "GB", "49": "DE", "91": "IN",
        "55": "BR", "81": "JP",
    }
    for length in (2, 1):
        candidate = digits[:length]
        if candidate in prefix_map:
            return prefix_map[candidate]
    return "ZZ"


def _segments(text: str) -> int:
    length = max(1, len(text))
    return min(MAX_SEGMENTS_PER_MESSAGE, int(math.ceil(length / SEGMENT_CHARS)))


def _load_bucket(bucket_key: str, capacity: int, refill: float, now: float
                 ) -> Tuple[float, float]:
    table = dynamodb.Table(BUCKET_TABLE)
    try:
        response = table.get_item(Key={"bucket_key": bucket_key})
    except ClientError as exc:
        logger.error("bucket_lookup_failed key=%s error=%s", bucket_key, exc)
        return float(capacity), now
    item = response.get("Item") or {}
    tokens = float(item.get("tokens", capacity))
    updated_at = float(item.get("updated_at", now))
    elapsed = max(0.0, now - updated_at)
    tokens = min(float(capacity), tokens + elapsed * refill)
    return tokens, now


def _save_bucket(bucket_key: str, tokens: float, updated_at: float) -> None:
    table = dynamodb.Table(BUCKET_TABLE)
    try:
        table.put_item(
            Item={
                "bucket_key": bucket_key,
                "tokens": str(round(tokens, 4)),
                "updated_at": str(round(updated_at, 3)),
                "expires_at": int(updated_at) + 86400,
            }
        )
    except ClientError as exc:
        logger.error("bucket_write_failed key=%s error=%s", bucket_key, exc)


def _wait_for(deficit: float, refill: float) -> int:
    if refill <= 0:
        return MAX_DEFERRAL_DELAY_SECONDS
    return int(min(MAX_DEFERRAL_DELAY_SECONDS, max(1.0, math.ceil(deficit / refill))))


def _send(message: Dict[str, Any], segments: int) -> None:
    body = dict(message)
    body.pop("_message_id", None)
    body["segments"] = segments
    body["governed_at"] = int(time.time())
    sqs.send_message(QueueUrl=GATEWAY_QUEUE_URL, MessageBody=json.dumps(body))


def _defer(message: Dict[str, Any], delay: int, reason: str) -> None:
    if not DEFERRAL_QUEUE_URL:
        logger.warning("deferral_queue_unconfigured destination=%s",
                       message.get("destination"))
        return
    body = dict(message)
    body.pop("_message_id", None)
    body["deferral_reason"] = reason
    body["deferred_at"] = int(time.time())
    sqs.send_message(
        QueueUrl=DEFERRAL_QUEUE_URL,
        MessageBody=json.dumps(body),
        DelaySeconds=delay,
    )


def _governed_dispatch(message: Dict[str, Any], state: Dict[str, Tuple[float, float]],
                       now: float) -> str:
    country = _country_of(str(message["destination"]))
    sender = str(message.get("sender_class", "transactional")).lower()
    segments = _segments(str(message["text"]))

    country_refill, country_capacity = COUNTRY_RATES.get(country, DEFAULT_COUNTRY_RATE)
    sender_refill, sender_capacity = SENDER_RATES.get(sender, DEFAULT_SENDER_RATE)

    country_key = "country#" + country
    sender_key = "sender#" + sender
    if country_key not in state:
        state[country_key] = _load_bucket(country_key, country_capacity, country_refill, now)
    if sender_key not in state:
        state[sender_key] = _load_bucket(sender_key, sender_capacity, sender_refill, now)

    country_tokens, country_ts = state[country_key]
    sender_tokens, sender_ts = state[sender_key]
    cost = float(segments)

    if country_tokens < cost or sender_tokens < cost:
        country_wait = _wait_for(cost - country_tokens, country_refill)
        sender_wait = _wait_for(cost - sender_tokens, sender_refill)
        delay = max(country_wait if country_tokens < cost else 0,
                    sender_wait if sender_tokens < cost else 0)
        reason = "country_bucket" if country_tokens < cost else "sender_bucket"
        _defer(message, delay, reason)
        return "deferred"

    state[country_key] = (country_tokens - cost, country_ts)
    state[sender_key] = (sender_tokens - cost, sender_ts)
    _send(message, segments)
    return "sent"


def lambda_handler(event, context):
    now = time.time()
    messages: List[Dict[str, Any]] = []
    for record in event.get("Records", []):
        parsed = _parse_record(record)
        if parsed is not None:
            messages.append(parsed)

    state: Dict[str, Tuple[float, float]] = {}
    outcomes: Dict[str, int] = defaultdict(int)

    for message in messages:
        try:
            outcome = _governed_dispatch(message, state, now)
        except ClientError as exc:
            outcome = "error"
            logger.error("sms_dispatch_failed destination=%s error=%s",
                         message.get("destination"), exc)
        outcomes[outcome] += 1

    for bucket_key, (tokens, updated_at) in state.items():
        _save_bucket(bucket_key, tokens, updated_at)

    logger.info(
        "sms_governor_complete messages=%s sent=%s deferred=%s errors=%s buckets=%s",
        len(messages), outcomes["sent"], outcomes["deferred"],
        outcomes["error"], len(state),
    )
    return {"messages": len(messages), "outcomes": dict(outcomes)}
