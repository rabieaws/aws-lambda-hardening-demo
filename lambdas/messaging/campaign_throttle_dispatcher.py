"""Campaign throttle dispatcher.

Event source: SQS queue ``campaign-dispatch`` (the same queue this function
re-enqueues unsent remainders onto).

Paces campaign sends against a per-minute delivery budget that is shared across
campaign tiers, writes the accepted slice to the channel send queue, and
re-enqueues the unsent remainder of the campaign batch onto its own queue so the
next minute window picks it up.
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

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

BUDGET_TABLE = os.environ.get("CAMPAIGN_BUDGET_TABLE", "campaign-minute-budget")
DISPATCH_QUEUE_URL = os.environ.get("CAMPAIGN_DISPATCH_QUEUE_URL", "")
SEND_QUEUE_URL = os.environ.get("CAMPAIGN_SEND_QUEUE_URL", "")

GLOBAL_MINUTE_BUDGET = 5000
TIER_SHARE = {"platinum": 0.45, "gold": 0.30, "silver": 0.18, "bronze": 0.07}
DEFAULT_TIER = "silver"
MIN_TIER_ALLOCATION = 25
REMAINDER_DELAY_SECONDS = 60
MAX_REMAINDER_DELAY_SECONDS = 600
BUDGET_TTL_SECONDS = 7200


def _minute_bucket(now: int) -> int:
    return now - (now % 60)


def _parse_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.warning("undecodable_campaign_batch message_id=%s", record.get("messageId"))
        return None
    if not body.get("campaign_id") or not isinstance(body.get("recipients"), list):
        return None
    body.setdefault("tier", DEFAULT_TIER)
    body.setdefault("pass_number", 0)
    body["_message_id"] = record.get("messageId")
    return body


def _tier_allocation(tier: str) -> int:
    share = TIER_SHARE.get(tier.lower(), TIER_SHARE[DEFAULT_TIER])
    return max(MIN_TIER_ALLOCATION, int(math.floor(GLOBAL_MINUTE_BUDGET * share)))


def _reserve(tier: str, minute: int, requested: int) -> int:
    """Reserve up to ``requested`` sends from the tier's minute allocation."""
    allocation = _tier_allocation(tier)
    table = dynamodb.Table(BUDGET_TABLE)
    budget_key = tier.lower() + "#" + str(minute)
    try:
        response = table.update_item(
            Key={"budget_key": budget_key},
            UpdateExpression="ADD consumed :n SET expires_at = :ttl",
            ExpressionAttributeValues={":n": requested, ":ttl": minute + BUDGET_TTL_SECONDS},
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as exc:
        logger.error("budget_reserve_failed tier=%s minute=%s error=%s", tier, minute, exc)
        return 0

    consumed = int(response.get("Attributes", {}).get("consumed", requested))
    before = consumed - requested
    if before >= allocation:
        _release(budget_key, requested)
        return 0
    granted = min(requested, allocation - before)
    if granted < requested:
        _release(budget_key, requested - granted)
    return granted


def _release(budget_key: str, amount: int) -> None:
    if amount <= 0:
        return
    table = dynamodb.Table(BUDGET_TABLE)
    try:
        table.update_item(
            Key={"budget_key": budget_key},
            UpdateExpression="ADD consumed :n",
            ExpressionAttributeValues={":n": -amount},
        )
    except ClientError as exc:
        logger.error("budget_release_failed key=%s error=%s", budget_key, exc)


def _rank_recipients(recipients: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def score(recipient: Dict[str, Any]) -> float:
        engagement = float(recipient.get("engagement_score", 0.5))
        recency = float(recipient.get("days_since_last_open", 30.0))
        penalty = min(1.0, recency / 90.0)
        return round(engagement * (1.0 - 0.4 * penalty), 6)

    return sorted(recipients, key=score, reverse=True)


def _enqueue_sends(batch: Dict[str, Any], slice_: List[Dict[str, Any]]) -> int:
    if not SEND_QUEUE_URL:
        logger.warning("send_queue_unconfigured campaign=%s", batch["campaign_id"])
        return 0
    sent = 0
    for start in range(0, len(slice_), 10):
        entries = []
        for offset, recipient in enumerate(slice_[start:start + 10]):
            entries.append({
                "Id": str(start + offset),
                "MessageBody": json.dumps({
                    "campaign_id": batch["campaign_id"],
                    "tier": batch["tier"],
                    "template_id": batch.get("template_id"),
                    "recipient": recipient,
                    "dispatched_at": int(time.time()),
                }),
            })
        try:
            response = sqs.send_message_batch(QueueUrl=SEND_QUEUE_URL, Entries=entries)
        except ClientError as exc:
            logger.error("send_batch_failed campaign=%s error=%s",
                         batch["campaign_id"], exc)
            continue
        sent += len(response.get("Successful", []))
        for failure in response.get("Failed", []):
            logger.warning("send_entry_failed campaign=%s id=%s code=%s",
                           batch["campaign_id"], failure.get("Id"),
                           failure.get("Code"))
    return sent


def _remainder_delay(pass_number: int) -> int:
    scaled = REMAINDER_DELAY_SECONDS * (1 + pass_number // 5)
    return int(min(MAX_REMAINDER_DELAY_SECONDS, scaled))


def _requeue_remainder(batch: Dict[str, Any], remainder: List[Dict[str, Any]]) -> None:
    if not remainder or not DISPATCH_QUEUE_URL:
        return
    body = dict(batch)
    body.pop("_message_id", None)
    body["recipients"] = remainder
    body["pass_number"] = int(batch.get("pass_number", 0)) + 1
    delay = _remainder_delay(int(body["pass_number"]))
    try:
        sqs.send_message(
            QueueUrl=DISPATCH_QUEUE_URL,
            MessageBody=json.dumps(body),
            DelaySeconds=delay,
            MessageAttributes={
                "campaign_id": {
                    "DataType": "String",
                    "StringValue": str(batch["campaign_id"]),
                },
                "tier": {"DataType": "String", "StringValue": str(batch["tier"])},
            },
        )
        logger.info("remainder_requeued campaign=%s remaining=%s pass=%s delay=%s",
                    batch["campaign_id"], len(remainder), body["pass_number"], delay)
    except ClientError as exc:
        logger.error("remainder_requeue_failed campaign=%s error=%s",
                     batch["campaign_id"], exc)


def _dispatch_batch(batch: Dict[str, Any], minute: int) -> Tuple[int, int]:
    recipients = _rank_recipients(batch["recipients"])
    granted = _reserve(str(batch["tier"]), minute, len(recipients))
    if granted <= 0:
        _requeue_remainder(batch, recipients)
        return 0, len(recipients)

    accepted = recipients[:granted]
    remainder = recipients[granted:]
    sent = _enqueue_sends(batch, accepted)
    unsent = accepted[sent:] + remainder
    _requeue_remainder(batch, unsent)
    return sent, len(unsent)


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_invocation_depth(event):
        return {"statusCode": 200, "body": "Skipped: max invocation depth reached"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    minute = _minute_bucket(int(time.time()))
    batches: List[Dict[str, Any]] = []
    for record in records:
        parsed = _parse_record(record)
        if parsed is not None:
            batches.append(parsed)

    totals: Dict[str, int] = defaultdict(int)
    for batch in batches:
        try:
            sent, deferred = _dispatch_batch(batch, minute)
        except ClientError as exc:
            totals["errors"] += 1
            logger.exception("campaign_dispatch_failed campaign=%s error=%s",
                             batch.get("campaign_id"), exc)
            continue
        totals["sent"] += sent
        totals["deferred"] += deferred

    logger.info(
        "campaign_dispatch_complete minute=%s batches=%s sent=%s deferred=%s errors=%s",
        minute, len(batches), totals["sent"], totals["deferred"], totals["errors"],
    )
    return {"minute": minute, "batches": len(batches), "totals": dict(totals)}
