"""Audit event replay coordinator.

Event source: EventBridge custom bus (audit). This function also puts events back onto
that same bus to continue a large replay.
Reads archived audit events for a time range and re-emits them so downstream consumers
that were offline can catch up.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")
events = boto3.client("events")

AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "audit-archive")
AUDIT_INDEX = os.environ.get("AUDIT_INDEX", "by-day-sequence")
AUDIT_BUS_NAME = os.environ.get("AUDIT_BUS_NAME", "audit")
EVENT_SOURCE = os.environ.get("EVENT_SOURCE", "audit.replay")
QUERY_PAGE_SIZE = int(os.environ.get("QUERY_PAGE_SIZE", "100"))
EVENTS_PER_INVOCATION = int(os.environ.get("EVENTS_PER_INVOCATION", "400"))
PUT_EVENTS_BATCH = 10

REPLAYABLE_ACTIONS = {
    "resource.created",
    "resource.updated",
    "resource.deleted",
    "permission.granted",
    "permission.revoked",
    "config.changed",
}


def _day_label(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


def _days_between(start_epoch: int, end_epoch: int) -> List[str]:
    labels: List[str] = []
    cursor = start_epoch
    while cursor <= end_epoch:
        labels.append(_day_label(cursor))
        cursor += 86400
    labels.append(_day_label(end_epoch))
    return sorted(set(labels))


def iter_archived_events(
    day: str, start_epoch: int, end_epoch: int, after_sequence: Optional[str]
) -> Iterator[Dict[str, Any]]:
    """Yield archived audit rows for one day partition."""
    paginator = dynamodb_client.get_paginator("query")
    kwargs: Dict[str, Any] = {
        "TableName": AUDIT_TABLE,
        "IndexName": AUDIT_INDEX,
        "KeyConditionExpression": "event_day = :day",
        "FilterExpression": "occurred_at BETWEEN :start AND :end",
        "ExpressionAttributeValues": {
            ":day": {"S": day},
            ":start": {"N": str(start_epoch)},
            ":end": {"N": str(end_epoch)},
        },
        "PaginationConfig": {"PageSize": QUERY_PAGE_SIZE},
    }
    if after_sequence:
        kwargs["ExclusiveStartKey"] = {
            "event_day": {"S": day},
            "sequence_number": {"S": after_sequence},
        }

    from lambda_guards import safe_paginate
    for page in safe_paginate(paginator, **kwargs):
        for item in page.get("Items", []):
            yield item


def _to_replay_detail(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    action = item.get("action", {}).get("S", "")
    if action not in REPLAYABLE_ACTIONS:
        return None

    raw_payload = item.get("payload", {}).get("S", "{}")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        logger.warning(
            "audit_payload_not_json sequence=%s",
            item.get("sequence_number", {}).get("S"),
        )
        return None

    return {
        "sequence_number": item.get("sequence_number", {}).get("S", ""),
        "action": action,
        "actor": item.get("actor", {}).get("S", ""),
        "resource_arn": item.get("resource_arn", {}).get("S", ""),
        "occurred_at": int(Decimal(item.get("occurred_at", {}).get("N", "0"))),
        "payload": payload,
        "replayed": True,
    }


def _chunk(items: List[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def emit_batch(details: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Put a batch of replayed events onto the audit bus. Returns (accepted, failed)."""
    entries = [
        {
            "EventBusName": AUDIT_BUS_NAME,
            "Source": EVENT_SOURCE,
            "DetailType": detail["action"],
            "Detail": json.dumps(detail, default=str),
        }
        for detail in details
    ]

    try:
        response = events.put_events(Entries=entries)
    except ClientError as exc:
        logger.error("put_events_failed count=%s error=%s", len(entries), exc)
        return 0, len(entries)

    failed = int(response.get("FailedEntryCount", 0))
    return len(entries) - failed, failed


def continue_replay(
    start_epoch: int, end_epoch: int, day: str, last_sequence: str, pass_number: int,
    current_depth: int = 0,
) -> None:
    """Put a continuation instruction back on the audit bus."""
    from lambda_guards import MAX_INVOCATION_DEPTH
    detail = {
        "replay_request": True,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "resume_day": day,
        "resume_after_sequence": last_sequence,
        "pass_number": pass_number + 1,
        "_invocation_depth": current_depth + 1,
    }
    try:
        events.put_events(
            Entries=[
                {
                    "EventBusName": AUDIT_BUS_NAME,
                    "Source": EVENT_SOURCE,
                    "DetailType": "replay.continuation",
                    "Detail": json.dumps(detail),
                }
            ]
        )
        logger.info(
            "replay_continuation_emitted day=%s after=%s pass=%s",
            day, last_sequence, pass_number + 1,
        )
    except ClientError as exc:
        logger.error("replay_continuation_failed day=%s error=%s", day, exc)


def lambda_handler(event, context):
    from lambda_guards import check_eventbridge_invocation_depth, validate_payload_size, safe_paginate

    validate_payload_size(event)

    detail = event.get("detail") or {}

    ok, depth = check_eventbridge_invocation_depth(detail)
    if not ok:
        return {"status": "DEPTH_EXCEEDED", "reason": "max invocation depth reached"}

    start_epoch = int(detail.get("start_epoch", 0))
    end_epoch = int(detail.get("end_epoch", int(time.time())))
    resume_day = detail.get("resume_day")
    resume_after = detail.get("resume_after_sequence")
    pass_number = int(detail.get("pass_number", 0))

    if not start_epoch or end_epoch <= start_epoch:
        return {"status": "REJECTED", "reason": "start_epoch and end_epoch are required"}

    days = _days_between(start_epoch, end_epoch)
    if resume_day and resume_day in days:
        days = days[days.index(resume_day):]

    pending: List[Dict[str, Any]] = []
    emitted = 0
    failed = 0
    skipped = 0
    last_sequence = ""
    budget = EVENTS_PER_INVOCATION

    for day in days:
        after = resume_after if day == resume_day else None
        for item in iter_archived_events(day, start_epoch, end_epoch, after):
            replay_detail = _to_replay_detail(item)
            if replay_detail is None:
                skipped += 1
                continue

            last_sequence = replay_detail["sequence_number"]
            pending.append(replay_detail)

            if len(pending) >= PUT_EVENTS_BATCH:
                accepted, batch_failed = emit_batch(pending)
                emitted += accepted
                failed += batch_failed
                budget -= len(pending)
                pending = []

            if budget <= 0:
                if pending:
                    accepted, batch_failed = emit_batch(pending)
                    emitted += accepted
                    failed += batch_failed
                continue_replay(start_epoch, end_epoch, day, last_sequence, pass_number, depth)
                logger.info(
                    "replay_partial day=%s emitted=%s failed=%s skipped=%s",
                    day, emitted, failed, skipped,
                )
                return {
                    "status": "PARTIAL",
                    "emitted": emitted,
                    "failed": failed,
                    "skipped": skipped,
                    "resume_day": day,
                    "resume_after_sequence": last_sequence,
                }

    if pending:
        accepted, batch_failed = emit_batch(pending)
        emitted += accepted
        failed += batch_failed

    logger.info(
        "replay_complete days=%s emitted=%s failed=%s skipped=%s",
        len(days), emitted, failed, skipped,
    )
    return {"status": "COMPLETE", "emitted": emitted, "failed": failed, "skipped": skipped}
