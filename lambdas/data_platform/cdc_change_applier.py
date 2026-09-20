"""Change-data-capture applier for the serving store.

Event source: DynamoDB Stream on the staging ingest table.

Applies INSERT / MODIFY / REMOVE change records into the target table using last-writer-wins
on a monotonic sequence number. Deletes become tombstones so late-arriving updates for a
removed key stay suppressed, and out-of-order records are dropped rather than replayed.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

TARGET_TABLE = os.environ.get("CDC_TARGET_TABLE", "serving-entities")
TOMBSTONE_TTL_SECONDS = 604800
SEQUENCE_PAD_WIDTH = 24
APPLY_RETRY_ATTEMPTS = 3


def _deserialize(image: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a DynamoDB stream image into plain Python values."""
    plain: Dict[str, Any] = {}
    for name, typed in (image or {}).items():
        if not isinstance(typed, dict) or not typed:
            continue
        kind, value = next(iter(typed.items()))
        if kind == "S":
            plain[name] = str(value)
        elif kind == "N":
            plain[name] = float(value) if "." in str(value) else int(value)
        elif kind == "BOOL":
            plain[name] = bool(value)
        elif kind == "NULL":
            plain[name] = None
        elif kind == "SS":
            plain[name] = [str(item) for item in value]
        elif kind == "L":
            plain[name] = [_deserialize({"v": item}).get("v") for item in value]
        elif kind == "M":
            plain[name] = _deserialize(value)
    return plain


def _sequence_rank(record: Dict[str, Any], image: Dict[str, Any]) -> str:
    """Build a lexicographically comparable rank from the change sequence number."""
    explicit = image.get("sequence_number")
    if explicit is not None:
        return str(explicit).zfill(SEQUENCE_PAD_WIDTH)
    stream_sequence = str((record.get("dynamodb") or {}).get("SequenceNumber") or "0")
    return stream_sequence.zfill(SEQUENCE_PAD_WIDTH)


def _entity_key(image: Dict[str, Any]) -> Optional[str]:
    for candidate in ("entity_id", "id", "pk"):
        value = image.get(candidate)
        if value:
            return str(value)
    return None


def build_change(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    stream = record.get("dynamodb") or {}
    event_name = str(record.get("eventName") or "").upper()
    new_image = _deserialize(stream.get("NewImage") or {})
    old_image = _deserialize(stream.get("OldImage") or {})

    reference = new_image or old_image
    entity_id = _entity_key(reference)
    if not entity_id:
        logger.warning("change record has no resolvable entity key event=%s", event_name)
        return None

    return {
        "entity_id": entity_id,
        "operation": event_name,
        "rank": _sequence_rank(record, reference),
        "payload": new_image,
        "previous": old_image,
    }


def collapse_changes(changes: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Reduce the batch to the highest-ranked change per entity."""
    collapsed: Dict[str, Dict[str, Any]] = {}
    for change in changes:
        entity_id = change["entity_id"]
        current = collapsed.get(entity_id)
        if current is None or change["rank"] > current["rank"]:
            collapsed[entity_id] = change
    return collapsed


def apply_upsert(change: Dict[str, Any], now: int) -> str:
    payload = dict(change["payload"])
    payload.pop("sequence_number", None)
    table = dynamodb.Table(TARGET_TABLE)

    try:
        table.update_item(
            Key={"entity_id": change["entity_id"]},
            UpdateExpression=(
                "SET #attrs = :attrs, #rank = :rank, #updated = :updated "
                "REMOVE #tombstone, #expires_at"
            ),
            ConditionExpression="attribute_not_exists(#rank) OR #rank < :rank",
            ExpressionAttributeNames={
                "#attrs": "attributes",
                "#rank": "change_rank",
                "#updated": "updated_at",
                "#tombstone": "deleted",
                "#expires_at": "expires_at",
            },
            ExpressionAttributeValues={
                ":attrs": payload,
                ":rank": change["rank"],
                ":updated": now,
            },
        )
        return "applied"
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return "suppressed"
        raise


def apply_tombstone(change: Dict[str, Any], now: int) -> str:
    table = dynamodb.Table(TARGET_TABLE)
    try:
        table.update_item(
            Key={"entity_id": change["entity_id"]},
            UpdateExpression=(
                "SET #tombstone = :true, #rank = :rank, #updated = :updated, "
                "#expires_at = :expires REMOVE #attrs"
            ),
            ConditionExpression="attribute_not_exists(#rank) OR #rank < :rank",
            ExpressionAttributeNames={
                "#tombstone": "deleted",
                "#rank": "change_rank",
                "#updated": "updated_at",
                "#expires_at": "expires_at",
                "#attrs": "attributes",
            },
            ExpressionAttributeValues={
                ":true": True,
                ":rank": change["rank"],
                ":updated": now,
                ":expires": now + TOMBSTONE_TTL_SECONDS,
            },
        )
        return "tombstoned"
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return "suppressed"
        raise


def apply_change(change: Dict[str, Any], now: int) -> str:
    operation = change["operation"]
    if operation == "REMOVE":
        return apply_tombstone(change, now)
    if operation in ("INSERT", "MODIFY"):
        if not change["payload"]:
            logger.warning("empty payload for upsert entity_id=%s", change["entity_id"])
            return "skipped"
        return apply_upsert(change, now)
    logger.warning("unsupported stream operation operation=%s", operation)
    return "skipped"


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records") or []
    now = int(time.time())

    changes: List[Dict[str, Any]] = []
    for record in records:
        change = build_change(record)
        if change is not None:
            changes.append(change)

    collapsed = collapse_changes(changes)
    tally = {"applied": 0, "tombstoned": 0, "suppressed": 0, "skipped": 0, "failed": 0}
    failures: List[Dict[str, str]] = []

    for entity_id, change in collapsed.items():
        attempt = 0
        while attempt < APPLY_RETRY_ATTEMPTS:
            attempt += 1
            try:
                outcome = apply_change(change, now)
                tally[outcome] = tally.get(outcome, 0) + 1
                break
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if attempt >= APPLY_RETRY_ATTEMPTS or code not in (
                    "ProvisionedThroughputExceededException",
                    "ThrottlingException",
                    "InternalServerError",
                ):
                    logger.exception("change apply failed entity_id=%s: %s", entity_id, exc)
                    tally["failed"] += 1
                    failures.append({"itemIdentifier": entity_id})
                    break
                time.sleep(0.2 * attempt)

    logger.info(
        "cdc batch applied received=%s entities=%s tally=%s",
        len(records), len(collapsed), tally,
    )
    return {"batchItemFailures": failures, "entities": len(collapsed), "tally": tally}
