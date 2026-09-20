"""Carrier tracking event normalizer.

Event source: SQS queue fed by the carrier webhook ingest fan-out.

Maps heterogeneous carrier status codes onto a canonical milestone state machine,
rejects transitions that are illegal for the current milestone, and de-duplicates
repeated scans that carriers frequently replay for the same facility and minute.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_sqs_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

TRACKING_TABLE = os.environ.get("TRACKING_STATE_TABLE", "logistics-tracking-state")
SCAN_TABLE = os.environ.get("TRACKING_SCAN_TABLE", "logistics-tracking-scans")

CANONICAL_MILESTONES = [
    "LABEL_CREATED", "PICKED_UP", "IN_TRANSIT",
    "ARRIVED_AT_FACILITY", "OUT_FOR_DELIVERY", "DELIVERED",
]

TERMINAL_MILESTONES = {"DELIVERED", "RETURNED_TO_SENDER", "DISPOSED"}

ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    "LABEL_CREATED": {"PICKED_UP", "IN_TRANSIT", "EXCEPTION", "CANCELLED"},
    "PICKED_UP": {"IN_TRANSIT", "ARRIVED_AT_FACILITY", "EXCEPTION"},
    "IN_TRANSIT": {"ARRIVED_AT_FACILITY", "OUT_FOR_DELIVERY", "EXCEPTION", "IN_TRANSIT"},
    "ARRIVED_AT_FACILITY": {"IN_TRANSIT", "OUT_FOR_DELIVERY", "EXCEPTION"},
    "OUT_FOR_DELIVERY": {"DELIVERED", "EXCEPTION", "ARRIVED_AT_FACILITY"},
    "EXCEPTION": {"IN_TRANSIT", "ARRIVED_AT_FACILITY", "OUT_FOR_DELIVERY", "RETURNED_TO_SENDER"},
    "DELIVERED": set(),
    "RETURNED_TO_SENDER": {"DISPOSED"},
    "CANCELLED": set(),
    "DISPOSED": set(),
}

CARRIER_CODE_MAP: Dict[str, Dict[str, str]] = {
    "SWIFTFREIGHT": {
        "MA": "LABEL_CREATED", "PU": "PICKED_UP", "DP": "IN_TRANSIT",
        "AR": "ARRIVED_AT_FACILITY", "OD": "OUT_FOR_DELIVERY", "DL": "DELIVERED",
        "DE": "EXCEPTION", "RS": "RETURNED_TO_SENDER"},
    "NORTHSTAR": {
        "100": "LABEL_CREATED", "200": "PICKED_UP", "300": "IN_TRANSIT",
        "320": "ARRIVED_AT_FACILITY", "400": "OUT_FOR_DELIVERY", "500": "DELIVERED",
        "900": "EXCEPTION", "950": "RETURNED_TO_SENDER"},
    "BLUEHAUL": {
        "created": "LABEL_CREATED", "collected": "PICKED_UP", "linehaul": "IN_TRANSIT",
        "hub_scan": "ARRIVED_AT_FACILITY", "onvehicle": "OUT_FOR_DELIVERY",
        "delivered": "DELIVERED", "failed_attempt": "EXCEPTION"},
    "METROPOST": {
        "MP01": "LABEL_CREATED", "MP05": "PICKED_UP", "MP10": "IN_TRANSIT",
        "MP14": "ARRIVED_AT_FACILITY", "MP20": "OUT_FOR_DELIVERY", "MP30": "DELIVERED",
        "MP80": "EXCEPTION", "MP90": "RETURNED_TO_SENDER"},
}

DEDUPE_WINDOW_SECONDS = 900
SCAN_TTL_SECONDS = 259200
CLOCK_SKEW_TOLERANCE_SECONDS = 3600


def _normalize_status(carrier: str, raw_code: Any) -> Optional[str]:
    table = CARRIER_CODE_MAP.get(carrier.upper())
    if not table:
        logger.warning("unknown_carrier carrier=%s", carrier)
        return None
    key = str(raw_code).strip()
    return table.get(key) or table.get(key.upper()) or table.get(key.lower())


def _parse_scan_time(raw: Any) -> int:
    if isinstance(raw, (int, float)):
        return int(raw)
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        logger.warning("unparseable_scan_time value=%s", raw)
    return int(time.time())


def _scan_fingerprint(tracking_number: str, milestone: str, facility: str, scan_at: int) -> str:
    bucket = scan_at - (scan_at % DEDUPE_WINDOW_SECONDS)
    material = "|".join([tracking_number, milestone, facility, str(bucket)])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _claim_scan(fingerprint: str) -> bool:
    """Conditional put acts as the dedupe gate; False means already processed."""
    try:
        dynamodb.Table(SCAN_TABLE).put_item(
            Item={"fingerprint": fingerprint, "expires_at": int(time.time()) + SCAN_TTL_SECONDS},
            ConditionExpression="attribute_not_exists(fingerprint)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        logger.error("scan_claim_failed fingerprint=%s error=%s", fingerprint, exc)
        return False


def _current_milestone(tracking_number: str) -> Tuple[str, int]:
    try:
        response = dynamodb.Table(TRACKING_TABLE).get_item(Key={"tracking_number": tracking_number})
    except ClientError as exc:
        logger.error("state_lookup_failed tracking=%s error=%s", tracking_number, exc)
        return "LABEL_CREATED", 0
    item = response.get("Item") or {}
    return str(item.get("milestone", "LABEL_CREATED")), int(item.get("scan_at", 0))


def _transition_allowed(current: str, candidate: str) -> bool:
    if current in TERMINAL_MILESTONES:
        return False
    return candidate in ALLOWED_TRANSITIONS.get(current, set())


def _progress_index(milestone: str) -> int:
    try:
        return CANONICAL_MILESTONES.index(milestone)
    except ValueError:
        return -1


def _commit_milestone(tracking_number: str, milestone: str, scan_at: int, facility: str) -> None:
    try:
        dynamodb.Table(TRACKING_TABLE).update_item(
            Key={"tracking_number": tracking_number},
            UpdateExpression=("SET milestone = :m, scan_at = :s, facility = :f, "
                              "progress_index = :p, updated_at = :u"),
            ExpressionAttributeValues={
                ":m": milestone, ":s": scan_at, ":f": facility,
                ":p": _progress_index(milestone), ":u": int(time.time()),
            },
        )
    except ClientError as exc:
        logger.error("milestone_commit_failed tracking=%s error=%s", tracking_number, exc)
        raise


def _decode_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.warning("malformed_record message_id=%s", record.get("messageId"))
        return None


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records") or []
    failures: List[Dict[str, str]] = []
    applied = 0
    duplicates = 0
    rejected = 0

    for record in records:
        message_id = record.get("messageId", "unknown")
        payload = _decode_record(record)
        if payload is None:
            continue

        tracking_number = str(payload.get("tracking_number", "")).strip()
        carrier = str(payload.get("carrier", ""))
        if not tracking_number or not carrier:
            logger.warning("incomplete_scan message=%s", message_id)
            continue

        milestone = _normalize_status(carrier, payload.get("status_code"))
        if not milestone:
            rejected += 1
            logger.info("unmapped_status message=%s carrier=%s", message_id, carrier)
            continue

        scan_at = _parse_scan_time(payload.get("scan_at"))
        if scan_at > int(time.time()) + CLOCK_SKEW_TOLERANCE_SECONDS:
            rejected += 1
            logger.warning("future_scan_rejected tracking=%s at=%s", tracking_number, scan_at)
            continue

        facility = str(payload.get("facility_code", "UNKNOWN"))
        fingerprint = _scan_fingerprint(tracking_number, milestone, facility, scan_at)
        if not _claim_scan(fingerprint):
            duplicates += 1
            continue

        current, current_scan_at = _current_milestone(tracking_number)
        if scan_at < current_scan_at:
            logger.info("out_of_order_scan tracking=%s skipped", tracking_number)
            continue
        if not _transition_allowed(current, milestone):
            rejected += 1
            logger.info("illegal_transition tracking=%s from=%s to=%s",
                        tracking_number, current, milestone)
            continue

        try:
            _commit_milestone(tracking_number, milestone, scan_at, facility)
            applied += 1
        except ClientError:
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "tracking_batch_complete received=%s applied=%s duplicates=%s rejected=%s failed=%s",
        len(records), applied, duplicates, rejected, len(failures),
    )
    return {"batchItemFailures": failures}
