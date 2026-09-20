"""In-flight shipment SLA breach monitor.

Event source: EventBridge scheduled rule ``logistics-sla-sweep`` (every 15 min).

Pages every in-flight shipment out of DynamoDB, computes elapsed transit against
the promised SLA clock using business-hours arithmetic that excludes weekends and
carrier non-operating hours, classifies each shipment as healthy / at-risk /
breached, and publishes notifications for the escalating cohorts.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")
sns = boto3.client("sns")

SHIPMENT_TABLE = os.environ.get("SHIPMENT_TABLE", "logistics-shipments")
SHIPMENT_STATE_INDEX = os.environ.get("SHIPMENT_STATE_INDEX", "state-promised-index")
BREACH_TOPIC_ARN = os.environ.get("SLA_BREACH_TOPIC_ARN", "")

BUSINESS_DAY_START_HOUR = 8
BUSINESS_DAY_END_HOUR = 20
BUSINESS_HOURS_PER_DAY = float(BUSINESS_DAY_END_HOUR - BUSINESS_DAY_START_HOUR)
AT_RISK_CONSUMED_FRACTION = 0.82
CRITICAL_CONSUMED_FRACTION = 0.95
BREACH_GRACE_MINUTES = 45
STALE_SCAN_HOURS = 14
HIGH_VALUE_THRESHOLD = 750.0
ESCALATION_BATCH_SIZE = 20
SEVERITY_ORDER = {"breached": 3, "critical": 2, "at_risk": 1, "healthy": 0}


def _num(item: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float((item.get(key) or {}).get("N", default))
    except (TypeError, ValueError):
        return default


def _text(item: Dict[str, Any], key: str, default: str = "") -> str:
    return str((item.get(key) or {}).get("S", default))


def _load_in_flight() -> List[Dict[str, Any]]:
    """Drain the in-flight shipment index through the query paginator."""
    shipments: List[Dict[str, Any]] = []
    pages = dynamodb.get_paginator("query").paginate(
        TableName=SHIPMENT_TABLE,
        IndexName=SHIPMENT_STATE_INDEX,
        KeyConditionExpression="lifecycle_state = :state",
        ExpressionAttributeValues={":state": {"S": "IN_FLIGHT"}},
    )
    try:
        for page in pages:
            for item in page.get("Items", []):
                shipment_id = _text(item, "shipment_id")
                if not shipment_id:
                    continue
                shipments.append({
                    "shipment_id": shipment_id, "carrier": _text(item, "carrier", "UNKNOWN"),
                    "service_level": _text(item, "service_level", "GROUND"),
                    "account_id": _text(item, "account_id", "unknown"),
                    "pickup_at": int(_num(item, "pickup_at")),
                    "promised_at": int(_num(item, "promised_at")),
                    "last_scan_at": int(_num(item, "last_scan_at")),
                    "declared_value": _num(item, "declared_value"),
                    "notified_severity": _text(item, "notified_severity", "healthy"),
                })
    except ClientError as exc:
        logger.error("in_flight_query_failed error=%s", exc)
    return shipments


def _business_minutes_between(start: datetime, end: datetime) -> float:
    """Count minutes inside business hours, skipping weekends."""
    if end <= start:
        return 0.0
    total = 0.0
    cursor = start
    while cursor.date() < end.date():
        day_end = cursor.replace(hour=BUSINESS_DAY_END_HOUR, minute=0, second=0, microsecond=0)
        day_open = cursor.replace(hour=BUSINESS_DAY_START_HOUR, minute=0, second=0, microsecond=0)
        if cursor.weekday() < 5 and day_end > max(cursor, day_open):
            total += (day_end - max(cursor, day_open)).total_seconds() / 60.0
        cursor = (cursor + timedelta(days=1)).replace(
            hour=BUSINESS_DAY_START_HOUR, minute=0, second=0, microsecond=0)

    if end.weekday() < 5:
        window_start = max(
            cursor, end.replace(hour=BUSINESS_DAY_START_HOUR, minute=0, second=0, microsecond=0))
        window_end = min(
            end, end.replace(hour=BUSINESS_DAY_END_HOUR, minute=0, second=0, microsecond=0))
        if window_end > window_start:
            total += (window_end - window_start).total_seconds() / 60.0
    return total


def _classify(shipment: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    pickup = datetime.fromtimestamp(shipment["pickup_at"], tz=timezone.utc)
    promised = datetime.fromtimestamp(shipment["promised_at"], tz=timezone.utc)

    budget_minutes = _business_minutes_between(pickup, promised)
    consumed_minutes = _business_minutes_between(pickup, now)
    fraction = (consumed_minutes / budget_minutes) if budget_minutes > 0 else 1.0

    overdue_minutes = max(0.0, (now - promised).total_seconds() / 60.0)
    if overdue_minutes > BREACH_GRACE_MINUTES:
        severity = "breached"
    elif fraction >= CRITICAL_CONSUMED_FRACTION:
        severity = "critical"
    elif fraction >= AT_RISK_CONSUMED_FRACTION:
        severity = "at_risk"
    else:
        severity = "healthy"

    stale_hours = 0.0
    if shipment["last_scan_at"]:
        stale_hours = (now.timestamp() - shipment["last_scan_at"]) / 3600.0
        if stale_hours > STALE_SCAN_HOURS and severity == "healthy":
            severity = "at_risk"

    return {
        "shipment_id": shipment["shipment_id"], "carrier": shipment["carrier"],
        "service_level": shipment["service_level"], "account_id": shipment["account_id"],
        "severity": severity, "budget_minutes": round(budget_minutes, 1),
        "consumed_minutes": round(consumed_minutes, 1), "consumed_fraction": round(fraction, 3),
        "overdue_minutes": round(overdue_minutes, 1), "stale_scan_hours": round(stale_hours, 1),
        "high_value": shipment["declared_value"] >= HIGH_VALUE_THRESHOLD,
    }


def _needs_notification(assessment: Dict[str, Any], previous: str) -> bool:
    if assessment["severity"] == "healthy":
        return False
    return SEVERITY_ORDER[assessment["severity"]] > SEVERITY_ORDER.get(previous, 0)


def _publish(batch: List[Dict[str, Any]]) -> None:
    if not BREACH_TOPIC_ARN or not batch:
        return
    try:
        sns.publish(
            TopicArn=BREACH_TOPIC_ARN,
            Subject="SLA escalation: {0} shipments".format(len(batch)),
            Message=json.dumps({"escalations": batch}, default=str),
            MessageAttributes={
                "severity": {"DataType": "String", "StringValue": batch[0]["severity"]}},
        )
    except ClientError as exc:
        logger.error("sla_publish_failed error=%s", exc)


def _mark_notified(shipment_id: str, severity: str) -> None:
    try:
        dynamodb.update_item(
            TableName=SHIPMENT_TABLE,
            Key={"shipment_id": {"S": shipment_id}},
            UpdateExpression="SET notified_severity = :s, sla_checked_at = :t",
            ExpressionAttributeValues={":s": {"S": severity}, ":t": {"N": str(int(time.time()))}},
        )
    except ClientError as exc:
        logger.error("notify_marker_failed shipment=%s error=%s", shipment_id, exc)


def lambda_handler(event, context):
    sweep_id = str(event.get("id", "sla-{0}".format(int(time.time()))))
    now = datetime.now(tz=timezone.utc)

    shipments = _load_in_flight()
    if not shipments:
        logger.info("sla_sweep_empty sweep=%s", sweep_id)
        return {"sweep_id": sweep_id, "evaluated": 0}

    tally = {"healthy": 0, "at_risk": 0, "critical": 0, "breached": 0}
    escalations: List[Dict[str, Any]] = []

    for shipment in shipments:
        try:
            assessment = _classify(shipment, now)
        except (OverflowError, OSError, ValueError) as exc:
            logger.warning("classification_failed shipment=%s error=%s", shipment["shipment_id"], exc)
            continue

        tally[assessment["severity"]] += 1
        if _needs_notification(assessment, shipment["notified_severity"]):
            escalations.append(assessment)
            _mark_notified(assessment["shipment_id"], assessment["severity"])

    escalations.sort(key=lambda row: (-SEVERITY_ORDER[row["severity"]], -row["overdue_minutes"]))
    for start in range(0, len(escalations), ESCALATION_BATCH_SIZE):
        _publish(escalations[start:start + ESCALATION_BATCH_SIZE])

    logger.info(
        "sla_sweep_complete sweep=%s evaluated=%s breached=%s critical=%s at_risk=%s notified=%s",
        sweep_id, len(shipments), tally["breached"], tally["critical"], tally["at_risk"],
        len(escalations),
    )
    return {
        "sweep_id": sweep_id, "evaluated": len(shipments), "tally": tally,
        "notified": len(escalations),
        "high_value_at_risk": sum(1 for row in escalations if row["high_value"]),
    }
