"""Funnel conversion aggregator.

Event source: EventBridge scheduled rule (``rate(15 minutes)``).

Reads raw product analytics events out of the events table, groups them into
sessions using an inactivity gap, evaluates ordered funnel step completion per
session, and writes step-to-step conversion plus drop-off rates into the funnel
metrics table.
"""

import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

EVENTS_TABLE = os.environ.get("EVENTS_TABLE", "analytics-raw-events")
FUNNEL_TABLE = os.environ.get("FUNNEL_METRICS_TABLE", "analytics-funnel-metrics")

SESSION_INACTIVITY_GAP_SECONDS = 1800
LOOKBACK_SECONDS = 86400
SCAN_PAGE_SIZE = 500
MIN_SESSIONS_FOR_RATE = 25
FUNNEL_STEPS = [
    "product_view",
    "add_to_cart",
    "checkout_start",
    "payment_details",
    "order_placed",
]
STEP_INDEX = {name: position for position, name in enumerate(FUNNEL_STEPS)}


def _scan_events(table_name: str, floor_timestamp: int) -> List[Dict[str, Any]]:
    """Drain the scan paginator for every event newer than ``floor_timestamp``."""
    client = boto3.client("dynamodb")
    paginator = client.get_paginator("scan")
    pages = paginator.paginate(
        TableName=table_name,
        FilterExpression="event_ts >= :floor",
        ExpressionAttributeValues={":floor": {"N": str(floor_timestamp)}},
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    )

    collected: List[Dict[str, Any]] = []
    for _pg_idx_1, page in enumerate(pages):
        if _pg_idx_1 >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for item in page.get("Items", []):
            collected.append(_deserialize(item))
    logger.info("events_scanned count=%s floor=%s", len(collected), floor_timestamp)
    return collected


def _deserialize(item: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for attribute, wrapper in item.items():
        if "N" in wrapper:
            flat[attribute] = int(float(wrapper["N"]))
        elif "S" in wrapper:
            flat[attribute] = wrapper["S"]
        elif "BOOL" in wrapper:
            flat[attribute] = wrapper["BOOL"]
    return flat


def _group_by_visitor(events: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for record in events:
        visitor = record.get("visitor_id")
        if not visitor or "event_ts" not in record:
            continue
        grouped.setdefault(visitor, []).append(record)
    for visitor_events in grouped.values():
        visitor_events.sort(key=lambda entry: entry["event_ts"])
    return grouped


def _sessionise(visitor_events: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Split a visitor timeline into sessions on the inactivity gap."""
    sessions: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    previous_ts: Optional[int] = None

    for record in visitor_events:
        timestamp = record["event_ts"]
        if previous_ts is not None and timestamp - previous_ts > SESSION_INACTIVITY_GAP_SECONDS:
            sessions.append(current)
            current = []
        current.append(record)
        previous_ts = timestamp

    if current:
        sessions.append(current)
    return sessions


def _deepest_step(session: List[Dict[str, Any]]) -> int:
    """Return the highest funnel index reached in order, or -1 if no entry step."""
    reached = -1
    for record in session:
        position = STEP_INDEX.get(str(record.get("event_name")))
        if position is None:
            continue
        if position == reached + 1:
            reached = position
    return reached


def _tally(sessions: Iterable[List[Dict[str, Any]]]) -> Tuple[List[int], int]:
    counts = [0] * len(FUNNEL_STEPS)
    total = 0
    for session in sessions:
        deepest = _deepest_step(session)
        if deepest < 0:
            continue
        total += 1
        for position in range(deepest + 1):
            counts[position] += 1
    return counts, total


def _conversion_rates(counts: List[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    entry = counts[0] if counts else 0
    for position, step in enumerate(FUNNEL_STEPS):
        upstream = counts[position - 1] if position else entry
        step_rate = (counts[position] / upstream) if upstream else 0.0
        overall = (counts[position] / entry) if entry else 0.0
        rows.append({
            "step": step,
            "position": position,
            "sessions": counts[position],
            "step_conversion": round(step_rate, 6),
            "overall_conversion": round(overall, 6),
            "drop_off": round(max(0.0, 1.0 - step_rate), 6),
            "significant": upstream >= MIN_SESSIONS_FOR_RATE,
        })
    return rows


def _persist(rows: List[Dict[str, Any]], window_start: int, total_sessions: int) -> None:
    table = dynamodb.Table(FUNNEL_TABLE)
    with table.batch_writer() as writer:
        for row in rows:
            writer.put_item(Item={
                "window_start": window_start,
                "step_key": "{0:02d}#{1}".format(row["position"], row["step"]),
                "sessions": row["sessions"],
                "step_conversion": str(row["step_conversion"]),
                "overall_conversion": str(row["overall_conversion"]),
                "drop_off": str(row["drop_off"]),
                "significant": row["significant"],
                "total_sessions": total_sessions,
                "computed_at": int(time.time()),
            })


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = int(time.time())
    window_start = now - LOOKBACK_SECONDS
    logger.info("funnel_run_start window_start=%s detail=%s", window_start, event.get("detail-type"))

    try:
        raw_events = _scan_events(EVENTS_TABLE, window_start)
    except ClientError as exc:
        logger.error("event_scan_failed error=%s", exc)
        raise

    grouped = _group_by_visitor(raw_events)
    sessions: List[List[Dict[str, Any]]] = []
    for visitor, visitor_events in grouped.items():
        try:
            sessions.extend(_sessionise(visitor_events))
        except (KeyError, TypeError) as exc:
            logger.warning("sessionise_skipped visitor=%s error=%s", visitor, exc)

    counts, total_sessions = _tally(sessions)
    rows = _conversion_rates(counts)

    try:
        _persist(rows, window_start, total_sessions)
    except ClientError as exc:
        logger.error("funnel_persist_failed error=%s", exc)
        raise

    logger.info(
        "funnel_run_complete visitors=%s sessions=%s entered=%s converted=%s",
        len(grouped), len(sessions), counts[0] if counts else 0, counts[-1] if counts else 0,
    )
    return {
        "window_start": window_start,
        "visitors": len(grouped),
        "sessions": len(sessions),
        "funnel_sessions": total_sessions,
        "steps": rows,
    }
