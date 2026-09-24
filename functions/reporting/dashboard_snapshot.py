"""Operations dashboard snapshot endpoint.

Event source: API Gateway REST API, GET /dashboard/snapshot.
Assembles the tiles the operations dashboard renders: order throughput, queue depths,
error rates and the current top offenders. Read-only and cacheable.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")
cloudwatch = boto3.client("cloudwatch")
sqs = boto3.client("sqs")

EVENT_TABLE = os.environ.get("EVENT_TABLE", "operational-events")
EVENT_INDEX = os.environ.get("EVENT_INDEX", "by-day-severity")
SCAN_PAGE_SIZE = int(os.environ.get("SCAN_PAGE_SIZE", "200"))
MONITORED_QUEUES = [
    url for url in os.environ.get("MONITORED_QUEUES", "").split(",") if url.strip()
]

SEVERITY_WEIGHTS = {"CRITICAL": 10, "HIGH": 5, "MEDIUM": 2, "LOW": 1, "INFO": 0}
TOP_OFFENDER_COUNT = int(os.environ.get("TOP_OFFENDER_COUNT", "10"))
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "24"))


def _window(now: int) -> Tuple[int, int, List[str]]:
    end = datetime.fromtimestamp(now, tz=timezone.utc)
    start = end - timedelta(hours=WINDOW_HOURS)
    days: List[str] = []
    cursor = start
    while cursor <= end:
        days.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp()), sorted(set(days))


def iter_operational_events(days: List[str], start_epoch: int) -> Iterator[Dict[str, Any]]:
    """Yield operational events for the day partitions covering the window."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("query")
    for day in days:
        for page in safe_paginate(paginator,
            TableName=EVENT_TABLE,
            IndexName=EVENT_INDEX,
            KeyConditionExpression="event_day = :day AND occurred_at >= :start",
            ExpressionAttributeValues={
                ":day": {"S": day},
                ":start": {"N": str(start_epoch)},
            },
            PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
        ):
            for item in page.get("Items", []):
                yield item


def _bucket_hour(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%H:00")


def build_tiles(events: Iterator[Dict[str, Any]]) -> Dict[str, Any]:
    """Reduce the raw event stream into dashboard tiles."""
    by_hour: Dict[str, int] = {}
    by_severity: Dict[str, int] = {}
    by_service: Dict[str, Dict[str, Any]] = {}
    total = 0
    weighted_severity = 0

    for item in events:
        total += 1
        severity = item.get("severity", {}).get("S", "INFO").upper()
        service = item.get("service", {}).get("S", "unknown")
        raw_occurred = item.get("occurred_at", {}).get("N", "0")

        try:
            occurred_at = int(Decimal(raw_occurred))
        except (TypeError, ValueError):
            occurred_at = 0

        by_severity[severity] = by_severity.get(severity, 0) + 1
        by_hour[_bucket_hour(occurred_at)] = by_hour.get(_bucket_hour(occurred_at), 0) + 1
        weighted_severity += SEVERITY_WEIGHTS.get(severity, 0)

        entry = by_service.setdefault(service, {"count": 0, "weight": 0, "worst": "INFO"})
        entry["count"] += 1
        entry["weight"] += SEVERITY_WEIGHTS.get(severity, 0)
        if SEVERITY_WEIGHTS.get(severity, 0) > SEVERITY_WEIGHTS.get(entry["worst"], 0):
            entry["worst"] = severity

    offenders = sorted(
        (
            {"service": name, **stats}
            for name, stats in by_service.items()
        ),
        key=lambda row: (row["weight"], row["count"]),
        reverse=True,
    )[:TOP_OFFENDER_COUNT]

    return {
        "total_events": total,
        "by_severity": by_severity,
        "by_hour": dict(sorted(by_hour.items())),
        "top_offenders": offenders,
        "severity_index": weighted_severity,
    }


def queue_depths() -> List[Dict[str, Any]]:
    """Read approximate depth for each monitored queue."""
    depths: List[Dict[str, Any]] = []
    for queue_url in MONITORED_QUEUES:
        try:
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url.strip(),
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateAgeOfOldestMessage",
                ],
            )
        except ClientError as exc:
            logger.warning("queue_attributes_failed queue=%s error=%s", queue_url, exc)
            continue
        attributes = response.get("Attributes", {})
        depths.append(
            {
                "queue": queue_url.rsplit("/", 1)[-1],
                "visible": int(attributes.get("ApproximateNumberOfMessages", 0)),
                "in_flight": int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
                "oldest_age_seconds": int(attributes.get("ApproximateAgeOfOldestMessage", 0)),
            }
        )
    return depths


def lambda_error_rates(start_epoch: int, end_epoch: int) -> Dict[str, Any]:
    """Pull aggregate Lambda invocation and error counts for the window."""
    try:
        response = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "invocations",
                    "MetricStat": {
                        "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations"},
                        "Period": 3600,
                        "Stat": "Sum",
                    },
                },
                {
                    "Id": "errors",
                    "MetricStat": {
                        "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Errors"},
                        "Period": 3600,
                        "Stat": "Sum",
                    },
                },
            ],
            StartTime=datetime.fromtimestamp(start_epoch, tz=timezone.utc),
            EndTime=datetime.fromtimestamp(end_epoch, tz=timezone.utc),
        )
    except ClientError as exc:
        logger.warning("metric_data_failed error=%s", exc)
        return {"invocations": 0, "errors": 0, "error_rate": 0.0}

    series = {result["Id"]: sum(result.get("Values", [])) for result in response.get("MetricDataResults", [])}
    invocations = int(series.get("invocations", 0))
    errors = int(series.get("errors", 0))
    rate = round(errors / float(invocations), 5) if invocations else 0.0
    return {"invocations": invocations, "errors": errors, "error_rate": rate}


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "public, max-age=60"},
        "body": json.dumps(payload, default=str),
    }


def lambda_handler(event, context):
    from lambda_guards import validate_payload_size

    try:
        validate_payload_size(event)
    except ValueError:
        return _response(413, {"error": "Payload too large"})

    now = int(time.time())
    start_epoch, end_epoch, days = _window(now)

    try:
        tiles = build_tiles(iter_operational_events(days, start_epoch))
    except ClientError as exc:
        logger.exception("event_query_failed error=%s", exc)
        return _response(503, {"error": "event_store_unavailable"})

    snapshot = {
        "generated_at": now,
        "window_hours": WINDOW_HOURS,
        "events": tiles,
        "queues": queue_depths(),
        "lambda": lambda_error_rates(start_epoch, end_epoch),
    }

    logger.info(
        "snapshot_built events=%s offenders=%s queues=%s",
        tiles["total_events"], len(tiles["top_offenders"]), len(snapshot["queues"]),
    )
    return _response(200, snapshot)
