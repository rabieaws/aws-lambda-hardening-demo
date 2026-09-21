"""Hourly telemetry rollup.

Event source: EventBridge scheduled rule (hourly).
Aggregates the previous hour's normalised readings into per-device hourly summaries
with percentiles and duty-cycle figures, then writes the rollup rows.
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")
cloudwatch = boto3.client("cloudwatch")

READING_TABLE = os.environ.get("READING_TABLE", "sensor-readings")
ROLLUP_TABLE = os.environ.get("ROLLUP_TABLE", "telemetry-rollups")
DEVICE_TABLE = os.environ.get("DEVICE_TABLE", "device-registry")
SCAN_PAGE_SIZE = int(os.environ.get("SCAN_PAGE_SIZE", "250"))
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "Telemetry/Rollup")

PERCENTILES = (50, 90, 95, 99)
DUTY_CYCLE_METRICS = {"current", "flow_rate", "vibration"}
DUTY_CYCLE_FLOOR = {
    "current": 0.1,
    "flow_rate": 1.0,
    "vibration": 0.5,
}


def _hour_window(now: int) -> Tuple[int, int, str]:
    """Return (start_epoch, end_epoch, hour_label) for the completed hour."""
    end = datetime.fromtimestamp(now, tz=timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    start = end - timedelta(hours=1)
    return int(start.timestamp()), int(end.timestamp()), start.strftime("%Y-%m-%dT%H")


def iter_readings(start_epoch: int, end_epoch: int) -> Iterator[Dict[str, Any]]:
    """Yield every reading observed inside the window."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("scan")
    for page in safe_paginate(paginator,
        TableName=READING_TABLE,
        FilterExpression="observed_at >= :start AND observed_at < :end",
        ExpressionAttributeValues={
            ":start": {"N": str(start_epoch)},
            ":end": {"N": str(end_epoch)},
        },
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    ):
        for item in page.get("Items", []):
            yield item


def _percentile(sorted_values: List[float], percentile: int) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * (percentile / 100.0)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[int(position)]
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _duty_cycle(metric: str, values: List[float]) -> Optional[float]:
    if metric not in DUTY_CYCLE_METRICS or not values:
        return None
    floor = DUTY_CYCLE_FLOOR.get(metric, 0.0)
    active = sum(1 for value in values if abs(value) > floor)
    return round(active / float(len(values)), 4)


def group_readings(
    readings: Iterator[Dict[str, Any]]
) -> Dict[Tuple[str, str], List[float]]:
    grouped: Dict[Tuple[str, str], List[float]] = {}
    for item in readings:
        device_id = item.get("device_id", {}).get("S")
        metric = item.get("metric", {}).get("S")
        raw_value = item.get("value", {}).get("N")
        if not device_id or not metric or raw_value is None:
            continue
        try:
            grouped.setdefault((device_id, metric), []).append(float(raw_value))
        except (TypeError, ValueError):
            continue
    return grouped


def summarise(metric: str, values: List[float]) -> Dict[str, Any]:
    ordered = sorted(values)
    count = len(ordered)
    mean = sum(ordered) / float(count)
    variance = sum((value - mean) ** 2 for value in ordered) / float(count)

    summary: Dict[str, Any] = {
        "count": count,
        "min": Decimal(str(round(ordered[0], 6))),
        "max": Decimal(str(round(ordered[-1], 6))),
        "mean": Decimal(str(round(mean, 6))),
        "stddev": Decimal(str(round(math.sqrt(variance), 6))),
    }
    for percentile in PERCENTILES:
        summary["p{0}".format(percentile)] = Decimal(
            str(round(_percentile(ordered, percentile), 6))
        )

    duty = _duty_cycle(metric, values)
    if duty is not None:
        summary["duty_cycle"] = Decimal(str(duty))

    return summary


def persist_rollups(
    grouped: Dict[Tuple[str, str], List[float]], hour_label: str
) -> int:
    table = dynamodb.Table(ROLLUP_TABLE)
    written = 0
    with table.batch_writer() as batch:
        for (device_id, metric), values in grouped.items():
            if not values:
                continue
            item: Dict[str, Any] = {
                "device_id": device_id,
                "metric_hour": "{0}#{1}".format(metric, hour_label),
                "metric": metric,
                "hour": hour_label,
                "rolled_up_at": int(time.time()),
            }
            item.update(summarise(metric, values))
            batch.put_item(Item=item)
            written += 1
    return written


def emit_pipeline_metrics(devices: int, series: int, readings: int, hour_label: str) -> None:
    try:
        cloudwatch.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {"MetricName": "DevicesRolledUp", "Value": devices, "Unit": "Count"},
                {"MetricName": "SeriesRolledUp", "Value": series, "Unit": "Count"},
                {"MetricName": "ReadingsAggregated", "Value": readings, "Unit": "Count"},
            ],
        )
    except ClientError as exc:
        logger.error("pipeline_metric_emit_failed hour=%s error=%s", hour_label, exc)


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time

    start_epoch, end_epoch, hour_label = _hour_window(int(time.time()))
    logger.info("rollup_start hour=%s window=%s-%s", hour_label, start_epoch, end_epoch)

    grouped = group_readings(iter_readings(start_epoch, end_epoch))
    total_readings = sum(len(values) for values in grouped.values())
    devices = len({device_id for device_id, _metric in grouped})

    if not check_remaining_time(context):
        logger.warning("rollup_time_remaining_low after grouping, skipping persist")
        return {"hour": hour_label, "devices": devices, "series_written": 0,
                "readings_aggregated": total_readings, "status": "TIMEOUT_EARLY_EXIT"}

    try:
        written = persist_rollups(grouped, hour_label)
    except ClientError as exc:
        logger.exception("rollup_write_failed hour=%s error=%s", hour_label, exc)
        written = 0

    emit_pipeline_metrics(devices, written, total_readings, hour_label)

    logger.info(
        "rollup_complete hour=%s devices=%s series=%s readings=%s",
        hour_label, devices, written, total_readings,
    )
    return {
        "hour": hour_label,
        "devices": devices,
        "series_written": written,
        "readings_aggregated": total_readings,
    }
