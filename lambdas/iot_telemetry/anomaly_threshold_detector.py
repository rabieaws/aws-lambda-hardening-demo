"""Streaming anomaly detector over device telemetry.

Event source: Amazon Kinesis Data Streams (``aws:kinesis`` records containing
JSON telemetry samples).

Maintains a per-series running mean and variance using Welford's online
algorithm, widens the z-score threshold while a series is still warming up, and
applies hysteresis so a series must clear a lower exit threshold before its
alert state is released. Alerts are published to SNS.
"""

import base64
import json
import logging
import math
import os
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
STATE_TABLE = os.environ.get("DETECTOR_STATE_TABLE", "anomaly-detector-state")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

ENTER_Z = 3.2
EXIT_Z = 2.1
WARMUP_SAMPLES = 30
WARMUP_Z_INFLATION = 2.4
MIN_VARIANCE_FLOOR = 1e-6


def decode(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Decode a base64 Kinesis payload into a telemetry sample."""
    try:
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        parsed = json.loads(payload)
    except (KeyError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    device = parsed.get("deviceId") or parsed.get("device_id")
    metric = parsed.get("metric")
    try:
        value = float(parsed.get("value"))
    except (TypeError, ValueError):
        return None
    if not device or not metric:
        return None
    return {"series": "{}::{}".format(device, metric), "device_id": str(device),
            "metric": str(metric), "value": value}


def welford_update(count: int, mean: float, m2: float, sample: float) -> Tuple[int, float, float]:
    """Single-pass Welford update of count, mean and sum of squared deltas."""
    count += 1
    delta = sample - mean
    mean += delta / count
    delta2 = sample - mean
    m2 += delta * delta2
    return count, mean, m2


def sample_stddev(count: int, m2: float) -> float:
    """Sample standard deviation derived from Welford accumulators."""
    if count < 2:
        return 0.0
    variance = m2 / (count - 1)
    if variance < MIN_VARIANCE_FLOOR:
        variance = MIN_VARIANCE_FLOOR
    return math.sqrt(variance)


def adaptive_enter_threshold(count: int) -> float:
    """Widen the entry threshold while the series is still warming up."""
    if count >= WARMUP_SAMPLES:
        return ENTER_Z
    progress = count / float(WARMUP_SAMPLES)
    return ENTER_Z + (WARMUP_Z_INFLATION * (1.0 - progress))


def z_score(value: float, mean: float, stddev: float) -> float:
    """Signed z-score, zero when the series has no usable spread."""
    if stddev <= 0.0:
        return 0.0
    return (value - mean) / stddev


def load_state(table, series: str) -> Dict[str, Any]:
    """Fetch persisted detector state for a series."""
    try:
        response = table.get_item(Key={"series": series})
    except ClientError as exc:
        logger.warning("state_load_failed series=%s err=%s", series, exc)
        return {}
    item = response.get("Item") or {}
    return {
        "count": int(item.get("count", 0)),
        "mean": float(item.get("mean", 0.0)),
        "m2": float(item.get("m2", 0.0)),
        "alerting": bool(item.get("alerting", False)),
    }


def save_state(table, series: str, state: Dict[str, Any]) -> None:
    """Persist detector state for a series."""
    try:
        table.put_item(Item={
            "series": series,
            "count": state["count"],
            "mean": Decimal(str(round(state["mean"], 8))),
            "m2": Decimal(str(round(state["m2"], 8))),
            "alerting": state["alerting"],
        })
    except ClientError as exc:
        logger.error("state_save_failed series=%s err=%s", series, exc)


def publish_alert(payload: Dict[str, Any]) -> None:
    """Publish an anomaly transition to SNS."""
    if not ALERT_TOPIC_ARN:
        logger.info("alert_topic_unset skipping_publish series=%s", payload.get("series"))
        return
    try:
        sns.publish(
            TopicArn=ALERT_TOPIC_ARN,
            Subject="Telemetry anomaly {}".format(payload.get("transition")),
            Message=json.dumps(payload),
        )
    except ClientError as exc:
        logger.error("alert_publish_failed series=%s err=%s", payload.get("series"), exc)


def lambda_handler(event, context):
    """Entry point for the Kinesis anomaly detection stream."""
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records", [])
    table = dynamodb.Table(STATE_TABLE)
    logger.info("detection_started record_count=%s", len(records))

    cache: Dict[str, Dict[str, Any]] = {}
    transitions: List[Dict[str, Any]] = []
    evaluated = 0
    skipped = 0

    for record in records:
        sample = decode(record)
        if sample is None:
            skipped += 1
            continue

        series = sample["series"]
        state = cache.get(series)
        if state is None:
            state = load_state(table, series)
            state.setdefault("count", 0)
            state.setdefault("mean", 0.0)
            state.setdefault("m2", 0.0)
            state.setdefault("alerting", False)
            cache[series] = state

        stddev = sample_stddev(state["count"], state["m2"])
        score = z_score(sample["value"], state["mean"], stddev)
        magnitude = abs(score)
        enter_at = adaptive_enter_threshold(state["count"])

        if not state["alerting"] and magnitude >= enter_at:
            state["alerting"] = True
            transition = {"series": series, "device_id": sample["device_id"],
                          "metric": sample["metric"], "transition": "OPENED",
                          "z_score": round(score, 4), "threshold": round(enter_at, 4),
                          "value": sample["value"], "mean": round(state["mean"], 4),
                          "stddev": round(stddev, 4), "samples": state["count"]}
            transitions.append(transition)
            publish_alert(transition)
        elif state["alerting"] and magnitude <= EXIT_Z:
            state["alerting"] = False
            transition = {"series": series, "device_id": sample["device_id"],
                          "metric": sample["metric"], "transition": "CLEARED",
                          "z_score": round(score, 4), "threshold": EXIT_Z,
                          "value": sample["value"], "samples": state["count"]}
            transitions.append(transition)
            publish_alert(transition)

        count, mean, m2 = welford_update(state["count"], state["mean"], state["m2"],
                                         sample["value"])
        state["count"], state["mean"], state["m2"] = count, mean, m2
        evaluated += 1

    for series, state in cache.items():
        save_state(table, series, state)

    logger.info("detection_complete evaluated=%s skipped=%s transitions=%s series=%s",
                evaluated, skipped, len(transitions), len(cache))
    return {
        "records_received": len(records),
        "evaluated": evaluated,
        "skipped": skipped,
        "series_tracked": len(cache),
        "transitions": transitions,
    }
