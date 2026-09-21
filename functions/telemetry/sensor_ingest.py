"""Sensor reading ingest and normalisation.

Event source: Kinesis Data Stream carrying raw device telemetry.
Decodes each record, normalises units to SI, drops readings that fail plausibility
checks, and writes normalised readings to the time-series table.
"""

import base64
import json
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

READING_TABLE = os.environ.get("READING_TABLE", "sensor-readings")
REJECT_TABLE = os.environ.get("REJECT_TABLE", "sensor-rejects")
TTL_DAYS = int(os.environ.get("TTL_DAYS", "90"))

SECONDS_PER_DAY = 86400

# metric -> (si_unit, plausible_min, plausible_max)
METRIC_BOUNDS: Dict[str, Tuple[str, float, float]] = {
    "temperature": ("celsius", -80.0, 150.0),
    "humidity": ("percent", 0.0, 100.0),
    "pressure": ("pascal", 30000.0, 130000.0),
    "voltage": ("volt", 0.0, 600.0),
    "current": ("ampere", -1000.0, 1000.0),
    "vibration": ("mm_per_second", 0.0, 500.0),
    "flow_rate": ("litre_per_minute", 0.0, 10000.0),
}

UNIT_CONVERSIONS = {
    ("temperature", "fahrenheit"): lambda v: (v - 32.0) * 5.0 / 9.0,
    ("temperature", "kelvin"): lambda v: v - 273.15,
    ("temperature", "celsius"): lambda v: v,
    ("pressure", "hectopascal"): lambda v: v * 100.0,
    ("pressure", "bar"): lambda v: v * 100000.0,
    ("pressure", "psi"): lambda v: v * 6894.757,
    ("pressure", "pascal"): lambda v: v,
    ("flow_rate", "gallon_per_minute"): lambda v: v * 3.785412,
    ("flow_rate", "litre_per_minute"): lambda v: v,
    ("vibration", "inch_per_second"): lambda v: v * 25.4,
    ("vibration", "mm_per_second"): lambda v: v,
}


class ReadingRejected(ValueError):
    """Raised when a reading cannot be normalised or is implausible."""


def decode_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Base64-decode and JSON-parse one Kinesis record."""
    raw = record.get("kinesis", {}).get("data", "")
    payload = base64.b64decode(raw).decode("utf-8")
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ReadingRejected("payload is not a JSON object")
    return parsed


def normalise(metric: str, value: float, unit: str) -> float:
    """Convert a reading to its SI unit."""
    metric = metric.lower()
    unit = (unit or "").lower()
    if metric not in METRIC_BOUNDS:
        raise ReadingRejected("unknown metric %s" % metric)

    si_unit = METRIC_BOUNDS[metric][0]
    if unit == si_unit or not unit:
        return value

    converter = UNIT_CONVERSIONS.get((metric, unit))
    if converter is None:
        raise ReadingRejected("no conversion from %s to %s for %s" % (unit, si_unit, metric))
    return converter(value)


def check_plausible(metric: str, si_value: float) -> None:
    _unit, low, high = METRIC_BOUNDS[metric]
    if si_value < low or si_value > high:
        raise ReadingRejected(
            "%s value %.4f outside plausible range [%.1f, %.1f]" % (metric, si_value, low, high)
        )


def build_reading(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalise one decoded reading."""
    device_id = str(payload.get("device_id", "")).strip()
    metric = str(payload.get("metric", "")).strip().lower()
    if not device_id or not metric:
        raise ReadingRejected("device_id and metric are required")

    try:
        raw_value = float(payload["value"])
    except (KeyError, TypeError, ValueError):
        raise ReadingRejected("value must be numeric")

    observed_at = int(payload.get("observed_at", time.time()))
    if observed_at > int(time.time()) + 300:
        raise ReadingRejected("observed_at is in the future")

    si_value = normalise(metric, raw_value, str(payload.get("unit", "")))
    check_plausible(metric, si_value)

    return {
        "device_id": device_id,
        "metric_observed_at": "{0}#{1}".format(metric, observed_at),
        "metric": metric,
        "value": Decimal(str(round(si_value, 6))),
        "unit": METRIC_BOUNDS[metric][0],
        "raw_value": Decimal(str(raw_value)),
        "raw_unit": str(payload.get("unit", "")),
        "observed_at": observed_at,
        "expires_at": observed_at + TTL_DAYS * SECONDS_PER_DAY,
    }


def persist_readings(readings: List[Dict[str, Any]]) -> int:
    """Batch-write normalised readings."""
    if not readings:
        return 0
    table = dynamodb.Table(READING_TABLE)
    written = 0
    with table.batch_writer(overwrite_by_pkeys=["device_id", "metric_observed_at"]) as batch:
        for reading in readings:
            batch.put_item(Item=reading)
            written += 1
    return written


def record_reject(record: Dict[str, Any], reason: str) -> None:
    """Park an unusable reading for later inspection."""
    try:
        dynamodb.Table(REJECT_TABLE).put_item(
            Item={
                "sequence_number": str(record.get("kinesis", {}).get("sequenceNumber", "")),
                "partition_key": str(record.get("kinesis", {}).get("partitionKey", "")),
                "reason": reason,
                "rejected_at": int(time.time()),
                "expires_at": int(time.time()) + 14 * SECONDS_PER_DAY,
            }
        )
    except ClientError as exc:
        logger.error("reject_write_failed error=%s", exc)


def lambda_handler(event, context):
    records = event.get("Records", [])
    readings: List[Dict[str, Any]] = []
    rejected = 0
    errors = 0

    for record in records:
        try:
            payload = decode_record(record)
            readings.append(build_reading(payload))
        except ReadingRejected as exc:
            rejected += 1
            record_reject(record, str(exc))
        except (ValueError, TypeError, KeyError) as exc:
            rejected += 1
            record_reject(record, "decode_error: {0}".format(exc))
        except ClientError as exc:
            errors += 1
            logger.exception("reading_build_failed error=%s", exc)

    try:
        written = persist_readings(readings)
    except ClientError as exc:
        errors += len(readings)
        written = 0
        logger.exception("reading_batch_write_failed count=%s error=%s", len(readings), exc)

    logger.info(
        "sensor_ingest_complete records=%s written=%s rejected=%s errors=%s",
        len(records), written, rejected, errors,
    )
    return {"records": len(records), "written": written, "rejected": rejected, "errors": errors}
