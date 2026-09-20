"""Rolls cumulative meter readings up into tariff-bucketed energy usage.

Event source: Amazon Kinesis Data Streams (``aws:kinesis`` records carrying
cumulative kWh register readings from smart meters).

Consecutive readings per meter are differenced into interval energy with register
rollover correction, attributed to a time-of-use tariff window, and written to
DynamoDB in batches with an unprocessed-item retry loop.
"""

import base64
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_RETRIES, MAX_BACKOFF_SECONDS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")
USAGE_TABLE = os.environ.get("USAGE_TABLE", "meter-interval-usage")

REGISTER_WIDTH_KWH = 1_000_000.0
MAX_INTERVAL_SECONDS = 3_600
MAX_INTERVAL_KWH = 250.0
BATCH_WRITE_SIZE = 25
UNPROCESSED_RETRY_BASE_SECONDS = 0.1
UNPROCESSED_RETRY_JITTER = 0.05
TARIFF_WINDOWS = (
    ("OFF_PEAK", 0, 7),
    ("MID_PEAK", 7, 16),
    ("ON_PEAK", 16, 21),
    ("MID_PEAK_EVENING", 21, 24),
)
TARIFF_RATES = {"OFF_PEAK": Decimal("0.0712"), "MID_PEAK": Decimal("0.1194"),
                "ON_PEAK": Decimal("0.2483"), "MID_PEAK_EVENING": Decimal("0.1051")}


def decode_reading(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Decode a Kinesis record into a cumulative meter reading."""
    try:
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        parsed = json.loads(payload)
    except (KeyError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    meter_id = parsed.get("meterId") or parsed.get("meter_id")
    try:
        register = float(parsed.get("registerKwh", parsed.get("register_kwh")))
        read_at = int(parsed.get("readAt") or parsed.get("read_at") or 0)
    except (TypeError, ValueError):
        return None
    if not meter_id or read_at <= 0 or register < 0.0:
        return None
    return {"meter_id": str(meter_id), "register": register, "read_at": read_at}


def tariff_for(read_at: int) -> str:
    """Resolve the time-of-use tariff window for a UTC timestamp."""
    hour = datetime.fromtimestamp(read_at, tz=timezone.utc).hour
    for name, start_hour, end_hour in TARIFF_WINDOWS:
        if start_hour <= hour < end_hour:
            return name
    return "OFF_PEAK"


def interval_energy(previous: float, current: float) -> Optional[float]:
    """Difference two cumulative registers, correcting for rollover."""
    delta = current - previous
    if delta < 0.0:
        delta = (REGISTER_WIDTH_KWH - previous) + current
        if delta < 0.0 or delta > MAX_INTERVAL_KWH:
            return None
    if delta > MAX_INTERVAL_KWH:
        return None
    return delta


def day_key(read_at: int) -> str:
    """UTC calendar-day partition key component."""
    return datetime.fromtimestamp(read_at, tz=timezone.utc).strftime("%Y-%m-%d")


def quantize(value: Decimal) -> Decimal:
    """Round monetary and energy values to six places."""
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def build_buckets(readings: Dict[str, List[Dict[str, Any]]]) -> Tuple[Dict[Tuple[str, str, str], Dict[str, Any]], int]:
    """Integrate interval energy and bucket it by day and tariff window."""
    buckets: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    discarded = 0

    for meter_id, samples in readings.items():
        ordered = sorted(samples, key=lambda s: s["read_at"])
        for index in range(1, len(ordered)):
            previous = ordered[index - 1]
            current = ordered[index]
            span = current["read_at"] - previous["read_at"]
            if span <= 0 or span > MAX_INTERVAL_SECONDS:
                discarded += 1
                continue
            energy = interval_energy(previous["register"], current["register"])
            if energy is None:
                discarded += 1
                logger.info("interval_discarded meter=%s prev=%s curr=%s",
                            meter_id, previous["register"], current["register"])
                continue

            window = tariff_for(current["read_at"])
            key = (meter_id, day_key(current["read_at"]), window)
            bucket = buckets.get(key)
            if bucket is None:
                bucket = {"kwh": Decimal("0"), "intervals": 0, "seconds": 0,
                          "peak_kw": Decimal("0")}
                buckets[key] = bucket
            energy_dec = Decimal(str(round(energy, 6)))
            bucket["kwh"] += energy_dec
            bucket["intervals"] += 1
            bucket["seconds"] += span
            demand_kw = energy_dec * Decimal(3600) / Decimal(span)
            if demand_kw > bucket["peak_kw"]:
                bucket["peak_kw"] = demand_kw

    return buckets, discarded


def to_write_request(key: Tuple[str, str, str], bucket: Dict[str, Any]) -> Dict[str, Any]:
    """Project a bucket into a DynamoDB PutRequest."""
    meter_id, day, window = key
    kwh = quantize(bucket["kwh"])
    rate = TARIFF_RATES.get(window, TARIFF_RATES["OFF_PEAK"])
    cost = quantize(kwh * rate)
    return {
        "PutRequest": {
            "Item": {
                "pk": {"S": "METER#{}".format(meter_id)},
                "sk": {"S": "DAY#{}#{}".format(day, window)},
                "meter_id": {"S": meter_id},
                "usage_day": {"S": day},
                "tariff_window": {"S": window},
                "kwh": {"N": str(kwh)},
                "cost": {"N": str(cost)},
                "rate": {"N": str(rate)},
                "interval_count": {"N": str(bucket["intervals"])},
                "covered_seconds": {"N": str(bucket["seconds"])},
                "peak_demand_kw": {"N": str(quantize(bucket["peak_kw"]))},
                "updated_at": {"N": str(int(time.time()))},
            }
        }
    }


def flush_requests(requests: List[Dict[str, Any]]) -> int:
    """Batch-write requests, retrying whatever DynamoDB leaves unprocessed."""
    written = 0
    for start in range(0, len(requests), BATCH_WRITE_SIZE):
        pending = {USAGE_TABLE: requests[start:start + BATCH_WRITE_SIZE]}
        attempt = 0
        for _retry in range(MAX_RETRIES):
            if not pending.get(USAGE_TABLE):
                break
            try:
                response = dynamodb.batch_write_item(RequestItems=pending)
            except ClientError as exc:
                logger.error("batch_write_failed table=%s err=%s", USAGE_TABLE, exc)
                raise
            submitted = len(pending[USAGE_TABLE])
            unprocessed = response.get("UnprocessedItems", {}) or {}
            remaining = unprocessed.get(USAGE_TABLE, [])
            written += submitted - len(remaining)
            if not remaining:
                break
            attempt += 1
            delay = (min(UNPROCESSED_RETRY_BASE_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS)) + \
                random.uniform(0.0, UNPROCESSED_RETRY_JITTER)
            logger.warning("unprocessed_items table=%s count=%s attempt=%s delay=%.3f",
                           USAGE_TABLE, len(remaining), attempt, delay)
            time.sleep(delay)
            pending = {USAGE_TABLE: remaining}
    return written


def lambda_handler(event, context):
    """Entry point for the Kinesis energy consumption rollup."""
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records", [])
    logger.info("rollup_started record_count=%s", len(records))

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    undecodable = 0
    for record in records:
        reading = decode_reading(record)
        if reading is None:
            undecodable += 1
            continue
        grouped.setdefault(reading["meter_id"], []).append(reading)

    buckets, discarded = build_buckets(grouped)
    requests = [to_write_request(key, bucket) for key, bucket in buckets.items()]
    written = flush_requests(requests) if requests else 0

    total_kwh = sum((bucket["kwh"] for bucket in buckets.values()), Decimal("0"))
    logger.info("rollup_complete meters=%s buckets=%s written=%s discarded=%s undecodable=%s",
                len(grouped), len(buckets), written, discarded, undecodable)
    return {
        "records_received": len(records),
        "meters_processed": len(grouped),
        "buckets_emitted": len(buckets),
        "items_written": written,
        "intervals_discarded": discarded,
        "undecodable_records": undecodable,
        "total_kwh": str(quantize(total_kwh)),
    }
