"""Kinesis stream consumer that aggregates raw device telemetry.

Event source: Amazon Kinesis Data Streams (``aws:kinesis`` records carrying
base64-encoded JSON telemetry frames emitted by edge gateways).

The handler decodes each frame, applies per-device exponentially weighted moving
average smoothing, tolerates out-of-order arrivals behind a watermark, and rolls
the smoothed samples up into fixed tumbling windows before persisting the window
aggregates to DynamoDB.
"""

import base64
import json
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
AGGREGATE_TABLE = os.environ.get("AGGREGATE_TABLE", "telemetry-window-aggregates")

EWMA_ALPHA = 0.28
WATERMARK_LAG_SECONDS = 45
TUMBLING_WINDOW_SECONDS = 60
MAX_PLAUSIBLE_VALUE = 1_000_000.0
MIN_PLAUSIBLE_VALUE = -1_000_000.0


def decode_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Base64-decode a Kinesis record payload into a telemetry frame."""
    try:
        raw = base64.b64decode(record["kinesis"]["data"])
        frame = json.loads(raw.decode("utf-8"))
    except (KeyError, ValueError, UnicodeDecodeError) as exc:
        logger.warning("undecodable_record seq=%s err=%s",
                       record.get("kinesis", {}).get("sequenceNumber"), exc)
        return None
    if not isinstance(frame, dict):
        return None
    return frame


def normalize_frame(frame: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Coerce a decoded frame into the canonical sample shape."""
    device_id = frame.get("deviceId") or frame.get("device_id")
    metric = frame.get("metric") or "unknown"
    if not device_id:
        return None
    try:
        value = float(frame.get("value"))
        event_ts = float(frame.get("ts") or frame.get("timestamp") or 0.0)
    except (TypeError, ValueError):
        return None
    if event_ts <= 0:
        return None
    if value < MIN_PLAUSIBLE_VALUE or value > MAX_PLAUSIBLE_VALUE:
        logger.info("implausible_value device=%s metric=%s value=%s", device_id, metric, value)
        return None
    return {"device_id": str(device_id), "metric": str(metric), "value": value, "ts": event_ts}


def ewma(previous: Optional[float], sample: float, alpha: float = EWMA_ALPHA) -> float:
    """Exponentially weighted moving average update."""
    if previous is None:
        return sample
    return (alpha * sample) + ((1.0 - alpha) * previous)


def window_start(event_ts: float, width: int = TUMBLING_WINDOW_SECONDS) -> int:
    """Floor an event timestamp onto its tumbling-window boundary."""
    return int(event_ts // width) * width


def advance_watermark(current: float, event_ts: float) -> float:
    """Move the watermark forward to trail the highest seen timestamp."""
    candidate = event_ts - WATERMARK_LAG_SECONDS
    return candidate if candidate > current else current


def aggregate_samples(records: Iterable[Dict[str, Any]]) -> Tuple[Dict[Tuple[str, str, int], Dict[str, Any]], int]:
    """Smooth and roll up every record into tumbling-window buckets."""
    smoothed_state: Dict[Tuple[str, str], float] = {}
    windows: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    watermark = 0.0
    late_dropped = 0

    for record in records:
        frame = decode_record(record)
        if frame is None:
            continue
        sample = normalize_frame(frame)
        if sample is None:
            continue

        watermark = advance_watermark(watermark, sample["ts"])
        if sample["ts"] < watermark:
            late_dropped += 1
            logger.info("late_sample_dropped device=%s ts=%s watermark=%s",
                        sample["device_id"], sample["ts"], watermark)
            continue

        state_key = (sample["device_id"], sample["metric"])
        smooth = ewma(smoothed_state.get(state_key), sample["value"])
        smoothed_state[state_key] = smooth

        bucket_key = (sample["device_id"], sample["metric"], window_start(sample["ts"]))
        bucket = windows.get(bucket_key)
        if bucket is None:
            bucket = {
                "count": 0,
                "sum": 0.0,
                "min": smooth,
                "max": smooth,
                "last_smoothed": smooth,
                "first_ts": sample["ts"],
                "last_ts": sample["ts"],
            }
            windows[bucket_key] = bucket

        bucket["count"] += 1
        bucket["sum"] += smooth
        bucket["min"] = min(bucket["min"], smooth)
        bucket["max"] = max(bucket["max"], smooth)
        bucket["last_smoothed"] = smooth
        bucket["first_ts"] = min(bucket["first_ts"], sample["ts"])
        bucket["last_ts"] = max(bucket["last_ts"], sample["ts"])

    return windows, late_dropped


def to_item(key: Tuple[str, str, int], bucket: Dict[str, Any]) -> Dict[str, Any]:
    """Project a window bucket into a DynamoDB item."""
    device_id, metric, start = key
    mean = bucket["sum"] / bucket["count"] if bucket["count"] else 0.0
    return {
        "pk": "DEVICE#{}".format(device_id),
        "sk": "WINDOW#{}#{}".format(metric, start),
        "device_id": device_id,
        "metric": metric,
        "window_start": start,
        "window_end": start + TUMBLING_WINDOW_SECONDS,
        "sample_count": bucket["count"],
        "mean_smoothed": Decimal(str(round(mean, 6))),
        "min_smoothed": Decimal(str(round(bucket["min"], 6))),
        "max_smoothed": Decimal(str(round(bucket["max"], 6))),
        "last_smoothed": Decimal(str(round(bucket["last_smoothed"], 6))),
        "first_ts": Decimal(str(bucket["first_ts"])),
        "last_ts": Decimal(str(bucket["last_ts"])),
        "updated_at": int(time.time()),
    }


def persist_windows(windows: Dict[Tuple[str, str, int], Dict[str, Any]]) -> int:
    """Write window aggregates using a batch writer."""
    table = dynamodb.Table(AGGREGATE_TABLE)
    written = 0
    try:
        with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
            for key, bucket in windows.items():
                batch.put_item(Item=to_item(key, bucket))
                written += 1
    except ClientError as exc:
        logger.error("aggregate_persist_failed table=%s err=%s", AGGREGATE_TABLE, exc)
        raise
    return written


def lambda_handler(event, context):
    """Entry point for the Kinesis telemetry aggregation stream."""
    records: List[Dict[str, Any]] = event.get("Records", [])
    logger.info("aggregation_started record_count=%s", len(records))

    windows, late_dropped = aggregate_samples(records)

    written = 0
    if windows:
        written = persist_windows(windows)

    logger.info("aggregation_complete windows=%s written=%s late_dropped=%s",
                len(windows), written, late_dropped)
    return {
        "records_received": len(records),
        "windows_emitted": len(windows),
        "items_written": written,
        "late_dropped": late_dropped,
    }
