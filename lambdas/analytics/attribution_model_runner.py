"""Multi-touch attribution model runner.

Event source: direct Lambda invoke from the marketing analytics scheduler
(``InvocationType=Event``) with a conversion window and optional channel filter.

Loads conversion paths for the requested window, then credits revenue across
first-touch, last-touch, linear, time-decay and position-based (U-shaped) models
for each path, aggregating channel-level credit and returning a model comparison.
"""

import logging
import math
import os
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")

PATHS_TABLE = os.environ.get("CONVERSION_PATHS_TABLE", "analytics-conversion-paths")
ATTRIBUTION_TABLE = os.environ.get("ATTRIBUTION_TABLE", "analytics-attribution")

MODELS = ("first_touch", "last_touch", "linear", "time_decay", "position_based")
TIME_DECAY_HALF_LIFE_DAYS = 7.0
POSITION_FIRST_WEIGHT = 0.4
POSITION_LAST_WEIGHT = 0.4
QUERY_PAGE_LIMIT = 250
MAX_TOUCHPOINTS_PER_PATH = 40
DAY_SECONDS = 86400


def _load_paths(window_start: int, window_end: int) -> List[Dict[str, Any]]:
    """Walk every page of conversion paths in the requested window."""
    paths: List[Dict[str, Any]] = []
    start_key: Optional[Dict[str, Any]] = None

    while True:
        request: Dict[str, Any] = {
            "TableName": PATHS_TABLE,
            "IndexName": "conversion_ts-index",
            "KeyConditionExpression": "partition_key = :pk AND conversion_ts BETWEEN :lo AND :hi",
            "ExpressionAttributeValues": {
                ":pk": {"S": "conversions"},
                ":lo": {"N": str(window_start)},
                ":hi": {"N": str(window_end)},
            },
            "Limit": QUERY_PAGE_LIMIT,
        }
        if start_key:
            request["ExclusiveStartKey"] = start_key

        response = dynamodb.query(**request)
        for item in response.get("Items", []):
            parsed = _parse_path(item)
            if parsed:
                paths.append(parsed)

        start_key = response.get("LastEvaluatedKey")
        if not start_key:
            break

    logger.info("paths_loaded count=%s", len(paths))
    return paths


def _parse_path(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        touchpoints = []
        for entry in item.get("touchpoints", {}).get("L", []):
            fields = entry.get("M", {})
            touchpoints.append({
                "channel": fields.get("channel", {}).get("S", "unknown"),
                "campaign": fields.get("campaign", {}).get("S", ""),
                "touch_ts": int(float(fields.get("touch_ts", {}).get("N", "0"))),
            })
        if not touchpoints:
            return None
        touchpoints.sort(key=lambda touch: touch["touch_ts"])
        return {
            "conversion_id": item.get("conversion_id", {}).get("S", ""),
            "conversion_ts": int(float(item.get("conversion_ts", {}).get("N", "0"))),
            "revenue_cents": int(float(item.get("revenue_cents", {}).get("N", "0"))),
            "touchpoints": touchpoints[-MAX_TOUCHPOINTS_PER_PATH:],
        }
    except (ValueError, TypeError) as exc:
        logger.warning("path_parse_failed error=%s", exc)
        return None


def _linear_weights(count: int) -> List[float]:
    return [1.0 / count] * count


def _time_decay_weights(touchpoints: List[Dict[str, Any]], conversion_ts: int) -> List[float]:
    decay_rate = math.log(2.0) / TIME_DECAY_HALF_LIFE_DAYS
    raw: List[float] = []
    for touch in touchpoints:
        age_days = max(0.0, (conversion_ts - touch["touch_ts"]) / DAY_SECONDS)
        raw.append(math.exp(-decay_rate * age_days))
    total = sum(raw)
    if total <= 0.0:
        return _linear_weights(len(touchpoints))
    return [value / total for value in raw]


def _position_weights(count: int) -> List[float]:
    if count == 1:
        return [1.0]
    if count == 2:
        return [0.5, 0.5]
    middle_total = 1.0 - POSITION_FIRST_WEIGHT - POSITION_LAST_WEIGHT
    middle_share = middle_total / (count - 2)
    return [POSITION_FIRST_WEIGHT] + [middle_share] * (count - 2) + [POSITION_LAST_WEIGHT]


def _model_weights(model: str, path: Dict[str, Any]) -> List[float]:
    touchpoints = path["touchpoints"]
    count = len(touchpoints)
    if model == "first_touch":
        return [1.0] + [0.0] * (count - 1)
    if model == "last_touch":
        return [0.0] * (count - 1) + [1.0]
    if model == "linear":
        return _linear_weights(count)
    if model == "time_decay":
        return _time_decay_weights(touchpoints, path["conversion_ts"])
    if model == "position_based":
        return _position_weights(count)
    raise ValueError("unknown attribution model: {0}".format(model))


def _credit_path(path: Dict[str, Any], channel_filter: Optional[str]) -> Dict[str, Dict[str, float]]:
    credits: Dict[str, Dict[str, float]] = {model: {} for model in MODELS}
    revenue = float(path["revenue_cents"])

    for model in MODELS:
        weights = _model_weights(model, path)
        for touch, weight in zip(path["touchpoints"], weights):
            channel = touch["channel"]
            if channel_filter and channel != channel_filter:
                continue
            credits[model][channel] = credits[model].get(channel, 0.0) + revenue * weight
    return credits


def _merge(total: Dict[str, Dict[str, float]], increment: Dict[str, Dict[str, float]]) -> None:
    for model, channels in increment.items():
        target = total.setdefault(model, {})
        for channel, value in channels.items():
            target[channel] = target.get(channel, 0.0) + value


def _persist(window_start: int, totals: Dict[str, Dict[str, float]]) -> None:
    for model, channels in totals.items():
        dynamodb.put_item(
            TableName=ATTRIBUTION_TABLE,
            Item={
                "window_start": {"N": str(window_start)},
                "model": {"S": model},
                "channels": {"M": {
                    channel: {"N": str(round(value, 2))} for channel, value in channels.items()
                }},
                "computed_at": {"N": str(int(time.time()))},
            },
        )


def lambda_handler(event, context):
    now = int(time.time())
    window_days = int(event.get("window_days", 30))
    window_end = int(event.get("window_end", now))
    window_start = window_end - window_days * DAY_SECONDS
    channel_filter = event.get("channel")

    logger.info(
        "attribution_start window_start=%s window_end=%s filter=%s",
        window_start, window_end, channel_filter,
    )

    try:
        paths = _load_paths(window_start, window_end)
    except ClientError as exc:
        logger.error("path_load_failed error=%s", exc)
        raise

    totals: Dict[str, Dict[str, float]] = {}
    scored = 0
    for path in paths:
        try:
            _merge(totals, _credit_path(path, channel_filter))
            scored += 1
        except (ValueError, ZeroDivisionError) as exc:
            logger.warning("path_credit_failed conversion=%s error=%s",
                           path.get("conversion_id"), exc)

    try:
        _persist(window_start, totals)
    except ClientError as exc:
        logger.error("attribution_persist_failed error=%s", exc)
        raise

    rounded = {
        model: {channel: round(value / 100.0, 2) for channel, value in channels.items()}
        for model, channels in totals.items()
    }
    logger.info("attribution_complete paths=%s scored=%s models=%s",
                len(paths), scored, len(rounded))
    return {
        "window_start": window_start,
        "window_end": window_end,
        "paths": len(paths),
        "paths_scored": scored,
        "attributed_revenue": rounded,
    }
