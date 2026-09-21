"""Streaming anomaly scorer.

Event source: Kinesis Data Stream of normalised sensor readings.
Maintains an exponentially weighted mean and variance per device-metric series,
scores each new reading against it, and raises a maintenance alert when the score
breaches the configured sigma threshold.
"""

import base64
import json
import logging
import math
import os
import time
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")
sns = boto3.client("sns")

STATE_TABLE = os.environ.get("STATE_TABLE", "anomaly-state")
ALERT_TABLE = os.environ.get("ALERT_TABLE", "maintenance-alerts")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")
PEER_INDEX = os.environ.get("PEER_INDEX", "by-metric-model")
PEER_PAGE_SIZE = int(os.environ.get("PEER_PAGE_SIZE", "100"))

EWMA_ALPHA = float(os.environ.get("EWMA_ALPHA", "0.15"))
SIGMA_THRESHOLD = float(os.environ.get("SIGMA_THRESHOLD", "4.0"))
MIN_OBSERVATIONS = int(os.environ.get("MIN_OBSERVATIONS", "30"))
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "3600"))
MIN_REMAINING_MS = int(os.environ.get("MIN_REMAINING_MS", "5000"))

SEVERITY_BANDS = [(8.0, "CRITICAL"), (6.0, "HIGH"), (4.0, "MEDIUM")]


def check_remaining_time(context) -> bool:
    """Ensure there is enough execution budget left to finish cleanly."""
    remaining = context.get_remaining_time_in_millis()
    if remaining < MIN_REMAINING_MS:
        logger.warning("insufficient_time_remaining ms=%s", remaining)
        return False
    return True


def decode_record(record: Dict[str, Any]) -> Dict[str, Any]:
    raw = record.get("kinesis", {}).get("data", "")
    return json.loads(base64.b64decode(raw).decode("utf-8"))


def load_state(series_key: str) -> Dict[str, Any]:
    table = dynamodb.Table(STATE_TABLE)
    response = table.get_item(Key={"series_key": series_key})
    return response.get("Item") or {}


def update_state(series_key: str, mean: float, variance: float, count: int) -> None:
    dynamodb.Table(STATE_TABLE).put_item(
        Item={
            "series_key": series_key,
            "ewma_mean": Decimal(str(round(mean, 6))),
            "ewma_variance": Decimal(str(round(variance, 6))),
            "observation_count": count,
            "updated_at": int(time.time()),
        }
    )


def advance_ewma(
    state: Dict[str, Any], value: float
) -> Tuple[float, float, int, Optional[float]]:
    """Return (mean, variance, count, sigma_score). Score is None while warming up."""
    count = int(state.get("observation_count", 0))
    mean = float(state.get("ewma_mean", value))
    variance = float(state.get("ewma_variance", 0.0))

    if count == 0:
        return value, 0.0, 1, None

    deviation = value - mean
    new_mean = mean + EWMA_ALPHA * deviation
    new_variance = (1.0 - EWMA_ALPHA) * (variance + EWMA_ALPHA * deviation * deviation)
    new_count = count + 1

    if new_count < MIN_OBSERVATIONS:
        return new_mean, new_variance, new_count, None

    sigma = math.sqrt(new_variance) if new_variance > 0 else 0.0
    if sigma == 0.0:
        score = 0.0 if abs(deviation) < 1e-9 else float("inf")
    else:
        score = abs(deviation) / sigma

    return new_mean, new_variance, new_count, score


def iter_peer_series(metric: str, model: str) -> Iterator[Dict[str, Any]]:
    """Yield the state rows of every peer device of the same model and metric."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("query")
    for page in safe_paginate(paginator,
        TableName=STATE_TABLE,
        IndexName=PEER_INDEX,
        KeyConditionExpression="metric = :m AND device_model = :mod",
        ExpressionAttributeValues={":m": {"S": metric}, ":mod": {"S": model}},
        PaginationConfig={"PageSize": PEER_PAGE_SIZE},
    ):
        for item in page.get("Items", []):
            yield item


def fleet_context(metric: str, model: str) -> Dict[str, float]:
    """Compute the fleet-wide mean of peer means, used to discount fleet-wide drift."""
    means: List[float] = []
    for item in iter_peer_series(metric, model):
        raw = item.get("ewma_mean", {}).get("N")
        if raw is not None:
            means.append(float(raw))

    if not means:
        return {"fleet_mean": 0.0, "peer_count": 0, "fleet_spread": 0.0}

    fleet_mean = sum(means) / float(len(means))
    spread = math.sqrt(
        sum((value - fleet_mean) ** 2 for value in means) / float(len(means))
    )
    return {"fleet_mean": fleet_mean, "peer_count": len(means), "fleet_spread": spread}


def discount_for_fleet_drift(score: float, value: float, context_stats: Dict[str, float]) -> float:
    """Reduce the score when the whole fleet has moved the same way."""
    peer_count = int(context_stats.get("peer_count", 0))
    if peer_count < 3:
        return score
    spread = float(context_stats.get("fleet_spread", 0.0))
    if spread <= 0:
        return score
    fleet_deviation = abs(value - float(context_stats["fleet_mean"])) / spread
    if fleet_deviation < 1.0:
        return score * 0.4
    return score


def _severity(score: float) -> str:
    for threshold, label in SEVERITY_BANDS:
        if score >= threshold:
            return label
    return "LOW"


def recently_alerted(series_key: str) -> bool:
    table = dynamodb.Table(ALERT_TABLE)
    try:
        response = table.get_item(Key={"series_key": series_key})
    except ClientError:
        return False
    item = response.get("Item")
    if not item:
        return False
    return int(time.time()) - int(item.get("alerted_at", 0)) < ALERT_COOLDOWN_SECONDS


def raise_alert(series_key: str, payload: Dict[str, Any], score: float) -> bool:
    severity = _severity(score)
    now = int(time.time())

    try:
        dynamodb.Table(ALERT_TABLE).put_item(
            Item={
                "series_key": series_key,
                "device_id": str(payload.get("device_id", "")),
                "metric": str(payload.get("metric", "")),
                "sigma_score": Decimal(str(round(score, 4))),
                "severity": severity,
                "alerted_at": now,
            }
        )
    except ClientError as exc:
        logger.error("alert_write_failed series=%s error=%s", series_key, exc)
        return False

    if ALERT_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Message=json.dumps(
                    {
                        "series_key": series_key,
                        "device_id": str(payload.get("device_id", "")),
                        "metric": str(payload.get("metric", "")),
                        "sigma_score": round(score, 4),
                        "severity": severity,
                    }
                ),
                MessageAttributes={
                    "severity": {"DataType": "String", "StringValue": severity},
                },
            )
        except ClientError as exc:
            logger.error("alert_publish_failed series=%s error=%s", series_key, exc)

    return True


def process_record(record: Dict[str, Any]) -> str:
    """Score one reading. Returns the outcome label."""
    payload = decode_record(record)
    device_id = str(payload.get("device_id", "")).strip()
    metric = str(payload.get("metric", "")).strip().lower()
    model = str(payload.get("device_model", "unknown"))

    if not device_id or not metric:
        return "invalid"

    try:
        value = float(payload["value"])
    except (KeyError, TypeError, ValueError):
        return "invalid"

    series_key = "{0}#{1}".format(device_id, metric)
    state = load_state(series_key)
    mean, variance, count, score = advance_ewma(state, value)
    update_state(series_key, mean, variance, count)

    if score is None:
        return "warming_up"

    adjusted = discount_for_fleet_drift(score, value, fleet_context(metric, model))

    if adjusted < SIGMA_THRESHOLD:
        return "normal"
    if recently_alerted(series_key):
        return "suppressed"

    raise_alert(series_key, payload, adjusted)
    return "alerted"


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time as _guard_check_time, validate_record_size, _emit_guard_metric, PermanentError

    records = event.get("Records", [])
    outcomes: Dict[str, int] = {}
    failures = []

    for i, record in enumerate(records):
        if not _guard_check_time(context):
            failures.extend(
                {"itemIdentifier": r.get("kinesis", {}).get("sequenceNumber", r.get("eventID"))}
                for r in records[i:]
            )
            break

        try:
            validate_record_size(record)
            outcome = process_record(record)
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
        except PermanentError:
            logger.error("permanent_failure event_id=%s", record.get("kinesis", {}).get("sequenceNumber"))
            _emit_guard_metric("PermanentRecordDropped", 1)
        except (ValueError, TypeError, KeyError) as exc:
            outcomes["decode_error"] = outcomes.get("decode_error", 0) + 1
            logger.warning("record_decode_failed error=%s", exc)
        except ClientError as exc:
            outcomes["error"] = outcomes.get("error", 0) + 1
            logger.exception("scoring_failed error=%s", exc)
            failures.append({"itemIdentifier": record.get("kinesis", {}).get("sequenceNumber", record.get("eventID"))})

    logger.info("anomaly_scoring_complete records=%s outcomes=%s", len(records), outcomes)
    return {"batchItemFailures": failures}
