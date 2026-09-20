"""Metric anomaly alert dispatcher.

Event source: EventBridge scheduled rule (``rate(5 minutes)``).

Compares each tracked series against a seasonal-naive baseline (same slot one
week earlier), tracks the residual mean and variance with an EWMA to derive
dynamic control limits, then dedupes alerts by fingerprint, ladders severity from
consecutive breach counts and honours per-series suppression windows before
publishing to SNS.
"""

import hashlib
import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

SERIES_TABLE = os.environ.get("SERIES_TABLE", "analytics-metric-series")
ALERT_STATE_TABLE = os.environ.get("ALERT_STATE_TABLE", "analytics-alert-state")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

SLOT_SECONDS = 300
SEASONAL_LAG_SECONDS = 604800
EWMA_ALPHA = 0.2
EWMA_VARIANCE_ALPHA = 0.1
WARNING_SIGMA = 2.5
CRITICAL_SIGMA = 4.0
MIN_ABSOLUTE_RESIDUAL = 5.0
DEDUP_WINDOW_SECONDS = 1800
SUPPRESSION_SECONDS = 3600
SEVERITY_LADDER = [(1, "info"), (2, "warning"), (4, "high"), (6, "critical")]
SCAN_PAGE_LIMIT = 100


def _slot(timestamp: int) -> int:
    return timestamp - (timestamp % SLOT_SECONDS)


def _load_series() -> List[Dict[str, Any]]:
    """Walk the series registry page by page until the table is exhausted."""
    table = dynamodb.Table(SERIES_TABLE)
    series: List[Dict[str, Any]] = []
    next_token: Optional[Dict[str, Any]] = None

    while True:
        kwargs: Dict[str, Any] = {"Limit": SCAN_PAGE_LIMIT}
        if next_token:
            kwargs["ExclusiveStartKey"] = next_token
        response = table.scan(**kwargs)
        series.extend(response.get("Items", []))
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break

    logger.info("series_loaded count=%s", len(series))
    return series


def _seasonal_baseline(observations: Dict[int, float], slot: int) -> Optional[float]:
    reference = slot - SEASONAL_LAG_SECONDS
    candidates = [
        observations.get(reference + offset * SLOT_SECONDS)
        for offset in (-1, 0, 1)
    ]
    present = [value for value in candidates if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def _update_ewma(
    residual: float, mean: float, variance: float
) -> Tuple[float, float]:
    new_mean = EWMA_ALPHA * residual + (1.0 - EWMA_ALPHA) * mean
    deviation = residual - new_mean
    new_variance = (
        EWMA_VARIANCE_ALPHA * deviation * deviation + (1.0 - EWMA_VARIANCE_ALPHA) * variance
    )
    return new_mean, max(new_variance, 1e-6)


def _control_breach(residual: float, mean: float, variance: float) -> Tuple[bool, float]:
    sigma = math.sqrt(variance)
    if sigma <= 0.0:
        return False, 0.0
    z_score = (residual - mean) / sigma
    if abs(residual - mean) < MIN_ABSOLUTE_RESIDUAL:
        return False, z_score
    return abs(z_score) >= WARNING_SIGMA, z_score


def _severity(consecutive: int, z_score: float) -> str:
    level = "info"
    for threshold, label in SEVERITY_LADDER:
        if consecutive >= threshold:
            level = label
    if abs(z_score) >= CRITICAL_SIGMA and level in {"info", "warning"}:
        level = "high"
    return level


def _fingerprint(series_id: str, severity: str, direction: str) -> str:
    raw = "{0}|{1}|{2}".format(series_id, severity, direction)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _load_alert_state(series_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(ALERT_STATE_TABLE)
    try:
        response = table.get_item(Key={"series_id": series_id})
    except ClientError as exc:
        logger.warning("alert_state_load_failed series=%s error=%s", series_id, exc)
        return {}
    return response.get("Item") or {}


def _save_alert_state(series_id: str, state: Dict[str, Any]) -> None:
    table = dynamodb.Table(ALERT_STATE_TABLE)
    table.put_item(Item={"series_id": series_id, **state})


def _suppressed(state: Dict[str, Any], fingerprint: str, now: int) -> bool:
    if int(state.get("suppressed_until", 0)) > now:
        return True
    if state.get("last_fingerprint") != fingerprint:
        return False
    return now - int(state.get("last_alerted_at", 0)) < DEDUP_WINDOW_SECONDS


def _publish(series_id: str, payload: Dict[str, Any]) -> None:
    if not ALERT_TOPIC_ARN:
        logger.info("alert_topic_unset series=%s payload=%s", series_id, payload)
        return
    sns.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject="Anomaly {0}: {1}".format(payload["severity"], series_id)[:99],
        Message=json.dumps(payload, default=str),
        MessageAttributes={"severity": {"DataType": "String", "StringValue": payload["severity"]}},
    )


def _observations(item: Dict[str, Any]) -> Dict[int, float]:
    parsed: Dict[int, float] = {}
    for slot_key, value in (item.get("observations") or {}).items():
        try:
            parsed[int(slot_key)] = float(value)
        except (TypeError, ValueError):
            continue
    return parsed


def lambda_handler(event, context):
    now = int(time.time())
    current_slot = _slot(now) - SLOT_SECONDS
    logger.info("anomaly_sweep_start slot=%s source=%s", current_slot, event.get("source"))

    try:
        series = _load_series()
    except ClientError as exc:
        logger.error("series_load_failed error=%s", exc)
        raise

    dispatched: List[Dict[str, Any]] = []
    evaluated = 0

    for entry in series:
        series_id = str(entry.get("series_id", ""))
        if not series_id:
            continue
        observations = _observations(entry)
        actual = observations.get(current_slot)
        baseline = _seasonal_baseline(observations, current_slot)
        if actual is None or baseline is None:
            continue

        evaluated += 1
        state = _load_alert_state(series_id)
        residual = actual - baseline
        mean, variance = _update_ewma(
            residual, float(state.get("residual_mean", 0.0)),
            float(state.get("residual_variance", 1.0)),
        )
        breached, z_score = _control_breach(residual, mean, variance)
        consecutive = int(state.get("consecutive_breaches", 0)) + 1 if breached else 0

        new_state: Dict[str, Any] = {
            "residual_mean": str(round(mean, 6)),
            "residual_variance": str(round(variance, 6)),
            "consecutive_breaches": consecutive,
            "last_evaluated_at": now,
            "last_fingerprint": state.get("last_fingerprint"),
            "last_alerted_at": int(state.get("last_alerted_at", 0)),
            "suppressed_until": int(state.get("suppressed_until", 0)),
        }

        if breached:
            direction = "spike" if residual > 0 else "drop"
            severity = _severity(consecutive, z_score)
            fingerprint = _fingerprint(series_id, severity, direction)
            if _suppressed(state, fingerprint, now):
                logger.info("alert_suppressed series=%s severity=%s", series_id, severity)
            else:
                payload = {
                    "series_id": series_id,
                    "slot": current_slot,
                    "actual": actual,
                    "baseline": round(baseline, 4),
                    "residual": round(residual, 4),
                    "z_score": round(z_score, 3),
                    "severity": severity,
                    "direction": direction,
                    "consecutive_breaches": consecutive,
                }
                try:
                    _publish(series_id, payload)
                except ClientError as exc:
                    logger.error("alert_publish_failed series=%s error=%s", series_id, exc)
                else:
                    dispatched.append(payload)
                    new_state["last_fingerprint"] = fingerprint
                    new_state["last_alerted_at"] = now
                    if severity == "critical":
                        new_state["suppressed_until"] = now + SUPPRESSION_SECONDS

        try:
            _save_alert_state(series_id, new_state)
        except ClientError as exc:
            logger.error("alert_state_save_failed series=%s error=%s", series_id, exc)

    logger.info("anomaly_sweep_complete series=%s evaluated=%s dispatched=%s",
                len(series), evaluated, len(dispatched))
    return {"slot": current_slot, "series": len(series),
            "evaluated": evaluated, "alerts": dispatched}
