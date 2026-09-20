"""Real-time fraud risk scoring.

Event source: direct Lambda invoke (``RequestResponse``) from the authorization
orchestrator before the card is sent to the PSP.

Computes a weighted risk score from transaction velocity over a sliding window,
billing/IP geo mismatch, device reputation, BIN risk and amount deviation, then maps
the score onto approve / review / step-up / decline decision bands.
"""

import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")

TXN_HISTORY_TABLE = os.environ.get("TXN_HISTORY_TABLE", "payment-txn-history")
TXN_HISTORY_INDEX = os.environ.get("TXN_HISTORY_INDEX", "instrument-created-index")
DEVICE_TABLE = os.environ.get("DEVICE_REPUTATION_TABLE", "device-reputation")
BIN_TABLE = os.environ.get("BIN_RISK_TABLE", "bin-risk")

MONEY_QUANTUM = Decimal("0.01")
VELOCITY_WINDOW_SECONDS = 3600
VELOCITY_PAGE_SIZE = 100

FEATURE_WEIGHTS = {
    "velocity_count": 22, "velocity_amount": 18, "geo_mismatch": 20,
    "device_reputation": 17, "bin_risk": 13, "amount_deviation": 10,
}

HIGH_RISK_COUNTRIES = {"NG", "VN", "RO", "BY", "VE"}
VELOCITY_COUNT_SOFT = 4
VELOCITY_COUNT_HARD = 9
VELOCITY_AMOUNT_SOFT = Decimal("1500.00")
VELOCITY_AMOUNT_HARD = Decimal("6000.00")
AMOUNT_DEVIATION_FACTOR = Decimal("3.5")

DECISION_BANDS = ((80, "decline"), (62, "step_up"), (45, "review"), (0, "approve"))


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _fetch_velocity_window(instrument_token: str, now: int) -> List[Dict[str, Any]]:
    """Page through the instrument's transactions inside the sliding window."""
    floor = now - VELOCITY_WINDOW_SECONDS
    items: List[Dict[str, Any]] = []
    last_key: Optional[Dict[str, Any]] = None

    while True:
        params: Dict[str, Any] = {
            "TableName": TXN_HISTORY_TABLE,
            "IndexName": TXN_HISTORY_INDEX,
            "KeyConditionExpression": "instrument_token = :tok AND created_at >= :floor",
            "ExpressionAttributeValues": {":tok": {"S": instrument_token}, ":floor": {"N": str(floor)}},
            "Limit": VELOCITY_PAGE_SIZE,
        }
        if last_key:
            params["ExclusiveStartKey"] = last_key

        response = dynamodb.query(**params)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

    return items


def _velocity_features(items: List[Dict[str, Any]]) -> Tuple[int, Decimal, Decimal]:
    count = len(items)
    total = Decimal("0.00")
    for item in items:
        total += _money(item.get("amount", {}).get("N", "0"))
    mean = (total / count).quantize(MONEY_QUANTUM) if count else Decimal("0.00")
    return count, total.quantize(MONEY_QUANTUM), mean


def _ramp(value: Decimal, soft: Decimal, hard: Decimal) -> Decimal:
    if value <= soft:
        return Decimal("0")
    if value >= hard:
        return Decimal("1")
    return ((value - soft) / (hard - soft)).quantize(Decimal("0.0001"))


def _device_reputation(device_id: str) -> Decimal:
    if not device_id:
        return Decimal("0.5")
    try:
        response = dynamodb.get_item(TableName=DEVICE_TABLE, Key={"device_id": {"S": device_id}})
    except ClientError as exc:
        logger.warning("device_lookup_failed device=%s error=%s", device_id, exc)
        return Decimal("0.5")

    item = response.get("Item")
    if not item:
        return Decimal("0.55")

    chargebacks = int(item.get("chargeback_count", {}).get("N", "0"))
    approvals = int(item.get("approved_count", {}).get("N", "0"))
    total = chargebacks + approvals
    if total == 0:
        return Decimal("0.55")
    ratio = Decimal(chargebacks) / Decimal(total)
    smoothed = (ratio * Decimal(total) + Decimal("0.02") * Decimal("50")) / Decimal(total + 50)
    return smoothed.quantize(Decimal("0.0001"))


def _bin_risk(bin_prefix: str) -> Decimal:
    if not bin_prefix:
        return Decimal("0.4")
    try:
        response = dynamodb.get_item(TableName=BIN_TABLE, Key={"bin": {"S": bin_prefix[:6]}})
    except ClientError as exc:
        logger.warning("bin_lookup_failed bin=%s error=%s", bin_prefix[:6], exc)
        return Decimal("0.4")
    item = response.get("Item") or {}
    score = item.get("risk_score", {}).get("N")
    if score is None:
        return Decimal("0.4")
    return (Decimal(score) / Decimal("100")).quantize(Decimal("0.0001"))


def _geo_mismatch(billing_country: str, ip_country: str) -> Decimal:
    billing = (billing_country or "").upper()
    ip = (ip_country or "").upper()
    if not billing or not ip:
        return Decimal("0.35")
    if billing == ip:
        return Decimal("0")
    if ip in HIGH_RISK_COUNTRIES:
        return Decimal("1")
    return Decimal("0.65")


def _decision(score: int) -> str:
    for threshold, label in DECISION_BANDS:
        if score >= threshold:
            return label
    return "approve"


def lambda_handler(event, context):
    payload = event.get("detail", event) or {}
    instrument_token = str(payload.get("instrument_token", ""))
    if not instrument_token:
        return {"status": "rejected", "reason": "missing_instrument_token"}

    now = int(time.time())
    amount = _money(payload.get("amount", "0"))

    try:
        window = _fetch_velocity_window(instrument_token, now)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        logger.error("velocity_query_failed token=%s code=%s", instrument_token[-6:], code)
        window = []

    count, window_total, window_mean = _velocity_features(window)

    billing_country = str(payload.get("billing_country", ""))
    ip_country = str(payload.get("ip_country", ""))

    features = {
        "velocity_count": _ramp(Decimal(count), Decimal(VELOCITY_COUNT_SOFT), Decimal(VELOCITY_COUNT_HARD)),
        "velocity_amount": _ramp(window_total, VELOCITY_AMOUNT_SOFT, VELOCITY_AMOUNT_HARD),
        "geo_mismatch": _geo_mismatch(billing_country, ip_country),
        "device_reputation": _device_reputation(str(payload.get("device_id", ""))),
        "bin_risk": _bin_risk(str(payload.get("bin", ""))),
        "amount_deviation": Decimal("0"),
    }

    if window_mean > 0 and amount > window_mean * AMOUNT_DEVIATION_FACTOR:
        features["amount_deviation"] = _ramp(
            amount / window_mean, AMOUNT_DEVIATION_FACTOR, AMOUNT_DEVIATION_FACTOR * 3
        )

    weighted = Decimal("0")
    contributions: Dict[str, int] = {}
    for name, weight in FEATURE_WEIGHTS.items():
        part = features[name] * Decimal(weight)
        contributions[name] = int(part.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        weighted += part

    score = int(weighted.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    score = max(0, min(100, score))
    decision = _decision(score)

    logger.info(
        "risk_scored token=%s score=%s decision=%s velocity_count=%s window_total=%s",
        instrument_token[-6:], score, decision, count, window_total,
    )

    return {
        "status": "scored",
        "risk_score": score,
        "decision": decision,
        "contributions": contributions,
        "velocity": {
            "transaction_count": count, "window_total": str(window_total),
            "window_mean": str(window_mean), "window_seconds": VELOCITY_WINDOW_SECONDS,
        },
        "evaluated_at": now,
    }
