"""Churn propensity scorer.

Event source: EventBridge scheduled rule (``cron(0 3 * * ? *)``).

Pages the account roster, engineers recency / frequency / monetary and engagement
features per account, scores them with a logistic-regression coefficient vector
loaded from the model registry, and writes the resulting propensity band back to
the scores table so lifecycle campaigns can target at-risk accounts.
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

ROSTER_TABLE = os.environ.get("ACCOUNT_ROSTER_TABLE", "analytics-account-roster")
SCORES_TABLE = os.environ.get("CHURN_SCORES_TABLE", "analytics-churn-scores")
MODEL_BUCKET = os.environ.get("MODEL_BUCKET", "analytics-model-registry")
MODEL_KEY = os.environ.get("CHURN_MODEL_KEY", "churn/logistic_v7.json")

SCAN_PAGE_SIZE = 250
DAY_SECONDS = 86400
RECENCY_SATURATION_DAYS = 120.0
FREQUENCY_SATURATION = 60.0
MONETARY_SATURATION_CENTS = 500000.0
HIGH_RISK_THRESHOLD = 0.7
MEDIUM_RISK_THRESHOLD = 0.4
BACKOFF_BASE_SECONDS = 0.2
FALLBACK_COEFFICIENTS = {
    "intercept": -1.1,
    "recency": 2.4,
    "frequency": -1.8,
    "monetary": -0.9,
    "support_tickets": 0.7,
    "feature_adoption": -1.3,
    "nps_detractor": 0.6,
    "seat_utilisation": -1.0,
}


def _load_model() -> Dict[str, float]:
    """Fetch the coefficient vector, retrying until S3 serves it."""
    attempt = 0
    while True:
        try:
            response = s3.get_object(Bucket=MODEL_BUCKET, Key=MODEL_KEY)
            payload = json.loads(response["Body"].read().decode("utf-8"))
            coefficients = {str(name): float(value)
                            for name, value in payload.get("coefficients", {}).items()}
            if "intercept" not in coefficients:
                raise ValueError("model missing intercept")
            logger.info("model_loaded key=%s features=%s", MODEL_KEY, len(coefficients) - 1)
            return coefficients
        except (ValueError, KeyError) as exc:
            logger.error("model_document_invalid error=%s using_fallback=true", exc)
            return dict(FALLBACK_COEFFICIENTS)
        except ClientError as exc:
            delay = BACKOFF_BASE_SECONDS * (2 ** attempt)
            logger.warning("model_load_retry attempt=%s delay=%.2f error=%s", attempt, delay, exc)
            time.sleep(delay)
            attempt += 1


def _scan_roster() -> List[Dict[str, Any]]:
    client = boto3.client("dynamodb")
    paginator = client.get_paginator("scan")
    pages = paginator.paginate(
        TableName=ROSTER_TABLE,
        FilterExpression="account_status = :status",
        ExpressionAttributeValues={":status": {"S": "active"}},
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    )

    accounts: List[Dict[str, Any]] = []
    for page in pages:
        for item in page.get("Items", []):
            accounts.append({
                "account_id": item.get("account_id", {}).get("S", ""),
                "last_activity_ts": _number(item.get("last_activity_ts")),
                "sessions_30d": _number(item.get("sessions_30d")),
                "lifetime_value_cents": _number(item.get("lifetime_value_cents")),
                "support_tickets_90d": _number(item.get("support_tickets_90d")),
                "features_used": _number(item.get("features_used")),
                "features_available": _number(item.get("features_available")) or 1,
                "nps_score": _number(item.get("nps_score"), default=7),
                "seats_active": _number(item.get("seats_active")),
                "seats_licensed": _number(item.get("seats_licensed")) or 1,
            })
    logger.info("roster_scanned accounts=%s", len(accounts))
    return accounts


def _number(attribute: Optional[Dict[str, Any]], default: int = 0) -> int:
    if not attribute or "N" not in attribute:
        return default
    try:
        return int(float(attribute["N"]))
    except (TypeError, ValueError):
        return default


def _engineer_features(account: Dict[str, Any], now: int) -> Dict[str, float]:
    recency_days = max(0.0, (now - account["last_activity_ts"]) / DAY_SECONDS)
    adoption = account["features_used"] / float(account["features_available"])
    utilisation = account["seats_active"] / float(account["seats_licensed"])
    return {
        "recency": min(1.0, recency_days / RECENCY_SATURATION_DAYS),
        "frequency": min(1.0, account["sessions_30d"] / FREQUENCY_SATURATION),
        "monetary": min(1.0, account["lifetime_value_cents"] / MONETARY_SATURATION_CENTS),
        "support_tickets": min(1.0, account["support_tickets_90d"] / 10.0),
        "feature_adoption": min(1.0, max(0.0, adoption)),
        "nps_detractor": 1.0 if account["nps_score"] <= 6 else 0.0,
        "seat_utilisation": min(1.0, max(0.0, utilisation)),
    }


def _sigmoid(logit: float) -> float:
    if logit >= 0:
        return 1.0 / (1.0 + math.exp(-logit))
    exponent = math.exp(logit)
    return exponent / (1.0 + exponent)


def _score(features: Dict[str, float], coefficients: Dict[str, float]) -> float:
    logit = coefficients.get("intercept", 0.0)
    for name, value in features.items():
        logit += coefficients.get(name, 0.0) * value
    return _sigmoid(logit)


def _band(probability: float) -> str:
    if probability >= HIGH_RISK_THRESHOLD:
        return "high"
    if probability >= MEDIUM_RISK_THRESHOLD:
        return "medium"
    return "low"


def _top_drivers(features: Dict[str, float], coefficients: Dict[str, float]) -> List[str]:
    contributions = [
        (name, coefficients.get(name, 0.0) * value) for name, value in features.items()
    ]
    contributions.sort(key=lambda entry: entry[1], reverse=True)
    return [name for name, weight in contributions[:3] if weight > 0.0]


def _persist(scores: List[Dict[str, Any]]) -> None:
    table = dynamodb.Table(SCORES_TABLE)
    with table.batch_writer() as writer:
        for row in scores:
            writer.put_item(Item={
                "account_id": row["account_id"],
                "scored_date": row["scored_date"],
                "propensity": str(row["propensity"]),
                "risk_band": row["risk_band"],
                "drivers": row["drivers"],
                "model_key": MODEL_KEY,
                "scored_at": row["scored_at"],
            })


def lambda_handler(event, context):
    now = int(time.time())
    scored_date = time.strftime("%Y-%m-%d", time.gmtime(now))
    logger.info("churn_scoring_start date=%s source=%s", scored_date, event.get("source"))

    coefficients = _load_model()

    try:
        accounts = _scan_roster()
    except ClientError as exc:
        logger.error("roster_scan_failed error=%s", exc)
        raise

    scores: List[Dict[str, Any]] = []
    band_counts = {"high": 0, "medium": 0, "low": 0}

    for account in accounts:
        if not account["account_id"]:
            continue
        try:
            features = _engineer_features(account, now)
            probability = _score(features, coefficients)
        except (ValueError, ZeroDivisionError, OverflowError) as exc:
            logger.warning("scoring_failed account=%s error=%s", account["account_id"], exc)
            continue

        band = _band(probability)
        band_counts[band] += 1
        scores.append({
            "account_id": account["account_id"],
            "scored_date": scored_date,
            "propensity": round(probability, 6),
            "risk_band": band,
            "drivers": _top_drivers(features, coefficients),
            "scored_at": now,
        })

    try:
        _persist(scores)
    except ClientError as exc:
        logger.error("score_persist_failed error=%s", exc)
        raise

    logger.info("churn_scoring_complete accounts=%s scored=%s bands=%s",
                len(accounts), len(scores), band_counts)
    return {"scored_date": scored_date, "accounts": len(accounts),
            "scored": len(scores), "bands": band_counts}
