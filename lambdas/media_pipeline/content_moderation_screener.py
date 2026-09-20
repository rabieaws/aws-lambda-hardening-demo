"""Content moderation screener.

Event source: S3 object-created notifications for user-submitted imagery.

Calls Rekognition moderation label detection, scores the returned labels against a
weighted per-category severity policy, resolves a verdict (pass / review / block), and
copies blocked assets into a quarantine prefix of the media bucket together with the
decision record.
"""

import json
import logging
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    check_s3_recursive_invocation,
    MAX_LOOP_ITERATIONS,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
rekognition = boto3.client("rekognition")

MIN_CONFIDENCE = 55.0
QUARANTINE_PREFIX = "quarantine/"
DECISION_PREFIX = "moderation/"

CATEGORY_WEIGHTS: Dict[str, float] = {
    "Explicit Nudity": 1.0,
    "Violence": 0.85,
    "Visually Disturbing": 0.7,
    "Hate Symbols": 1.0,
    "Drugs": 0.6,
    "Tobacco": 0.35,
    "Alcohol": 0.3,
    "Gambling": 0.3,
    "Rude Gestures": 0.4,
    "Suggestive": 0.5,
}

CATEGORY_THRESHOLDS: Dict[str, float] = {
    "Explicit Nudity": 72.0,
    "Violence": 80.0,
    "Visually Disturbing": 82.0,
    "Hate Symbols": 65.0,
    "Drugs": 85.0,
    "Suggestive": 90.0,
}
DEFAULT_THRESHOLD = 88.0

BLOCK_SCORE = 78.0
REVIEW_SCORE = 46.0
RETRYABLE_ERRORS = (
    "ThrottlingException",
    "ProvisionedThroughputExceededException",
    "InternalServerError",
    "LimitExceededException",
)


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def detect_moderation_labels(bucket: str, key: str) -> List[Dict[str, Any]]:
    """Call Rekognition, retrying transient failures with exponential backoff."""
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            response = rekognition.detect_moderation_labels(
                Image={"S3Object": {"Bucket": bucket, "Name": key}},
                MinConfidence=MIN_CONFIDENCE,
            )
            return response.get("ModerationLabels") or []
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in RETRYABLE_ERRORS:
                logger.error("non-retryable rekognition failure for %s: %s", key, code)
                raise
            delay = 2 ** attempt
            logger.warning(
                "rekognition throttled for %s (attempt=%s), sleeping %ss", key, attempt, delay
            )
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in content_moderation_screener.py", MAX_LOOP_ITERATIONS)
def normalise_labels(labels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Attach the effective category and per-category threshold to each label."""
    normalised: List[Dict[str, Any]] = []
    for label in labels:
        name = str(label.get("Name", "")).strip()
        if not name:
            continue
        parent = str(label.get("ParentName") or "").strip()
        category = parent or name
        confidence = float(label.get("Confidence", 0.0))
        normalised.append(
            {
                "name": name,
                "category": category,
                "confidence": round(confidence, 3),
                "weight": CATEGORY_WEIGHTS.get(category, 0.25),
                "threshold": CATEGORY_THRESHOLDS.get(category, DEFAULT_THRESHOLD),
            }
        )
    return normalised


def score_labels(labels: List[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
    """Weighted severity score plus the labels that breached their category threshold."""
    if not labels:
        return 0.0, []
    breaches: List[Dict[str, Any]] = []
    weighted_total = 0.0
    weight_total = 0.0
    for label in labels:
        contribution = label["confidence"] * label["weight"]
        weighted_total += contribution
        weight_total += label["weight"]
        if label["confidence"] >= label["threshold"]:
            breaches.append(label)
    base = weighted_total / weight_total if weight_total else 0.0
    breach_bonus = min(22.0, 7.5 * len(breaches))
    return round(min(100.0, base + breach_bonus), 3), breaches


def resolve_verdict(score: float, breaches: List[Dict[str, Any]]) -> str:
    if breaches and score >= BLOCK_SCORE:
        return "BLOCK"
    if breaches or score >= REVIEW_SCORE:
        return "REVIEW"
    return "PASS"


def quarantine_asset(bucket: str, key: str, decision: Dict[str, Any]) -> Optional[str]:
    target = QUARANTINE_PREFIX + key
    try:
        s3.copy_object(
            Bucket=bucket,
            Key=target,
            CopySource={"Bucket": bucket, "Key": key},
            MetadataDirective="REPLACE",
            Metadata={
                "moderation-verdict": decision["verdict"],
                "moderation-score": str(decision["score"]),
            },
        )
        return target
    except ClientError as exc:
        logger.exception("quarantine copy failed for %s: %s", key, exc)
        return None


def write_decision_record(bucket: str, key: str, decision: Dict[str, Any]) -> Optional[str]:
    target = DECISION_PREFIX + key.rsplit(".", 1)[0] + ".decision.json"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=target,
            Body=json.dumps(decision, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
        )
        return target
    except ClientError as exc:
        logger.exception("decision record write failed for %s: %s", target, exc)
        return None


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    decisions: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])

        try:
            raw_labels = detect_moderation_labels(bucket, key)
        except ClientError:
            continue

        labels = normalise_labels(raw_labels)
        score, breaches = score_labels(labels)
        verdict = resolve_verdict(score, breaches)

        decision: Dict[str, Any] = {
            "source_key": key,
            "verdict": verdict,
            "score": score,
            "label_count": len(labels),
            "breached_categories": sorted({breach["category"] for breach in breaches}),
            "labels": labels,
            "evaluated_at": int(time.time()),
        }

        if verdict == "BLOCK":
            decision["quarantine_key"] = quarantine_asset(bucket, key, decision)

        decision["record_key"] = write_decision_record(bucket, key, decision)

        logger.info(
            "moderation verdict=%s score=%.2f labels=%s key=%s",
            verdict,
            score,
            len(labels),
            key,
        )
        decisions.append(
            {"key": key, "verdict": verdict, "score": score, "breaches": len(breaches)}
        )

    return {"screened": len(decisions), "decisions": decisions}
