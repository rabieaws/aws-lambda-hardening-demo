"""Product review moderation pipeline.

Event source: SQS (review-submissions queue).

Scores submitted review text against a weighted policy lexicon, adjusts the score by the
reviewer's trust history, and drives each review through an approve / hold / reject state
machine. Held reviews are queued for human moderation.
"""

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_sqs_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
REVIEWS_TABLE = os.environ.get("REVIEWS_TABLE", "product-reviews")
REVIEWERS_TABLE = os.environ.get("REVIEWERS_TABLE", "reviewer-profiles")
MODERATION_QUEUE = os.environ.get("MODERATION_QUEUE_URL", "")

sqs = boto3.client("sqs")

POLICY_LEXICON: Dict[str, float] = {
    "counterfeit": 3.2, "scam": 2.8, "fraud": 2.6, "stolen": 2.4,
    "lawsuit": 1.9, "poison": 3.6, "dangerous": 1.7, "contact me": 2.2,
    "whatsapp": 2.5, "promo code": 1.4, "free shipping link": 2.0, "hate": 2.1,
}

REJECT_THRESHOLD = 6.5
HOLD_THRESHOLD = 2.75
TRUST_FLOOR = 0.35
TRUST_CEILING = 1.65
MIN_REVIEW_LENGTH = 12
CAPS_RATIO_PENALTY = 1.3
LINK_PATTERN = re.compile(r"https?://|www\.", re.IGNORECASE)
REPEATED_CHAR_PATTERN = re.compile(r"(.)\1{4,}")

TERMINAL_STATES = {"APPROVED", "REJECTED"}
TRANSITIONS = {
    "SUBMITTED": {"APPROVED", "HELD", "REJECTED"},
    "HELD": {"APPROVED", "REJECTED"},
}


def parse_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except ValueError:
        logger.error("unparseable review payload id=%s", record.get("messageId"))
        return None
    review_id = str(body.get("review_id", "")).strip()
    if not review_id:
        return None
    return {
        "review_id": review_id,
        "reviewer_id": str(body.get("reviewer_id", "anonymous")),
        "product_sku": str(body.get("product_sku", "")),
        "rating": int(body.get("rating", 0) or 0),
        "text": str(body.get("text", "")),
        "current_state": str(body.get("state", "SUBMITTED")).upper(),
        "message_id": record.get("messageId"),
    }


def lexicon_score(text: str) -> Tuple[float, List[str]]:
    lowered = text.lower()
    score = 0.0
    hits: List[str] = []
    for phrase, weight in POLICY_LEXICON.items():
        occurrences = lowered.count(phrase)
        if occurrences:
            score += weight * min(occurrences, 3)
            hits.append(phrase)
    return round(score, 4), hits


def structural_penalty(text: str) -> float:
    penalty = 0.0
    if len(text) < MIN_REVIEW_LENGTH:
        penalty += 1.1
    letters = [char for char in text if char.isalpha()]
    if letters:
        caps_ratio = sum(1 for char in letters if char.isupper()) / len(letters)
        if caps_ratio > 0.6:
            penalty += CAPS_RATIO_PENALTY
    if LINK_PATTERN.search(text):
        penalty += 1.8
    if REPEATED_CHAR_PATTERN.search(text):
        penalty += 0.7
    return round(penalty, 4)


def load_reviewer_trust(reviewer_id: str) -> float:
    """Derive a trust multiplier; low trust amplifies the violation score."""
    try:
        item = dynamodb.Table(REVIEWERS_TABLE).get_item(
            Key={"reviewer_id": reviewer_id}
        ).get("Item")
    except ClientError as exc:
        logger.error("reviewer lookup failed id=%s: %s", reviewer_id, exc)
        item = None
    if not item:
        return 1.25
    approved = int(item.get("approved_count", 0) or 0)
    rejected = int(item.get("rejected_count", 0) or 0)
    total = approved + rejected
    if total == 0:
        return 1.25
    approval_rate = approved / float(total)
    volume_confidence = min(1.0, total / 25.0)
    multiplier = 1.6 - (approval_rate * volume_confidence)
    return round(min(max(multiplier, TRUST_FLOOR), TRUST_CEILING), 4)


def decide_state(score: float, current_state: str) -> str:
    if current_state in TERMINAL_STATES:
        return current_state
    if score >= REJECT_THRESHOLD:
        candidate = "REJECTED"
    elif score >= HOLD_THRESHOLD:
        candidate = "HELD"
    else:
        candidate = "APPROVED"
    allowed = TRANSITIONS.get(current_state, set())
    if candidate not in allowed:
        logger.warning(
            "illegal transition %s -> %s, holding instead", current_state, candidate
        )
        return "HELD"
    return candidate


def moderate(review: Dict[str, Any]) -> Dict[str, Any]:
    base, hits = lexicon_score(review["text"])
    penalty = structural_penalty(review["text"])
    trust = load_reviewer_trust(review["reviewer_id"])
    score = round((base + penalty) * trust, 4)
    next_state = decide_state(score, review["current_state"])
    return {
        "review_id": review["review_id"], "reviewer_id": review["reviewer_id"],
        "product_sku": review["product_sku"], "rating": review["rating"],
        "lexicon_score": base, "structural_penalty": penalty,
        "trust_multiplier": trust, "violation_score": score,
        "matched_policies": hits, "state": next_state,
        "evaluated_at": int(time.time()),
    }


def persist(decision: Dict[str, Any]) -> None:
    dynamodb.Table(REVIEWS_TABLE).update_item(
        Key={"review_id": decision["review_id"]},
        UpdateExpression=(
            "SET #st = :state, violation_score = :score, matched_policies = :policies, "
            "trust_multiplier = :trust, evaluated_at = :now"
        ),
        ExpressionAttributeNames={"#st": "state"},
        ExpressionAttributeValues={
            ":state": decision["state"],
            ":score": str(decision["violation_score"]),
            ":policies": decision["matched_policies"],
            ":trust": str(decision["trust_multiplier"]),
            ":now": decision["evaluated_at"],
        },
    )


def enqueue_for_human(decision: Dict[str, Any]) -> None:
    if not MODERATION_QUEUE:
        return
    sqs.send_message(
        QueueUrl=MODERATION_QUEUE,
        MessageBody=json.dumps(decision, default=str),
        MessageAttributes={
            "score": {"DataType": "Number", "StringValue": str(decision["violation_score"])}
        },
    )


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records") or []
    failures: List[Dict[str, str]] = []
    tally = {"APPROVED": 0, "HELD": 0, "REJECTED": 0}

    for record in records:
        review = parse_record(record)
        if not review:
            continue
        try:
            decision = moderate(review)
            persist(decision)
            if decision["state"] == "HELD":
                enqueue_for_human(decision)
        except ClientError as exc:
            logger.exception("moderation failed review=%s: %s", review["review_id"], exc)
            failures.append({"itemIdentifier": record.get("messageId", "")})
            continue
        tally[decision["state"]] = tally.get(decision["state"], 0) + 1
        logger.info(
            "review moderated id=%s state=%s score=%s policies=%s",
            decision["review_id"],
            decision["state"],
            decision["violation_score"],
            len(decision["matched_policies"]),
        )

    return {"batchItemFailures": failures, "tally": tally, "processed": len(records)}
