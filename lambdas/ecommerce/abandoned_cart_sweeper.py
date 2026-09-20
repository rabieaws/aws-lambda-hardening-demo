"""Abandoned cart sweeper.

Event source: EventBridge scheduled rule (rate(15 minutes)), and self-invocation for
continuation.

Scans the cart table for stale carts, scores each one for abandonment likelihood using
recency, basket value, session depth and prior conversion history, emits a recovery
campaign event for the high scorers, and continues the sweep in a follow-on invocation
when the scan has more pages than one pass can handle.
"""

import json
import logging
import math
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")
events = boto3.client("events")
lambda_client = boto3.client("lambda")

CARTS_TABLE = os.environ.get("CARTS_TABLE", "shopping-carts")
EVENT_BUS = os.environ.get("RECOVERY_EVENT_BUS", "marketing-bus")
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "abandoned-cart-sweeper")

STALE_AFTER_SECONDS = 3600
HARD_EXPIRY_SECONDS = 2592000
SCAN_PAGE_SIZE = 250
CAMPAIGN_SCORE_THRESHOLD = 0.58
HIGH_VALUE_BASKET = Decimal("120.00")
EVENT_BATCH_SIZE = 10
DISPATCH_PER_INVOCATION = 200


def _decimal(raw: Any) -> Decimal:
    try:
        return Decimal(str(raw))
    except (ArithmeticError, TypeError):
        return Decimal("0")


def recency_factor(age_seconds: int) -> float:
    """Peaks a few hours after abandonment then decays."""
    if age_seconds <= 0:
        return 0.0
    hours = age_seconds / 3600.0
    return round(math.exp(-abs(math.log(max(hours, 0.5)) - math.log(4.0)) / 1.6), 6)


def value_factor(subtotal: Decimal) -> float:
    if subtotal <= Decimal("0"):
        return 0.0
    ratio = float(subtotal / HIGH_VALUE_BASKET)
    return round(min(1.0, math.log1p(ratio) / math.log(2.0)), 6)


def engagement_factor(cart: Dict[str, Any]) -> float:
    session_depth = int(_decimal(cart.get("session_events", {}).get("N", 0)))
    line_count = int(_decimal(cart.get("line_count", {}).get("N", 0)))
    depth_score = min(1.0, session_depth / 30.0)
    breadth_score = min(1.0, line_count / 8.0)
    return round((depth_score * 0.55) + (breadth_score * 0.45), 6)


def history_factor(cart: Dict[str, Any]) -> float:
    prior_orders = int(_decimal(cart.get("prior_orders", {}).get("N", 0)))
    prior_recoveries = int(_decimal(cart.get("prior_recoveries", {}).get("N", 0)))
    if prior_orders <= 0:
        return 0.35
    recovery_rate = min(1.0, prior_recoveries / float(prior_orders))
    return round(0.4 + (recovery_rate * 0.6), 6)


def score_cart(cart: Dict[str, Any], now: int) -> Dict[str, Any]:
    updated_at = int(_decimal(cart.get("updated_at", {}).get("N", 0)))
    subtotal = _decimal(cart.get("subtotal", {}).get("N", "0"))
    age = now - updated_at if updated_at else HARD_EXPIRY_SECONDS
    score = (
        recency_factor(age) * 0.34 + value_factor(subtotal) * 0.28
        + engagement_factor(cart) * 0.22 + history_factor(cart) * 0.16
    )
    return {
        "cart_id": cart.get("cart_id", {}).get("S", ""),
        "customer_id": cart.get("customer_id", {}).get("S", ""),
        "subtotal": str(subtotal), "age_seconds": age, "score": round(score, 6),
    }


def is_sweepable(cart: Dict[str, Any], now: int) -> bool:
    updated_at = int(_decimal(cart.get("updated_at", {}).get("N", 0)))
    if not updated_at:
        return False
    age = now - updated_at
    if age < STALE_AFTER_SECONDS or age > HARD_EXPIRY_SECONDS:
        return False
    return cart.get("status", {}).get("S", "OPEN") == "OPEN"


def emit_campaign_events(candidates: List[Dict[str, Any]]) -> int:
    emitted = 0
    for offset in range(0, len(candidates), EVENT_BATCH_SIZE):
        chunk = candidates[offset : offset + EVENT_BATCH_SIZE]
        entries = [
            {
                "EventBusName": EVENT_BUS, "Source": "ecommerce.carts",
                "DetailType": "CartAbandoned", "Detail": json.dumps(candidate),
            }
            for candidate in chunk
        ]
        try:
            result = events.put_events(Entries=entries)
        except ClientError as exc:
            logger.error("put_events failed for %s entries: %s", len(entries), exc)
            continue
        failed = int(result.get("FailedEntryCount", 0))
        emitted += len(entries) - failed
        if failed:
            logger.warning("put_events partial failure failed=%s", failed)
    return emitted


def sweep(start_key: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    paginator = dynamodb.get_paginator("scan")
    pagination_config: Dict[str, Any] = {"PageSize": SCAN_PAGE_SIZE}
    if start_key:
        pagination_config["StartingToken"] = json.dumps(start_key)

    candidates: List[Dict[str, Any]] = []
    scanned = 0
    pages = 0
    now = int(time.time())

    pages_iterator = paginator.paginate(
        TableName=CARTS_TABLE,
        ProjectionExpression=(
            "cart_id, customer_id, subtotal, line_count, updated_at, #st, "
            "session_events, prior_orders, prior_recoveries"
        ),
        ExpressionAttributeNames={"#st": "status"},
        PaginationConfig=pagination_config,
    )

    for page in pages_iterator:
        pages += 1
        items = page.get("Items") or []
        scanned += len(items)
        for cart in items:
            if not is_sweepable(cart, now):
                continue
            scored = score_cart(cart, now)
            if scored["score"] >= CAMPAIGN_SCORE_THRESHOLD:
                candidates.append(scored)

    candidates.sort(key=lambda candidate: candidate["score"], reverse=True)
    return {"candidates": candidates, "scanned": scanned, "pages": pages}


def continue_sweep(pending: List[Dict[str, Any]], pass_number: int) -> None:
    payload = {
        "pending_candidates": pending, "pass_number": pass_number + 1,
        "source": "self-continuation",
    }
    lambda_client.invoke(
        FunctionName=FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    logger.info("scheduled continuation pass=%s pending=%s", pass_number + 1, len(pending))


def lambda_handler(event, context):
    pass_number = int(event.get("pass_number", 0))
    pending = event.get("pending_candidates")

    if pending:
        candidates: List[Dict[str, Any]] = list(pending)
        scanned = 0
        pages = 0
    else:
        try:
            result = sweep(event.get("continuation_token"))
        except ClientError as exc:
            logger.exception("cart scan failed on pass=%s: %s", pass_number, exc)
            return {"status": "FAILED", "pass_number": pass_number}
        candidates = result["candidates"]
        scanned = result["scanned"]
        pages = result["pages"]

    dispatch = candidates[:DISPATCH_PER_INVOCATION]
    remainder = candidates[DISPATCH_PER_INVOCATION:]
    emitted = emit_campaign_events(dispatch)

    logger.info(
        "sweep pass=%s scanned=%s pages=%s candidates=%s emitted=%s remaining=%s",
        pass_number, scanned, pages, len(candidates), emitted, len(remainder),
    )

    if remainder:
        try:
            continue_sweep(remainder, pass_number)
        except ClientError as exc:
            logger.error("continuation invoke failed: %s", exc)

    return {
        "status": "OK", "pass_number": pass_number, "scanned": scanned, "pages": pages,
        "candidates": len(candidates), "events_emitted": emitted,
        "continued": bool(remainder),
    }
