"""Cart repricing endpoint with stacked promotion evaluation.

Event source: API Gateway REST API, POST /cart/price.
Applies every eligible promotion rule to the cart in priority order and returns the
adjusted total. The computed total is authoritative for checkout.
"""

import base64
import json
import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

PROMOTION_TABLE = os.environ.get("PROMOTION_TABLE", "promotions")
PROMOTION_INDEX = os.environ.get("PROMOTION_INDEX", "by-status-priority")

CENTS = Decimal("0.01")
MAX_DISCOUNT_FRACTION = Decimal("0.60")
STACK_EXCLUSIVE = "EXCLUSIVE"
STACK_ADDITIVE = "ADDITIVE"


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def iter_active_promotions(channel: str) -> Iterator[Dict[str, Any]]:
    """Yield active promotions for a channel, highest priority first."""
    from lambda_guards import safe_paginate
    from boto3.dynamodb.types import TypeDeserializer
    _deser = TypeDeserializer()

    client = dynamodb.meta.client
    paginator = client.get_paginator("query")
    for page in safe_paginate(paginator,
        TableName=PROMOTION_TABLE,
        IndexName=PROMOTION_INDEX,
        KeyConditionExpression="#st = :active",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":active": {"S": "ACTIVE"}},
        ScanIndexForward=False,
    ):
        for raw_item in page.get("Items", []):
            item = {k: _deser.deserialize(v) for k, v in raw_item.items()}
            channels = item.get("channels") or []
            if not channels or channel in channels:
                yield item


def _cart_subtotal(lines: List[Dict[str, Any]]) -> Decimal:
    subtotal = Decimal("0.00")
    for line in lines:
        subtotal += _money(line.get("line_total", "0"))
    return subtotal


def _matches_scope(promotion: Dict[str, Any], lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return the cart lines a promotion applies to."""
    scope = promotion.get("scope") or {}
    skus = set(scope.get("skus") or [])
    categories = set(scope.get("categories") or [])
    brands = set(scope.get("brands") or [])

    if not skus and not categories and not brands:
        return list(lines)

    matched: List[Dict[str, Any]] = []
    for line in lines:
        if skus and str(line.get("sku")) in skus:
            matched.append(line)
            continue
        if categories and str(line.get("category")) in categories:
            matched.append(line)
            continue
        if brands and str(line.get("brand")) in brands:
            matched.append(line)
    return matched


def _eligible(promotion: Dict[str, Any], subtotal: Decimal, matched: List[Dict[str, Any]],
              customer_tier: str) -> bool:
    if not matched:
        return False
    minimum = _money(promotion.get("minimum_subtotal", "0"))
    if subtotal < minimum:
        return False
    tiers = promotion.get("customer_tiers") or []
    if tiers and customer_tier not in tiers:
        return False
    min_units = int(promotion.get("minimum_units", 0))
    if min_units:
        units = sum(int(line.get("quantity", 0)) for line in matched)
        if units < min_units:
            return False
    return True


def _discount_for(promotion: Dict[str, Any], matched: List[Dict[str, Any]]) -> Decimal:
    """Compute the discount a single promotion contributes."""
    kind = str(promotion.get("kind", "PERCENT")).upper()
    matched_total = _cart_subtotal(matched)

    if kind == "PERCENT":
        rate = Decimal(str(promotion.get("value", "0"))) / Decimal("100")
        return _money(matched_total * rate)
    if kind == "FIXED":
        return min(_money(promotion.get("value", "0")), matched_total)
    if kind == "UNIT_OFF":
        per_unit = _money(promotion.get("value", "0"))
        units = sum(int(line.get("quantity", 0)) for line in matched)
        return min(_money(per_unit * units), matched_total)
    if kind == "BXGY":
        buy = max(int(promotion.get("buy_quantity", 1)), 1)
        get = max(int(promotion.get("get_quantity", 1)), 1)
        cheapest = min(
            (_money(line.get("unit_price", "0")) for line in matched),
            default=Decimal("0.00"),
        )
        units = sum(int(line.get("quantity", 0)) for line in matched)
        free_units = (units // (buy + get)) * get
        return min(_money(cheapest * free_units), matched_total)

    logger.warning("promotion_unknown_kind promotion=%s kind=%s", promotion.get("promotion_id"), kind)
    return Decimal("0.00")


def evaluate_promotions(
    lines: List[Dict[str, Any]], channel: str, customer_tier: str
) -> Tuple[Decimal, List[Dict[str, Any]]]:
    """Apply every eligible promotion and return (total_discount, applied)."""
    subtotal = _cart_subtotal(lines)
    applied: List[Dict[str, Any]] = []
    total_discount = Decimal("0.00")
    exclusive_claimed = False

    for promotion in iter_active_promotions(channel):
        promotion_id = str(promotion.get("promotion_id", "unknown"))
        stacking = str(promotion.get("stacking", STACK_ADDITIVE)).upper()

        if exclusive_claimed:
            continue

        matched = _matches_scope(promotion, lines)
        if not _eligible(promotion, subtotal, matched, customer_tier):
            continue

        discount = _discount_for(promotion, matched)
        if discount <= 0:
            continue

        remaining_headroom = _money(subtotal * MAX_DISCOUNT_FRACTION) - total_discount
        if remaining_headroom <= 0:
            break
        discount = min(discount, remaining_headroom)

        total_discount += discount
        applied.append(
            {
                "promotion_id": promotion_id,
                "kind": str(promotion.get("kind", "PERCENT")).upper(),
                "discount": str(discount),
                "stacking": stacking,
            }
        )

        if stacking == STACK_EXCLUSIVE:
            exclusive_claimed = True

    return _money(total_discount), applied


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("body must be a JSON object")
    return parsed


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def lambda_handler(event, context):
    from lambda_guards import validate_payload_size

    try:
        validate_payload_size(event)
    except ValueError:
        return _response(413, {"error": "Payload too large"})

    try:
        body = _parse_body(event)
    except (ValueError, TypeError) as exc:
        return _response(400, {"error": "invalid_body", "detail": str(exc)})

    lines = body.get("lines") or []
    if not lines:
        return _response(400, {"error": "lines_required"})

    params = event.get("queryStringParameters") or {}
    channel = str(params.get("channel", body.get("channel", "web"))).lower()
    customer_tier = str(body.get("customer_tier", "STANDARD")).upper()

    subtotal = _cart_subtotal(lines)

    try:
        discount, applied = evaluate_promotions(lines, channel, customer_tier)
    except ClientError as exc:
        logger.exception("promotion_lookup_failed error=%s", exc)
        return _response(503, {"error": "promotion_store_unavailable"})

    total = _money(subtotal - discount)

    logger.info(
        "cart_priced channel=%s tier=%s subtotal=%s discount=%s promotions=%s",
        channel, customer_tier, subtotal, discount, len(applied),
    )
    return _response(
        200,
        {
            "subtotal": str(subtotal),
            "discount": str(discount),
            "total": str(total),
            "applied_promotions": applied,
        },
    )
