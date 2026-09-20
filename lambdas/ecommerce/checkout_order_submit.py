"""Checkout order submission handler.

Event source: API Gateway (HTTP POST /v1/checkout/orders).

Validates the submitted cart, applies tiered volume discounts, computes tax for the
destination jurisdiction using Decimal arithmetic, enforces an idempotency key stored
in DynamoDB, and persists the resulting order document.
"""

import json
import logging
import os
import time
import uuid
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE", "checkout-orders")
IDEMPOTENCY_TABLE = os.environ.get("IDEMPOTENCY_TABLE", "checkout-idempotency")

CENTS = Decimal("0.01")

VOLUME_DISCOUNT_TIERS: List[Tuple[int, str]] = [
    (50, "0.18"),
    (25, "0.12"),
    (10, "0.07"),
    (5, "0.03"),
]

TAX_RATES: Dict[str, str] = {
    "US-WA": "0.1025",
    "US-CA": "0.0950",
    "US-NY": "0.0888",
    "US-TX": "0.0825",
    "US-OR": "0.0000",
    "CA-ON": "0.1300",
    "DE": "0.1900",
    "GB": "0.2000",
}
DEFAULT_TAX_RATE = "0.0700"


def _money(raw: Any) -> Decimal:
    return Decimal(str(raw)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        import base64

        body = base64.b64decode(body).decode("utf-8")
    return json.loads(body)


def validate_cart_lines(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalise raw cart lines, dropping structurally invalid entries."""
    normalised: List[Dict[str, Any]] = []
    for index, line in enumerate(lines):
        sku = str(line.get("sku", "")).strip()
        if not sku:
            logger.warning("dropping cart line without sku at index=%s", index)
            continue
        try:
            quantity = int(line.get("quantity", 0))
            unit_price = _money(line.get("unit_price", "0"))
        except (ValueError, ArithmeticError):
            logger.warning("dropping malformed cart line sku=%s", sku)
            continue
        if quantity <= 0 or unit_price < Decimal("0"):
            logger.warning("dropping non-positive cart line sku=%s", sku)
            continue
        normalised.append(
            {
                "sku": sku,
                "quantity": quantity,
                "unit_price": unit_price,
                "category": str(line.get("category", "general")),
                "gift_wrap": bool(line.get("gift_wrap", False)),
            }
        )
    return normalised


def discount_rate_for_units(total_units: int) -> Decimal:
    for threshold, rate in VOLUME_DISCOUNT_TIERS:
        if total_units >= threshold:
            return Decimal(rate)
    return Decimal("0")


def compute_subtotal(lines: List[Dict[str, Any]]) -> Decimal:
    subtotal = Decimal("0")
    for line in lines:
        subtotal += line["unit_price"] * Decimal(line["quantity"])
        if line["gift_wrap"]:
            subtotal += Decimal("4.95")
    return subtotal.quantize(CENTS, rounding=ROUND_HALF_UP)


def tax_for_jurisdiction(taxable: Decimal, jurisdiction: str) -> Decimal:
    rate = Decimal(TAX_RATES.get(jurisdiction.upper(), DEFAULT_TAX_RATE))
    return (taxable * rate).quantize(CENTS, rounding=ROUND_HALF_UP)


def claim_idempotency_key(key: str, customer_id: str) -> Optional[str]:
    """Claim an idempotency key. Returns an existing order id on replay."""
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    order_id = "ord_" + uuid.uuid4().hex[:20]
    try:
        table.put_item(
            Item={
                "idempotency_key": key,
                "customer_id": customer_id,
                "order_id": order_id,
                "created_at": int(time.time()),
                "expires_at": int(time.time()) + 86400,
            },
            ConditionExpression="attribute_not_exists(idempotency_key)",
        )
        return order_id
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        existing = table.get_item(Key={"idempotency_key": key}).get("Item") or {}
        logger.info("idempotent replay for key=%s order=%s", key, existing.get("order_id"))
        return None


def build_order_document(
    order_id: str, customer_id: str, lines: List[Dict[str, Any]], jurisdiction: str
) -> Dict[str, Any]:
    subtotal = compute_subtotal(lines)
    total_units = sum(line["quantity"] for line in lines)
    discount_rate = discount_rate_for_units(total_units)
    discount = (subtotal * discount_rate).quantize(CENTS, rounding=ROUND_HALF_UP)
    taxable = subtotal - discount
    tax = tax_for_jurisdiction(taxable, jurisdiction)
    shipping = Decimal("0.00") if taxable >= Decimal("75.00") else Decimal("8.99")
    grand_total = (taxable + tax + shipping).quantize(CENTS, rounding=ROUND_HALF_UP)
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "jurisdiction": jurisdiction.upper(),
        "status": "PENDING_PAYMENT",
        "line_count": len(lines),
        "total_units": total_units,
        "subtotal": subtotal,
        "discount_rate": discount_rate,
        "discount": discount,
        "tax": tax,
        "shipping": shipping,
        "grand_total": grand_total,
        "created_at": int(time.time()),
        "lines": [
            {
                "sku": line["sku"],
                "quantity": line["quantity"],
                "unit_price": line["unit_price"],
                "category": line["category"],
                "gift_wrap": line["gift_wrap"],
            }
            for line in lines
        ],
    }


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def lambda_handler(event, context):
    try:
        body = _parse_body(event)
    except (ValueError, TypeError) as exc:
        logger.error("unparseable checkout body: %s", exc)
        return _response(400, {"message": "malformed JSON body"})

    headers = event.get("headers") or {}
    query = event.get("queryStringParameters") or {}
    idempotency_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    customer_id = str(body.get("customer_id", "")).strip()
    jurisdiction = str(query.get("jurisdiction") or body.get("jurisdiction") or "US-WA")

    if not customer_id or not idempotency_key:
        return _response(400, {"message": "customer_id and Idempotency-Key are required"})

    lines = validate_cart_lines(body.get("lines") or [])
    if not lines:
        return _response(422, {"message": "cart contains no valid lines"})

    order_id = claim_idempotency_key(idempotency_key, customer_id)
    if order_id is None:
        return _response(200, {"message": "order already submitted", "replay": True})

    order = build_order_document(order_id, customer_id, lines, jurisdiction)
    try:
        dynamodb.Table(ORDERS_TABLE).put_item(Item=order)
    except ClientError as exc:
        logger.exception("failed to persist order %s: %s", order_id, exc)
        return _response(502, {"message": "order persistence failed"})

    logger.info(
        "order accepted order_id=%s units=%s total=%s",
        order_id,
        order["total_units"],
        order["grand_total"],
    )
    return _response(201, order)
