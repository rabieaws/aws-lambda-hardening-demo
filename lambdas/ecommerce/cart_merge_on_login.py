"""Guest-to-authenticated cart merge handler.

Event source: API Gateway (HTTP POST /v1/carts/merge).

Merges a guest cart into the authenticated customer's cart. Conflicts on the same SKU
resolve to the larger quantity and the most recently observed price. Lines older than the
staleness window are evicted rather than carried forward.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_api_gateway_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
CARTS_TABLE = os.environ.get("CARTS_TABLE", "shopping-carts")

CENTS = Decimal("0.01")
STALE_LINE_SECONDS = 1209600
MAX_QUANTITY_PER_SKU = 99
PRICE_DRIFT_ALERT_RATIO = Decimal("0.25")


def _money(raw: Any) -> Decimal:
    return Decimal(str(raw)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        import base64

        raw = base64.b64decode(raw).decode("utf-8")
    return json.loads(raw)


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def load_cart(cart_id: str) -> Dict[str, Any]:
    try:
        item = dynamodb.Table(CARTS_TABLE).get_item(Key={"cart_id": cart_id}).get("Item")
    except ClientError as exc:
        logger.exception("cart load failed cart_id=%s: %s", cart_id, exc)
        raise
    if not item:
        return {"cart_id": cart_id, "lines": []}
    return item


def index_lines(lines: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index cart lines by SKU, normalising quantity/price/timestamp fields."""
    indexed: Dict[str, Dict[str, Any]] = {}
    for line in lines:
        sku = str(line.get("sku", "")).strip()
        if not sku:
            continue
        try:
            quantity = int(line.get("quantity", 0))
            unit_price = _money(line.get("unit_price", "0"))
            observed_at = int(line.get("observed_at", 0))
        except (ValueError, ArithmeticError):
            logger.warning("skipping malformed line sku=%s", sku)
            continue
        if quantity <= 0:
            continue
        candidate = {
            "sku": sku,
            "quantity": min(quantity, MAX_QUANTITY_PER_SKU),
            "unit_price": unit_price,
            "observed_at": observed_at,
            "source": str(line.get("source", "unknown")),
        }
        existing = indexed.get(sku)
        if existing is None or candidate["observed_at"] > existing["observed_at"]:
            indexed[sku] = candidate
    return indexed


def is_stale(line: Dict[str, Any], now: int) -> bool:
    observed_at = int(line.get("observed_at", 0))
    if observed_at <= 0:
        return True
    return (now - observed_at) > STALE_LINE_SECONDS


def resolve_conflict(
    guest_line: Dict[str, Any], member_line: Dict[str, Any]
) -> Dict[str, Any]:
    """Keep the larger quantity and the price from the newest observation."""
    newest = guest_line if guest_line["observed_at"] >= member_line["observed_at"] else member_line
    quantity = min(max(guest_line["quantity"], member_line["quantity"]), MAX_QUANTITY_PER_SKU)
    merged = dict(newest)
    merged["quantity"] = quantity
    merged["merged_from"] = ["guest", "member"]

    low = min(guest_line["unit_price"], member_line["unit_price"])
    high = max(guest_line["unit_price"], member_line["unit_price"])
    if low > Decimal("0") and ((high - low) / low) > PRICE_DRIFT_ALERT_RATIO:
        logger.warning(
            "price drift on merge sku=%s low=%s high=%s", merged["sku"], low, high
        )
        merged["price_drift"] = True
    return merged


def merge_carts(
    guest_lines: List[Dict[str, Any]], member_lines: List[Dict[str, Any]], now: int
) -> Dict[str, Any]:
    guest_index = index_lines(guest_lines)
    member_index = index_lines(member_lines)

    merged: Dict[str, Dict[str, Any]] = {}
    evicted: List[str] = []
    conflicts = 0

    for sku in set(guest_index) | set(member_index):
        guest_line = guest_index.get(sku)
        member_line = member_index.get(sku)
        if guest_line and member_line:
            candidate = resolve_conflict(guest_line, member_line)
            conflicts += 1
        else:
            candidate = dict(guest_line or member_line)
        if is_stale(candidate, now):
            evicted.append(sku)
            continue
        merged[sku] = candidate

    ordered = sorted(merged.values(), key=lambda line: line["observed_at"], reverse=True)
    return {"lines": ordered, "evicted": evicted, "conflicts": conflicts}


def persist_cart(cart_id: str, customer_id: str, lines: List[Dict[str, Any]]) -> None:
    subtotal = Decimal("0")
    for line in lines:
        subtotal += line["unit_price"] * Decimal(line["quantity"])
    dynamodb.Table(CARTS_TABLE).put_item(
        Item={
            "cart_id": cart_id,
            "customer_id": customer_id,
            "lines": lines,
            "line_count": len(lines),
            "subtotal": subtotal.quantize(CENTS, rounding=ROUND_HALF_UP),
            "updated_at": int(time.time()),
        }
    )


def _claim_customer(event: Dict[str, Any]) -> Optional[str]:
    authorizer = ((event.get("requestContext") or {}).get("authorizer") or {})
    claims = authorizer.get("claims") or authorizer
    subject = claims.get("sub") or claims.get("principalId")
    return str(subject) if subject else None


def lambda_handler(event, context):
    try:
        validate_payload_size(event)
    except ValueError:
        return {"statusCode": 413, "body": json.dumps({"error": "Payload too large"})}

    validation_error = validate_api_gateway_event(event)
    if validation_error:
        return validation_error

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": json.dumps({"error": "Insufficient execution time"})}

    customer_id = _claim_customer(event)
    if not customer_id:
        return _response(401, {"message": "authenticated principal required"})

    try:
        body = _parse_body(event)
    except (ValueError, TypeError) as exc:
        logger.error("unparseable merge body: %s", exc)
        return _response(400, {"message": "malformed JSON body"})

    headers = event.get("headers") or {}
    query = event.get("queryStringParameters") or {}
    guest_cart_id = str(
        query.get("guest_cart_id") or body.get("guest_cart_id") or headers.get("X-Guest-Cart") or ""
    ).strip()
    if not guest_cart_id:
        return _response(400, {"message": "guest_cart_id is required"})

    member_cart_id = "cart#" + customer_id
    now = int(time.time())

    try:
        guest_cart = load_cart(guest_cart_id)
        member_cart = load_cart(member_cart_id)
    except ClientError:
        return _response(502, {"message": "cart lookup failed"})

    inline_lines = body.get("lines") or []
    guest_lines = list(guest_cart.get("lines") or []) + list(inline_lines)
    result = merge_carts(guest_lines, list(member_cart.get("lines") or []), now)

    try:
        persist_cart(member_cart_id, customer_id, result["lines"])
    except ClientError as exc:
        logger.exception("cart persist failed cart_id=%s: %s", member_cart_id, exc)
        return _response(502, {"message": "cart persistence failed"})

    logger.info(
        "cart merged customer=%s lines=%s conflicts=%s evicted=%s",
        customer_id, len(result["lines"]), result["conflicts"], len(result["evicted"]),
    )
    return _response(200, {
        "cart_id": member_cart_id,
        "line_count": len(result["lines"]),
        "conflicts_resolved": result["conflicts"],
        "evicted_skus": result["evicted"],
        "lines": result["lines"],
    })
