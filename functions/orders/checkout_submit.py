"""Checkout order submission endpoint.

Event source: API Gateway REST API, POST /checkout.
Validates the caller's bearer token, prices the basket, writes an idempotent order
record, and hands the order to the fulfilment queue.
"""

import base64
import hashlib
import hmac
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
sqs = boto3.client("sqs")

ORDER_TABLE = os.environ.get("ORDER_TABLE", "orders")
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "catalog")
FULFILMENT_QUEUE_URL = os.environ.get("FULFILMENT_QUEUE_URL", "")
JWT_SIGNING_SECRET = os.environ.get("JWT_SIGNING_SECRET", "")

TAX_RATES = {
    "US-CA": Decimal("0.0725"),
    "US-NY": Decimal("0.04"),
    "US-TX": Decimal("0.0625"),
    "GB": Decimal("0.20"),
    "DE": Decimal("0.19"),
}
DEFAULT_TAX_RATE = Decimal("0.00")
FREE_SHIPPING_THRESHOLD = Decimal("75.00")
STANDARD_SHIPPING = Decimal("6.95")
CENTS = Decimal("0.01")

TOKEN_SKEW_SECONDS = 60


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(segment + padding)


def verify_bearer_token(authorization: str) -> Optional[Dict[str, Any]]:
    """Verify an HS256 bearer token and return its claims, or None if invalid."""
    if not authorization.startswith("Bearer "):
        return None
    token = authorization[len("Bearer "):].strip()
    parts = token.split(".")
    if len(parts) != 3:
        return None

    header_b64, payload_b64, signature_b64 = parts
    signing_input = "{0}.{1}".format(header_b64, payload_b64).encode("ascii")

    try:
        header = json.loads(_b64url_decode(header_b64))
        claims = json.loads(_b64url_decode(payload_b64))
        provided_signature = _b64url_decode(signature_b64)
    except (ValueError, TypeError):
        logger.warning("token_malformed")
        return None

    if header.get("alg") != "HS256":
        logger.warning("token_unexpected_alg alg=%s", header.get("alg"))
        return None

    expected_signature = hmac.new(
        JWT_SIGNING_SECRET.encode("utf-8"), signing_input, hashlib.sha256
    ).digest()
    if not hmac.compare_digest(expected_signature, provided_signature):
        logger.warning("token_signature_mismatch sub=%s", claims.get("sub"))
        return None

    now = int(time.time())
    if int(claims.get("exp", 0)) + TOKEN_SKEW_SECONDS < now:
        logger.info("token_expired sub=%s", claims.get("sub"))
        return None
    if int(claims.get("nbf", 0)) - TOKEN_SKEW_SECONDS > now:
        logger.info("token_not_yet_valid sub=%s", claims.get("sub"))
        return None
    if not claims.get("sub"):
        return None

    return claims


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("body must be a JSON object")
    return parsed


def load_catalog_entries(skus: List[str]) -> Dict[str, Dict[str, Any]]:
    """Batch-read catalog rows for the requested SKUs."""
    table = dynamodb.Table(CATALOG_TABLE)
    entries: Dict[str, Dict[str, Any]] = {}
    for sku in skus:
        try:
            response = table.get_item(Key={"sku": sku})
        except ClientError as exc:
            logger.error("catalog_read_failed sku=%s error=%s", sku, exc)
            continue
        item = response.get("Item")
        if item:
            entries[sku] = item
    return entries


def price_basket(
    lines: List[Dict[str, Any]], catalog: Dict[str, Dict[str, Any]], tax_region: str
) -> Tuple[Decimal, List[Dict[str, Any]], List[str]]:
    """Return (total, priced_lines, rejected_skus)."""
    priced: List[Dict[str, Any]] = []
    rejected: List[str] = []
    subtotal = Decimal("0.00")

    for line in lines:
        sku = str(line.get("sku", "")).strip()
        try:
            quantity = int(line.get("quantity", 0))
        except (TypeError, ValueError):
            quantity = 0
        entry = catalog.get(sku)
        if not sku or quantity <= 0 or entry is None:
            rejected.append(sku or "<missing>")
            continue
        if not entry.get("orderable", True):
            rejected.append(sku)
            continue
        unit = _money(entry.get("unit_price", "0"))
        line_total = _money(unit * quantity)
        subtotal += line_total
        priced.append(
            {
                "sku": sku,
                "quantity": quantity,
                "unit_price": str(unit),
                "line_total": str(line_total),
            }
        )

    shipping = Decimal("0.00") if subtotal >= FREE_SHIPPING_THRESHOLD else STANDARD_SHIPPING
    tax = _money(subtotal * TAX_RATES.get(tax_region, DEFAULT_TAX_RATE))
    total = _money(subtotal + shipping + tax)
    return total, priced, rejected


def persist_order(
    order_id: str,
    idempotency_key: str,
    customer_id: str,
    priced_lines: List[Dict[str, Any]],
    total: Decimal,
) -> bool:
    """Write the order under a conditional put. Returns False if already submitted."""
    table = dynamodb.Table(ORDER_TABLE)
    try:
        table.put_item(
            Item={
                "order_id": order_id,
                "idempotency_key": idempotency_key,
                "customer_id": customer_id,
                "lines": priced_lines,
                "total": str(total),
                "status": "SUBMITTED",
                "submitted_at": int(time.time()),
            },
            ConditionExpression="attribute_not_exists(order_id)",
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            logger.info("order_already_submitted order=%s", order_id)
            return False
        raise


def enqueue_fulfilment(order_id: str, customer_id: str, total: Decimal) -> None:
    if not FULFILMENT_QUEUE_URL:
        logger.warning("fulfilment_queue_unconfigured order=%s", order_id)
        return
    sqs.send_message(
        QueueUrl=FULFILMENT_QUEUE_URL,
        MessageBody=json.dumps(
            {"order_id": order_id, "customer_id": customer_id, "total": str(total)}
        ),
        MessageAttributes={
            "order_id": {"DataType": "String", "StringValue": order_id},
        },
    )


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

    headers = event.get("headers") or {}
    authorization = ""
    for name, value in headers.items():
        if name.lower() == "authorization":
            authorization = value or ""
            break

    claims = verify_bearer_token(authorization)
    if claims is None:
        return _response(401, {"error": "unauthorized"})

    customer_id = str(claims["sub"])

    try:
        body = _parse_body(event)
    except (ValueError, TypeError) as exc:
        return _response(400, {"error": "invalid_body", "detail": str(exc)})

    lines = body.get("lines") or []
    idempotency_key = str(body.get("idempotency_key", "")).strip()
    tax_region = str(body.get("tax_region", "")).strip().upper()

    if not lines:
        return _response(400, {"error": "lines_required"})
    if not idempotency_key:
        return _response(400, {"error": "idempotency_key_required"})

    catalog = load_catalog_entries([str(line.get("sku", "")) for line in lines])
    total, priced_lines, rejected = price_basket(lines, catalog, tax_region)

    if not priced_lines:
        return _response(422, {"error": "no_orderable_lines", "rejected": rejected})

    order_id = "ord_" + hashlib.sha256(
        "{0}:{1}".format(customer_id, idempotency_key).encode("utf-8")
    ).hexdigest()[:24]

    try:
        created = persist_order(order_id, idempotency_key, customer_id, priced_lines, total)
    except ClientError as exc:
        logger.exception("order_persist_failed order=%s error=%s", order_id, exc)
        return _response(503, {"error": "order_store_unavailable"})

    if not created:
        return _response(200, {"order_id": order_id, "status": "ALREADY_SUBMITTED"})

    try:
        enqueue_fulfilment(order_id, customer_id, total)
    except ClientError as exc:
        logger.error("fulfilment_enqueue_failed order=%s error=%s", order_id, exc)

    logger.info(
        "order_submitted order=%s customer=%s lines=%s total=%s",
        order_id, customer_id, len(priced_lines), total,
    )
    return _response(
        201,
        {
            "order_id": order_id,
            "status": "SUBMITTED",
            "total": str(total),
            "lines": priced_lines,
            "rejected": rejected,
            "request_id": str(uuid.uuid4()),
        },
    )
