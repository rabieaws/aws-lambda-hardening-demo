"""Return and refund eligibility evaluator.

Event source: API Gateway (HTTP POST /v1/returns/eligibility).

Runs the returns policy engine over purchase date, declared item condition, category rules
and the customer's prior return abuse rate, then computes the refundable amount including
restocking fees and return shipping deductions.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_api_gateway_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE", "checkout-orders")
RETURNS_TABLE = os.environ.get("RETURNS_TABLE", "return-requests")

CENTS = Decimal("0.01")
DEFAULT_RETURN_WINDOW_DAYS = 30
SECONDS_PER_DAY = 86400
ABUSE_RATE_HARD_BLOCK = 0.55
ABUSE_RATE_REVIEW = 0.30
MIN_ORDERS_FOR_ABUSE_RATE = 4
RETURN_SHIPPING_DEDUCTION = Decimal("7.50")

CATEGORY_RULES: Dict[str, Dict[str, Any]] = {
    "electronics": {"window_days": 15, "restocking_fee": "0.15", "opened_allowed": True},
    "software": {"window_days": 0, "restocking_fee": "0.00", "opened_allowed": False},
    "grocery": {"window_days": 0, "restocking_fee": "0.00", "opened_allowed": False},
    "apparel": {"window_days": 60, "restocking_fee": "0.00", "opened_allowed": True},
    "furniture": {"window_days": 30, "restocking_fee": "0.20", "opened_allowed": True},
    "jewellery": {"window_days": 14, "restocking_fee": "0.10", "opened_allowed": False},
    "general": {"window_days": 30, "restocking_fee": "0.00", "opened_allowed": True},
}

CONDITION_RECOVERY: Dict[str, str] = {
    "unopened": "1.00", "opened_unused": "0.95", "used_good": "0.80",
    "used_worn": "0.55", "damaged": "0.25",
}


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


def load_order(order_id: str) -> Optional[Dict[str, Any]]:
    try:
        return dynamodb.Table(ORDERS_TABLE).get_item(Key={"order_id": order_id}).get("Item")
    except ClientError as exc:
        logger.error("order load failed order=%s: %s", order_id, exc)
        raise


def prior_return_stats(customer_id: str) -> Tuple[int, float]:
    """Return (order_count, abuse_rate) for the customer's return history."""
    table = dynamodb.Table(RETURNS_TABLE)
    try:
        response = table.query(
            IndexName="customer-index",
            KeyConditionExpression=Key("customer_id").eq(customer_id),
        )
    except ClientError as exc:
        logger.error("return history lookup failed customer=%s: %s", customer_id, exc)
        return 0, 0.0
    items = response.get("Items") or []
    orders = {str(item.get("order_id")) for item in items if item.get("order_id")}
    flagged = sum(
        1 for item in items
        if str(item.get("resolution", "")).upper() in ("DENIED", "ABUSE_FLAGGED", "FRAUD")
    )
    total = max(len(orders), 1)
    return len(orders), round(flagged / float(total), 4)


def days_since(epoch_seconds: int, now: int) -> int:
    if epoch_seconds <= 0:
        return DEFAULT_RETURN_WINDOW_DAYS + 1
    return max(0, (now - epoch_seconds) // SECONDS_PER_DAY)


def evaluate_line(line: Dict[str, Any], order_age_days: int, declared_condition: str) -> Dict[str, Any]:
    category = str(line.get("category", "general"))
    rules = CATEGORY_RULES.get(category.lower(), CATEGORY_RULES["general"])
    reasons: List[str] = []

    window = int(rules["window_days"])
    if window == 0:
        reasons.append("category_not_returnable")
    elif order_age_days > window:
        reasons.append("outside_return_window")

    condition = declared_condition.lower()
    if condition not in CONDITION_RECOVERY:
        reasons.append("unknown_condition")
        condition = "damaged"
    if condition != "unopened" and not rules["opened_allowed"]:
        reasons.append("opened_item_not_returnable")

    quantity = int(line.get("quantity", 1) or 1)
    gross = (_money(line.get("unit_price", "0")) * Decimal(quantity)).quantize(CENTS, rounding=ROUND_HALF_UP)
    recovery = Decimal(CONDITION_RECOVERY.get(condition, "0.25"))
    restocking = Decimal(str(rules["restocking_fee"]))
    refundable = (gross * recovery * (Decimal("1") - restocking)).quantize(CENTS, rounding=ROUND_HALF_UP)

    return {
        "sku": str(line.get("sku", "")), "category": category, "quantity": quantity,
        "gross": gross, "condition": condition, "recovery_ratio": recovery,
        "restocking_fee_ratio": restocking,
        "refundable": refundable if not reasons else Decimal("0.00"),
        "eligible": not reasons, "reasons": reasons, "window_days": window,
    }


def decide(line_results: List[Dict[str, Any]], order_count: int, abuse_rate: float) -> Dict[str, Any]:
    eligible_lines = [result for result in line_results if result["eligible"]]
    refundable = sum((result["refundable"] for result in eligible_lines), Decimal("0.00"))

    decision = "APPROVED" if eligible_lines else "DENIED"
    notes: List[str] = []

    if order_count >= MIN_ORDERS_FOR_ABUSE_RATE:
        if abuse_rate >= ABUSE_RATE_HARD_BLOCK:
            decision = "DENIED"
            notes.append("abuse_rate_hard_block")
        elif abuse_rate >= ABUSE_RATE_REVIEW and decision == "APPROVED":
            decision = "MANUAL_REVIEW"
            notes.append("abuse_rate_review")

    if decision == "APPROVED" and refundable > Decimal("0.00"):
        refundable = max(Decimal("0.00"), refundable - RETURN_SHIPPING_DEDUCTION)
        notes.append("return_shipping_deducted")

    return {
        "decision": decision, "notes": notes,
        "refundable_total": refundable.quantize(CENTS, rounding=ROUND_HALF_UP),
        "eligible_line_count": len(eligible_lines),
    }


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

    try:
        body = _parse_body(event)
    except (ValueError, TypeError) as exc:
        logger.error("unparseable eligibility body: %s", exc)
        return _response(400, {"message": "malformed JSON body"})

    headers = event.get("headers") or {}
    order_id = str(body.get("order_id", "")).strip()
    declared_condition = str(body.get("condition", "unopened"))
    requested_skus = {str(sku) for sku in (body.get("skus") or []) if sku}
    if not order_id:
        return _response(400, {"message": "order_id is required"})

    try:
        order = load_order(order_id)
    except ClientError:
        return _response(502, {"message": "order lookup failed"})
    if not order:
        return _response(404, {"message": "order not found"})

    caller = str(headers.get("X-Customer-Id") or body.get("customer_id") or "")
    customer_id = str(order.get("customer_id", ""))
    if caller and caller != customer_id:
        logger.warning("customer mismatch order=%s caller=%s", order_id, caller)

    now = int(time.time())
    order_age_days = days_since(int(order.get("created_at", 0) or 0), now)
    lines = [
        line for line in (order.get("lines") or [])
        if not requested_skus or str(line.get("sku")) in requested_skus
    ]
    if not lines:
        return _response(422, {"message": "no matching lines on order"})

    line_results = [evaluate_line(line, order_age_days, declared_condition) for line in lines]
    order_count, abuse_rate = prior_return_stats(customer_id)
    outcome = decide(line_results, order_count, abuse_rate)

    logger.info(
        "return eligibility order=%s decision=%s refundable=%s abuse_rate=%s age_days=%s",
        order_id, outcome["decision"], outcome["refundable_total"], abuse_rate, order_age_days,
    )
    return _response(200, {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_age_days": order_age_days,
        "prior_order_count": order_count,
        "abuse_rate": abuse_rate,
        "decision": outcome["decision"],
        "refundable_total": outcome["refundable_total"],
        "notes": outcome["notes"],
        "lines": line_results,
    })
