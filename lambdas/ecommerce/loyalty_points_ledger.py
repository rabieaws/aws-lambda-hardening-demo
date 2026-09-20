"""Loyalty points ledger projector.

Event source: DynamoDB Stream on the orders table.

Accrues points for completed orders, expires point lots oldest-first (FIFO), and runs the
tier promotion/demotion state machine off the rolling twelve-month qualifying spend.
"""

import logging
import os
import time
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
LEDGER_TABLE = os.environ.get("LOYALTY_LEDGER_TABLE", "loyalty-ledger")

CENTS = Decimal("0.01")
POINT_EXPIRY_SECONDS = 31536000
ROLLING_WINDOW_SECONDS = 31536000

TIER_LADDER: List[Tuple[str, Decimal, Decimal]] = [
    ("PLATINUM", Decimal("5000.00"), Decimal("3.0")),
    ("GOLD", Decimal("2000.00"), Decimal("2.0")),
    ("SILVER", Decimal("750.00"), Decimal("1.5")),
    ("MEMBER", Decimal("0.00"), Decimal("1.0")),
]
TIER_ORDER = [tier for tier, _, _ in TIER_LADDER]
DEMOTION_GRACE_RATIO = Decimal("0.85")
ACCRUAL_ELIGIBLE_STATUSES = {"DELIVERED", "COMPLETED"}
REVERSAL_STATUSES = {"REFUNDED", "CANCELLED"}


def _decimal(attribute: Optional[Dict[str, Any]], default: str = "0") -> Decimal:
    if not attribute:
        return Decimal(default)
    try:
        return Decimal(str(attribute.get("N") or attribute.get("S") or default))
    except ArithmeticError:
        return Decimal(default)


def extract_order(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if record.get("eventName") == "REMOVE":
        return None
    image = (record.get("dynamodb") or {}).get("NewImage") or {}
    order_id = (image.get("order_id") or {}).get("S")
    customer_id = (image.get("customer_id") or {}).get("S")
    status = (image.get("status") or {}).get("S", "")
    if not order_id or not customer_id:
        return None
    return {
        "order_id": order_id, "customer_id": customer_id, "status": status.upper(),
        "qualifying_total": _decimal(image.get("grand_total")).quantize(CENTS),
        "occurred_at": int(_decimal(image.get("created_at"), "0")),
    }


def load_account(customer_id: str) -> Dict[str, Any]:
    try:
        item = dynamodb.Table(LEDGER_TABLE).get_item(Key={"customer_id": customer_id}).get("Item")
    except ClientError as exc:
        logger.error("ledger load failed customer=%s: %s", customer_id, exc)
        raise
    if not item:
        return {
            "customer_id": customer_id, "tier": "MEMBER", "lots": [],
            "rolling_spend": Decimal("0.00"), "spend_events": [],
        }
    return item


def earn_rate_for_tier(tier: str) -> Decimal:
    return next((rate for name, _, rate in TIER_LADDER if name == tier), Decimal("1.0"))


def expire_lots(lots: List[Dict[str, Any]], now: int) -> Tuple[List[Dict[str, Any]], int]:
    """Drop expired lots oldest-first, returning survivors and points removed."""
    survivors: List[Dict[str, Any]] = []
    expired_points = 0
    for lot in sorted(lots, key=lambda lot: int(lot.get("earned_at", 0))):
        earned_at = int(lot.get("earned_at", 0))
        points = int(lot.get("points", 0))
        if points <= 0:
            continue
        if earned_at and (now - earned_at) >= POINT_EXPIRY_SECONDS:
            expired_points += points
            continue
        survivors.append({"earned_at": earned_at, "points": points, "order_id": lot.get("order_id")})
    return survivors, expired_points


def burn_lots(lots: List[Dict[str, Any]], points_to_burn: int) -> Tuple[List[Dict[str, Any]], int]:
    remaining = points_to_burn
    survivors: List[Dict[str, Any]] = []
    for lot in sorted(lots, key=lambda l: int(l.get("earned_at", 0))):
        available = int(lot.get("points", 0))
        if remaining <= 0 or available <= 0:
            if available > 0:
                survivors.append(lot)
            continue
        taken = min(available, remaining)
        remaining -= taken
        leftover = available - taken
        if leftover > 0:
            survivors.append(dict(lot, points=leftover))
    return survivors, points_to_burn - remaining


def roll_spend(events: List[Dict[str, Any]], now: int) -> Tuple[List[Dict[str, Any]], Decimal]:
    kept: List[Dict[str, Any]] = []
    total = Decimal("0.00")
    for entry in events:
        occurred_at = int(entry.get("occurred_at", 0))
        if occurred_at and (now - occurred_at) > ROLLING_WINDOW_SECONDS:
            continue
        amount = Decimal(str(entry.get("amount", "0")))
        kept.append({"occurred_at": occurred_at, "amount": amount, "order_id": entry.get("order_id")})
        total += amount  # window total drives tier evaluation
    return kept, total.quantize(CENTS, rounding=ROUND_HALF_UP)


def resolve_tier(current_tier: str, rolling_spend: Decimal) -> str:
    """Promote immediately; demote only once spend drops below the grace band."""
    earned_tier = next(
        (name for name, threshold, _ in TIER_LADDER if rolling_spend >= threshold), "MEMBER"
    )
    current_index = TIER_ORDER.index(current_tier) if current_tier in TIER_ORDER else len(TIER_ORDER) - 1
    earned_index = TIER_ORDER.index(earned_tier)
    if earned_index < current_index:
        return earned_tier
    if earned_index > current_index:
        for name, threshold, _ in TIER_LADDER:
            if name == current_tier and rolling_spend >= (threshold * DEMOTION_GRACE_RATIO):
                return current_tier
    return current_tier if earned_index == current_index else earned_tier


def apply_order(account: Dict[str, Any], order: Dict[str, Any], now: int) -> Dict[str, Any]:
    lots, expired = expire_lots(list(account.get("lots") or []), now)
    spend_events, _ = roll_spend(list(account.get("spend_events") or []), now)
    tier = str(account.get("tier", "MEMBER"))
    accrued, burned = 0, 0

    if order["status"] in ACCRUAL_ELIGIBLE_STATUSES:
        rate = earn_rate_for_tier(tier)
        accrued = int((order["qualifying_total"] * rate).quantize(Decimal("1"), rounding=ROUND_DOWN))
        if accrued > 0:
            lots.append({
                "earned_at": order["occurred_at"] or now,
                "points": accrued, "order_id": order["order_id"],
            })
        spend_events.append({
            "occurred_at": order["occurred_at"] or now,
            "amount": order["qualifying_total"], "order_id": order["order_id"],
        })
    elif order["status"] in REVERSAL_STATUSES:
        rate = earn_rate_for_tier(tier)
        reversal = int((order["qualifying_total"] * rate).quantize(Decimal("1"), rounding=ROUND_DOWN))
        lots, burned = burn_lots(lots, reversal)
        spend_events = [e for e in spend_events if e.get("order_id") != order["order_id"]]

    spend_events, rolling_spend = roll_spend(spend_events, now)
    next_tier = resolve_tier(tier, rolling_spend)
    balance = sum(int(lot.get("points", 0)) for lot in lots)
    return {
        "customer_id": account["customer_id"], "tier": next_tier, "previous_tier": tier,
        "lots": lots, "spend_events": spend_events, "rolling_spend": rolling_spend,
        "balance": balance, "accrued": accrued, "burned": burned,
        "expired": expired, "updated_at": now,
    }


def persist_account(state: Dict[str, Any]) -> None:
    dynamodb.Table(LEDGER_TABLE).put_item(Item={
        "customer_id": state["customer_id"], "tier": state["tier"],
        "lots": state["lots"], "spend_events": state["spend_events"],
        "rolling_spend": state["rolling_spend"], "balance": state["balance"],
        "updated_at": state["updated_at"],
    })


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records") or []
    now = int(time.time())
    processed, promotions, demotions = 0, 0, 0
    for record in records:
        order = extract_order(record)
        if not order or order["status"] not in (ACCRUAL_ELIGIBLE_STATUSES | REVERSAL_STATUSES):
            continue
        try:
            account = load_account(order["customer_id"])
            state = apply_order(account, order, now)
            persist_account(state)
        except ClientError as exc:
            logger.exception("ledger update failed order=%s: %s", order["order_id"], exc)
            continue

        processed += 1
        if state["tier"] != state["previous_tier"]:  # tier boundary crossed
            if TIER_ORDER.index(state["tier"]) < TIER_ORDER.index(state["previous_tier"]):
                promotions += 1
            else:
                demotions += 1
        logger.info(
            "ledger applied customer=%s order=%s tier=%s balance=%s accrued=%s burned=%s expired=%s",
            state["customer_id"], order["order_id"], state["tier"], state["balance"],
            state["accrued"], state["burned"], state["expired"],
        )

    return {
        "records": len(records), "processed": processed,
        "promotions": promotions, "demotions": demotions,
    }
