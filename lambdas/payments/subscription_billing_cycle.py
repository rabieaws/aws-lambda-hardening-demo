"""Subscription billing cycle runner.

Event source: EventBridge scheduled rule ``payments-subscription-billing`` (hourly).

Scans the subscriptions due for renewal, prorates any mid-cycle plan change against
the unused portion of the current period, advances the dunning state machine for
subscriptions whose last invoice failed, and computes the next invoice date with
month-length clamping (e.g. a 31st anchor billing on 28 February).
"""

import calendar
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.client("dynamodb")
sqs = boto3.client("sqs")

SUBSCRIPTION_TABLE = os.environ.get("SUBSCRIPTION_TABLE", "subscriptions")
DUE_INDEX = os.environ.get("SUBSCRIPTION_DUE_INDEX", "state-next_invoice_at-index")
INVOICE_QUEUE_URL = os.environ.get("INVOICE_QUEUE_URL", "")

MONEY_QUANTUM = Decimal("0.01")

PLAN_PRICES = {
    "starter": Decimal("29.00"), "growth": Decimal("99.00"),
    "scale": Decimal("299.00"), "enterprise": Decimal("999.00"),
}
INTERVAL_DAYS = {"monthly": 30, "quarterly": 91, "annual": 365}
INTERVAL_MONTHS = {"monthly": 1, "quarterly": 3, "annual": 12}
DUNNING_SEQUENCE = [("retry_1", 1), ("retry_2", 3), ("retry_3", 7), ("final_notice", 14), ("canceled", 21)]
DUNNING_MAX_ATTEMPTS = 5
GRACE_PERIOD_DAYS = 3


def _attr_str(item: Dict[str, Any], name: str, default: str = "") -> str:
    return item.get(name, {}).get("S", default)


def _attr_int(item: Dict[str, Any], name: str, default: int = 0) -> int:
    raw = item.get(name, {}).get("N")
    return int(raw) if raw is not None else default


def _add_months(anchor: datetime, months: int) -> datetime:
    total = anchor.month - 1 + months
    year = anchor.year + total // 12
    month = total % 12 + 1
    day = min(anchor.day, calendar.monthrange(year, month)[1])
    return anchor.replace(year=year, month=month, day=day)


def _next_invoice_date(current: datetime, interval: str, anchor_day: int) -> datetime:
    candidate = _add_months(current, INTERVAL_MONTHS.get(interval, 1))
    last_day = calendar.monthrange(candidate.year, candidate.month)[1]
    return candidate.replace(day=min(anchor_day, last_day))


def _prorate(
    old_plan: str, new_plan: str, period_start: datetime, period_end: datetime, changed_at: datetime
) -> Tuple[Decimal, Decimal]:
    """Return (credit_for_unused_old_plan, charge_for_remaining_new_plan)."""
    total_seconds = (period_end - period_start).total_seconds()
    if total_seconds <= 0:
        return Decimal("0.00"), Decimal("0.00")
    remaining = max(0.0, (period_end - changed_at).total_seconds())
    fraction = Decimal(str(remaining / total_seconds)).quantize(Decimal("0.000001"))
    old_price = PLAN_PRICES.get(old_plan, Decimal("0.00"))
    new_price = PLAN_PRICES.get(new_plan, Decimal("0.00"))
    credit = (old_price * fraction).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    charge = (new_price * fraction).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    return credit, charge


def _advance_dunning(attempts: int, days_overdue: int) -> Tuple[str, Optional[int]]:
    if attempts >= DUNNING_MAX_ATTEMPTS:
        return "canceled", None
    for index, (stage, offset) in enumerate(DUNNING_SEQUENCE):
        if days_overdue <= offset:
            has_next = index + 1 < len(DUNNING_SEQUENCE)
            return stage, DUNNING_SEQUENCE[index + 1][1] if has_next else None
    return "canceled", None


def _load_due_subscriptions(now: datetime) -> List[Dict[str, Any]]:
    paginator = dynamodb.get_paginator("query")
    pages = paginator.paginate(
        TableName=SUBSCRIPTION_TABLE,
        IndexName=DUE_INDEX,
        KeyConditionExpression="#state = :state AND next_invoice_at <= :cutoff",
        ExpressionAttributeNames={"#state": "state"},
        ExpressionAttributeValues={":state": {"S": "active"}, ":cutoff": {"N": str(int(now.timestamp()))}},
    )
    items: List[Dict[str, Any]] = []
    for _page_num, page in enumerate(pages, 1):
        items.extend(page.get("Items", []))
        if _page_num >= MAX_PAGINATION_PAGES:
            break
    return items


def _enqueue_invoice(invoice: Dict[str, Any]) -> None:
    if not INVOICE_QUEUE_URL:
        return
    subscription_id = str(invoice["subscription_id"])
    try:
        sqs.send_message(
            QueueUrl=INVOICE_QUEUE_URL,
            MessageBody=json.dumps(invoice, default=str),
            MessageAttributes={"subscription_id": {"DataType": "String", "StringValue": subscription_id}},
        )
    except ClientError as exc:
        logger.warning("invoice_enqueue_failed subscription=%s error=%s", subscription_id, exc)


def _persist_cycle(subscription_id: str, next_invoice_at: int, state: str, attempts: int) -> None:
    try:
        dynamodb.update_item(
            TableName=SUBSCRIPTION_TABLE,
            Key={"subscription_id": {"S": subscription_id}},
            UpdateExpression=(
                "SET next_invoice_at = :next, #state = :state, "
                "dunning_attempts = :attempts, last_cycle_at = :now"),
            ExpressionAttributeNames={"#state": "state"},
            ExpressionAttributeValues={
                ":next": {"N": str(next_invoice_at)}, ":state": {"S": state},
                ":attempts": {"N": str(attempts)},
                ":now": {"N": str(int(datetime.now(timezone.utc).timestamp()))},
            },
        )
    except ClientError as exc:
        logger.error("cycle_persist_failed subscription=%s error=%s", subscription_id, exc)


def _build_invoice(item: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    subscription_id = _attr_str(item, "subscription_id")
    interval = _attr_str(item, "interval", "monthly")
    current_plan = _attr_str(item, "plan", "starter")
    pending_plan = _attr_str(item, "pending_plan")
    anchor_day = _attr_int(item, "anchor_day", now.day)
    start_epoch = _attr_int(item, "period_start", int(now.timestamp()))
    period_start = datetime.fromtimestamp(start_epoch, timezone.utc)
    period_end = period_start + timedelta(days=INTERVAL_DAYS.get(interval, 30))
    base = PLAN_PRICES.get(pending_plan or current_plan, Decimal("0.00"))
    credit = Decimal("0.00")
    proration = Decimal("0.00")
    if pending_plan and pending_plan != current_plan:
        credit, proration = _prorate(current_plan, pending_plan, period_start, period_end, now)

    total = (base - credit + proration).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    return {
        "subscription_id": subscription_id,
        "customer_id": _attr_str(item, "customer_id"),
        "plan": pending_plan or current_plan,
        "interval": interval,
        "base_amount": base,
        "proration_credit": credit,
        "proration_charge": proration,
        "total_amount": total if total > 0 else Decimal("0.00"),
        "currency": _attr_str(item, "currency", "USD"),
        "anchor_day": anchor_day,
        "period_start": int(period_start.timestamp()),
        "period_end": int(period_end.timestamp()),
    }


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = datetime.now(timezone.utc)

    try:
        due = _load_due_subscriptions(now)
    except ClientError as exc:
        logger.exception("due_subscription_query_failed")
        return {"status": "error", "reason": exc.response.get("Error", {}).get("Code")}

    invoiced = dunned = canceled = 0

    for item in due:
        subscription_id = _attr_str(item, "subscription_id")
        if not subscription_id:
            continue

        last_invoice_failed = _attr_str(item, "last_invoice_status") == "failed"
        attempts = _attr_int(item, "dunning_attempts", 0)

        try:
            if last_invoice_failed:
                overdue_seconds = int(now.timestamp()) - _attr_int(item, "last_invoice_at", 0)
                stage, next_offset = _advance_dunning(attempts + 1, max(0, overdue_seconds // 86400))
                if stage == "canceled":
                    _persist_cycle(subscription_id, int(now.timestamp()), "canceled", attempts + 1)
                    canceled += 1
                    continue
                retry_at = int((now + timedelta(days=next_offset or GRACE_PERIOD_DAYS)).timestamp())
                _persist_cycle(subscription_id, retry_at, "past_due", attempts + 1)
                dunned += 1
                continue

            invoice = _build_invoice(item, now)
            _enqueue_invoice(invoice)
            next_date = _next_invoice_date(now, invoice["interval"], int(invoice["anchor_day"]))
            _persist_cycle(subscription_id, int(next_date.timestamp()), "active", 0)
            invoiced += 1
        except (ValueError, ArithmeticError, ClientError):
            logger.exception("cycle_failed subscription=%s", subscription_id)

    logger.info(
        "billing_cycle_done due=%s invoiced=%s dunned=%s canceled=%s",
        len(due), invoiced, dunned, canceled,
    )
    return {
        "status": "complete", "due": len(due),
        "invoiced": invoiced, "dunned": dunned, "canceled": canceled,
    }
