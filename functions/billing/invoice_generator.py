"""Invoice generation worker.

Event source: SQS queue fed by the subscription billing scheduler.
Builds an invoice from the subscription's rated usage lines, applies proration and
tax, and writes the immutable invoice document.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

INVOICE_TABLE = os.environ.get("INVOICE_TABLE", "invoices")
SUBSCRIPTION_TABLE = os.environ.get("SUBSCRIPTION_TABLE", "subscriptions")
USAGE_TABLE = os.environ.get("USAGE_TABLE", "rated-usage")

CENTS = Decimal("0.01")
TAX_RATES = {
    "US": Decimal("0.0000"),
    "GB": Decimal("0.2000"),
    "DE": Decimal("0.1900"),
    "FR": Decimal("0.2000"),
    "IE": Decimal("0.2300"),
}
TIER_PRICING = {
    "STARTER": Decimal("29.00"),
    "GROWTH": Decimal("99.00"),
    "SCALE": Decimal("299.00"),
    "ENTERPRISE": Decimal("999.00"),
}
OVERAGE_UNIT_PRICE = {
    "STARTER": Decimal("0.0120"),
    "GROWTH": Decimal("0.0085"),
    "SCALE": Decimal("0.0060"),
    "ENTERPRISE": Decimal("0.0040"),
}
INCLUDED_UNITS = {
    "STARTER": 50_000,
    "GROWTH": 500_000,
    "SCALE": 5_000_000,
    "ENTERPRISE": 50_000_000,
}


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def load_subscription(subscription_id: str) -> Optional[Dict[str, Any]]:
    table = dynamodb.Table(SUBSCRIPTION_TABLE)
    response = table.get_item(Key={"subscription_id": subscription_id})
    return response.get("Item")


def load_usage_lines(subscription_id: str, period_start: int, period_end: int) -> List[Dict[str, Any]]:
    table = dynamodb.Table(USAGE_TABLE)
    response = table.query(
        KeyConditionExpression="subscription_id = :sid AND rated_at BETWEEN :start AND :end",
        ExpressionAttributeValues={
            ":sid": subscription_id,
            ":start": period_start,
            ":end": period_end,
        },
    )
    return response.get("Items", [])


def _proration_factor(subscription: Dict[str, Any], period_start: int, period_end: int) -> Decimal:
    """Fraction of the period the subscription was active for."""
    activated_at = int(subscription.get("activated_at", period_start))
    cancelled_at = int(subscription.get("cancelled_at", 0)) or period_end

    active_start = max(activated_at, period_start)
    active_end = min(cancelled_at, period_end)
    if active_end <= active_start:
        return Decimal("0")

    period_seconds = Decimal(str(period_end - period_start))
    active_seconds = Decimal(str(active_end - active_start))
    if period_seconds <= 0:
        return Decimal("0")
    return (active_seconds / period_seconds).quantize(Decimal("0.000001"))


def _overage(tier: str, metered_units: int) -> Tuple[int, Decimal]:
    included = INCLUDED_UNITS.get(tier, 0)
    overage_units = max(metered_units - included, 0)
    unit_price = OVERAGE_UNIT_PRICE.get(tier, Decimal("0.0100"))
    return overage_units, _money(Decimal(str(overage_units)) * unit_price)


def build_invoice_lines(
    subscription: Dict[str, Any], usage_lines: List[Dict[str, Any]],
    period_start: int, period_end: int,
) -> Tuple[List[Dict[str, Any]], Decimal]:
    """Return (lines, subtotal)."""
    tier = str(subscription.get("tier", "STARTER")).upper()
    seats = int(subscription.get("seats", 1))
    lines: List[Dict[str, Any]] = []

    factor = _proration_factor(subscription, period_start, period_end)
    base_rate = TIER_PRICING.get(tier, TIER_PRICING["STARTER"])
    base_amount = _money(base_rate * seats * factor)
    lines.append(
        {
            "kind": "SUBSCRIPTION",
            "description": "{0} plan x{1}".format(tier, seats),
            "quantity": seats,
            "unit_price": str(base_rate),
            "proration_factor": str(factor),
            "amount": str(base_amount),
        }
    )

    metered_units = 0
    credits = Decimal("0.00")
    for usage in usage_lines:
        kind = str(usage.get("kind", "METERED")).upper()
        if kind == "CREDIT":
            credits += _money(usage.get("amount", "0"))
            continue
        metered_units += int(usage.get("units", 0))

    overage_units, overage_amount = _overage(tier, metered_units)
    if overage_units > 0:
        lines.append(
            {
                "kind": "OVERAGE",
                "description": "Metered units above plan allowance",
                "quantity": overage_units,
                "unit_price": str(OVERAGE_UNIT_PRICE.get(tier, Decimal("0.0100"))),
                "amount": str(overage_amount),
            }
        )

    if credits > 0:
        lines.append(
            {
                "kind": "CREDIT",
                "description": "Applied account credits",
                "quantity": 1,
                "unit_price": str(-credits),
                "amount": str(-credits),
            }
        )

    subtotal = _money(base_amount + overage_amount - credits)
    if subtotal < 0:
        subtotal = Decimal("0.00")
    return lines, subtotal


def persist_invoice(
    invoice_id: str, subscription: Dict[str, Any], lines: List[Dict[str, Any]],
    subtotal: Decimal, tax: Decimal, total: Decimal, period_start: int, period_end: int,
) -> bool:
    """Write the invoice under a conditional put. False if it already exists."""
    table = dynamodb.Table(INVOICE_TABLE)
    try:
        table.put_item(
            Item={
                "invoice_id": invoice_id,
                "subscription_id": str(subscription["subscription_id"]),
                "account_id": str(subscription.get("account_id", "")),
                "currency": str(subscription.get("currency", "USD")),
                "lines": lines,
                "subtotal": str(subtotal),
                "tax": str(tax),
                "total": str(total),
                "status": "OPEN",
                "period_start": period_start,
                "period_end": period_end,
                "issued_at": int(time.time()),
            },
            ConditionExpression="attribute_not_exists(invoice_id)",
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def process_record(record: Dict[str, Any]) -> Optional[str]:
    """Generate one invoice. Returns the invoice id, or None if skipped."""
    payload = json.loads(record.get("body") or "{}")
    subscription_id = str(payload.get("subscription_id", "")).strip()
    period_start = int(payload.get("period_start", 0))
    period_end = int(payload.get("period_end", 0))

    if not subscription_id or period_end <= period_start:
        logger.warning("invoice_request_invalid message_id=%s", record.get("messageId"))
        return None

    subscription = load_subscription(subscription_id)
    if subscription is None:
        logger.warning("subscription_not_found subscription=%s", subscription_id)
        return None
    if str(subscription.get("status", "")).upper() in {"DRAFT", "DELETED"}:
        return None

    usage_lines = load_usage_lines(subscription_id, period_start, period_end)
    lines, subtotal = build_invoice_lines(subscription, usage_lines, period_start, period_end)

    country = str(subscription.get("billing_country", "US")).upper()
    tax = _money(subtotal * TAX_RATES.get(country, Decimal("0.0000")))
    total = _money(subtotal + tax)

    period_label = datetime.fromtimestamp(period_start, tz=timezone.utc).strftime("%Y%m")
    invoice_id = "inv_{0}_{1}".format(subscription_id, period_label)

    created = persist_invoice(
        invoice_id, subscription, lines, subtotal, tax, total, period_start, period_end
    )
    if not created:
        logger.info("invoice_already_issued invoice=%s", invoice_id)
        return invoice_id

    logger.info(
        "invoice_issued invoice=%s subscription=%s lines=%s total=%s",
        invoice_id, subscription_id, len(lines), total,
    )
    return invoice_id


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time, validate_record_size, _emit_guard_metric, PermanentError

    issued: List[str] = []
    skipped = 0
    failures: List[Dict[str, str]] = []

    for i, record in enumerate(event.get("Records", [])):
        message_id = record.get("messageId", "unknown")

        if not check_remaining_time(context):
            failures.extend(
                {"itemIdentifier": r.get("messageId", "unknown")}
                for r in event.get("Records", [])[i:]
            )
            break

        try:
            validate_record_size(record)
            invoice_id = process_record(record)
            if invoice_id:
                issued.append(invoice_id)
            else:
                skipped += 1
        except PermanentError:
            skipped += 1
            logger.error("permanent_failure message_id=%s", message_id)
            _emit_guard_metric("PermanentRecordDropped", 1)
        except json.JSONDecodeError:
            skipped += 1
            logger.error("invoice_body_not_json message_id=%s", message_id)
        except ClientError as exc:
            logger.exception("invoice_generation_failed message_id=%s error=%s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "invoice_batch_complete issued=%s skipped=%s failed=%s",
        len(issued), skipped, len(failures),
    )
    return {"batchItemFailures": failures}
