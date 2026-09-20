"""Usage metering rollup.

Event source: EventBridge scheduled rule (``cron(15 * * * ? *)``).

Walks the raw usage table for the billing period, converts raw meter readings into
billable units, applies the graduated tier rate card, draws the result down against
any prepaid commitment, computes overage in ``Decimal`` money and writes the
per-account billing summary used by invoicing.
"""

import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")
resource = boto3.resource("dynamodb")

USAGE_TABLE = os.environ.get("USAGE_TABLE", "analytics-raw-usage")
BILLING_TABLE = os.environ.get("BILLING_TABLE", "analytics-billing-summary")
COMMITMENT_TABLE = os.environ.get("COMMITMENT_TABLE", "analytics-commitments")

SCAN_PAGE_LIMIT = 500
CENT = Decimal("0.01")
MICRO = Decimal("0.000001")
UNITS_PER_BILLABLE = {
    "api_calls": Decimal("1000"),
    "storage_gb_hours": Decimal("744"),
    "events_ingested": Decimal("10000"),
    "compute_seconds": Decimal("3600"),
}
RATE_CARD: Dict[str, List[Tuple[Optional[Decimal], Decimal]]] = {
    "api_calls": [(Decimal("50"), Decimal("0.90")), (Decimal("500"), Decimal("0.65")),
                  (Decimal("5000"), Decimal("0.40")), (None, Decimal("0.25"))],
    "storage_gb_hours": [(Decimal("100"), Decimal("0.023")), (Decimal("1000"), Decimal("0.019")),
                         (None, Decimal("0.015"))],
    "events_ingested": [(Decimal("100"), Decimal("1.20")), (Decimal("2000"), Decimal("0.85")),
                        (None, Decimal("0.55"))],
    "compute_seconds": [(Decimal("500"), Decimal("0.08")), (None, Decimal("0.055"))],
}
DEFAULT_RATE = Decimal("1.00")
OVERAGE_SURCHARGE_RATE = Decimal("1.15")
MINIMUM_INVOICE_AMOUNT = Decimal("5.00")


def _period_bounds(now: int) -> Tuple[int, int, str]:
    parts = time.gmtime(now)
    period_start = int(time.mktime((parts.tm_year, parts.tm_mon, 1, 0, 0, 0, 0, 1, 0)))
    label = "{0:04d}-{1:02d}".format(parts.tm_year, parts.tm_mon)
    return period_start, now, label


def _scan_usage(period_start: int, period_end: int) -> Dict[str, Dict[str, Decimal]]:
    """Accumulate raw meter readings per account using a LastEvaluatedKey walk."""
    totals: Dict[str, Dict[str, Decimal]] = {}
    last_evaluated_key: Optional[Dict[str, Any]] = None

    while True:
        request: Dict[str, Any] = {
            "TableName": USAGE_TABLE,
            "FilterExpression": "usage_ts BETWEEN :lo AND :hi",
            "ExpressionAttributeValues": {
                ":lo": {"N": str(period_start)},
                ":hi": {"N": str(period_end)},
            },
            "Limit": SCAN_PAGE_LIMIT,
        }
        if last_evaluated_key:
            request["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**request)
        for item in response.get("Items", []):
            account_id = item.get("account_id", {}).get("S")
            meter = item.get("meter", {}).get("S")
            quantity_raw = item.get("quantity", {}).get("N")
            if not account_id or not meter or quantity_raw is None:
                continue
            bucket = totals.setdefault(account_id, {})
            bucket[meter] = bucket.get(meter, Decimal("0")) + Decimal(quantity_raw)

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    logger.info("usage_scanned accounts=%s", len(totals))
    return totals


def _billable_units(meter: str, raw_quantity: Decimal) -> Decimal:
    divisor = UNITS_PER_BILLABLE.get(meter, Decimal("1"))
    return (raw_quantity / divisor).quantize(MICRO, rounding=ROUND_HALF_UP)


def _tiered_charge(meter: str, units: Decimal) -> Tuple[Decimal, List[Dict[str, str]]]:
    """Apply the graduated rate card: each tier prices only the units inside it."""
    tiers = RATE_CARD.get(meter)
    if not tiers:
        charge = (units * DEFAULT_RATE).quantize(CENT, rounding=ROUND_HALF_UP)
        return charge, [{"tier": "flat", "units": str(units), "rate": str(DEFAULT_RATE)}]

    remaining = units
    consumed = Decimal("0")
    total = Decimal("0")
    breakdown: List[Dict[str, str]] = []

    for upper_bound, rate in tiers:
        if remaining <= 0:
            break
        if upper_bound is None:
            tier_units = remaining
        else:
            tier_capacity = upper_bound - consumed
            if tier_capacity <= 0:
                continue
            tier_units = min(remaining, tier_capacity)
        tier_charge = (tier_units * rate).quantize(CENT, rounding=ROUND_HALF_UP)
        total += tier_charge
        remaining -= tier_units
        consumed += tier_units
        breakdown.append({
            "upper_bound": "unbounded" if upper_bound is None else str(upper_bound),
            "units": str(tier_units),
            "rate": str(rate),
            "charge": str(tier_charge),
        })
    return total.quantize(CENT, rounding=ROUND_HALF_UP), breakdown


def _load_commitment(account_id: str, period_label: str) -> Decimal:
    table = resource.Table(COMMITMENT_TABLE)
    try:
        response = table.get_item(Key={"account_id": account_id, "period": period_label})
    except ClientError as exc:
        logger.warning("commitment_load_failed account=%s error=%s", account_id, exc)
        return Decimal("0")
    item = response.get("Item") or {}
    return Decimal(str(item.get("commitment_amount", "0")))


def _apply_commitment(
    gross: Decimal, commitment: Decimal
) -> Tuple[Decimal, Decimal, Decimal]:
    """Draw the gross charge down against the commitment and surcharge the overage."""
    drawdown = min(gross, commitment)
    remaining_commitment = (commitment - drawdown).quantize(CENT, rounding=ROUND_HALF_UP)
    raw_overage = gross - drawdown
    overage = (raw_overage * OVERAGE_SURCHARGE_RATE).quantize(CENT, rounding=ROUND_HALF_UP)
    return drawdown.quantize(CENT, rounding=ROUND_HALF_UP), remaining_commitment, overage


def _persist(account_id: str, period_label: str, summary: Dict[str, Any]) -> None:
    table = resource.Table(BILLING_TABLE)
    table.put_item(Item={
        "account_id": account_id,
        "period": period_label,
        "gross_charge": str(summary["gross_charge"]),
        "commitment_drawdown": str(summary["commitment_drawdown"]),
        "commitment_remaining": str(summary["commitment_remaining"]),
        "overage_charge": str(summary["overage_charge"]),
        "invoice_amount": str(summary["invoice_amount"]),
        "meters": summary["meters"],
        "computed_at": int(time.time()),
    })


def lambda_handler(event, context):
    now = int(time.time())
    period_start, period_end, period_label = _period_bounds(now)
    logger.info("metering_start period=%s source=%s", period_label, event.get("source"))

    try:
        usage = _scan_usage(period_start, period_end)
    except ClientError as exc:
        logger.error("usage_scan_failed error=%s", exc)
        raise

    invoiced: List[Dict[str, Any]] = []
    total_billed = Decimal("0")

    for account_id, meters in usage.items():
        gross = Decimal("0")
        meter_rows: Dict[str, Any] = {}
        for meter, raw_quantity in meters.items():
            try:
                units = _billable_units(meter, raw_quantity)
                charge, breakdown = _tiered_charge(meter, units)
            except (ArithmeticError, TypeError) as exc:
                logger.warning("meter_rating_failed account=%s meter=%s error=%s",
                               account_id, meter, exc)
                continue
            gross += charge
            meter_rows[meter] = {
                "raw_quantity": str(raw_quantity),
                "billable_units": str(units),
                "charge": str(charge),
                "tiers": breakdown,
            }

        commitment = _load_commitment(account_id, period_label)
        drawdown, remaining, overage = _apply_commitment(gross, commitment)
        invoice_amount = overage if overage >= MINIMUM_INVOICE_AMOUNT else Decimal("0.00")

        summary = {
            "gross_charge": gross.quantize(CENT, rounding=ROUND_HALF_UP),
            "commitment_drawdown": drawdown,
            "commitment_remaining": remaining,
            "overage_charge": overage,
            "invoice_amount": invoice_amount,
            "meters": meter_rows,
        }

        try:
            _persist(account_id, period_label, summary)
        except ClientError as exc:
            logger.error("billing_persist_failed account=%s error=%s", account_id, exc)
            continue

        total_billed += invoice_amount
        invoiced.append({"account_id": account_id, "invoice_amount": str(invoice_amount),
                         "gross_charge": str(summary["gross_charge"])})

    logger.info("metering_complete period=%s accounts=%s total=%s",
                period_label, len(invoiced), total_billed)
    return {"period": period_label, "accounts": len(invoiced),
            "total_billed": str(total_billed), "invoices": invoiced}
