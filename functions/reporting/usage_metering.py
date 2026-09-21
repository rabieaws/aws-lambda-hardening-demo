"""Billable usage metering rollup.

Event source: EventBridge scheduled rule (hourly).
Aggregates raw metered events into billable units per account, applies the tiered
rate card, and writes the rated usage rows the invoice generator bills from.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

EVENT_TABLE = os.environ.get("EVENT_TABLE", "metered-events")
RATED_TABLE = os.environ.get("RATED_TABLE", "rated-usage")
ACCOUNT_TABLE = os.environ.get("ACCOUNT_TABLE", "accounts")
EVENT_INDEX = os.environ.get("EVENT_INDEX", "by-window")
SCAN_PAGE_SIZE = int(os.environ.get("SCAN_PAGE_SIZE", "250"))

CENTS = Decimal("0.01")
MICRO = Decimal("0.000001")

# metric -> list of (upper_bound_units, price_per_unit). None means unbounded.
RATE_CARD: Dict[str, List[Tuple[Optional[int], Decimal]]] = {
    "api_requests": [
        (1_000_000, Decimal("0.0000040")),
        (10_000_000, Decimal("0.0000030")),
        (None, Decimal("0.0000022")),
    ],
    "gb_egress": [
        (1_000, Decimal("0.0900")),
        (10_000, Decimal("0.0850")),
        (None, Decimal("0.0700")),
    ],
    "compute_seconds": [
        (100_000, Decimal("0.0000180")),
        (None, Decimal("0.0000150")),
    ],
    "storage_gb_hours": [
        (None, Decimal("0.0000320")),
    ],
}

COMMITTED_DISCOUNT = {
    "NONE": Decimal("0.00"),
    "ANNUAL": Decimal("0.10"),
    "MULTI_YEAR": Decimal("0.18"),
}


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _micro(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(MICRO, rounding=ROUND_HALF_UP)


def _window(now: int) -> Tuple[int, int, str]:
    end = datetime.fromtimestamp(now, tz=timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    start = end - timedelta(hours=1)
    return int(start.timestamp()), int(end.timestamp()), start.strftime("%Y-%m-%dT%H")


def iter_metered_events(start_epoch: int, end_epoch: int) -> Iterator[Dict[str, Any]]:
    """Yield every metered event in the window."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("scan")
    for page in safe_paginate(paginator,
        TableName=EVENT_TABLE,
        FilterExpression="emitted_at >= :start AND emitted_at < :end AND attribute_not_exists(rated)",
        ExpressionAttributeValues={
            ":start": {"N": str(start_epoch)},
            ":end": {"N": str(end_epoch)},
        },
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    ):
        for item in page.get("Items", []):
            yield item


def aggregate(events: Iterator[Dict[str, Any]]) -> Dict[Tuple[str, str], int]:
    """Sum raw units per (account_id, metric)."""
    totals: Dict[Tuple[str, str], int] = {}
    for item in events:
        account_id = item.get("account_id", {}).get("S")
        metric = item.get("metric", {}).get("S")
        raw_units = item.get("units", {}).get("N")
        if not account_id or not metric or raw_units is None:
            continue
        if metric not in RATE_CARD:
            logger.warning("unrated_metric metric=%s account=%s", metric, account_id)
            continue
        try:
            units = int(Decimal(raw_units))
        except (TypeError, ValueError):
            continue
        key = (account_id, metric)
        totals[key] = totals.get(key, 0) + max(units, 0)
    return totals


def load_account(account_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(ACCOUNT_TABLE)
    try:
        response = table.get_item(Key={"account_id": account_id})
    except ClientError as exc:
        logger.error("account_read_failed account=%s error=%s", account_id, exc)
        return {}
    return response.get("Item") or {}


def month_to_date_units(account_id: str, metric: str, window_start: int) -> int:
    """Units already rated this calendar month, which sets the tier entry point."""
    month_start = int(
        datetime.fromtimestamp(window_start, tz=timezone.utc)
        .replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        .timestamp()
    )
    table = dynamodb.Table(RATED_TABLE)
    try:
        response = table.query(
            KeyConditionExpression="account_id = :aid AND rated_at BETWEEN :start AND :end",
            FilterExpression="metric = :metric",
            ExpressionAttributeValues={
                ":aid": account_id,
                ":start": month_start,
                ":end": window_start,
                ":metric": metric,
            },
        )
    except ClientError as exc:
        logger.error("mtd_query_failed account=%s metric=%s error=%s", account_id, metric, exc)
        return 0

    total = 0
    for item in response.get("Items", []):
        try:
            total += int(item.get("units", 0))
        except (TypeError, ValueError):
            continue
    return total


def rate_units(metric: str, units: int, already_billed: int) -> Tuple[Decimal, List[Dict[str, Any]]]:
    """Apply the tiered rate card, entering at the account's month-to-date position."""
    tiers = RATE_CARD[metric]
    remaining = units
    position = already_billed
    amount = Decimal("0")
    breakdown: List[Dict[str, Any]] = []

    for upper_bound, price in tiers:
        if remaining <= 0:
            break
        if upper_bound is None:
            tier_units = remaining
        else:
            headroom = upper_bound - position
            if headroom <= 0:
                continue
            tier_units = min(remaining, headroom)

        tier_amount = _micro(Decimal(str(tier_units)) * price)
        amount += tier_amount
        breakdown.append(
            {
                "upper_bound": upper_bound,
                "units": tier_units,
                "unit_price": str(price),
                "amount": str(tier_amount),
            }
        )
        remaining -= tier_units
        position += tier_units

    return amount, breakdown


def persist_rated_usage(
    account_id: str, metric: str, units: int, amount: Decimal,
    breakdown: List[Dict[str, Any]], window_label: str, window_start: int,
) -> None:
    dynamodb.Table(RATED_TABLE).put_item(
        Item={
            "account_id": account_id,
            "metric_window": "{0}#{1}".format(metric, window_label),
            "metric": metric,
            "units": units,
            "amount": _money(amount),
            "tier_breakdown": breakdown,
            "window": window_label,
            "rated_at": window_start,
            "kind": "METERED",
        }
    )


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time

    window_start, window_end, window_label = _window(int(time.time()))
    logger.info("metering_start window=%s", window_label)

    totals = aggregate(iter_metered_events(window_start, window_end))

    rated_rows = 0
    billed_total = Decimal("0.00")
    accounts_touched = set()

    for (account_id, metric), units in sorted(totals.items()):
        if not check_remaining_time(context):
            logger.warning("metering_time_remaining_low, stopping early")
            break
        if units <= 0:
            continue

        account = load_account(account_id)
        commitment = str(account.get("commitment", "NONE")).upper()
        discount = COMMITTED_DISCOUNT.get(commitment, Decimal("0.00"))

        already = month_to_date_units(account_id, metric, window_start)
        gross, breakdown = rate_units(metric, units, already)
        net = _money(gross * (Decimal("1") - discount))

        try:
            persist_rated_usage(
                account_id, metric, units, net, breakdown, window_label, window_start
            )
        except ClientError as exc:
            logger.error(
                "rated_usage_write_failed account=%s metric=%s error=%s",
                account_id, metric, exc,
            )
            continue

        rated_rows += 1
        billed_total += net
        accounts_touched.add(account_id)

    logger.info(
        "metering_complete window=%s accounts=%s rows=%s billed=%s",
        window_label, len(accounts_touched), rated_rows, billed_total,
    )
    return {
        "window": window_label,
        "accounts": len(accounts_touched),
        "rated_rows": rated_rows,
        "billed_total": str(billed_total),
    }
