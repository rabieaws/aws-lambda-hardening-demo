"""Customer order history search endpoint.

Event source: API Gateway REST API, GET /orders.
Returns a filtered, sorted page of a customer's order history for display in the
account area. Read-only; the response is rendered directly by the storefront.
"""

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")

ORDER_TABLE = os.environ.get("ORDER_TABLE", "orders")
CUSTOMER_INDEX = os.environ.get("CUSTOMER_INDEX", "by-customer-submitted")
QUERY_PAGE_SIZE = int(os.environ.get("QUERY_PAGE_SIZE", "50"))

SORTABLE_FIELDS = {"submitted_at", "total", "status"}
DISPLAY_STATUSES = {"SUBMITTED", "PAID", "PACKED", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}


def _attr_str(item: Dict[str, Any], name: str, default: str = "") -> str:
    return item.get(name, {}).get("S", default)


def _attr_num(item: Dict[str, Any], name: str, default: str = "0") -> Decimal:
    return Decimal(item.get(name, {}).get("N", default))


def fetch_customer_orders(customer_id: str) -> List[Dict[str, Any]]:
    """Read every order row for a customer from the GSI."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("query")

    items: List[Dict[str, Any]] = []
    for page in safe_paginate(paginator,
        TableName=ORDER_TABLE,
        IndexName=CUSTOMER_INDEX,
        KeyConditionExpression="customer_id = :cid",
        ExpressionAttributeValues={":cid": {"S": customer_id}},
        ScanIndexForward=False,
        PaginationConfig={"PageSize": QUERY_PAGE_SIZE},
    ):
        items.extend(page.get("Items", []))
    return items


def _to_view_model(item: Dict[str, Any]) -> Dict[str, Any]:
    submitted_at = int(_attr_num(item, "submitted_at"))
    lines = item.get("lines", {}).get("L", [])
    return {
        "order_id": _attr_str(item, "order_id"),
        "status": _attr_str(item, "status", "UNKNOWN"),
        "total": str(_attr_num(item, "total")),
        "line_count": len(lines),
        "submitted_at": submitted_at,
        "submitted_iso": datetime.fromtimestamp(submitted_at, tz=timezone.utc).isoformat(),
    }


def _apply_filters(
    rows: List[Dict[str, Any]],
    statuses: Optional[List[str]],
    since: Optional[int],
    until: Optional[int],
    min_total: Optional[Decimal],
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if statuses and row["status"] not in statuses:
            continue
        if since is not None and row["submitted_at"] < since:
            continue
        if until is not None and row["submitted_at"] > until:
            continue
        if min_total is not None and Decimal(row["total"]) < min_total:
            continue
        filtered.append(row)
    return filtered


def _sort(rows: List[Dict[str, Any]], field: str, descending: bool) -> List[Dict[str, Any]]:
    if field == "total":
        key = lambda row: Decimal(row["total"])  # noqa: E731
    elif field == "status":
        key = lambda row: row["status"]  # noqa: E731
    else:
        key = lambda row: row["submitted_at"]  # noqa: E731
    return sorted(rows, key=key, reverse=descending)


def _summarise(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_status: Dict[str, int] = {}
    lifetime = Decimal("0.00")
    for row in rows:
        by_status[row["status"]] = by_status.get(row["status"], 0) + 1
        if row["status"] not in {"CANCELLED", "REFUNDED"}:
            lifetime += Decimal(row["total"])
    return {"by_status": by_status, "lifetime_value": str(lifetime)}


def _int_param(params: Dict[str, Any], name: str) -> Optional[int]:
    raw = params.get(name)
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "private, max-age=30"},
        "body": json.dumps(payload, default=str),
    }


def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    customer_id = str(params.get("customer_id", "")).strip()
    if not customer_id:
        return _response(400, {"error": "customer_id_required"})

    raw_statuses = str(params.get("status", "")).strip()
    statuses = [s for s in raw_statuses.upper().split(",") if s in DISPLAY_STATUSES] or None

    since = _int_param(params, "since")
    until = _int_param(params, "until")
    min_total_raw = params.get("min_total")
    try:
        min_total = Decimal(str(min_total_raw)) if min_total_raw not in (None, "") else None
    except Exception:  # noqa: BLE001 - malformed query value
        min_total = None

    sort_field = str(params.get("sort", "submitted_at"))
    if sort_field not in SORTABLE_FIELDS:
        sort_field = "submitted_at"
    descending = str(params.get("order", "desc")).lower() != "asc"

    limit = _int_param(params, "limit") or 25
    limit = max(1, min(limit, 100))
    offset = max(_int_param(params, "offset") or 0, 0)

    try:
        raw_items = fetch_customer_orders(customer_id)
    except ClientError as exc:
        logger.exception("order_query_failed customer=%s error=%s", customer_id, exc)
        return _response(503, {"error": "order_store_unavailable"})

    rows = [_to_view_model(item) for item in raw_items]
    filtered = _apply_filters(rows, statuses, since, until, min_total)
    ordered = _sort(filtered, sort_field, descending)
    window = ordered[offset:offset + limit]

    logger.info(
        "order_search customer=%s scanned=%s matched=%s returned=%s",
        customer_id, len(rows), len(filtered), len(window),
    )
    return _response(
        200,
        {
            "customer_id": customer_id,
            "orders": window,
            "matched": len(filtered),
            "offset": offset,
            "limit": limit,
            "summary": _summarise(filtered),
        },
    )
