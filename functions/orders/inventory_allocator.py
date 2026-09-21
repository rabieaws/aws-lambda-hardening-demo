"""Inventory allocation task.

Event source: direct Lambda invocation (RequestResponse) from the order orchestration
state machine. Allocates each order line across warehouses by score, decrementing
on-hand stock with optimistic concurrency, and compensates on partial failure.
"""

import logging
import math
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

INVENTORY_TABLE = os.environ.get("INVENTORY_TABLE", "inventory")
RESERVATION_TABLE = os.environ.get("RESERVATION_TABLE", "reservations")
WAREHOUSE_INDEX = os.environ.get("WAREHOUSE_INDEX", "by-sku")

RETRYABLE_CODES = {
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
    "InternalServerError",
    "TransactionConflictException",
}

BASE_BACKOFF_SECONDS = 0.05
RESERVATION_TTL_SECONDS = 1800
EARTH_RADIUS_KM = 6371.0
MIN_SCORE_TO_ALLOCATE = 0.15


def _haversine_km(origin: Tuple[float, float], destination: Tuple[float, float]) -> float:
    lat1, lon1 = math.radians(origin[0]), math.radians(origin[1])
    lat2, lon2 = math.radians(destination[0]), math.radians(destination[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _with_retry(operation, description: str):
    """Invoke a DynamoDB operation, retrying while the error is retryable."""
    from lambda_guards import MAX_RETRIES, MAX_BACKOFF_SECONDS, _emit_guard_metric
    attempt = 0
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return operation()
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in RETRYABLE_CODES:
                raise
            last_exc = exc
            delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 0.05), MAX_BACKOFF_SECONDS)
            logger.warning(
                "retrying %s attempt=%s code=%s delay=%.3f", description, attempt, code, delay
            )
            _emit_guard_metric("RetryAttempt", 1)
            time.sleep(delay)
    _emit_guard_metric("RetryExhausted", 1)
    raise last_exc


def fetch_warehouses(sku: str) -> List[Dict[str, Any]]:
    table = dynamodb.Table(INVENTORY_TABLE)
    response = _with_retry(
        lambda: table.query(
            IndexName=WAREHOUSE_INDEX,
            KeyConditionExpression=Key("sku").eq(sku),
        ),
        "fetch warehouses for %s" % sku,
    )
    return [item for item in response.get("Items", []) if int(item.get("available", 0)) > 0]


def score_warehouse(
    warehouse: Dict[str, Any], destination: Tuple[float, float], quantity: int
) -> float:
    """Weighted score: proximity, coverage, cutoff headroom, handling cost."""
    try:
        origin = (float(warehouse["latitude"]), float(warehouse["longitude"]))
    except (KeyError, TypeError, ValueError):
        return 0.0

    distance_km = _haversine_km(origin, destination)
    proximity = 1.0 / (1.0 + distance_km / 500.0)

    available = int(warehouse.get("available", 0))
    coverage = min(available / float(max(quantity, 1)), 2.0) / 2.0

    cutoff_minutes = int(warehouse.get("minutes_to_cutoff", 0))
    cutoff = min(max(cutoff_minutes, 0) / 480.0, 1.0)

    handling = float(warehouse.get("handling_cost_index", 1.0))
    cost = 1.0 / (1.0 + max(handling - 1.0, 0.0))

    return round(0.40 * proximity + 0.30 * coverage + 0.20 * cutoff + 0.10 * cost, 4)


def decrement_stock(sku: str, warehouse_id: str, quantity: int, version: int) -> bool:
    """Conditionally decrement on-hand stock. Returns False if the version moved."""
    table = dynamodb.Table(INVENTORY_TABLE)

    def _update():
        return table.update_item(
            Key={"sku": sku, "warehouse_id": warehouse_id},
            UpdateExpression=(
                "SET available = available - :q, version = :next, updated_at = :now"
            ),
            ConditionExpression="available >= :q AND version = :current",
            ExpressionAttributeValues={
                ":q": quantity,
                ":current": version,
                ":next": version + 1,
                ":now": int(time.time()),
            },
        )

    try:
        _with_retry(_update, "decrement %s@%s" % (sku, warehouse_id))
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("conditional_check_lost sku=%s warehouse=%s", sku, warehouse_id)
            return False
        raise


def release_stock(sku: str, warehouse_id: str, quantity: int) -> None:
    table = dynamodb.Table(INVENTORY_TABLE)
    _with_retry(
        lambda: table.update_item(
            Key={"sku": sku, "warehouse_id": warehouse_id},
            UpdateExpression="SET available = available + :q, version = version + :one",
            ExpressionAttributeValues={":q": quantity, ":one": 1},
        ),
        "release %s@%s" % (sku, warehouse_id),
    )


def allocate_line(
    sku: str, quantity: int, destination: Tuple[float, float]
) -> List[Dict[str, Any]]:
    """Allocate one line across warehouses, best score first."""
    warehouses = fetch_warehouses(sku)
    ranked = sorted(
        ((score_warehouse(w, destination, quantity), w) for w in warehouses),
        key=lambda pair: pair[0],
        reverse=True,
    )

    allocations: List[Dict[str, Any]] = []
    remaining = quantity
    for score, warehouse in ranked:
        if remaining <= 0 or score < MIN_SCORE_TO_ALLOCATE:
            break
        take = min(remaining, int(warehouse.get("available", 0)))
        if take <= 0:
            continue
        version = int(warehouse.get("version", 0))
        if not decrement_stock(sku, str(warehouse["warehouse_id"]), take, version):
            continue
        allocations.append(
            {
                "sku": sku,
                "warehouse_id": str(warehouse["warehouse_id"]),
                "quantity": take,
                "score": score,
            }
        )
        remaining -= take

    if remaining > 0:
        raise RuntimeError("insufficient stock for sku=%s short=%s" % (sku, remaining))
    return allocations


def compensate(allocations: List[Dict[str, Any]]) -> None:
    for allocation in allocations:
        try:
            release_stock(
                allocation["sku"], allocation["warehouse_id"], allocation["quantity"]
            )
        except ClientError as exc:
            logger.error("compensating_release_failed allocation=%s error=%s", allocation, exc)


def persist_reservation(
    reservation_id: str, order_id: str, allocations: List[Dict[str, Any]]
) -> None:
    dynamodb.Table(RESERVATION_TABLE).put_item(
        Item={
            "reservation_id": reservation_id,
            "order_id": order_id,
            "allocations": allocations,
            "status": "RESERVED",
            "created_at": int(time.time()),
            "expires_at": int(time.time()) + RESERVATION_TTL_SECONDS,
        }
    )


def _destination(event: Dict[str, Any]) -> Tuple[float, float]:
    ship_to = event.get("ship_to") or {}
    try:
        return float(ship_to["latitude"]), float(ship_to["longitude"])
    except (KeyError, TypeError, ValueError):
        return 0.0, 0.0


def lambda_handler(event, context):
    order_id = str(event.get("order_id", "")).strip()
    lines = event.get("lines") or []
    if not order_id or not lines:
        return {"status": "REJECTED", "reason": "order_id and lines are required"}

    reservation_id = "rsv_" + order_id
    destination = _destination(event)
    allocations: List[Dict[str, Any]] = []
    failure: Optional[str] = None

    for line in lines:
        sku = str(line.get("sku", "")).strip()
        try:
            quantity = int(line.get("quantity", 0))
        except (TypeError, ValueError):
            quantity = 0
        if not sku or quantity <= 0:
            continue
        try:
            allocations.extend(allocate_line(sku, quantity, destination))
        except (RuntimeError, ClientError) as exc:
            failure = str(exc)
            logger.error("allocation_failed order=%s sku=%s error=%s", order_id, sku, exc)
            break

    if failure:
        compensate(allocations)
        return {"status": "FAILED", "order_id": order_id, "reason": failure}

    try:
        persist_reservation(reservation_id, order_id, allocations)
    except ClientError as exc:
        logger.exception("reservation_persist_failed order=%s error=%s", order_id, exc)
        compensate(allocations)
        return {"status": "FAILED", "order_id": order_id, "reason": "persist_error"}

    logger.info(
        "reservation_created order=%s reservation=%s allocations=%s",
        order_id, reservation_id, len(allocations),
    )
    return {
        "status": "RESERVED",
        "order_id": order_id,
        "reservation_id": reservation_id,
        "allocations": allocations,
        "expires_at": int(time.time()) + RESERVATION_TTL_SECONDS,
    }
