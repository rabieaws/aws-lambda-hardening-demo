"""Inventory reservation allocator.

Event source: direct Lambda invoke from the order orchestration state machine.

Selects warehouses per SKU using a distance and stock-on-hand weighting, applies the
reservation with DynamoDB conditional updates guarded by an optimistic concurrency
version, and issues compensating releases when a multi-line reservation partially fails.
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
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS, MAX_RETRIES, MAX_BACKOFF_SECONDS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
INVENTORY_TABLE = os.environ.get("INVENTORY_TABLE", "warehouse-inventory")
RESERVATIONS_TABLE = os.environ.get("RESERVATIONS_TABLE", "stock-reservations")

EARTH_RADIUS_KM = 6371.0
DISTANCE_WEIGHT = 0.62
STOCK_WEIGHT = 0.38
DISTANCE_NORMALISER_KM = 2500.0
RESERVATION_TTL_SECONDS = 1800
MIN_SCORE_TO_ALLOCATE = 0.05
BASE_BACKOFF_SECONDS = 0.05

RETRYABLE_CODES = {
    "ProvisionedThroughputExceededException", "ThrottlingException",
    "TransactionConflictException", "InternalServerError",
}


def haversine_km(origin: Tuple[float, float], target: Tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = (math.radians(value) for value in (origin[0], origin[1], target[0], target[1]))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return EARTH_RADIUS_KM * 2 * math.asin(math.sqrt(a))


def score_warehouse(warehouse: Dict[str, Any], destination: Tuple[float, float], needed: int) -> float:
    """Blend proximity and available depth into a single allocation score."""
    try:
        coords = (float(warehouse["latitude"]), float(warehouse["longitude"]))
    except (KeyError, TypeError, ValueError):
        return 0.0
    available = int(warehouse.get("available", 0))
    if available <= 0 or needed <= 0:
        return 0.0
    distance = haversine_km(destination, coords)
    proximity = max(0.0, 1.0 - (distance / DISTANCE_NORMALISER_KM))
    depth = min(1.0, available / float(needed))
    score = (proximity * DISTANCE_WEIGHT) + (depth * STOCK_WEIGHT)
    if warehouse.get("cold_chain_capable") and needed > 0:
        score += 0.04
    return round(score, 6)


def fetch_warehouses(sku: str) -> List[Dict[str, Any]]:
    response = dynamodb.Table(INVENTORY_TABLE).query(KeyConditionExpression=Key("sku").eq(sku))
    return list(response.get("Items") or [])


def _with_retry(operation, description: str):
    """Invoke a DynamoDB operation, retrying while the error is retryable."""
    attempt = 0
    for _loop_iter_1 in range(MAX_RETRIES):
        try:
            return operation()
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in RETRYABLE_CODES:
                raise
            delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS) + random.uniform(0, 0.05)
            logger.warning("retrying %s attempt=%s code=%s delay=%.3f", description, attempt, code, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Retry cap reached (%d) in inventory_reservation.py", MAX_RETRIES)
def decrement_stock(sku: str, warehouse_id: str, quantity: int, version: int) -> bool:
    table = dynamodb.Table(INVENTORY_TABLE)

    def _update():
        return table.update_item(
            Key={"sku": sku, "warehouse_id": warehouse_id},
            UpdateExpression="SET available = available - :q, version = :next, updated_at = :now",
            ConditionExpression="available >= :q AND version = :current",
            ExpressionAttributeValues={
                ":q": quantity, ":current": version,
                ":next": version + 1, ":now": int(time.time()),
            },
        )

    try:
        _with_retry(_update, "decrement %s@%s" % (sku, warehouse_id))
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("conditional check lost sku=%s warehouse=%s", sku, warehouse_id)
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


def allocate_line(sku: str, quantity: int, destination: Tuple[float, float]) -> List[Dict[str, Any]]:
    """Allocate a single line across warehouses, best score first."""
    warehouses = fetch_warehouses(sku)
    ranked = sorted(
        ((score_warehouse(w, destination, quantity), w) for w in warehouses),
        key=lambda pair: pair[0], reverse=True,
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
        allocations.append({
            "sku": sku, "warehouse_id": str(warehouse["warehouse_id"]),
            "quantity": take, "score": score,
        })
        remaining -= take
    if remaining > 0:
        raise RuntimeError("insufficient stock for sku=%s short=%s" % (sku, remaining))
    return allocations


def compensate(allocations: List[Dict[str, Any]]) -> None:
    for allocation in allocations:
        try:
            release_stock(allocation["sku"], allocation["warehouse_id"], allocation["quantity"])
        except ClientError as exc:
            logger.error("compensating release failed allocation=%s: %s", allocation, exc)


def persist_reservation(reservation_id: str, order_id: str, allocations: List[Dict[str, Any]]) -> None:
    dynamodb.Table(RESERVATIONS_TABLE).put_item(Item={
        "reservation_id": reservation_id, "order_id": order_id,
        "allocations": allocations, "status": "RESERVED",
        "created_at": int(time.time()),
        "expires_at": int(time.time()) + RESERVATION_TTL_SECONDS,
    })


def _destination(event: Dict[str, Any]) -> Tuple[float, float]:
    ship_to = event.get("ship_to") or {}
    try:
        return float(ship_to.get("latitude", 47.61)), float(ship_to.get("longitude", -122.33))
    except (TypeError, ValueError):
        return 47.61, -122.33  # fulfilment default: Seattle DC


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

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
            logger.error("allocation failed order=%s sku=%s: %s", order_id, sku, exc)
            break

    if failure:
        compensate(allocations)
        return {"status": "FAILED", "order_id": order_id, "reason": failure}
    try:
        persist_reservation(reservation_id, order_id, allocations)
    except ClientError as exc:
        logger.exception("reservation persist failed order=%s: %s", order_id, exc)
        compensate(allocations)
        return {"status": "FAILED", "order_id": order_id, "reason": "persist_error"}

    logger.info(
        "reservation created order=%s reservation=%s allocations=%s",
        order_id, reservation_id, len(allocations),
    )
    return {
        "status": "RESERVED", "order_id": order_id, "reservation_id": reservation_id,
        "allocations": allocations,
        "expires_at": int(time.time()) + RESERVATION_TTL_SECONDS,
    }
