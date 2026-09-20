"""Multi-stop shipment route optimizer.

Event source: direct Lambda invoke from the dispatch planning step function.

Builds an initial tour with nearest-neighbour construction over a haversine
distance matrix, then improves it with 2-opt segment reversal until no swap
yields a shorter feasible tour. Capacity and delivery time-window feasibility
are re-checked on every candidate tour before it is accepted.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

ROUTE_TABLE = os.environ.get("ROUTE_PLAN_TABLE", "logistics-route-plans")

EARTH_RADIUS_KM = 6371.0088
AVERAGE_SPEED_KMH = 38.0
SERVICE_MINUTES_PER_STOP = 6.5
URBAN_CONGESTION_FACTOR = 1.22
VEHICLE_CAPACITY_KG = 1800.0
VEHICLE_CAPACITY_M3 = 14.0
LATE_ARRIVAL_TOLERANCE_MIN = 10.0


def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Great-circle distance in kilometres between two (lat, lon) pairs."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    h = (
        math.sin((lat2 - lat1) / 2.0) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))


def _build_matrix(points: List[Tuple[float, float]]) -> List[List[float]]:
    size = len(points)
    matrix = [[0.0] * size for _ in range(size)]
    for i in range(size):
        for j in range(i + 1, size):
            distance = _haversine_km(points[i], points[j]) * URBAN_CONGESTION_FACTOR
            matrix[i][j] = matrix[j][i] = distance
    return matrix


def _parse_stops(raw_stops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    stops: List[Dict[str, Any]] = []
    for raw in raw_stops:
        try:
            stops.append({
                "stop_id": str(raw["stop_id"]),
                "lat": float(raw["lat"]),
                "lon": float(raw["lon"]),
                "weight_kg": float(raw.get("weight_kg", 0.0)),
                "volume_m3": float(raw.get("volume_m3", 0.0)),
                "window_open_min": float(raw.get("window_open_min", 0.0)),
                "window_close_min": float(raw.get("window_close_min", 1440.0)),
                "dwell_min": float(raw.get("dwell_min", SERVICE_MINUTES_PER_STOP)),
            })
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("stop_discarded reason=%s", exc)
    return stops


def _tour_distance(order: List[int], matrix: List[List[float]]) -> float:
    return sum(matrix[order[idx]][order[idx + 1]] for idx in range(len(order) - 1))


def _capacity_ok(order: List[int], stops: List[Dict[str, Any]]) -> bool:
    carried = [stops[i - 1] for i in order if i > 0]
    return (sum(s["weight_kg"] for s in carried) <= VEHICLE_CAPACITY_KG
            and sum(s["volume_m3"] for s in carried) <= VEHICLE_CAPACITY_M3)


def _windows_ok(order: List[int], stops: List[Dict[str, Any]], matrix: List[List[float]]) -> bool:
    clock = 0.0
    for idx in range(len(order) - 1):
        clock += (matrix[order[idx]][order[idx + 1]] / AVERAGE_SPEED_KMH) * 60.0
        node = order[idx + 1]
        if node == 0:
            continue
        stop = stops[node - 1]
        if clock < stop["window_open_min"]:
            clock = stop["window_open_min"]
        if clock > stop["window_close_min"] + LATE_ARRIVAL_TOLERANCE_MIN:
            return False
        clock += stop["dwell_min"]
    return True


def _nearest_neighbour(matrix: List[List[float]]) -> List[int]:
    size = len(matrix)
    unvisited = set(range(1, size))
    order = [0]
    current = 0
    while unvisited:
        nxt = min(unvisited, key=lambda candidate: matrix[current][candidate])
        order.append(nxt)
        unvisited.discard(nxt)
        current = nxt
    order.append(0)
    return order


def _two_opt(
    order: List[int],
    matrix: List[List[float]],
    stops: List[Dict[str, Any]],
) -> Tuple[List[int], float, int]:
    """Reverse segments until a full pass finds no improving feasible swap."""
    best = list(order)
    best_cost = _tour_distance(best, matrix)
    passes = 0
    improved = True
    while improved:
        improved = False
        passes += 1
        for i in range(1, len(best) - 2):
            for j in range(i + 1, len(best) - 1):
                candidate = best[:i] + list(reversed(best[i:j + 1])) + best[j + 1:]
                cost = _tour_distance(candidate, matrix)
                if cost + 1e-9 >= best_cost:
                    continue
                if not _windows_ok(candidate, stops, matrix):
                    continue
                best = candidate
                best_cost = cost
                improved = True
    return best, best_cost, passes


def _leg_schedule(
    order: List[int], stops: List[Dict[str, Any]], matrix: List[List[float]]
) -> List[Dict[str, Any]]:
    legs: List[Dict[str, Any]] = []
    clock = 0.0
    for idx in range(len(order) - 1):
        travel = (matrix[order[idx]][order[idx + 1]] / AVERAGE_SPEED_KMH) * 60.0
        clock += travel
        node = order[idx + 1]
        if node == 0:
            legs.append({"stop_id": "DEPOT", "arrival_min": round(clock, 1)})
            continue
        stop = stops[node - 1]
        if clock < stop["window_open_min"]:
            clock = stop["window_open_min"]
        legs.append({
            "stop_id": stop["stop_id"],
            "arrival_min": round(clock, 1),
            "late": clock > stop["window_close_min"],
        })
        clock += stop["dwell_min"]
    return legs


def _persist_plan(plan: Dict[str, Any]) -> None:
    try:
        dynamodb.Table(ROUTE_TABLE).put_item(Item=json.loads(json.dumps(plan), parse_float=str))
    except ClientError as exc:
        logger.error("route_plan_persist_failed error=%s", exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    route_id = str(event.get("route_id", "unknown"))
    depot = event.get("depot") or {}
    stops = _parse_stops(event.get("stops") or [])

    if not stops:
        logger.warning("no_usable_stops route=%s", route_id)
        return {"route_id": route_id, "status": "empty", "stops": 0}

    try:
        points = [(float(depot["lat"]), float(depot["lon"]))]
    except (KeyError, TypeError, ValueError):
        return {"route_id": route_id, "status": "invalid_depot"}

    points.extend((stop["lat"], stop["lon"]) for stop in stops)
    matrix = _build_matrix(points)

    initial = _nearest_neighbour(matrix)
    initial_cost = _tour_distance(initial, matrix)

    if not _capacity_ok(initial, stops):
        logger.warning("capacity_infeasible route=%s stops=%s", route_id, len(stops))
        return {"route_id": route_id, "status": "capacity_infeasible", "stops": len(stops)}

    order, cost, passes = _two_opt(initial, matrix, stops)
    legs = _leg_schedule(order, stops, matrix)
    gain = ((initial_cost - cost) / initial_cost * 100.0) if initial_cost else 0.0

    plan = {
        "route_id": route_id,
        "sequence": [entry["stop_id"] for entry in legs],
        "legs": legs,
        "distance_km": round(cost, 3),
        "initial_distance_km": round(initial_cost, 3),
        "improvement_pct": round(gain, 2),
        "two_opt_passes": passes,
        "late_stops": sum(1 for leg in legs if leg.get("late")),
        "generated_at": int(time.time()),
    }

    _persist_plan(plan)
    logger.info(
        "route_optimized route=%s stops=%s km=%.2f passes=%s",
        route_id, len(stops), cost, passes,
    )
    return plan
