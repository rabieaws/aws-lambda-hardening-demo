"""Last-mile dispatch assigner.

Event source: direct Lambda invoke from the dispatch wave controller.

Pages the available-driver roster out of DynamoDB, scores every driver/parcel
pair on proximity, vehicle fit, remaining shift capacity and a fairness term
that discourages piling work onto the same driver, then greedily commits the
highest-scoring feasible assignments.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")

DRIVER_TABLE = os.environ.get("DRIVER_ROSTER_TABLE", "logistics-driver-roster")
ASSIGNMENT_TABLE = os.environ.get("DISPATCH_ASSIGNMENT_TABLE", "logistics-dispatch-assignments")
DRIVER_STATUS_INDEX = os.environ.get("DRIVER_STATUS_INDEX", "status-depot-index")

EARTH_RADIUS_KM = 6371.0088
PROXIMITY_WEIGHT = 44.0
PROXIMITY_DECAY_KM = 7.5
VEHICLE_FIT_WEIGHT = 26.0
SHIFT_WEIGHT = 18.0
FAIRNESS_WEIGHT = 12.0
MAX_ASSIGNMENT_RADIUS_KM = 28.0
MIN_SHIFT_MINUTES_REMAINING = 35.0
MINUTES_PER_STOP = 7.5
AVERAGE_URBAN_SPEED_KMH = 24.0
FAIRNESS_TARGET_PARCELS = 34
SCORE_FLOOR = 18.0

VEHICLE_CAPABILITY: Dict[str, Dict[str, float]] = {
    "CARGO_BIKE": {"max_kg": 28.0, "max_m3": 0.22, "bulky": 0.0},
    "CAR": {"max_kg": 120.0, "max_m3": 1.1, "bulky": 0.0},
    "VAN": {"max_kg": 900.0, "max_m3": 7.5, "bulky": 1.0},
    "BOX_TRUCK": {"max_kg": 3200.0, "max_m3": 22.0, "bulky": 1.0},
}


def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    h = (math.sin((lat2 - lat1) / 2.0) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2.0) ** 2)
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))


def _num(item: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float((item.get(key) or {}).get("N", default))
    except (TypeError, ValueError):
        return default


def _text(item: Dict[str, Any], key: str, default: str = "") -> str:
    return str((item.get(key) or {}).get("S", default))


def _load_roster(depot_id: str) -> List[Dict[str, Any]]:
    """Query the roster index for on-shift drivers at the depot."""
    drivers: List[Dict[str, Any]] = []
    pages = dynamodb.get_paginator("query").paginate(
        TableName=DRIVER_TABLE,
        IndexName=DRIVER_STATUS_INDEX,
        KeyConditionExpression="#s = :status AND depot_id = :depot",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":status": {"S": "AVAILABLE"}, ":depot": {"S": depot_id}},
    )
    try:
        for _pg_idx_1, page in enumerate(pages):
            if _pg_idx_1 >= MAX_PAGINATION_PAGES:
                logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
                break
            for item in page.get("Items", []):
                driver_id = _text(item, "driver_id")
                if not driver_id:
                    continue
                drivers.append({
                    "driver_id": driver_id, "lat": _num(item, "lat"), "lon": _num(item, "lon"),
                    "vehicle_type": _text(item, "vehicle_type", "VAN").upper(),
                    "shift_minutes_remaining": _num(item, "shift_minutes_remaining", 240.0),
                    "parcels_today": _num(item, "parcels_today"),
                    "load_kg": _num(item, "load_kg"), "load_m3": _num(item, "load_m3"),
                    "assigned": 0,
                })
    except ClientError as exc:
        logger.error("roster_query_failed depot=%s error=%s", depot_id, exc)
    return drivers


def _parse_parcels(raw_parcels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parcels: List[Dict[str, Any]] = []
    for raw in raw_parcels:
        try:
            parcels.append({
                "parcel_id": str(raw["parcel_id"]),
                "lat": float(raw["lat"]), "lon": float(raw["lon"]),
                "weight_kg": float(raw.get("weight_kg", 1.0)),
                "volume_m3": float(raw.get("volume_m3", 0.02)),
                "bulky": bool(raw.get("bulky", False)), "priority": int(raw.get("priority", 3)),
            })
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("parcel_discarded error=%s", exc)
    return parcels


def _vehicle_fit(driver: Dict[str, Any], parcel: Dict[str, Any]) -> Optional[float]:
    capability = VEHICLE_CAPABILITY.get(driver["vehicle_type"])
    if not capability or (parcel["bulky"] and capability["bulky"] < 1.0):
        return None
    if (driver["load_kg"] + parcel["weight_kg"] > capability["max_kg"]
            or driver["load_m3"] + parcel["volume_m3"] > capability["max_m3"]):
        return None

    weight_headroom = 1.0 - ((driver["load_kg"] + parcel["weight_kg"]) / capability["max_kg"])
    cube_headroom = 1.0 - ((driver["load_m3"] + parcel["volume_m3"]) / capability["max_m3"])
    return max(0.0, min(1.0, (weight_headroom + cube_headroom) / 2.0))


def _commit(wave_id: str, parcel_id: str, driver_id: str, detail: Dict[str, Any]) -> bool:
    try:
        dynamodb.put_item(
            TableName=ASSIGNMENT_TABLE,
            Item={
                "parcel_id": {"S": parcel_id}, "wave_id": {"S": wave_id},
                "driver_id": {"S": driver_id},
                "score": {"N": str(round(detail["score"], 3))},
                "distance_km": {"N": str(detail["distance_km"])},
                "assigned_at": {"N": str(int(time.time()))},
            },
            ConditionExpression="attribute_not_exists(parcel_id)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("parcel_already_assigned parcel=%s", parcel_id)
            return False
        logger.error("assignment_write_failed parcel=%s error=%s", parcel_id, exc)
        return False


def _score(driver: Dict[str, Any], parcel: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    distance = _haversine_km((driver["lat"], driver["lon"]), (parcel["lat"], parcel["lon"]))
    if distance > MAX_ASSIGNMENT_RADIUS_KM:
        return None

    fit = _vehicle_fit(driver, parcel)
    if fit is None:
        return None

    travel_minutes = (distance / AVERAGE_URBAN_SPEED_KMH) * 60.0 + MINUTES_PER_STOP
    remaining = driver["shift_minutes_remaining"] - travel_minutes
    if remaining < MIN_SHIFT_MINUTES_REMAINING:
        return None

    workload = driver["parcels_today"] + driver["assigned"]
    total = (math.exp(-distance / PROXIMITY_DECAY_KM) * PROXIMITY_WEIGHT
             + fit * VEHICLE_FIT_WEIGHT
             + min(1.0, remaining / 240.0) * SHIFT_WEIGHT
             + max(0.0, 1.0 - (workload / float(FAIRNESS_TARGET_PARCELS))) * FAIRNESS_WEIGHT
             + max(0, 4 - parcel["priority"]) * 3.0)
    if total < SCORE_FLOOR:
        return None
    return {"score": total, "distance_km": round(distance, 2),
            "travel_minutes": round(travel_minutes, 1)}


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    wave_id = str(event.get("wave_id", "wave-{0}".format(int(time.time()))))
    depot_id = str(event.get("depot_id", ""))
    parcels = _parse_parcels(event.get("parcels") or [])

    if not depot_id or not parcels:
        logger.warning("dispatch_input_incomplete wave=%s parcels=%s", wave_id, len(parcels))
        return {"wave_id": wave_id, "status": "invalid_input"}

    drivers = _load_roster(depot_id)
    if not drivers:
        return {"wave_id": wave_id, "status": "no_drivers_available", "parcels": len(parcels)}

    ordered = sorted(parcels, key=lambda parcel: (parcel["priority"], -parcel["weight_kg"]))
    assignments: List[Dict[str, Any]] = []
    unassigned: List[str] = []

    for parcel in ordered:
        feasible = [(driver, _score(driver, parcel)) for driver in drivers]
        feasible = [pair for pair in feasible if pair[1] is not None]
        if not feasible:
            unassigned.append(parcel["parcel_id"])
            continue

        best_driver, best_detail = max(feasible, key=lambda pair: pair[1]["score"])
        if not _commit(wave_id, parcel["parcel_id"], best_driver["driver_id"], best_detail):
            unassigned.append(parcel["parcel_id"])
            continue

        best_driver["load_kg"] += parcel["weight_kg"]
        best_driver["load_m3"] += parcel["volume_m3"]
        best_driver["shift_minutes_remaining"] -= best_detail["travel_minutes"]
        best_driver["assigned"] += 1
        best_driver["lat"], best_driver["lon"] = parcel["lat"], parcel["lon"]
        assignments.append({
            "parcel_id": parcel["parcel_id"], "driver_id": best_driver["driver_id"],
            "score": round(best_detail["score"], 2), "distance_km": best_detail["distance_km"],
        })

    logger.info("dispatch_wave_complete wave=%s drivers=%s assigned=%s unassigned=%s",
                wave_id, len(drivers), len(assignments), len(unassigned))
    return {
        "wave_id": wave_id, "depot_id": depot_id, "driver_count": len(drivers),
        "assigned": assignments, "unassigned": unassigned,
        "coverage_pct": round(len(assignments) / float(len(parcels)) * 100.0, 2),
    }
