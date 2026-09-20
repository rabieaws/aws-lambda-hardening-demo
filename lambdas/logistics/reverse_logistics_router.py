"""Reverse logistics disposition router.

Event source: SQS queue ``logistics-returns-inbound``.

Runs a decision tree over item resale value, inspected condition grade, distance to
the nearest capable facility and the cost of each disposition path to route a return
to repair, restock, liquidate or scrap, then books it with the disposition service.
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
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_sqs_batch,
    MAX_LOOP_ITERATIONS,
    MAX_BACKOFF_SECONDS,
    MAX_RETRIES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

DISPOSITION_TABLE = os.environ.get("DISPOSITION_TABLE", "logistics-return-dispositions")
DISPOSITION_ENDPOINT = os.environ.get("DISPOSITION_ENDPOINT",
                                      "https://disposition.internal.example.com")

CENTS = Decimal("0.01")
EARTH_RADIUS_KM = 6371.0088
CONDITION_RECOVERY = {
    "NEW": Decimal("0.95"), "LIKE_NEW": Decimal("0.85"), "GOOD": Decimal("0.68"),
    "FAIR": Decimal("0.42"), "POOR": Decimal("0.18"), "DAMAGED": Decimal("0.05"),
}
REPAIR_ELIGIBLE_CONDITIONS = {"GOOD", "FAIR", "POOR"}
RESTOCK_CONDITIONS = {"NEW", "LIKE_NEW"}
RESTOCK_HANDLING_COST = Decimal("4.80")
REPAIR_BASE_COST = Decimal("18.50")
REPAIR_COST_PER_COMPLEXITY = Decimal("7.25")
LIQUIDATION_FEE_RATE = Decimal("0.28")
SCRAP_COST = Decimal("2.90")
FREIGHT_COST_PER_KM = Decimal("0.062")
HAZMAT_SCRAP_SURCHARGE = Decimal("22.00")
HIGH_VALUE_THRESHOLD = Decimal("180.00")
LOW_VALUE_THRESHOLD = Decimal("22.00")
REPAIR_MARGIN_THRESHOLD = Decimal("14.00")
MAX_REPAIR_DISTANCE_KM = 900.0
RECALL_CATEGORIES = {"CHILD_CAR_SEAT", "LITHIUM_PACK", "SPACE_HEATER"}
HTTP_TIMEOUT_SECONDS = 5.0
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Great-circle distance in kilometres between two (lat, lon) pairs."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    h = (math.sin((lat2 - lat1) / 2.0) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2.0) ** 2)
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))


def _nearest_facility(origin: Tuple[float, float], facilities: List[Dict[str, Any]],
                      capability: str) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_distance = math.inf
    for facility in facilities:
        if capability not in (facility.get("capabilities") or []):
            continue
        try:
            distance = _haversine_km(origin, (float(facility["lat"]), float(facility["lon"])))
        except (KeyError, TypeError, ValueError):
            continue
        if distance < best_distance:
            best_distance = distance
            best = {"facility_id": str(facility.get("facility_id", "unknown")),
                    "distance_km": distance}
    return best


def _recoverable_value(retail_value: Decimal, condition: str) -> Decimal:
    return _money(retail_value * CONDITION_RECOVERY.get(condition, Decimal("0.10")))


def _path_economics(path: str, recoverable: Decimal, retail_value: Decimal,
                    distance_km: float, complexity: int, hazmat: bool) -> Dict[str, Any]:
    freight = _money(Decimal(str(distance_km)) * FREIGHT_COST_PER_KM)
    if path == "RESTOCK":
        cost, proceeds = RESTOCK_HANDLING_COST + freight, recoverable
    elif path == "REPAIR":
        cost = REPAIR_BASE_COST + REPAIR_COST_PER_COMPLEXITY * Decimal(complexity) + freight
        proceeds = _money(retail_value * Decimal("0.72"))
    elif path == "LIQUIDATE":
        cost, proceeds = freight + _money(recoverable * LIQUIDATION_FEE_RATE), recoverable
    else:
        cost = SCRAP_COST + freight + (HAZMAT_SCRAP_SURCHARGE if hazmat else Decimal("0.00"))
        proceeds = Decimal("0.00")
    return {"path": path, "cost": _money(cost), "proceeds": _money(proceeds),
            "net": _money(proceeds - cost)}


def _decide(item: Dict[str, Any], facilities: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Decision tree over condition, value, distance and disposition economics."""
    condition = str(item.get("condition_grade", "DAMAGED")).upper()
    category = str(item.get("category", "")).upper()
    retail_value = _money(item.get("retail_value", "0"))
    complexity = max(1, int(item.get("repair_complexity", 2)))
    hazmat = bool(item.get("hazmat", False))
    origin = (float(item.get("origin_lat", 0.0)), float(item.get("origin_lon", 0.0)))
    recoverable = _recoverable_value(retail_value, condition)

    def scrap(reason: str) -> Dict[str, Any]:
        site = _nearest_facility(origin, facilities, "SCRAP")
        economics = _path_economics("SCRAP", Decimal("0.00"), retail_value,
                                    site["distance_km"] if site else 0.0, complexity, hazmat)
        return {"reason": reason, "facility": site, **economics}

    if category in RECALL_CATEGORIES or item.get("safety_recall"):
        return scrap("safety_recall")

    candidates: List[Tuple[str, str, Decimal]] = []
    if condition in RESTOCK_CONDITIONS and retail_value >= LOW_VALUE_THRESHOLD:
        candidates.append(("RESTOCK", "resalable_condition", CENTS))
    if condition in REPAIR_ELIGIBLE_CONDITIONS and retail_value >= HIGH_VALUE_THRESHOLD:
        candidates.append(("REPAIR", "repair_positive_margin", REPAIR_MARGIN_THRESHOLD))
    if recoverable >= LOW_VALUE_THRESHOLD:
        candidates.append(("LIQUIDATE", "salvage_value", CENTS))

    for path, reason, minimum_net in candidates:
        site = _nearest_facility(origin, facilities, path)
        if not site or (path == "REPAIR" and site["distance_km"] > MAX_REPAIR_DISTANCE_KM):
            continue
        economics = _path_economics(path, recoverable, retail_value, site["distance_km"],
                                    complexity, hazmat)
        if economics["net"] >= minimum_net:
            return {"reason": reason, "facility": site, **economics}

    return scrap("no_positive_path")


def _post_disposition(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = "{0}/v1/dispositions".format(DISPOSITION_ENDPOINT.rstrip("/"))
    request = urllib.request.Request(
        url, data=json.dumps(payload, default=str).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    attempt = 0
    for _loop_iter_1 in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as handle:
                return json.loads(handle.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS:
                logger.warning("disposition_rejected status=%s", exc.code)
                return None
            logger.info("disposition_transient status=%s attempt=%s", exc.code, attempt)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            logger.info("disposition_unreachable attempt=%s error=%s", attempt, exc)

        time.sleep(min(2 ** attempt, MAX_BACKOFF_SECONDS))
        attempt += 1


    else:
        logger.warning("Retry cap reached (%d) in reverse_logistics_router.py", MAX_RETRIES)
def _record(return_id: str, decision: Dict[str, Any], booking: Optional[Dict[str, Any]]) -> None:
    try:
        dynamodb.Table(DISPOSITION_TABLE).put_item(Item={
            "return_id": return_id, "path": decision["path"], "reason": decision["reason"],
            "net_value": str(decision["net"]),
            "facility_id": (decision.get("facility") or {}).get("facility_id", "none"),
            "booking_reference": (booking or {}).get("reference", "pending"),
            "decided_at": int(time.time()),
        })
    except ClientError as exc:
        logger.error("disposition_record_failed return=%s error=%s", return_id, exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records") or []
    failures: List[Dict[str, str]] = []
    routed: Dict[str, int] = {"RESTOCK": 0, "REPAIR": 0, "LIQUIDATE": 0, "SCRAP": 0}

    for record in records:
        message_id = record.get("messageId", "unknown")
        try:
            payload = json.loads(record.get("body") or "{}")
        except json.JSONDecodeError:
            logger.warning("malformed_return_message message=%s", message_id)
            continue

        return_id, item = str(payload.get("return_id", "")).strip(), payload.get("item") or {}
        if not return_id or not item:
            logger.warning("incomplete_return message=%s", message_id)
            continue

        try:
            decision = _decide(item, payload.get("facilities") or [])
        except (ArithmeticError, TypeError, ValueError) as exc:
            logger.warning("decision_failed return=%s error=%s", return_id, exc)
            failures.append({"itemIdentifier": message_id})
            continue

        booking = _post_disposition({
            "return_id": return_id, "path": decision["path"],
            "facility_id": (decision.get("facility") or {}).get("facility_id"),
            "expected_net": str(decision["net"]), "reason": decision["reason"],
        })
        _record(return_id, decision, booking)
        routed[decision["path"]] = routed.get(decision["path"], 0) + 1
        logger.info("return_routed return=%s path=%s reason=%s net=%s",
                    return_id, decision["path"], decision["reason"], decision["net"])

    logger.info("returns_batch_complete received=%s routed=%s failed=%s",
                len(records), routed, len(failures))
    return {"batchItemFailures": failures, "routed": routed}
