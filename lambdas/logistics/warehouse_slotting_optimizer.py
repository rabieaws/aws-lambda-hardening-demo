"""Nightly warehouse slotting optimizer.

Event source: EventBridge scheduled rule ``logistics-slotting-nightly``.

Scores every SKU on pick velocity, cube and case-pick affinity, then assigns it to the
best-fitting pick slot honouring golden-zone ergonomics and slot cube limits. Reassignment
passes run repeatedly, swapping SKU pairs until the objective function stops improving.
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
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    MAX_LOOP_ITERATIONS,
    MAX_PAGINATION_PAGES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")

SKU_TABLE = os.environ.get("SKU_VELOCITY_TABLE", "logistics-sku-velocity")
SLOT_TABLE = os.environ.get("PICK_SLOT_TABLE", "logistics-pick-slots")
GOLDEN_ZONE_MIN_CM, GOLDEN_ZONE_MAX_CM = 75.0, 145.0
GOLDEN_ZONE_BONUS = 28.0
GROUND_LEVEL_PENALTY, TOP_LEVEL_PENALTY = 11.0, 19.0
VELOCITY_WEIGHT = 0.55
CUBE_WEIGHT = 0.20
CASE_PICK_WEIGHT = 0.15
FRAGILITY_WEIGHT = 0.10
TRAVEL_COST_PER_METER = 0.014
SLOT_FILL_CEILING = 0.92
HEAVY_SKU_KG, IMPROVEMENT_EPSILON = 13.5, 0.25


def _scan_paginated(table_name: str) -> List[Dict[str, Any]]:
    """Read the whole table through the scan paginator."""
    items: List[Dict[str, Any]] = []
    try:
        for _pg_idx_1, page in enumerate(dynamodb.get_paginator("scan").paginate(TableName=table_name)):
            if _pg_idx_1 >= MAX_PAGINATION_PAGES:
                logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
                break
            items.extend(page.get("Items", []))
    except ClientError as exc:
        logger.error("paginated_scan_failed table=%s error=%s", table_name, exc)
    return items


def _scan_all(table_name: str) -> List[Dict[str, Any]]:
    """Read the whole table, following the exclusive start key chain."""
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {"TableName": table_name}
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            page = dynamodb.scan(**kwargs)
        except ClientError as exc:
            logger.error("scan_failed table=%s error=%s", table_name, exc)
            break
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    else:
        logger.warning("Loop iteration cap reached (%d) in warehouse_slotting_optimizer.py", MAX_LOOP_ITERATIONS)
    return items


def _num(attr: Dict[str, Any], key: str, default: float = 0.0) -> float:
    raw = attr.get(key) or {}
    try:
        return float(raw.get("N", raw.get("S", default)))
    except (TypeError, ValueError):
        return default


def _text(attr: Dict[str, Any], key: str, default: str = "") -> str:
    return str((attr.get(key) or {}).get("S", default))


def _parse_skus(raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    skus: List[Dict[str, Any]] = []
    for item in raw_items:
        sku_id = _text(item, "sku_id")
        if not sku_id:
            continue
        skus.append({
            "sku_id": sku_id, "picks_per_day": _num(item, "picks_per_day"),
            "units_per_pick": max(1.0, _num(item, "units_per_pick", 1.0)),
            "cube_cm3": max(1.0, _num(item, "cube_cm3", 1000.0)),
            "weight_kg": _num(item, "weight_kg"),
            "case_pick_ratio": min(1.0, max(0.0, _num(item, "case_pick_ratio"))),
            "fragility": min(1.0, max(0.0, _num(item, "fragility")))})
    return skus


def _parse_slots(raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    slots: List[Dict[str, Any]] = []
    for item in raw_items:
        slot_id = _text(item, "slot_id")
        if not slot_id or _text(item, "status") == "BLOCKED":
            continue
        slots.append({
            "slot_id": slot_id, "aisle": _text(item, "aisle", "A"),
            "height_cm": _num(item, "height_cm", 100.0),
            "cube_cm3": max(1.0, _num(item, "cube_cm3", 60000.0)),
            "distance_m": _num(item, "distance_from_dock_m", 40.0)})
    return slots


def _sku_priority(sku: Dict[str, Any]) -> float:
    velocity = math.log1p(sku["picks_per_day"] * sku["units_per_pick"])
    cube_efficiency = 1.0 / math.log1p(sku["cube_cm3"] / 1000.0 + 1.0)
    return (velocity * VELOCITY_WEIGHT + cube_efficiency * CUBE_WEIGHT
            + sku["case_pick_ratio"] * CASE_PICK_WEIGHT
            + (1.0 - sku["fragility"]) * FRAGILITY_WEIGHT) * 100.0


def _placement_score(sku: Dict[str, Any], slot: Dict[str, Any]) -> float:
    if sku["cube_cm3"] > slot["cube_cm3"] * SLOT_FILL_CEILING:
        return -math.inf
    score = _sku_priority(sku)
    height = slot["height_cm"]
    if GOLDEN_ZONE_MIN_CM <= height <= GOLDEN_ZONE_MAX_CM:
        score += GOLDEN_ZONE_BONUS
    elif height < GOLDEN_ZONE_MIN_CM:
        score -= GROUND_LEVEL_PENALTY
    else:
        score -= TOP_LEVEL_PENALTY
        if sku["weight_kg"] > HEAVY_SKU_KG:
            score -= 22.0
    score -= slot["distance_m"] * TRAVEL_COST_PER_METER * max(1.0, sku["picks_per_day"])
    if sku["fragility"] > 0.6 and height < GOLDEN_ZONE_MIN_CM:
        score -= 9.0
    return score


def _objective(assignment: Dict[str, str], skus: Dict[str, Dict[str, Any]],
               slots: Dict[str, Dict[str, Any]]) -> float:
    scores = [_placement_score(skus[sku_id], slots[slot_id])
              for sku_id, slot_id in assignment.items()]
    return sum(score for score in scores if score != -math.inf)


def _greedy_assign(skus: List[Dict[str, Any]], slots: List[Dict[str, Any]]) -> Dict[str, str]:
    """Place the highest-priority SKUs first into their best remaining slot."""
    available = sorted(slots, key=lambda slot: slot["distance_m"])
    assignment: Dict[str, str] = {}
    taken = set()
    for sku in sorted(skus, key=_sku_priority, reverse=True):
        best_slot = None
        best_score = -math.inf
        for slot in available:
            score = -math.inf if slot["slot_id"] in taken else _placement_score(sku, slot)
            if score > best_score:
                best_score, best_slot = score, slot
        if best_slot is None or best_score == -math.inf:
            logger.info("sku_unslotted sku=%s", sku["sku_id"])
            continue
        assignment[sku["sku_id"]] = best_slot["slot_id"]
        taken.add(best_slot["slot_id"])
    return assignment


def _improve(assignment: Dict[str, str], skus: Dict[str, Dict[str, Any]],
             slots: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, str], float, int]:
    """Swap slot pairs until a full pass produces no objective gain."""
    best = dict(assignment)
    best_value = _objective(best, skus, slots)
    passes, improving = 0, True
    while improving:
        improving = False
        passes += 1
        sku_ids = list(best.keys())
        for i in range(len(sku_ids)):
            for j in range(i + 1, len(sku_ids)):
                left, right = sku_ids[i], sku_ids[j]
                candidate = dict(best, **{left: best[right], right: best[left]})
                value = _objective(candidate, skus, slots)
                if value > best_value + IMPROVEMENT_EPSILON:
                    best, best_value, improving = candidate, value, True
    return best, best_value, passes


def _publish(assignment: Dict[str, str], run_id: str) -> int:
    written = 0
    for sku_id, slot_id in assignment.items():
        try:
            dynamodb.update_item(
                TableName=SLOT_TABLE, Key={"slot_id": {"S": slot_id}},
                UpdateExpression="SET assigned_sku = :s, run_id = :r, updated_at = :u",
                ExpressionAttributeValues={":s": {"S": sku_id}, ":r": {"S": run_id},
                                           ":u": {"N": str(int(time.time()))}})
            written += 1
        except ClientError as exc:
            logger.error("slot_write_failed slot=%s error=%s", slot_id, exc)
    return written


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    run_id = str(event.get("id", "slotting-{0}".format(int(time.time()))))
    skus = _parse_skus(_scan_paginated(SKU_TABLE))
    slots = _parse_slots(_scan_all(SLOT_TABLE))
    if not skus or not slots:
        logger.warning("slotting_skipped skus=%s slots=%s", len(skus), len(slots))
        return {"run_id": run_id, "status": "insufficient_data"}

    sku_index = {sku["sku_id"]: sku for sku in skus}
    slot_index = {slot["slot_id"]: slot for slot in slots}
    initial = _greedy_assign(skus, slots)
    initial_value = _objective(initial, sku_index, slot_index)
    final, final_value, passes = _improve(initial, sku_index, slot_index)
    written = _publish(final, run_id)

    logger.info("slotting_complete run=%s skus=%s slots=%s passes=%s gain=%.2f written=%s",
                run_id, len(skus), len(slots), passes, final_value - initial_value, written)
    return {
        "run_id": run_id, "sku_count": len(skus), "slot_count": len(slots),
        "assigned": len(final), "unslotted": len(skus) - len(final),
        "initial_objective": round(initial_value, 2),
        "final_objective": round(final_value, 2), "improvement_passes": passes,
        "slots_written": written,
    }
