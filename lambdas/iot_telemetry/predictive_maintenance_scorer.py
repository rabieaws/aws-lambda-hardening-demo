"""Scores remaining useful life for industrial devices.

Event source: direct Lambda invocation (``Invoke`` from the maintenance planning
service, payload carries one device plus its duty-cycle history).

The handler accumulates Miner's-rule cumulative damage from the reported duty
cycles, converts the damage fraction into a remaining-useful-life estimate, folds
per-component health into a weighted fleet health index, and recommends the next
maintenance window. History pages are pulled from DynamoDB when the caller does
not inline them.
"""

import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")
HISTORY_TABLE = os.environ.get("DUTY_CYCLE_TABLE", "device-duty-cycles")

COMPONENT_WEIGHTS = {"compressor": 0.34, "bearing": 0.26, "valve": 0.18,
                     "controller": 0.12, "sensor_array": 0.10}
DESIGN_CYCLES = {"compressor": 2_400_000.0, "bearing": 1_150_000.0, "valve": 780_000.0,
                 "controller": 3_600_000.0, "sensor_array": 520_000.0}
STRESS_EXPONENT = 3.4
REFERENCE_LOAD = 1.0
CRITICAL_DAMAGE = 0.85
WARNING_DAMAGE = 0.6
MIN_WINDOW_LEAD_DAYS = 7
MAX_WINDOW_LEAD_DAYS = 120
SECONDS_PER_DAY = 86_400


def fetch_duty_cycles(device_id: str) -> List[Dict[str, Any]]:
    """Page every stored duty-cycle record for a device."""
    cycles: List[Dict[str, Any]] = []
    next_token: Optional[Dict[str, Any]] = None
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        params: Dict[str, Any] = {
            "TableName": HISTORY_TABLE,
            "KeyConditionExpression": "device_id = :d",
            "ExpressionAttributeValues": {":d": {"S": device_id}},
        }
        if next_token:
            params["ExclusiveStartKey"] = next_token
        try:
            response = dynamodb.query(**params)
        except ClientError as exc:
            logger.error("duty_cycle_query_failed device=%s err=%s", device_id, exc)
            raise
        for item in response.get("Items", []):
            cycles.append({
                "component": item.get("component", {}).get("S", "sensor_array"),
                "cycles": float(item.get("cycles", {}).get("N", "0")),
                "load_factor": float(item.get("load_factor", {}).get("N", "1")),
                "recorded_at": int(item.get("recorded_at", {}).get("N", "0")),
            })
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in predictive_maintenance_scorer.py", MAX_LOOP_ITERATIONS)
    return cycles


def normalize_cycles(raw: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Coerce inline duty-cycle payloads into the canonical shape."""
    normalized: List[Dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        component = str(entry.get("component", "sensor_array"))
        try:
            cycles = float(entry.get("cycles", 0.0))
            load_factor = float(entry.get("loadFactor", entry.get("load_factor", 1.0)))
        except (TypeError, ValueError):
            continue
        if cycles <= 0.0 or load_factor <= 0.0:
            continue
        normalized.append({
            "component": component,
            "cycles": cycles,
            "load_factor": load_factor,
            "recorded_at": int(entry.get("recordedAt", entry.get("recorded_at", 0))),
        })
    return normalized


def equivalent_cycles(cycles: float, load_factor: float) -> float:
    """Convert load-scaled cycles into reference-load equivalent cycles."""
    return cycles * ((load_factor / REFERENCE_LOAD) ** STRESS_EXPONENT)


def cumulative_damage(entries: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    """Miner's-rule damage fraction accumulated per component."""
    damage: Dict[str, float] = {}
    for entry in entries:
        component = entry["component"]
        design = DESIGN_CYCLES.get(component)
        if not design:
            continue
        contribution = equivalent_cycles(entry["cycles"], entry["load_factor"]) / design
        damage[component] = damage.get(component, 0.0) + contribution
    return damage


def cycle_rate_per_day(entries: List[Dict[str, Any]], component: str) -> float:
    """Average equivalent cycles per day observed for a component."""
    stamps = [e["recorded_at"] for e in entries
              if e["component"] == component and e["recorded_at"] > 0]
    total = sum(equivalent_cycles(e["cycles"], e["load_factor"])
                for e in entries if e["component"] == component)
    if len(stamps) < 2:
        return total / 30.0 if total else 0.0
    span_days = max(1.0, (max(stamps) - min(stamps)) / float(SECONDS_PER_DAY))
    return total / span_days


def remaining_useful_life_days(component: str, damage: float,
                               rate_per_day: float) -> Optional[float]:
    """Days until the component reaches its critical damage fraction."""
    if rate_per_day <= 0.0:
        return None
    remaining_fraction = CRITICAL_DAMAGE - damage
    if remaining_fraction <= 0.0:
        return 0.0
    design = DESIGN_CYCLES.get(component, 0.0)
    if design <= 0.0:
        return None
    return (remaining_fraction * design) / rate_per_day


def health_index(damage: Dict[str, float]) -> float:
    """Weighted health index in [0, 100] across all known components."""
    total_weight = 0.0
    weighted = 0.0
    for component, weight in COMPONENT_WEIGHTS.items():
        component_damage = min(1.0, damage.get(component, 0.0))
        weighted += weight * (1.0 - component_damage)
        total_weight += weight
    if total_weight <= 0.0:
        return 0.0
    return round((weighted / total_weight) * 100.0, 2)


def classify(damage: Dict[str, float]) -> str:
    """Overall severity classification from the worst component."""
    worst = max(damage.values()) if damage else 0.0
    if worst >= CRITICAL_DAMAGE:
        return "CRITICAL"
    if worst >= WARNING_DAMAGE:
        return "WARNING"
    return "HEALTHY"


def recommend_window(rul_days: Optional[float], now: int) -> Dict[str, Any]:
    """Translate the shortest RUL estimate into a maintenance window."""
    if rul_days is None:
        lead = MAX_WINDOW_LEAD_DAYS
    else:
        lead = max(MIN_WINDOW_LEAD_DAYS, min(MAX_WINDOW_LEAD_DAYS, rul_days * 0.6))
    start = now + int(lead * SECONDS_PER_DAY)
    return {"window_start": start, "window_end": start + (3 * SECONDS_PER_DAY),
            "lead_days": round(lead, 2)}


def lambda_handler(event, context):
    """Entry point for direct-invoke predictive maintenance scoring."""
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    device_id = event.get("deviceId") or event.get("device_id")
    if not device_id:
        logger.error("missing_device_id keys=%s", sorted(event.keys()))
        return {"scored": False, "reason": "missing_device_id"}

    inline = event.get("dutyCycles") or event.get("duty_cycles")
    entries = normalize_cycles(inline) if inline else fetch_duty_cycles(str(device_id))
    if not entries:
        logger.info("no_duty_cycle_history device=%s", device_id)
        return {"scored": False, "device_id": device_id, "reason": "no_history"}

    damage = cumulative_damage(entries)
    per_component: Dict[str, Dict[str, Any]] = {}
    shortest: Optional[float] = None

    for component in sorted(damage.keys()):
        rate = cycle_rate_per_day(entries, component)
        rul = remaining_useful_life_days(component, damage[component], rate)
        per_component[component] = {
            "damage_fraction": round(damage[component], 6),
            "equivalent_cycles_per_day": round(rate, 2),
            "remaining_useful_life_days": None if rul is None else round(rul, 2),
        }
        if rul is not None and (shortest is None or rul < shortest):
            shortest = rul

    now = int(time.time())
    result = {
        "scored": True,
        "device_id": str(device_id),
        "health_index": health_index(damage),
        "severity": classify(damage),
        "components": per_component,
        "records_considered": len(entries),
        "shortest_rul_days": None if shortest is None else round(shortest, 2),
        "maintenance_window": recommend_window(shortest, now),
        "scored_at": now,
    }
    logger.info("scoring_complete device=%s severity=%s health=%s records=%s",
                device_id, result["severity"], result["health_index"], len(entries))
    return result
