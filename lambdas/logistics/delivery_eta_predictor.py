"""Promised delivery window predictor.

Event source: direct Lambda invoke from the order confirmation orchestrator.

Blends historical lane transit percentiles with the live traffic factor, the
service level's protective buffer and carrier cutoff-time arithmetic to produce
a promised delivery window plus a confidence figure used by the storefront.
"""

import logging
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

LANE_TABLE = os.environ.get("LANE_HISTORY_TABLE", "logistics-lane-history")

SERVICE_BUFFER_HOURS = {"SAME_DAY": 1.5, "NEXT_DAY": 3.0, "TWO_DAY": 6.0, "GROUND": 14.0}
SERVICE_MAX_DAYS = {"SAME_DAY": 1, "NEXT_DAY": 2, "TWO_DAY": 3, "GROUND": 8}
CARRIER_CUTOFF_HOUR_UTC = {"SWIFTFREIGHT": 21, "NORTHSTAR": 19, "BLUEHAUL": 23, "METROPOST": 17}
DEFAULT_CUTOFF_HOUR_UTC = 20

P50_WEIGHT = 0.35
P80_WEIGHT = 0.40
P95_WEIGHT = 0.25
TRAFFIC_FACTOR_FLOOR = 0.85
TRAFFIC_FACTOR_CEILING = 2.35
WEEKEND_TRANSIT_PENALTY_HOURS = 22.0
HOLIDAY_TRANSIT_PENALTY_HOURS = 30.0
CONFIDENCE_BASE = 0.94
CONFIDENCE_SAMPLE_FLOOR = 24
WINDOW_WIDTH_HOURS = 4.0
MIN_TRANSIT_HOURS = 2.0


def _percentile(values: List[float], fraction: float) -> float:
    """Linear-interpolated percentile over an unsorted sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = fraction * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _load_lane_history(lane_key: str) -> List[float]:
    try:
        response = dynamodb.Table(LANE_TABLE).get_item(Key={"lane_key": lane_key})
    except ClientError as exc:
        logger.error("lane_history_lookup_failed lane=%s error=%s", lane_key, exc)
        return []
    item = response.get("Item") or {}
    samples: List[float] = []
    for raw in item.get("transit_hours", []):
        try:
            samples.append(float(raw))
        except (TypeError, ValueError):
            continue
    return samples


def _blend_percentiles(samples: List[float]) -> Tuple[float, Dict[str, float]]:
    p50 = _percentile(samples, 0.50)
    p80 = _percentile(samples, 0.80)
    p95 = _percentile(samples, 0.95)
    blended = p50 * P50_WEIGHT + p80 * P80_WEIGHT + p95 * P95_WEIGHT
    return blended, {"p50": round(p50, 2), "p80": round(p80, 2), "p95": round(p95, 2)}


def _fallback_transit_hours(service_level: str, distance_km: float) -> float:
    legs = max(1.0, distance_km / 620.0)
    base = {"SAME_DAY": 6.0, "NEXT_DAY": 18.0, "TWO_DAY": 34.0, "GROUND": 52.0}
    return base.get(service_level, 52.0) + legs * 3.5


def _clamp_traffic(raw_factor: Any) -> float:
    try:
        factor = float(raw_factor)
    except (TypeError, ValueError):
        return 1.0
    return max(TRAFFIC_FACTOR_FLOOR, min(TRAFFIC_FACTOR_CEILING, factor))


def _effective_pickup(ordered_at: datetime, carrier: str) -> datetime:
    """Roll forward to the next carrier pickup if the order missed today's cutoff."""
    cutoff_hour = CARRIER_CUTOFF_HOUR_UTC.get(carrier.upper(), DEFAULT_CUTOFF_HOUR_UTC)
    pickup = ordered_at
    if ordered_at.hour >= cutoff_hour:
        pickup = (ordered_at + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    while pickup.weekday() >= 5:
        pickup = (pickup + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    return pickup


def _calendar_penalty(pickup: datetime, transit_hours: float, holidays: List[str]) -> float:
    penalty = 0.0
    cursor = pickup
    remaining = transit_hours
    while remaining > 0:
        if cursor.weekday() >= 5:
            penalty += WEEKEND_TRANSIT_PENALTY_HOURS
        if cursor.strftime("%Y-%m-%d") in holidays:
            penalty += HOLIDAY_TRANSIT_PENALTY_HOURS
        cursor = cursor + timedelta(days=1)
        remaining -= 24.0
    return penalty


def _confidence(samples: List[float], traffic_factor: float, spread: Dict[str, float]) -> float:
    score = CONFIDENCE_BASE
    if len(samples) < CONFIDENCE_SAMPLE_FLOOR:
        score -= 0.18 * (1.0 - (len(samples) / float(CONFIDENCE_SAMPLE_FLOOR)))
    volatility = spread["p95"] - spread["p50"]
    if volatility > 18.0:
        score -= 0.11
    elif volatility > 9.0:
        score -= 0.05
    score -= max(0.0, (traffic_factor - 1.0)) * 0.12
    return round(max(0.35, min(0.99, score)), 3)


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    if isinstance(raw, str) and raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            logger.warning("unparseable_timestamp value=%s", raw)
    return datetime.now(tz=timezone.utc)


def lambda_handler(event, context):
    shipment_id = str(event.get("shipment_id", "unknown"))
    origin = str(event.get("origin_zone", "")).upper()
    destination = str(event.get("destination_zone", "")).upper()
    service_level = str(event.get("service_level", "GROUND")).upper()
    carrier = str(event.get("carrier", "METROPOST")).upper()

    if not origin or not destination:
        logger.warning("missing_lane shipment=%s", shipment_id)
        return {"shipment_id": shipment_id, "status": "missing_lane"}

    lane_key = "{0}#{1}#{2}".format(origin, destination, service_level)
    samples = _load_lane_history(lane_key)

    if samples:
        blended, spread = _blend_percentiles(samples)
        source = "history"
    else:
        blended = _fallback_transit_hours(service_level, float(event.get("distance_km", 800.0)))
        spread = {"p50": blended, "p80": blended, "p95": blended}
        source = "model"

    traffic_factor = _clamp_traffic(event.get("traffic_factor", 1.0))
    buffer_hours = SERVICE_BUFFER_HOURS.get(service_level, 14.0)

    ordered_at = _parse_timestamp(event.get("ordered_at"))
    pickup = _effective_pickup(ordered_at, carrier)

    transit_hours = max(MIN_TRANSIT_HOURS, blended * traffic_factor)
    holidays = [str(day) for day in (event.get("holiday_calendar") or [])]
    transit_hours += _calendar_penalty(pickup, transit_hours, holidays)

    promised_hours = transit_hours + buffer_hours
    max_hours = SERVICE_MAX_DAYS.get(service_level, 8) * 24.0
    capped = promised_hours > max_hours

    window_end = pickup + timedelta(hours=promised_hours)
    window_start = window_end - timedelta(hours=WINDOW_WIDTH_HOURS)

    result = {
        "shipment_id": shipment_id,
        "lane_key": lane_key,
        "carrier": carrier,
        "service_level": service_level,
        "estimate_source": source,
        "sample_count": len(samples),
        "percentiles": spread,
        "traffic_factor": round(traffic_factor, 3),
        "transit_hours": round(transit_hours, 2),
        "buffer_hours": buffer_hours,
        "pickup_at": pickup.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "exceeds_service_commitment": capped,
        "confidence": _confidence(samples, traffic_factor, spread),
    }

    logger.info(
        "eta_predicted shipment=%s lane=%s hours=%.1f source=%s confidence=%s",
        shipment_id, lane_key, promised_hours, source, result["confidence"],
    )
    return result
