"""Evaluates device position reports against configured geofences.

Event source: AWS IoT Core rule action (position reports published on
``fleet/+/position`` and routed to this function).

Great-circle distance to the fence centroid acts as a cheap prefilter before a
ray-casting point-in-polygon test runs against the fence boundary. A breach only
fires once the device has been continuously outside for the dwell-debounce
period, which suppresses GPS jitter.
"""

import json
import logging
import math
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
events = boto3.client("events")
FENCE_TABLE = os.environ.get("FENCE_TABLE", "geofence-definitions")
DWELL_TABLE = os.environ.get("DWELL_TABLE", "geofence-dwell-state")
EVENT_BUS = os.environ.get("EVENT_BUS", "default")

EARTH_RADIUS_METERS = 6371008.8
DWELL_DEBOUNCE_SECONDS = 120
PREFILTER_SLACK_METERS = 500.0
MAX_ACCURACY_METERS = 75.0
STALE_DWELL_RESET_SECONDS = 1800

Point = Tuple[float, float]


def haversine_meters(a: Point, b: Point) -> float:
    """Great-circle distance in meters between two (lat, lon) points."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    sin_dlat = math.sin(dlat / 2.0)
    sin_dlon = math.sin(dlon / 2.0)
    h = (sin_dlat * sin_dlat) + (math.cos(lat1) * math.cos(lat2) * sin_dlon * sin_dlon)
    h = min(1.0, max(0.0, h))
    return 2.0 * EARTH_RADIUS_METERS * math.asin(math.sqrt(h))


def point_in_polygon(point: Point, polygon: Sequence[Point]) -> bool:
    """Ray-casting containment test for a (lat, lon) point."""
    if len(polygon) < 3:
        return False
    lat, lon = point
    inside = False
    count = len(polygon)
    j = count - 1
    for i in range(count):
        lat_i, lon_i = polygon[i]
        lat_j, lon_j = polygon[j]
        straddles = (lon_i > lon) != (lon_j > lon)
        if straddles:
            denominator = lon_j - lon_i
            if denominator != 0.0:
                crossing_lat = lat_i + ((lon - lon_i) / denominator) * (lat_j - lat_i)
                if lat < crossing_lat:
                    inside = not inside
        j = i
    return inside


def polygon_centroid(polygon: Sequence[Point]) -> Point:
    """Arithmetic centroid of the fence vertices."""
    count = float(len(polygon))
    return (sum(p[0] for p in polygon) / count, sum(p[1] for p in polygon) / count)


def polygon_radius(polygon: Sequence[Point], centroid: Point) -> float:
    """Distance from centroid to the furthest vertex."""
    return max(haversine_meters(centroid, vertex) for vertex in polygon)


def parse_position(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract the position report from an IoT Core event."""
    device_id = event.get("deviceId") or event.get("device_id") or event.get("thingName")
    try:
        lat = float(event.get("lat", event.get("latitude")))
        lon = float(event.get("lon", event.get("longitude")))
    except (TypeError, ValueError):
        return None
    if not device_id or not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        return None
    try:
        accuracy = float(event.get("accuracy", 10.0))
    except (TypeError, ValueError):
        accuracy = 10.0
    return {"device_id": str(device_id), "point": (lat, lon), "accuracy": accuracy,
            "reported_at": int(event.get("ts") or event.get("timestamp") or time.time())}


def load_fences(device_id: str) -> List[Dict[str, Any]]:
    """Load the fence definitions assigned to a device."""
    table = dynamodb.Table(FENCE_TABLE)
    try:
        response = table.query(KeyConditionExpression=Key("device_id").eq(device_id))
    except ClientError as exc:
        logger.error("fence_load_failed device=%s err=%s", device_id, exc)
        return []
    fences: List[Dict[str, Any]] = []
    for item in response.get("Items", []):
        vertices = item.get("vertices") or []
        polygon: List[Point] = []
        for vertex in vertices:
            try:
                polygon.append((float(vertex["lat"]), float(vertex["lon"])))
            except (KeyError, TypeError, ValueError):
                continue
        if len(polygon) >= 3:
            fences.append({"fence_id": str(item.get("fence_id", "")), "polygon": polygon})
    return fences


def load_dwell(device_id: str, fence_id: str) -> Dict[str, Any]:
    """Read the outside-dwell tracking record for a device/fence pair."""
    table = dynamodb.Table(DWELL_TABLE)
    try:
        response = table.get_item(Key={"device_id": device_id, "fence_id": fence_id})
    except ClientError as exc:
        logger.warning("dwell_load_failed device=%s fence=%s err=%s", device_id, fence_id, exc)
        return {}
    return response.get("Item") or {}


def save_dwell(device_id: str, fence_id: str, outside_since: Optional[int],
               breached: bool, now: int) -> None:
    """Persist dwell state, or clear it when the device is back inside."""
    table = dynamodb.Table(DWELL_TABLE)
    try:
        if outside_since is None:
            table.delete_item(Key={"device_id": device_id, "fence_id": fence_id})
            return
        table.put_item(Item={"device_id": device_id, "fence_id": fence_id,
                             "outside_since": Decimal(str(outside_since)),
                             "breach_emitted": breached,
                             "updated_at": Decimal(str(now))})
    except ClientError as exc:
        logger.error("dwell_save_failed device=%s fence=%s err=%s", device_id, fence_id, exc)


def emit_breach(device_id: str, fence_id: str, point: Point, dwell_seconds: int) -> None:
    """Publish a breach event onto EventBridge."""
    try:
        events.put_events(Entries=[{
            "EventBusName": EVENT_BUS,
            "Source": "iot.geofence",
            "DetailType": "GeofenceBreach",
            "Detail": json.dumps({"deviceId": device_id, "fenceId": fence_id, "lat": point[0],
                                  "lon": point[1], "dwellSeconds": dwell_seconds}),
        }])
    except ClientError as exc:
        logger.error("breach_emit_failed device=%s fence=%s err=%s", device_id, fence_id, exc)


def lambda_handler(event, context):
    """Entry point for IoT Core geofence evaluation."""
    position = parse_position(event)
    if position is None:
        logger.error("unparseable_position keys=%s", sorted(event.keys()))
        return {"evaluated": 0, "reason": "unparseable_position"}
    if position["accuracy"] > MAX_ACCURACY_METERS:
        logger.info("position_too_imprecise device=%s accuracy=%s",
                    position["device_id"], position["accuracy"])
        return {"evaluated": 0, "reason": "accuracy_rejected"}

    device_id = position["device_id"]
    now = position["reported_at"]
    fences = load_fences(device_id)
    breaches: List[Dict[str, Any]] = []
    evaluated = 0

    for fence in fences:
        polygon = fence["polygon"]
        centroid = polygon_centroid(polygon)
        radius = polygon_radius(polygon, centroid)
        distance = haversine_meters(position["point"], centroid)
        evaluated += 1

        if distance <= radius - PREFILTER_SLACK_METERS:
            inside = True
        elif distance > radius + PREFILTER_SLACK_METERS:
            inside = False
        else:
            inside = point_in_polygon(position["point"], polygon)

        if inside:
            save_dwell(device_id, fence["fence_id"], None, False, now)
            continue

        dwell = load_dwell(device_id, fence["fence_id"])
        outside_since = int(dwell.get("outside_since", 0)) or now
        if now - outside_since > STALE_DWELL_RESET_SECONDS:
            outside_since = now
        already_emitted = bool(dwell.get("breach_emitted", False))
        dwell_seconds = now - outside_since

        if dwell_seconds >= DWELL_DEBOUNCE_SECONDS and not already_emitted:
            emit_breach(device_id, fence["fence_id"], position["point"], dwell_seconds)
            already_emitted = True
            breaches.append({"fence_id": fence["fence_id"], "dwell_seconds": dwell_seconds,
                             "distance_meters": round(distance, 2)})
        save_dwell(device_id, fence["fence_id"], outside_since, already_emitted, now)

    logger.info("geofence_evaluated device=%s fences=%s breaches=%s",
                device_id, evaluated, len(breaches))
    return {"device_id": device_id, "evaluated": evaluated, "breaches": breaches}
