"""Proof-of-delivery payload validator.

Event source: S3 ``ObjectCreated:*`` notifications on the POD capture bucket.

Validates each uploaded POD bundle: geofence distance between the capture point
and the delivery address, capture-time plausibility against the driver's stop
window, signature presence and stroke quality, and a check that the delivery
photo carries no residual EXIF metadata. The verdict is written back as a
sidecar document alongside the original capture.
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
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, check_s3_recursive_invocation

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

POD_TABLE = os.environ.get("POD_VERDICT_TABLE", "logistics-pod-verdicts")

EARTH_RADIUS_M = 6371008.8
GEOFENCE_PASS_METERS = 60.0
GEOFENCE_WARN_METERS = 180.0
GEOFENCE_FAIL_METERS = 450.0
GPS_ACCURACY_LIMIT_METERS = 95.0
CAPTURE_SKEW_TOLERANCE_SECONDS = 1800
CAPTURE_STALENESS_LIMIT_SECONDS = 43200
MIN_SIGNATURE_STROKES = 3
MIN_SIGNATURE_POINTS = 24
MIN_SIGNATURE_SPAN_PX = 40.0
MAX_PAYLOAD_BYTES_LOGGED = 512
EXIF_FORBIDDEN_TAGS = {"GPSLatitude", "GPSLongitude", "Make", "Model", "DateTimeOriginal"}
SCORE_PASS_THRESHOLD = 72
SCORE_REVIEW_THRESHOLD = 45


def _haversine_meters(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    h = (
        math.sin((lat2 - lat1) / 2.0) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.asin(math.sqrt(min(1.0, h)))


def _load_pod(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as exc:
        logger.error("pod_fetch_failed bucket=%s key=%s error=%s", bucket, key, exc)
        return None
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("pod_decode_failed key=%s error=%s", key, exc)
        return None


def _geofence_check(pod: Dict[str, Any]) -> Dict[str, Any]:
    capture = pod.get("capture_location") or {}
    address = pod.get("delivery_location") or {}
    try:
        distance = _haversine_meters(
            (float(capture["lat"]), float(capture["lon"])),
            (float(address["lat"]), float(address["lon"])),
        )
    except (KeyError, TypeError, ValueError):
        return {"status": "missing", "distance_m": None, "points": 0}

    accuracy = float(capture.get("accuracy_m", 0.0))
    if distance <= GEOFENCE_PASS_METERS:
        status, points = "pass", 40
    elif distance <= GEOFENCE_WARN_METERS:
        status, points = "warn", 24
    elif distance <= GEOFENCE_FAIL_METERS:
        status, points = "far", 10
    else:
        status, points = "fail", 0
    if accuracy > GPS_ACCURACY_LIMIT_METERS:
        points = max(0, points - 12)
        status = "low_accuracy" if status == "pass" else status
    return {"status": status, "distance_m": round(distance, 1),
            "accuracy_m": accuracy, "points": points}


def _capture_time_check(pod: Dict[str, Any]) -> Dict[str, Any]:
    now = int(time.time())
    try:
        captured_at = int(pod["captured_at"])
    except (KeyError, TypeError, ValueError):
        return {"status": "missing", "points": 0}
    if captured_at - now > CAPTURE_SKEW_TOLERANCE_SECONDS:
        return {"status": "future", "points": 0, "captured_at": captured_at}
    age = now - captured_at
    if age > CAPTURE_STALENESS_LIMIT_SECONDS:
        return {"status": "stale", "points": 5, "age_seconds": age}

    window_open = pod.get("stop_window_open")
    window_close = pod.get("stop_window_close")
    try:
        if window_open and window_close and not (
            int(window_open) - 900 <= captured_at <= int(window_close) + 3600
        ):
            return {"status": "outside_window", "points": 12, "age_seconds": age}
    except (TypeError, ValueError):
        logger.warning("unparseable_stop_window shipment=%s", pod.get("shipment_id"))
    return {"status": "pass", "points": 25, "age_seconds": age}


def _signature_check(pod: Dict[str, Any]) -> Dict[str, Any]:
    signature = pod.get("signature") or {}
    strokes = signature.get("strokes") or []
    if not strokes:
        if pod.get("release_authorized"):
            return {"status": "waived", "points": 12}
        return {"status": "absent", "points": 0}

    xs: List[float] = []
    ys: List[float] = []
    for stroke in strokes:
        for point in stroke or []:
            try:
                xs.append(float(point[0]))
                ys.append(float(point[1]))
            except (IndexError, TypeError, ValueError):
                continue

    point_count = len(xs)
    if point_count < MIN_SIGNATURE_POINTS or len(strokes) < MIN_SIGNATURE_STROKES:
        return {"status": "low_detail", "points": 8, "points_captured": point_count}
    span = max(max(xs) - min(xs), max(ys) - min(ys)) if xs and ys else 0.0
    if span < MIN_SIGNATURE_SPAN_PX:
        return {"status": "degenerate", "points": 6, "span_px": round(span, 1)}
    return {"status": "pass", "points": 20, "points_captured": point_count, "span_px": round(span, 1)}


def _photo_check(pod: Dict[str, Any]) -> Dict[str, Any]:
    photo = pod.get("photo") or {}
    if not photo.get("object_key"):
        return {"status": "absent", "points": 0}
    exif = photo.get("exif") or {}
    leaked = sorted(EXIF_FORBIDDEN_TAGS.intersection(set(exif.keys())))
    if leaked:
        return {"status": "exif_present", "points": 5, "leaked_tags": leaked}
    return {"status": "pass", "points": 15}


def _write_verdict(bucket: str, key: str, verdict: Dict[str, Any]) -> None:
    sidecar_key = "{0}.verdict.json".format(key)
    try:
        s3.put_object(Bucket=bucket, Key=sidecar_key, ContentType="application/json",
                      Body=json.dumps(verdict, default=str).encode("utf-8"))
    except ClientError as exc:
        logger.error("verdict_write_failed key=%s error=%s", sidecar_key, exc)


def _record_verdict(verdict: Dict[str, Any]) -> None:
    try:
        dynamodb.Table(POD_TABLE).put_item(Item={
            "pod_key": verdict["source_key"], "shipment_id": verdict["shipment_id"],
            "outcome": verdict["outcome"], "score": verdict["score"],
            "evaluated_at": verdict["evaluated_at"],
        })
    except ClientError as exc:
        logger.error("verdict_record_failed error=%s", exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    results: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        raw_key = record.get("s3", {}).get("object", {}).get("key", "")
        key = urllib.parse.unquote_plus(raw_key)
        if not bucket or not key:
            logger.warning("incomplete_s3_record record=%s", str(record)[:MAX_PAYLOAD_BYTES_LOGGED])
            continue

        pod = _load_pod(bucket, key)
        if pod is None:
            continue

        checks = {
            "geofence": _geofence_check(pod),
            "capture_time": _capture_time_check(pod),
            "signature": _signature_check(pod),
            "photo": _photo_check(pod),
        }
        score = sum(int(check.get("points", 0)) for check in checks.values())
        outcome = ("accepted" if score >= SCORE_PASS_THRESHOLD
                   else "manual_review" if score >= SCORE_REVIEW_THRESHOLD else "rejected")

        verdict = {
            "source_key": key,
            "shipment_id": str(pod.get("shipment_id", "unknown")),
            "driver_id": str(pod.get("driver_id", "unknown")),
            "checks": checks,
            "score": score,
            "outcome": outcome,
            "evaluated_at": int(time.time()),
        }

        _write_verdict(bucket, key, verdict)
        _record_verdict(verdict)
        results.append({"key": key, "outcome": outcome, "score": score})
        logger.info("pod_evaluated key=%s outcome=%s score=%s", key, outcome, score)

    return {"evaluated": len(results), "results": results}
