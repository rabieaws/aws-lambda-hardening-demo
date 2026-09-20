"""Device trust evaluation endpoint.

Event source: API Gateway REST API, ``POST /identity/devices/evaluate``.

Compares a submitted device fingerprint against the fingerprints already enrolled
for the principal using a weighted Jaccard similarity over attribute sets, assigns
a trust tier from the best match plus enrolment age and attestation signals, and
first-seen enrols the device when no prior fingerprint is close enough.
"""

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

DEVICE_TABLE = os.environ.get("DEVICE_TABLE", "identity-trusted-devices")

ATTRIBUTE_WEIGHTS = {
    "platform": 3.0, "platform_version": 1.5, "browser": 2.0, "browser_version": 1.0,
    "screen": 2.0, "timezone": 1.5, "languages": 1.5, "gpu_renderer": 2.5, "fonts": 2.0,
    "audio_hash": 2.5, "canvas_hash": 3.0, "cpu_class": 1.0, "touch_points": 0.5,
    "color_depth": 0.5}
MATCH_STRONG = 0.92
MATCH_PARTIAL = 0.78
MIN_ATTRIBUTES = 4
TIER_TRUSTED_AGE_SECONDS = 1209600
TIER_TRUSTED_SIGHTINGS = 5
STALE_DEVICE_SECONDS = 15552000
ATTESTATION_BONUS = 0.05
DEVICE_TTL_SECONDS = 31536000
ATTESTATION_PASS = ("pass", "meets_device_integrity")


def _normalize_attribute(name: str, value: Any) -> Set[str]:
    items = value if isinstance(value, list) else [value]
    return {"{0}={1}".format(name, str(item).strip().lower())
            for item in items if str(item).strip()}


def _build_signature(attributes: Dict[str, Any]) -> Dict[str, Set[str]]:
    signature: Dict[str, Set[str]] = {}
    for name, value in attributes.items():
        key = str(name).strip().lower()
        tokens = _normalize_attribute(key, value) if key in ATTRIBUTE_WEIGHTS else set()
        if tokens:
            signature[key] = tokens
    return signature


def _weighted_jaccard(left: Dict[str, Set[str]], right: Dict[str, Set[str]]) -> float:
    numerator, denominator = 0.0, 0.0
    for attribute in set(left) | set(right):
        left_tokens, right_tokens = left.get(attribute, set()), right.get(attribute, set())
        union = left_tokens | right_tokens
        if not union:
            continue
        weight = ATTRIBUTE_WEIGHTS.get(attribute, 1.0)
        numerator += weight * (len(left_tokens & right_tokens) / float(len(union)))
        denominator += weight
    return numerator / denominator if denominator else 0.0


def _signature_hash(signature: Dict[str, Set[str]]) -> str:
    canonical = "|".join("{0}:{1}".format(name, ",".join(sorted(tokens)))
                         for name, tokens in sorted(signature.items()))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _stored_signature(item: Dict[str, Any]) -> Dict[str, Set[str]]:
    raw = item.get("signature") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = {}
    return {str(name): {str(token) for token in tokens} for name, tokens in raw.items()}


def _load_devices(subject: str) -> List[Dict[str, Any]]:
    table = dynamodb.Table(DEVICE_TABLE)
    devices: List[Dict[str, Any]] = []
    next_token: Optional[Dict[str, Any]] = None
    while True:
        request: Dict[str, Any] = {"KeyConditionExpression": Key("subject").eq(subject)}
        if next_token:
            request["ExclusiveStartKey"] = next_token
        response = table.query(**request)
        devices.extend(response.get("Items", []))
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    return devices


def _best_match(signature: Dict[str, Set[str]], devices: List[Dict[str, Any]],
                now: int) -> Tuple[Optional[Dict[str, Any]], float]:
    best: Optional[Dict[str, Any]] = None
    best_score = 0.0
    for device in devices:
        if now - int(device.get("last_seen_at", 0)) > STALE_DEVICE_SECONDS:
            continue
        score = _weighted_jaccard(signature, _stored_signature(device))
        if score > best_score:
            best, best_score = device, score
    return best, best_score


def _assign_tier(match: Optional[Dict[str, Any]], score: float, attested: bool, now: int) -> str:
    adjusted = min(1.0, score + (ATTESTATION_BONUS if attested else 0.0))
    if match is None or adjusted < MATCH_PARTIAL:
        return "provisional"
    if adjusted < MATCH_STRONG:
        return "changed"
    aged = now - int(match.get("first_seen_at", now)) >= TIER_TRUSTED_AGE_SECONDS
    often = int(match.get("sighting_count", 1)) >= TIER_TRUSTED_SIGHTINGS
    return "trusted" if aged and often else "recognized"


def _enrol(subject: str, device_id: str, signature: Dict[str, Set[str]], now: int,
           metadata: Dict[str, str]) -> None:
    dynamodb.Table(DEVICE_TABLE).put_item(
        Item={"subject": subject, "device_id": device_id, "sighting_count": 1,
              "signature": {name: sorted(tokens) for name, tokens in signature.items()},
              "first_seen_at": now, "last_seen_at": now, "trust_tier": "provisional",
              "expires_at": now + DEVICE_TTL_SECONDS, "enrolled_from": metadata["source_ip"],
              "user_agent": metadata["user_agent"]},
        ConditionExpression="attribute_not_exists(device_id)")


def _touch(subject: str, device_id: str, signature: Dict[str, Set[str]], tier: str,
           score: float, now: int) -> None:
    dynamodb.Table(DEVICE_TABLE).update_item(
        Key={"subject": subject, "device_id": device_id},
        UpdateExpression=(
            "SET last_seen_at = :now, trust_tier = :tier, last_score = :score, "
            "signature = :signature, expires_at = :expiry ADD sighting_count :one"),
        ExpressionAttributeValues={
            ":now": now, ":tier": tier, ":score": "{0:.4f}".format(score), ":one": 1,
            ":signature": {name: sorted(tokens) for name, tokens in signature.items()},
            ":expiry": now + DEVICE_TTL_SECONDS})


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {"statusCode": status, "body": json.dumps(body, default=str),
            "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"}}


def lambda_handler(event, context):
    headers = {name.lower(): str(value) for name, value in (event.get("headers") or {}).items()}
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    request_context = event.get("requestContext") or {}
    authorizer = request_context.get("authorizer") or {}
    subject = str(authorizer.get("subject") or body.get("subject")
                  or headers.get("x-subject-id") or "")
    if not subject:
        return _response(401, {"error": "unidentified_subject"})
    attributes: Dict[str, Any] = dict(body.get("fingerprint") or {})
    for name, value in headers.items():
        if name.startswith("x-device-"):
            attributes[name[len("x-device-"):].replace("-", "_")] = value
    for name, values in (event.get("multiValueQueryStringParameters") or {}).items():
        if name.startswith("fp."):
            attributes[name[len("fp."):]] = values
    for name, value in (event.get("queryStringParameters") or {}).items():
        if name.startswith("attr."):
            attributes[name[len("attr."):]] = value
    if headers.get("user-agent"):
        attributes.setdefault("browser", headers["user-agent"].split("/")[0])

    signature = _build_signature(attributes)
    if len(signature) < MIN_ATTRIBUTES:
        return _response(422, {"error": "insufficient_fingerprint_attributes",
                               "supplied": sorted(signature)})
    now = int(time.time())
    try:
        devices = _load_devices(subject)
    except ClientError as exc:
        logger.error("device_load_failed subject=%s error=%s", subject, exc)
        return _response(503, {"error": "device_store_unavailable"})
    match, score = _best_match(signature, devices, now)
    attested = str(body.get("attestation_verdict", "")).lower() in ATTESTATION_PASS
    tier = _assign_tier(match, score, attested, now)
    metadata = {"user_agent": headers.get("user-agent", ""), "source_ip": str(
        (request_context.get("identity") or {}).get("sourceIp", ""))}
    try:
        if match is not None and score >= MATCH_PARTIAL:
            device_id = str(match["device_id"])
            _touch(subject, device_id, signature, tier, score, now)
            enrolled = False
        else:
            device_id = _signature_hash(signature)
            _enrol(subject, device_id, signature, now, metadata)
            enrolled = True
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            logger.exception("device_persist_failed subject=%s", subject)
            return _response(503, {"error": "device_store_unavailable"})
        device_id, enrolled = _signature_hash(signature), False

    logger.info("device_trust_evaluated subject=%s device=%s tier=%s score=%.4f known=%s",
                subject, device_id, tier, score, len(devices))
    return _response(200, {
        "device_id": device_id, "trust_tier": tier, "similarity": round(score, 4),
        "newly_enrolled": enrolled, "attested": attested, "known_devices": len(devices),
        "step_up_required": tier in ("provisional", "changed")})
