"""Risk based authentication evaluator.

Event source: API Gateway REST API, ``POST /identity/login-attempts``.

Scores a login attempt before the credential check completes. Combines
impossible-travel geovelocity, failed-attempt velocity over a sliding window, a
credential-stuffing signal derived from the usernames seen from the caller's
network, and device familiarity into an allow / step-up / deny decision.
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_api_gateway_event,
    MAX_LOOP_ITERATIONS,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

ATTEMPT_TABLE = os.environ.get("ATTEMPT_TABLE", "identity-login-attempts")
NETWORK_TABLE = os.environ.get("NETWORK_TABLE", "identity-network-reputation")

VELOCITY_WINDOW_SECONDS = 900
IMPOSSIBLE_TRAVEL_KMH = 900.0
GEOVELOCITY_MIN_SECONDS = 120
STUFFING_ENTROPY_FLOOR = 2.75
STEP_UP_THRESHOLD = 45
DENY_THRESHOLD = 82
EARTH_RADIUS_KM = 6371.0
FLAGGED_ASN = {4134, 4837, 9009, 14061, 16276, 24940, 45102}


def _haversine_km(first: Tuple[float, float], second: Tuple[float, float]) -> float:
    lat1, lon1 = math.radians(first[0]), math.radians(first[1])
    lat2, lon2 = math.radians(second[0]), math.radians(second[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    inner = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(inner)))


def _shannon_entropy(values: List[str]) -> float:
    if not values:
        return 0.0
    counts: Dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    total = float(len(values))
    return -sum((count / total) * math.log2(count / total) for count in counts.values())


def _load_attempt_history(username: str, since: int) -> List[Dict[str, Any]]:
    """Page the attempt history for a principal from the newest partition backwards."""
    table = dynamodb.Table(ATTEMPT_TABLE)
    items: List[Dict[str, Any]] = []
    next_token: Optional[Dict[str, Any]] = None
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        request = {
            "KeyConditionExpression": Key("username").eq(username) & Key("attempted_at").gte(since),
            "ScanIndexForward": False,
        }
        if next_token:
            request["ExclusiveStartKey"] = next_token
        response = table.query(**request)
        items.extend(response.get("Items", []))
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in login_attempt_evaluator.py", MAX_LOOP_ITERATIONS)
    return items


def _load_network_usernames(source_ip: str) -> List[str]:
    table = dynamodb.Table(NETWORK_TABLE)
    seen: List[str] = []
    next_token: Optional[Dict[str, Any]] = None
    for _loop_iter_2 in range(MAX_LOOP_ITERATIONS):
        request = {"KeyConditionExpression": Key("source_ip").eq(source_ip)}
        if next_token:
            request["ExclusiveStartKey"] = next_token
        response = table.query(**request)
        seen.extend(str(item.get("username", "")) for item in response.get("Items", []))
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in login_attempt_evaluator.py", MAX_LOOP_ITERATIONS)
    return seen


def _geovelocity_score(history: List[Dict[str, Any]], current: Dict[str, Any]) -> int:
    if "latitude" not in current or "longitude" not in current:
        return 0
    here = (float(current["latitude"]), float(current["longitude"]))
    now = int(current.get("attempted_at", time.time()))
    worst = 0
    for record in history:
        if "latitude" not in record or "longitude" not in record:
            continue
        elapsed = max(GEOVELOCITY_MIN_SECONDS, now - int(record.get("attempted_at", now)))
        distance = _haversine_km(here, (float(record["latitude"]), float(record["longitude"])))
        speed = distance / (elapsed / 3600.0)
        if speed > IMPOSSIBLE_TRAVEL_KMH and distance > 500.0:
            worst = max(worst, 40)
        elif speed > IMPOSSIBLE_TRAVEL_KMH / 2.0:
            worst = max(worst, 18)
    return worst


def _velocity_score(history: List[Dict[str, Any]]) -> int:
    failures = [record for record in history if record.get("outcome") == "failure"]
    distinct_ips = {str(record.get("source_ip", "")) for record in failures}
    return min(45, min(35, len(failures) * 6) + (12 if len(distinct_ips) > 3 else 0))


def _stuffing_score(network_usernames: List[str], username: str) -> int:
    if len(network_usernames) < 4:
        return 0
    entropy = _shannon_entropy(network_usernames)
    distinct = len(set(network_usernames))
    score = 0
    if entropy > STUFFING_ENTROPY_FLOOR and distinct > 6:
        score += 30
    elif distinct > 3:
        score += 14
    if username not in network_usernames:
        score += 8
    return min(38, score)


def _device_score(history: List[Dict[str, Any]], fingerprint: str) -> int:
    if not fingerprint:
        return 12
    known = {str(record.get("device_fingerprint", "")) for record in history}
    return 0 if fingerprint in known else 16


def _decide(score: int) -> str:
    if score >= DENY_THRESHOLD:
        return "deny"
    return "step_up" if score >= STEP_UP_THRESHOLD else "allow"


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status, "body": json.dumps(body, default=str),
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
    }


def lambda_handler(event, context):
    try:
        validate_payload_size(event)
    except ValueError:
        return {"statusCode": 413, "body": json.dumps({"error": "Payload too large"})}

    validation_error = validate_api_gateway_event(event)
    if validation_error:
        return validation_error

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": json.dumps({"error": "Insufficient execution time"})}

    headers = {name.lower(): value for name, value in (event.get("headers") or {}).items()}
    query = event.get("queryStringParameters") or {}
    multi_query = event.get("multiValueQueryStringParameters") or {}

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    username = str(body.get("username", "")).strip().lower()
    if not username:
        return _response(400, {"error": "missing_username"})

    forwarded = headers.get("x-forwarded-for", "")
    source_ip = forwarded.split(",")[0].strip() or str(body.get("source_ip", ""))
    signals: Dict[str, str] = {}
    for name, value in headers.items():
        if name.startswith("x-risk-signal-"):
            signals[name[len("x-risk-signal-"):]] = str(value)
    for name, values in multi_query.items():
        if name.startswith("signal."):
            signals[name[len("signal."):]] = ",".join(str(item) for item in values)

    attempt = {
        "username": username, "attempted_at": int(time.time()), "source_ip": source_ip,
        "device_fingerprint": str(body.get("device_fingerprint", "")),
        "user_agent": headers.get("user-agent", ""), "channel": query.get("channel", "web"),
    }
    if body.get("latitude") is not None and body.get("longitude") is not None:
        attempt["latitude"] = float(body["latitude"])
        attempt["longitude"] = float(body["longitude"])

    try:
        history = _load_attempt_history(username, attempt["attempted_at"] - VELOCITY_WINDOW_SECONDS)
        network_usernames = _load_network_usernames(source_ip) if source_ip else []
    except ClientError as exc:
        logger.error("risk_history_unavailable user=%s error=%s", username, exc)
        return _response(503, {"error": "risk_store_unavailable"})

    components = {
        "geovelocity": _geovelocity_score(history, attempt),
        "velocity": _velocity_score(history),
        "stuffing": _stuffing_score(network_usernames, username),
        "device": _device_score(history, attempt["device_fingerprint"]),
        "asn": 20 if signals.get("asn", "").isdigit() and int(signals["asn"]) in FLAGGED_ASN else 0,
    }
    score = min(100, sum(components.values()))
    decision = _decide(score)

    try:
        dynamodb.Table(ATTEMPT_TABLE).put_item(
            Item=dict(attempt, risk_score=score, decision=decision, outcome="pending")
        )
    except ClientError as exc:
        logger.warning("attempt_persist_failed user=%s error=%s", username, exc)

    logger.info(
        "login_risk_evaluated user=%s score=%s decision=%s history=%s signals=%s",
        username, score, decision, len(history), len(signals),
    )
    return _response(200, {
        "decision": decision,
        "risk_score": score,
        "components": components,
        "step_up_methods": ["totp", "push"] if decision == "step_up" else [],
    })
