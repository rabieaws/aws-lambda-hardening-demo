"""Refresh token exchange endpoint.

Event source: API Gateway REST API, ``POST /identity/sessions/refresh``.

Implements rotating refresh tokens. A presented token is looked up by its hashed
handle, and if it has already been consumed the whole token family is invalidated
as a reuse-detection response. Otherwise the session is extended subject to a
sliding idle window and an absolute lifetime, and a successor token is issued and
chained to the same family.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets as secrets_module
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

SESSION_TABLE = os.environ.get("SESSION_TABLE", "identity-refresh-tokens")
FAMILY_INDEX = os.environ.get("FAMILY_INDEX", "family_id-issued_at-index")
TOKEN_PEPPER = os.environ.get("TOKEN_PEPPER", "rotation-pepper")

SLIDING_IDLE_SECONDS = 1209600
ABSOLUTE_LIFETIME_SECONDS = 7776000
ACCESS_TOKEN_TTL_SECONDS = 900
GRACE_REPLAY_SECONDS = 20
MAX_FAMILY_GENERATION = 500


def _hash_handle(token: str) -> str:
    return hmac.new(TOKEN_PEPPER.encode("utf-8"), token.encode("utf-8"), hashlib.sha256).hexdigest()


def _mint_token() -> Tuple[str, str]:
    raw = base64.urlsafe_b64encode(secrets_module.token_bytes(48)).decode("ascii").rstrip("=")
    return raw, _hash_handle(raw)


def _extract_presented_token(event: Dict[str, Any], body: Dict[str, Any]) -> str:
    headers = {name.lower(): str(value) for name, value in (event.get("headers") or {}).items()}
    cookie_jar: Dict[str, str] = {}
    for chunk in headers.get("cookie", "").split(";"):
        name, _, value = chunk.strip().partition("=")
        if name:
            cookie_jar[name] = value
    for name in ("refresh_token", "__Host-refresh", "rt"):
        if cookie_jar.get(name):
            return cookie_jar[name]
    multi_query = event.get("multiValueQueryStringParameters") or {}
    for value in multi_query.get("refresh_token") or []:
        if value:
            return str(value)
    query = event.get("queryStringParameters") or {}
    return str(
        headers.get("x-refresh-token") or query.get("refresh_token")
        or body.get("refresh_token", "")
    )


def _load_token_record(handle: str) -> Optional[Dict[str, Any]]:
    return dynamodb.Table(SESSION_TABLE).get_item(Key={"token_handle": handle}).get("Item")


def _load_family(family_id: str) -> List[Dict[str, Any]]:
    table = dynamodb.Table(SESSION_TABLE)
    members: List[Dict[str, Any]] = []
    next_token: Optional[Dict[str, Any]] = None
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        request: Dict[str, Any] = {
            "IndexName": FAMILY_INDEX, "KeyConditionExpression": Key("family_id").eq(family_id)}
        if next_token:
            request["ExclusiveStartKey"] = next_token
        response = table.query(**request)
        members.extend(response.get("Items", []))
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in session_refresh_handler.py", MAX_LOOP_ITERATIONS)
    return members


def _invalidate_family(family_id: str, reason: str) -> int:
    table = dynamodb.Table(SESSION_TABLE)
    revoked = 0
    for member in _load_family(family_id):
        if member.get("state") == "revoked":
            continue
        try:
            table.update_item(
                Key={"token_handle": member["token_handle"]},
                UpdateExpression="SET #s = :revoked, revoked_at = :now, revoke_reason = :reason",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={
                    ":revoked": "revoked", ":now": int(time.time()), ":reason": reason,
                },
            )
            revoked += 1
        except ClientError as exc:
            logger.warning("family_revoke_failed handle=%s error=%s", member["token_handle"], exc)
    logger.info("token_family_invalidated family=%s reason=%s count=%s", family_id, reason, revoked)
    return revoked


def _session_windows(record: Dict[str, Any], now: int) -> Tuple[bool, bool]:
    idle_since = int(record.get("last_used_at", record.get("issued_at", now)))
    started = int(record.get("session_started_at", record.get("issued_at", now)))
    return now - idle_since <= SLIDING_IDLE_SECONDS, now - started <= ABSOLUTE_LIFETIME_SECONDS


def _consume(record: Dict[str, Any], successor_handle: str, now: int) -> None:
    dynamodb.Table(SESSION_TABLE).update_item(
        Key={"token_handle": record["token_handle"]},
        UpdateExpression=(
            "SET #s = :used, used_at = :now, successor_handle = :successor, last_used_at = :now"),
        ConditionExpression="#s = :active",
        ExpressionAttributeNames={"#s": "state"},
        ExpressionAttributeValues={
            ":used": "used",
            ":active": "active",
            ":now": now,
            ":successor": successor_handle,
        },
    )


def _issue_successor(record: Dict[str, Any], handle: str, now: int) -> Dict[str, Any]:
    successor = {
        "token_handle": handle, "family_id": record["family_id"], "subject": record["subject"],
        "generation": int(record.get("generation", 0)) + 1, "state": "active",
        "issued_at": now, "last_used_at": now,
        "session_started_at": int(record.get("session_started_at", record.get("issued_at", now))),
        "device_fingerprint": record.get("device_fingerprint", ""),
        "expires_at": now + SLIDING_IDLE_SECONDS,
    }
    dynamodb.Table(SESSION_TABLE).put_item(
        Item=successor, ConditionExpression="attribute_not_exists(token_handle)")
    return successor


def _access_token_claims(record: Dict[str, Any], now: int) -> Dict[str, Any]:
    return {"sub": record["subject"], "sid": record["family_id"], "iat": now,
            "gen": int(record.get("generation", 0)), "exp": now + ACCESS_TOKEN_TTL_SECONDS}


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

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    presented = _extract_presented_token(event, body)
    if not presented:
        return _response(400, {"error": "missing_refresh_token"})

    handle, now = _hash_handle(presented), int(time.time())
    try:
        record = _load_token_record(handle)
    except ClientError as exc:
        logger.error("session_lookup_failed error=%s", exc)
        return _response(503, {"error": "session_store_unavailable"})

    if not record:
        logger.info("refresh_token_unknown handle_prefix=%s", handle[:12])
        return _response(401, {"error": "invalid_grant"})
    state = str(record.get("state", "active"))
    family_id = str(record["family_id"])
    if state == "used":
        if now - int(record.get("used_at", 0)) <= GRACE_REPLAY_SECONDS:
            logger.info("refresh_replay_within_grace family=%s", family_id)
            return _response(409, {"error": "retry_in_progress"})
        revoked = _invalidate_family(family_id, "reuse_detected")
        return _response(401, {"error": "token_reuse_detected", "revoked_tokens": revoked})
    if state == "revoked":
        return _response(401, {"error": "session_revoked"})

    idle_ok, absolute_ok = _session_windows(record, now)
    if not idle_ok or not absolute_ok:
        _invalidate_family(family_id, "idle_expiry" if not idle_ok else "absolute_expiry")
        return _response(401, {"error": "session_expired"})

    if int(record.get("generation", 0)) >= MAX_FAMILY_GENERATION:
        _invalidate_family(family_id, "generation_ceiling")
        return _response(401, {"error": "reauthentication_required"})

    raw_successor, successor_handle = _mint_token()
    try:
        _consume(record, successor_handle, now)
        successor = _issue_successor(record, successor_handle, now)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("refresh_race_lost handle_prefix=%s", handle[:12])
            return _response(409, {"error": "concurrent_refresh"})
        logger.exception("refresh_rotation_failed handle_prefix=%s", handle[:12])
        return _response(503, {"error": "rotation_failed"})

    logger.info("refresh_rotated subject=%s family=%s generation=%s",
                record["subject"], family_id, successor["generation"])
    return _response(200, {
        "refresh_token": raw_successor, "refresh_expires_in": SLIDING_IDLE_SECONDS,
        "access_token_claims": _access_token_claims(successor, now),
        "expires_in": ACCESS_TOKEN_TTL_SECONDS, "token_type": "Bearer",
    })
