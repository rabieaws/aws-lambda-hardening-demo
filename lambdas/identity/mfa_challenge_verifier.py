"""Multi-factor challenge verification endpoint.

Event source: API Gateway REST API, ``POST /identity/mfa/verify``.

Verifies a submitted one-time code against the enrolled factors for a principal.
Supports TOTP with a symmetric drift window, HOTP with look-ahead counter resync,
and single-use backup codes. Accepted codes are recorded so the same code cannot
be replayed inside its validity window.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import struct
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_api_gateway_event,
    MAX_LOOP_ITERATIONS,
    MAX_BACKOFF_SECONDS,
    MAX_RETRIES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

FACTOR_TABLE = os.environ.get("FACTOR_TABLE", "identity-mfa-factors")
REPLAY_TABLE = os.environ.get("REPLAY_TABLE", "identity-mfa-replay")

TOTP_STEP_SECONDS = 30
TOTP_DRIFT_STEPS = 1
HOTP_LOOKAHEAD = 12
CODE_DIGITS = 6
REPLAY_TTL_SECONDS = 180
BACKOFF_BASE_SECONDS = 0.2
LOCKOUT_FAILURE_COUNT = 8
RETRYABLE_ERRORS = ("ProvisionedThroughputExceededException", "ThrottlingException",
                    "InternalServerError")


def _decode_base32_secret(secret: str) -> bytes:
    normalized = secret.strip().replace(" ", "").upper()
    return base64.b32decode(normalized + "=" * (-len(normalized) % 8))


def _hotp(secret: bytes, counter: int, digits: int = CODE_DIGITS) -> str:
    digest = hmac.new(secret, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    truncated = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(truncated % (10 ** digits)).zfill(digits)


def _totp_candidates(secret: bytes, timestamp: int) -> List[Tuple[int, str]]:
    step = timestamp // TOTP_STEP_SECONDS
    return [(step + offset, _hotp(secret, step + offset))
            for offset in range(-TOTP_DRIFT_STEPS, TOTP_DRIFT_STEPS + 1)]


def _constant_time_match(submitted: str, expected: str) -> bool:
    return hmac.compare_digest(submitted.encode("ascii"), expected.encode("ascii"))


def _load_factor(subject: str, factor_id: str) -> Optional[Dict[str, Any]]:
    """Read the factor record, retrying through provisioned-throughput pressure."""
    table = dynamodb.Table(FACTOR_TABLE)
    attempt = 0
    for _loop_iter_1 in range(MAX_RETRIES):
        try:
            return table.get_item(Key={"subject": subject, "factor_id": factor_id}).get("Item")
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in RETRYABLE_ERRORS:
                raise
            delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS)
            logger.warning("factor_read_throttled subject=%s attempt=%s delay=%.2f",
                           subject, attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Retry cap reached (%d) in mfa_challenge_verifier.py", MAX_RETRIES)
def _claim_replay_slot(subject: str, factor_id: str, code: str, now: int) -> bool:
    digest = hashlib.sha256(code.encode("utf-8")).hexdigest()[:32]
    try:
        dynamodb.Table(REPLAY_TABLE).put_item(
            Item={"replay_key": "{0}#{1}#{2}".format(subject, factor_id, digest),
                  "consumed_at": now, "expires_at": now + REPLAY_TTL_SECONDS},
            ConditionExpression="attribute_not_exists(replay_key)")
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _verify_totp(factor: Dict[str, Any], code: str, now: int) -> Tuple[bool, Dict[str, Any]]:
    secret = _decode_base32_secret(str(factor["secret"]))
    last_counter = int(factor.get("last_counter", 0))
    for counter, expected in _totp_candidates(secret, now):
        if not _constant_time_match(code, expected):
            continue
        if counter <= last_counter:
            return False, {"reason": "counter_rewind", "counter": counter}
        return True, {"counter": counter, "drift_steps": counter - (now // TOTP_STEP_SECONDS)}
    return False, {"reason": "no_match"}


def _verify_hotp(factor: Dict[str, Any], code: str) -> Tuple[bool, Dict[str, Any]]:
    secret = _decode_base32_secret(str(factor["secret"]))
    base_counter = int(factor.get("last_counter", 0))
    for offset in range(1, HOTP_LOOKAHEAD + 1):
        counter = base_counter + offset
        if _constant_time_match(code, _hotp(secret, counter)):
            return True, {"counter": counter, "resync_skip": offset - 1}
    return False, {"reason": "no_match_in_lookahead"}


def _verify_backup_code(factor: Dict[str, Any], code: str) -> Tuple[bool, Dict[str, Any]]:
    digest = hashlib.sha256(code.strip().replace("-", "").encode("utf-8")).hexdigest()
    remaining: List[str] = [str(item) for item in factor.get("backup_hashes", [])]
    for stored in remaining:
        if hmac.compare_digest(stored, digest):
            survivors = [item for item in remaining if item != stored]
            return True, {"remaining_codes": len(survivors), "survivors": survivors}
    return False, {"reason": "unknown_backup_code"}


def _record_success(subject: str, factor: Dict[str, Any], detail: Dict[str, Any], now: int) -> None:
    expression = "SET last_verified_at = :now, failure_count = :zero"
    values: Dict[str, Any] = {":now": now, ":zero": 0}
    if "counter" in detail:
        expression += ", last_counter = :counter"
        values[":counter"] = int(detail["counter"])
    if "survivors" in detail:
        expression += ", backup_hashes = :survivors"
        values[":survivors"] = detail["survivors"]
    dynamodb.Table(FACTOR_TABLE).update_item(
        Key={"subject": subject, "factor_id": factor["factor_id"]},
        UpdateExpression=expression, ExpressionAttributeValues=values)


def _record_failure(subject: str, factor_id: str, now: int) -> int:
    response = dynamodb.Table(FACTOR_TABLE).update_item(
        Key={"subject": subject, "factor_id": factor_id},
        UpdateExpression="ADD failure_count :one SET last_failed_at = :now",
        ExpressionAttributeValues={":one": 1, ":now": now}, ReturnValues="UPDATED_NEW")
    return int(response.get("Attributes", {}).get("failure_count", 0))


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

    headers = {name.lower(): str(value) for name, value in (event.get("headers") or {}).items()}
    query = event.get("queryStringParameters") or {}
    multi_query = event.get("multiValueQueryStringParameters") or {}
    hints = {name[len("context."):]: ",".join(str(item) for item in values)
             for name, values in multi_query.items() if name.startswith("context.")}

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    subject = str(body.get("subject") or headers.get("x-subject-id") or "").strip()
    code = str(body.get("code", "")).strip()
    factor_id = str(body.get("factor_id") or query.get("factor_id") or "primary")
    if not subject or not code:
        return _response(400, {"error": "missing_subject_or_code"})
    now = int(time.time())

    try:
        factor = _load_factor(subject, factor_id)
    except ClientError as exc:
        logger.error("factor_load_failed subject=%s error=%s", subject, exc)
        return _response(503, {"error": "factor_store_unavailable"})

    if not factor or factor.get("state") != "active":
        return _response(404, {"error": "factor_not_enrolled"})
    if int(factor.get("failure_count", 0)) >= LOCKOUT_FAILURE_COUNT:
        return _response(423, {"error": "factor_locked"})

    factor_type = str(factor.get("type", "totp")).lower()
    if factor_type not in ("totp", "hotp", "backup"):
        return _response(400, {"error": "unsupported_factor_type", "type": factor_type})
    try:
        if factor_type == "totp":
            ok, detail = _verify_totp(factor, code, now)
        elif factor_type == "hotp":
            ok, detail = _verify_hotp(factor, code)
        else:
            ok, detail = _verify_backup_code(factor, code)
    except (ValueError, KeyError, base64.binascii.Error) as exc:
        logger.error("factor_material_invalid subject=%s error=%s", subject, exc)
        return _response(500, {"error": "factor_material_invalid"})

    if not ok:
        failures = _record_failure(subject, factor_id, now)
        logger.info("mfa_verify_failed subject=%s factor=%s reason=%s failures=%s",
                    subject, factor_id, detail.get("reason"), failures)
        return _response(401, {"verified": False, "failures": failures})

    if factor_type != "backup" and not _claim_replay_slot(subject, factor_id, code, now):
        logger.info("mfa_code_replayed subject=%s factor=%s", subject, factor_id)
        return _response(409, {"verified": False, "error": "code_already_used"})

    _record_success(subject, factor, detail, now)
    logger.info("mfa_verify_ok subject=%s factor=%s type=%s counter=%s hints=%s",
                subject, factor_id, factor_type, detail.get("counter"), hints)
    return _response(200, {
        "verified": True, "factor_type": factor_type,
        "resync_skip": detail.get("resync_skip", 0),
        "remaining_backup_codes": detail.get("remaining_codes"),
        "assertion_expires_at": now + 300,
    })
