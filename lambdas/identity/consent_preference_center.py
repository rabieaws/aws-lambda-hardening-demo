"""Consent preference center endpoint.

Event source: API Gateway REST API, ``GET /identity/consents`` and
``PUT /identity/consents``.

Reads and writes per-purpose consent records for a data subject. Resolves the
applicable jurisdiction from the request, applies that jurisdiction's opt-in versus
opt-out default, and reconciles the requested state against the legal-basis
precedence order so a contractual basis is not overwritten by a withdrawal flag.
"""

import json
import logging
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

CONSENT_TABLE = os.environ.get("CONSENT_TABLE", "identity-consent-records")
AUDIT_TABLE = os.environ.get("CONSENT_AUDIT_TABLE", "identity-consent-audit")

OPT_IN_JURISDICTIONS = {"DE", "FR", "ES", "IT", "NL", "SE", "IE", "PL", "BE", "AT", "BR", "KR"}
OPT_OUT_JURISDICTIONS = {"US", "CA", "AU", "JP", "SG", "MX"}
KNOWN_PURPOSES = {"essential", "analytics", "personalization", "advertising", "profiling",
                  "third_party_sharing", "email_marketing", "sms_marketing", "research"}
LEGAL_BASIS_PRECEDENCE = {"legal_obligation": 5, "vital_interest": 4, "contract": 3,
                          "legitimate_interest": 2, "consent": 1}
ALWAYS_ON_PURPOSES = {"essential"}
GRANT_TOKENS = {"1", "true", "yes", "granted", "opt_in", "allow"}
DENY_TOKENS = {"0", "false", "no", "denied", "opt_out", "deny", "withdrawn"}
CONSENT_TTL_SECONDS = 31536000
RECONFIRM_AFTER_SECONDS = 15552000


def _resolve_jurisdiction(query: Dict[str, str], headers: Dict[str, str], residency: str) -> str:
    for candidate in (query.get("jurisdiction"), query.get("region"),
                      headers.get("cloudfront-viewer-country"), headers.get("x-viewer-country"),
                      residency):
        normalized = str(candidate or "").strip().upper()[:2]
        if normalized.isalpha():
            return normalized
    return "US"


def _default_state(jurisdiction: str, purpose: str) -> str:
    on = purpose in ALWAYS_ON_PURPOSES or jurisdiction in OPT_OUT_JURISDICTIONS
    return "granted" if on else "denied"


def _load_records(subject: str) -> Dict[str, Dict[str, Any]]:
    table = dynamodb.Table(CONSENT_TABLE)
    records: Dict[str, Dict[str, Any]] = {}
    next_token: Optional[Dict[str, Any]] = None
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        request: Dict[str, Any] = {"KeyConditionExpression": Key("subject").eq(subject)}
        if next_token:
            request["ExclusiveStartKey"] = next_token
        response = table.query(**request)
        records.update({str(item.get("purpose", "")): item for item in response.get("Items", [])})
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in consent_preference_center.py", MAX_LOOP_ITERATIONS)
    return records


def _requested_purposes(event: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    """Collect purpose -> desired state from the body, query string and headers."""
    requested: Dict[str, Any] = {str(purpose).strip().lower(): value
                                 for purpose, value in (body.get("purposes") or {}).items()}
    for name, values in (event.get("multiValueQueryStringParameters") or {}).items():
        if name.startswith("purpose."):
            requested[name[len("purpose."):].lower()] = values[-1]
    for name, value in (event.get("queryStringParameters") or {}).items():
        if name.startswith("consent."):
            requested[name[len("consent."):].lower()] = value
    for name, value in (event.get("headers") or {}).items():
        if name.lower().startswith("x-consent-"):
            requested[name.lower()[len("x-consent-"):].replace("-", "_")] = value
    return requested


def _coerce_state(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return "granted" if value else "denied"
    text = str(value).strip().lower()
    if text in GRANT_TOKENS:
        return "granted"
    return "denied" if text in DENY_TOKENS else None


def _reconcile(existing: Optional[Dict[str, Any]], desired: str, basis: str, jurisdiction: str,
               now: int) -> Tuple[str, str, str]:
    """Return (state, effective_basis, reason) after applying precedence rules."""
    if existing is None:
        return desired, basis, "initial_capture"

    current_basis = str(existing.get("legal_basis", "consent"))
    current_rank = LEGAL_BASIS_PRECEDENCE.get(current_basis, 1)
    if current_rank > LEGAL_BASIS_PRECEDENCE.get(basis, 1) and existing.get("state") == "granted":
        return "granted", current_basis, "retained_higher_basis"
    if desired == "granted" and current_basis == "legal_obligation":
        return "granted", current_basis, "already_mandated"
    stale = now - int(existing.get("updated_at", now)) > RECONFIRM_AFTER_SECONDS
    reason = "reconfirmed" if stale else "updated"
    if jurisdiction in OPT_IN_JURISDICTIONS and desired == "granted" and basis == "consent":
        reason = "explicit_opt_in"
    return desired, basis, reason


def _write_record(subject: str, purpose: str, state: str, basis: str, jurisdiction: str,
                  now: int, source_ip: str) -> Dict[str, Any]:
    record = {
        "subject": subject, "purpose": purpose, "state": state, "legal_basis": basis,
        "jurisdiction": jurisdiction, "updated_at": now, "captured_from": source_ip,
        "expires_at": now + CONSENT_TTL_SECONDS}
    dynamodb.Table(CONSENT_TABLE).put_item(Item=record)
    return record


def _audit(subject: str, changes: List[Dict[str, Any]], now: int) -> None:
    if not changes:
        return
    try:
        dynamodb.Table(AUDIT_TABLE).put_item(Item={
            "subject": subject, "recorded_at": now, "changes": changes,
            "expires_at": now + CONSENT_TTL_SECONDS * 7})
    except ClientError as exc:
        logger.warning("consent_audit_write_failed subject=%s error=%s", subject, exc)


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {"statusCode": status, "body": json.dumps(body, default=str),
            "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"}}


def _render_view(stored: Dict[str, Dict[str, Any]], jurisdiction: str, now: int) -> Dict[str, Any]:
    view: Dict[str, Any] = {}
    for purpose in sorted(KNOWN_PURPOSES):
        record = stored.get(purpose)
        updated_at = int(record["updated_at"]) if record else None
        view[purpose] = {
            "state": str(record["state"]) if record else _default_state(jurisdiction, purpose),
            "legal_basis": str(record.get("legal_basis", "consent")) if record else "consent",
            "updated_at": updated_at,
            "requires_reconfirmation": bool(updated_at is not None
                                            and now - updated_at > RECONFIRM_AFTER_SECONDS),
        }
    return view


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
    request_context = event.get("requestContext") or {}
    authorizer = request_context.get("authorizer") or {}
    subject = str(authorizer.get("subject") or query.get("subject")
                  or headers.get("x-subject-id") or "")
    if not subject:
        return _response(401, {"error": "unidentified_subject"})

    residency = str(authorizer.get("residency_country", ""))
    now, jurisdiction = int(time.time()), _resolve_jurisdiction(query, headers, residency)
    try:
        stored = _load_records(subject)
    except ClientError as exc:
        logger.error("consent_load_failed subject=%s error=%s", subject, exc)
        return _response(503, {"error": "consent_store_unavailable"})

    if str(event.get("httpMethod", "GET")).upper() == "GET":
        logger.info("consent_read subject=%s jurisdiction=%s", subject, jurisdiction)
        return _response(200, {"subject": subject, "jurisdiction": jurisdiction,
                               "consents": _render_view(stored, jurisdiction, now)})

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    requested = _requested_purposes(event, body)
    if not requested:
        return _response(400, {"error": "no_purposes_supplied"})
    default_basis = str(body.get("legal_basis") or query.get("legal_basis") or "consent").lower()
    if default_basis not in LEGAL_BASIS_PRECEDENCE:
        return _response(400, {"error": "unknown_legal_basis", "value": default_basis})

    source_ip = str((request_context.get("identity") or {}).get("sourceIp", ""))
    changes: List[Dict[str, Any]] = []
    rejected: List[str] = []
    for purpose, raw_state in requested.items():
        desired = _coerce_state(raw_state) if purpose in KNOWN_PURPOSES else None
        if desired is None:
            rejected.append(purpose)
            continue
        if purpose in ALWAYS_ON_PURPOSES:
            desired = "granted"
        state, basis, reason = _reconcile(stored.get(purpose), desired, default_basis,
                                         jurisdiction, now)
        try:
            record = _write_record(subject, purpose, state, basis, jurisdiction, now, source_ip)
        except ClientError as exc:
            logger.error("consent_write_failed subject=%s purpose=%s error=%s",
                         subject, purpose, exc)
            return _response(503, {"error": "consent_store_unavailable", "purpose": purpose})
        changes.append({"purpose": purpose, "state": record["state"], "basis": basis,
                        "reason": reason})

    _audit(subject, changes, now)
    logger.info("consent_written subject=%s jurisdiction=%s applied=%s rejected=%s",
                subject, jurisdiction, len(changes), len(rejected))
    return _response(200, {"subject": subject, "jurisdiction": jurisdiction,
                           "applied": changes, "rejected_purposes": rejected})
