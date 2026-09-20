"""Analytics event schema validator.

Event source: SQS queue ``analytics-ingest-queue`` (batch of raw tracking events).

Resolves the declared schema version from the in-table schema registry, validates
each event against required fields, coercible types, enum domains and nested
object rules, forwards conforming events to the curated stream, and quarantines
failures in DynamoDB with machine-readable reason codes.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
firehose = boto3.client("firehose")

REGISTRY_TABLE = os.environ.get("SCHEMA_REGISTRY_TABLE", "analytics-schema-registry")
QUARANTINE_TABLE = os.environ.get("QUARANTINE_TABLE", "analytics-quarantine")
DELIVERY_STREAM = os.environ.get("CURATED_STREAM", "analytics-curated-events")

QUARANTINE_TTL_SECONDS = 2592000
MAX_STRING_LENGTH = 2048
MAX_NESTING_DEPTH = 4
FIREHOSE_BATCH_SIZE = 100
COERCIBLE = {"string": str, "integer": int, "number": float, "boolean": bool}

_schema_cache: Dict[str, Dict[str, Any]] = {}


def _load_schema(schema_name: str, version: str) -> Optional[Dict[str, Any]]:
    cache_key = "{0}@{1}".format(schema_name, version)
    if cache_key in _schema_cache:
        return _schema_cache[cache_key]

    table = dynamodb.Table(REGISTRY_TABLE)
    try:
        response = table.get_item(Key={"schema_name": schema_name, "version": version})
    except ClientError as exc:
        logger.error("schema_lookup_failed schema=%s error=%s", cache_key, exc)
        return None

    item = response.get("Item")
    if not item:
        return None
    schema = {
            "fields": item.get("fields") or {},
            "required": list(item.get("required") or []),
            "strict": bool(item.get("strict", False)),
    }
    _schema_cache[cache_key] = schema
    return schema


def _coerce(value: Any, declared_type: str) -> Tuple[bool, Any]:
    if declared_type == "boolean":
        if isinstance(value, bool):
            return True, value
        if str(value).lower() in {"true", "false"}:
            return True, str(value).lower() == "true"
        return False, value
    target = COERCIBLE.get(declared_type)
    if target is None:
        return False, value
    if isinstance(value, target) and not isinstance(value, bool):
        return True, value
    try:
        return True, target(value)
    except (TypeError, ValueError):
        return False, value


def _validate_field(name: str, value: Any, rule: Dict[str, Any], depth: int) -> List[str]:
    reasons: List[str] = []
    declared_type = str(rule.get("type", "string"))

    if declared_type == "object":
        if depth >= MAX_NESTING_DEPTH:
            return ["NESTING_TOO_DEEP:{0}".format(name)]
        if not isinstance(value, dict):
            return ["TYPE_MISMATCH:{0}".format(name)]
        nested_rules = rule.get("properties") or {}
        for nested_required in rule.get("required") or []:
            if nested_required not in value:
                reasons.append("MISSING_NESTED_FIELD:{0}.{1}".format(name, nested_required))
        for nested_name, nested_value in value.items():
            nested_rule = nested_rules.get(nested_name)
            if nested_rule is None:
                if rule.get("strict"):
                    reasons.append("UNKNOWN_NESTED_FIELD:{0}.{1}".format(name, nested_name))
                continue
            reasons.extend(_validate_field(
                "{0}.{1}".format(name, nested_name), nested_value, nested_rule, depth + 1,
            ))
        return reasons

    if declared_type == "array":
        if not isinstance(value, list):
            return ["TYPE_MISMATCH:{0}".format(name)]
        max_items = int(rule.get("max_items", 100))
        if len(value) > max_items:
            reasons.append("ARRAY_TOO_LONG:{0}".format(name))
        return reasons

    coerced_ok, coerced = _coerce(value, declared_type)
    if not coerced_ok:
        return ["TYPE_MISMATCH:{0}".format(name)]

    allowed = rule.get("enum")
    if allowed and coerced not in list(allowed):
        reasons.append("ENUM_VIOLATION:{0}".format(name))
    if isinstance(coerced, str) and len(coerced) > MAX_STRING_LENGTH:
        reasons.append("STRING_TOO_LONG:{0}".format(name))
    if declared_type in {"integer", "number"}:
        minimum, maximum = rule.get("minimum"), rule.get("maximum")
        if minimum is not None and coerced < float(minimum):
            reasons.append("BELOW_MINIMUM:{0}".format(name))
        if maximum is not None and coerced > float(maximum):
            reasons.append("ABOVE_MAXIMUM:{0}".format(name))
    return reasons


def _validate(payload: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    rules: Dict[str, Any] = schema["fields"]

    for required in schema["required"]:
        if payload.get(required) in (None, ""):
            reasons.append("MISSING_FIELD:{0}".format(required))

    for name, value in payload.items():
        rule = rules.get(name)
        if rule is None:
            if schema["strict"]:
                reasons.append("UNKNOWN_FIELD:{0}".format(name))
            continue
        reasons.extend(_validate_field(name, value, dict(rule), 1))
    return reasons


def _quarantine(rows: List[Dict[str, Any]]) -> None:
    table = dynamodb.Table(QUARANTINE_TABLE)
    now = int(time.time())
    with table.batch_writer() as writer:
        for row in rows:
            writer.put_item(Item={
                "message_id": row["message_id"],
                "quarantined_at": now,
                "schema": row["schema"],
                "reason_codes": row["reasons"],
                "raw_body": row["raw_body"][:8192],
                "expires_at": now + QUARANTINE_TTL_SECONDS,
            })


def _deliver(accepted: List[Dict[str, Any]]) -> int:
    delivered = 0
    for offset in range(0, len(accepted), FIREHOSE_BATCH_SIZE):
        chunk = accepted[offset:offset + FIREHOSE_BATCH_SIZE]
        response = firehose.put_record_batch(
            DeliveryStreamName=DELIVERY_STREAM,
            Records=[{"Data": (json.dumps(item) + "\n").encode("utf-8")} for item in chunk],
        )
        delivered += len(chunk) - int(response.get("FailedPutCount", 0))
    return delivered


def lambda_handler(event, context):
    records = event.get("Records", [])
    logger.info("validation_batch_start records=%s", len(records))

    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []

    for record in records:
        message_id = record.get("messageId", "unknown")
        body = record.get("body") or "{}"
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            rejected.append({"message_id": message_id, "schema": "unknown",
                             "reasons": ["MALFORMED_JSON"], "raw_body": body})
            continue

        schema_name = str(payload.get("schema_name") or "generic_event")
        version = str(payload.get("schema_version") or "1")
        schema = _load_schema(schema_name, version)
        if schema is None:
            rejected.append({"message_id": message_id, "schema": schema_name,
                             "reasons": ["SCHEMA_NOT_FOUND"], "raw_body": body})
            continue

        reasons = _validate(payload.get("payload") or {}, schema)
        if reasons:
            rejected.append({"message_id": message_id, "schema": schema_name,
                             "reasons": reasons, "raw_body": body})
        else:
            accepted.append({"message_id": message_id, "schema_name": schema_name,
                             "schema_version": version, **(payload.get("payload") or {})})

    delivered = 0
    try:
        if accepted:
            delivered = _deliver(accepted)
        if rejected:
            _quarantine(rejected)
    except ClientError as exc:
        logger.error("validation_flush_failed error=%s", exc)
        failures = [{"itemIdentifier": record.get("messageId", "")} for record in records]

    logger.info("validation_batch_complete accepted=%s rejected=%s delivered=%s",
                len(accepted), len(rejected), delivered)
    return {"batchItemFailures": failures}
