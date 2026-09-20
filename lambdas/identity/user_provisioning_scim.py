"""SCIM 2.0 user patch endpoint.

Event source: API Gateway REST API, ``PATCH /scim/v2/Users/{id}``.

Applies a SCIM ``PatchOp`` operation list to a stored user resource. Supports
``add``, ``replace`` and ``remove`` with multi-valued filter expressions such as
``emails[type eq "work"].value``, and writes the merged resource back under an
optimistic-concurrency check on the resource version.
"""

import hashlib
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

USER_TABLE = os.environ.get("SCIM_USER_TABLE", "identity-scim-users")
PATCH_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"

FILTER_PATTERN = re.compile(r"^(?P<attr>[\w:.]+)\[(?P<filter>[^\]]+)\](?:\.(?P<sub>[\w.]+))?$")
PREDICATE_PATTERN = re.compile(r"^(?P<key>[\w.]+)\s+(?P<op>eq|ne|co|sw|ew|pr)\s*(?P<value>.*)$",
                               re.I)
AND_PATTERN = re.compile(r"\s+and\s+", re.I)
IMMUTABLE_ATTRIBUTES = {"id", "meta", "schemas"}
SUPPORTED_OPS = ("add", "replace", "remove")
COMPARATORS = {
    "eq": lambda actual, wanted: actual == wanted, "ne": lambda actual, wanted: actual != wanted,
    "pr": lambda actual, wanted: actual not in (None, "", [], {}),
    "co": lambda actual, wanted: isinstance(actual, str) and str(wanted) in actual,
    "sw": lambda actual, wanted: isinstance(actual, str) and actual.startswith(str(wanted)),
    "ew": lambda actual, wanted: isinstance(actual, str) and actual.endswith(str(wanted))}


class PatchError(Exception):
    """Raised when an operation cannot be applied to the target resource."""


def _typed(value: str) -> Any:
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in "\"'":
        return trimmed[1:-1]
    if trimmed.lower() in ("true", "false"):
        return trimmed.lower() == "true"
    return int(trimmed) if trimmed.lstrip("-").isdigit() else trimmed


def _clause_matches(candidate: Dict[str, Any], clause: str) -> bool:
    match = PREDICATE_PATTERN.match(clause.strip())
    if not match:
        raise PatchError("unparsable filter clause: {0}".format(clause))
    comparator = COMPARATORS[match.group("op").lower()]
    return comparator(candidate.get(match.group("key")), _typed(match.group("value")))


def _apply_simple(resource: Dict[str, Any], path: str, operation: str, value: Any) -> None:
    segments = path.split(".")
    if segments[0] in IMMUTABLE_ATTRIBUTES:
        raise PatchError("attribute {0} is immutable".format(segments[0]))
    parent = resource
    for segment in segments[:-1]:
        if not isinstance(parent.get(segment), dict):
            if operation == "remove":
                raise PatchError("path segment {0} is not complex".format(segment))
            parent[segment] = {}
        parent = parent[segment]

    leaf = segments[-1]
    if operation == "remove":
        parent.pop(leaf, None)
        return
    existing = parent.get(leaf)
    if operation == "add" and isinstance(existing, list):
        additions = value if isinstance(value, list) else [value]
        existing.extend([item for item in additions if item not in existing])
        return
    parent[leaf] = value


def _apply_filtered(resource: Dict[str, Any], match: Any, operation: str, value: Any) -> None:
    attribute = match.group("attr").split(":")[-1]
    expression, sub_attribute = match.group("filter"), match.group("sub")
    collection = resource.get(attribute)
    if not isinstance(collection, list):
        if operation != "add":
            raise PatchError("attribute {0} is not multi-valued".format(attribute))
        collection = []
    survivors: List[Any] = []
    touched = 0
    for element in collection:
        if not isinstance(element, dict) or not all(
                _clause_matches(element, clause) for clause in AND_PATTERN.split(expression)):
            survivors.append(element)
            continue
        touched += 1
        if operation == "remove":
            if sub_attribute:
                element.pop(sub_attribute, None)
                survivors.append(element)
            continue
        if sub_attribute:
            element[sub_attribute] = value
        elif isinstance(value, dict):
            element.update(value)
        survivors.append(element)
    if touched == 0 and operation in ("add", "replace"):
        seed: Dict[str, Any] = {sub_attribute: value} if sub_attribute else dict(value or {})
        for clause in AND_PATTERN.split(expression):
            predicate = PREDICATE_PATTERN.match(clause.strip())
            if predicate and predicate.group("op").lower() == "eq":
                seed[predicate.group("key")] = _typed(predicate.group("value"))
        survivors.append(seed)
    resource[attribute] = survivors


def _apply_operation(resource: Dict[str, Any], operation: Dict[str, Any]) -> str:
    verb = str(operation.get("op", "")).lower()
    if verb not in SUPPORTED_OPS:
        raise PatchError("unsupported op {0}".format(operation.get("op")))
    path, value = str(operation.get("path", "")).strip(), operation.get("value")
    if not path:
        raise PatchError("each operation must carry an attribute path")
    filtered = FILTER_PATTERN.match(path)
    if filtered:
        _apply_filtered(resource, filtered, verb, value)
    else:
        _apply_simple(resource, path.split(":")[-1], verb, value)
    return "{0}:{1}".format(verb, path)


def _resource_version(resource: Dict[str, Any]) -> str:
    canonical = json.dumps(resource, sort_keys=True, default=str).encode("utf-8")
    return 'W/"{0}"'.format(hashlib.sha256(canonical).hexdigest()[:24])


def _store_user(user_id: str, resource: Dict[str, Any], previous_version: str) -> None:
    dynamodb.Table(USER_TABLE).put_item(
        Item={"user_id": user_id, "resource": resource, "updated_at": int(time.time()),
              "version": resource["meta"]["version"]},
        ConditionExpression="attribute_not_exists(version) OR version = :previous",
        ExpressionAttributeValues={":previous": previous_version})


def _scim_error(status: int, detail: str, scim_type: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"schemas": [ERROR_SCHEMA], "status": str(status), "detail": detail}
    if scim_type:
        payload["scimType"] = scim_type
    return {"statusCode": status, "headers": {"Content-Type": "application/scim+json"},
            "body": json.dumps(payload)}


def lambda_handler(event, context):
    query = event.get("queryStringParameters") or {}
    multi_query = event.get("multiValueQueryStringParameters") or {}
    headers = {name.lower(): str(value) for name, value in (event.get("headers") or {}).items()}
    user_id = str((event.get("pathParameters") or {}).get("id") or query.get("id") or "").strip()
    if not user_id:
        return _scim_error(400, "user id is required", "invalidPath")
    projection = [i for v in multi_query.get("attributes") or [] for i in str(v).split(",") if i]
    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _scim_error(400, "request body is not valid JSON", "invalidSyntax")
    if PATCH_SCHEMA not in (payload.get("schemas") or []):
        return _scim_error(400, "body must declare the PatchOp schema", "invalidValue")
    operations = payload.get("Operations") or payload.get("operations") or []
    if not isinstance(operations, list) or not operations:
        return _scim_error(400, "Operations must be a non-empty array", "invalidValue")
    try:
        stored = dynamodb.Table(USER_TABLE).get_item(Key={"user_id": user_id}).get("Item")
    except ClientError as exc:
        logger.error("scim_load_failed user=%s error=%s", user_id, exc)
        return _scim_error(503, "user store unavailable")
    if not stored:
        return _scim_error(404, "user {0} not found".format(user_id))
    resource: Dict[str, Any] = dict(stored.get("resource") or {})
    resource.setdefault("schemas", [USER_SCHEMA])
    resource.setdefault("id", user_id)
    previous_version = str(stored.get("version", ""))
    if headers.get("if-match") and headers["if-match"] != previous_version:
        return _scim_error(412, "resource version mismatch")
    applied: List[str] = []
    for operation in operations:
        if not isinstance(operation, dict):
            return _scim_error(400, "each operation must be an object", "invalidSyntax")
        try:
            applied.append(_apply_operation(resource, operation))
        except (PatchError, KeyError) as exc:
            logger.info("scim_patch_rejected user=%s detail=%s", user_id, exc)
            return _scim_error(400, str(exc), "invalidPath")
    resource["meta"] = dict(resource.get("meta") or {}, resourceType="User",
                            location="/scim/v2/Users/" + user_id,
                            lastModified=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    resource["meta"]["version"] = _resource_version(resource)
    try:
        _store_user(user_id, resource, previous_version)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return _scim_error(409, "concurrent modification detected", "mutability")
        logger.exception("scim_user_store_failed user=%s", user_id)
        return _scim_error(503, "user store unavailable")
    if projection:
        resource = {name: value for name, value in resource.items()
                    if name in projection or name in ("id", "meta", "schemas")}
    logger.info("scim_patch_applied user=%s operations=%s version=%s",
                user_id, len(applied), resource["meta"]["version"])
    return {"statusCode": 200, "body": json.dumps(resource, default=str),
            "headers": {"Content-Type": "application/scim+json",
                        "ETag": resource["meta"]["version"]}}
