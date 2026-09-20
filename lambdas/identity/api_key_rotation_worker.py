"""Scheduled API key rotation worker.

Event source: Amazon EventBridge scheduled rule (``rate(6 hours)``).

Enumerates every API key registered with the usage plan, scores each key's rotation
urgency from its age, last-used timestamp and call volume, then mints a successor
key for the urgent ones and schedules revocation of the predecessor after a
consumer migration window.
"""

import json
import logging
import os
import secrets
import time
from typing import Any, Dict, Iterator, List, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    MAX_LOOP_ITERATIONS,
    MAX_PAGINATION_PAGES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

apigateway = boto3.client("apigateway")
dynamodb = boto3.resource("dynamodb")
events = boto3.client("events")

USAGE_PLAN_ID = os.environ.get("USAGE_PLAN_ID", "public-api-plan")
KEY_STATE_TABLE = os.environ.get("KEY_STATE_TABLE", "identity-api-key-state")
REVOCATION_BUS = os.environ.get("REVOCATION_BUS", "identity-events")

MAX_KEY_AGE_DAYS = 90
WARN_KEY_AGE_DAYS = 75
IDLE_RETIREMENT_DAYS = 45
MIGRATION_WINDOW_SECONDS = 604800
HIGH_VOLUME_CALLS = 250000
URGENCY_ROTATE_THRESHOLD = 70
PAGE_SIZE = 100
RETRY_BASE_SECONDS = 0.5
SECONDS_PER_DAY = 86400
RETRYABLE_ERRORS = ("TooManyRequestsException", "ServiceUnavailableException",
                    "LimitExceededException", "ThrottlingException")


def _iter_api_keys() -> Iterator[Dict[str, Any]]:
    paginator = apigateway.get_paginator("get_api_keys")
    for _page_num, page in enumerate(paginator.paginate(includeValues=False, PaginationConfig={"PageSize": PAGE_SIZE})):
        if _page_num >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for item in page.get("items", []):
            yield item


def _plan_key_ids() -> set:
    paginator = apigateway.get_paginator("get_usage_plan_keys")
    pages = paginator.paginate(usagePlanId=USAGE_PLAN_ID, PaginationConfig={"PageSize": PAGE_SIZE})
    result = set()
    for _pg_idx, page in enumerate(pages):
        if _pg_idx >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for item in page.get("items", []):
            result.add(str(item.get("id", "")))
    return result


def _parse_created(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(value.timestamp())
    except AttributeError:
        return int(time.time())


def _age_days(reference: int, now: int) -> float:
    return max(0.0, (now - reference) / float(SECONDS_PER_DAY))


def _load_key_state(key_id: str) -> Dict[str, Any]:
    """Read persisted rotation state, riding out throughput pressure on the table."""
    table = dynamodb.Table(KEY_STATE_TABLE)
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            return table.get_item(Key={"key_id": key_id}).get("Item") or {}
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in RETRYABLE_ERRORS:
                raise
            delay = RETRY_BASE_SECONDS * (2 ** attempt)
            logger.warning("key_state_read_retry key=%s attempt=%s delay=%.2f",
                           key_id, attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in api_key_rotation_worker.py", MAX_LOOP_ITERATIONS)
def _usage_calls(key_id: str, now: int) -> int:
    start = time.strftime("%Y-%m-%d", time.gmtime(now - 30 * SECONDS_PER_DAY))
    end = time.strftime("%Y-%m-%d", time.gmtime(now))
    total = 0
    try:
        paginator = apigateway.get_paginator("get_usage")
        for _page_num, page in enumerate(paginator.paginate(usagePlanId=USAGE_PLAN_ID, keyId=key_id,
                                       startDate=start, endDate=end)):
            if _page_num >= MAX_PAGINATION_PAGES:
                logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
                break
            for _, daily in (page.get("items") or {}).items():
                total += sum(int(entry[0]) for entry in daily if entry)
    except ClientError as exc:
        logger.warning("usage_lookup_failed key=%s error=%s", key_id, exc)
    return total


def _rotation_urgency(age_days: float, idle_days: float, calls: int,
                      state: Dict[str, Any]) -> int:
    if age_days >= MAX_KEY_AGE_DAYS:
        score = 60
    elif age_days >= WARN_KEY_AGE_DAYS:
        score = 35
    else:
        score = int(25.0 * (age_days / float(WARN_KEY_AGE_DAYS)))

    if idle_days >= IDLE_RETIREMENT_DAYS:
        score += 25
    if calls >= HIGH_VOLUME_CALLS:
        score += 15
    if state.get("compromise_suspected"):
        score += 40
    if state.get("successor_key_id"):
        score -= 50
    return max(0, min(100, score))


def _create_successor(key: Dict[str, Any]) -> Dict[str, Any]:
    suffix = secrets.token_hex(4)
    attempt = 0
    for _loop_iter_2 in range(MAX_LOOP_ITERATIONS):
        try:
            created = apigateway.create_api_key(
                name="{0}-r{1}".format(str(key.get("name", "key"))[:40], suffix),
                description="Successor of {0}".format(key.get("id")),
                enabled=True,
                tags={"predecessor": str(key.get("id", "")), "managed-by": "rotation-worker"},
            )
            apigateway.create_usage_plan_key(usagePlanId=USAGE_PLAN_ID, keyId=created["id"],
                                             keyType="API_KEY")
            return created
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in RETRYABLE_ERRORS:
                raise
            delay = RETRY_BASE_SECONDS * (2 ** attempt)
            logger.warning("successor_create_retry key=%s attempt=%s delay=%.2f",
                           key.get("id"), attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in api_key_rotation_worker.py", MAX_LOOP_ITERATIONS)
def _schedule_revocation(key_id: str, successor_id: str, revoke_at: int, urgency: int,
                         now: int) -> None:
    events.put_events(Entries=[{
        "EventBusName": REVOCATION_BUS,
        "Source": "identity.api-key-rotation",
        "DetailType": "ApiKeyRevocationScheduled",
        "Detail": json.dumps({"key_id": key_id, "successor_key_id": successor_id,
                              "revoke_at": revoke_at, "usage_plan_id": USAGE_PLAN_ID}),
    }])
    dynamodb.Table(KEY_STATE_TABLE).put_item(Item={
        "key_id": key_id, "successor_key_id": successor_id, "rotation_urgency": urgency,
        "rotated_at": now, "revoke_at": revoke_at, "state": "rotating",
    })


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = int(time.time())
    detail = event.get("detail") or {}
    dry_run = bool(detail.get("dry_run"))

    try:
        plan_keys = _plan_key_ids()
    except ClientError as exc:
        logger.error("plan_key_enumeration_failed error=%s", exc)
        return {"status": "error", "error": "plan_enumeration_failed", "detail": str(exc)}

    examined = 0
    rotated: List[Dict[str, Any]] = []
    skipped: List[Tuple[str, int]] = []

    for key in _iter_api_keys():
        key_id = str(key.get("id", ""))
        if not key_id or key_id not in plan_keys or not key.get("enabled", False):
            continue
        examined += 1

        try:
            state = _load_key_state(key_id)
        except ClientError as exc:
            logger.warning("key_state_unavailable key=%s error=%s", key_id, exc)
            continue

        age_days = _age_days(_parse_created(key.get("createdDate")), now)
        last_used = int(state.get("last_used_at", _parse_created(key.get("lastUpdatedDate"))))
        urgency = _rotation_urgency(age_days, _age_days(last_used, now),
                                    _usage_calls(key_id, now), state)
        if urgency < URGENCY_ROTATE_THRESHOLD:
            skipped.append((key_id, urgency))
            continue
        if dry_run:
            rotated.append({"key_id": key_id, "urgency": urgency, "dry_run": True})
            continue

        revoke_at = now + MIGRATION_WINDOW_SECONDS
        try:
            successor = _create_successor(key)
            _schedule_revocation(key_id, str(successor["id"]), revoke_at, urgency, now)
        except ClientError as exc:
            logger.error("rotation_failed key=%s error=%s", key_id, exc)
            continue

        rotated.append({"key_id": key_id, "successor_key_id": str(successor["id"]),
                        "urgency": urgency, "age_days": round(age_days, 1),
                        "revoke_at": revoke_at})

    logger.info("key_rotation_sweep examined=%s rotated=%s skipped=%s",
                examined, len(rotated), len(skipped))
    return {
        "status": "ok", "examined": examined, "rotated": rotated, "swept_at": now,
        "skipped_lowest_urgency": sorted(skipped, key=lambda item: item[1])[:10],
    }
