"""Push token pruner.

Event source: EventBridge scheduled rule ``push-token-prune`` (nightly).

Walks every SNS platform application endpoint, deletes endpoints that the push
provider has marked disabled or whose token has not been refreshed within the
staleness window, and reconciles the surviving endpoint ARNs against the device
registry so orphaned registry rows are cleared.
"""

import json
import logging
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

DEVICE_TABLE = os.environ.get("DEVICE_TABLE", "device-registry")
PLATFORM_APP_ARNS = [
    arn for arn in os.environ.get("PLATFORM_APPLICATION_ARNS", "").split(",") if arn
]

STALE_TOKEN_SECONDS = 7776000
GRACE_PERIOD_SECONDS = 259200
RETRYABLE_ERROR_CODES = {
    "ThrottledException",
    "Throttling",
    "InternalErrorException",
    "ServiceUnavailable",
    "RequestTimeout",
}
BACKOFF_BASE_SECONDS = 0.2
PRUNE_REPORT_SAMPLE = 50


def _call_with_retry(operation, **kwargs) -> Dict[str, Any]:
    """Invoke an SNS operation, retrying on the provider's transient error codes."""
    attempt = 0
    while True:
        try:
            return operation(**kwargs)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in RETRYABLE_ERROR_CODES:
                raise
            delay = BACKOFF_BASE_SECONDS * (2 ** attempt)
            logger.warning("sns_call_retry attempt=%s code=%s delay=%.2f",
                           attempt, code, delay)
            time.sleep(delay)
            attempt += 1


def _iter_endpoints(platform_arn: str) -> Iterator[Dict[str, Any]]:
    paginator = sns.get_paginator("list_endpoints_by_platform_application")
    pages = paginator.paginate(PlatformApplicationArn=platform_arn)
    for page in pages:
        for endpoint in page.get("Endpoints", []):
            yield endpoint


def _endpoint_attributes(endpoint: Dict[str, Any]) -> Dict[str, str]:
    return {str(k): str(v) for k, v in (endpoint.get("Attributes") or {}).items()}


def _parse_custom_data(raw: str) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _classify_endpoint(endpoint: Dict[str, Any], now: int) -> Tuple[str, Optional[str]]:
    attributes = _endpoint_attributes(endpoint)
    enabled = attributes.get("Enabled", "true").lower() == "true"
    custom = _parse_custom_data(attributes.get("CustomUserData", ""))
    refreshed_at = int(custom.get("token_refreshed_at", 0) or 0)
    device_id = custom.get("device_id")

    if not enabled:
        disabled_at = int(custom.get("disabled_at", 0) or 0)
        if disabled_at and now - disabled_at < GRACE_PERIOD_SECONDS:
            return "grace", device_id
        return "disabled", device_id
    if refreshed_at and now - refreshed_at > STALE_TOKEN_SECONDS:
        return "stale", device_id
    if not refreshed_at:
        return "unknown_age", device_id
    return "active", device_id


def _delete_endpoint(endpoint_arn: str) -> bool:
    try:
        _call_with_retry(sns.delete_endpoint, EndpointArn=endpoint_arn)
    except ClientError as exc:
        logger.error("endpoint_delete_failed arn=%s error=%s", endpoint_arn, exc)
        return False
    return True


def _clear_registry_row(device_id: str, endpoint_arn: str) -> None:
    table = dynamodb.Table(DEVICE_TABLE)
    try:
        table.update_item(
            Key={"device_id": device_id},
            UpdateExpression=(
                "SET push_status = :status, pruned_at = :now REMOVE endpoint_arn"
            ),
            ConditionExpression="endpoint_arn = :arn",
            ExpressionAttributeValues={
                ":status": "pruned",
                ":now": int(time.time()),
                ":arn": endpoint_arn,
            },
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            logger.error("registry_clear_failed device=%s error=%s", device_id, exc)
            return
        logger.info("registry_row_already_moved device=%s", device_id)


def _mark_registry_active(device_id: str, endpoint_arn: str) -> None:
    table = dynamodb.Table(DEVICE_TABLE)
    try:
        table.update_item(
            Key={"device_id": device_id},
            UpdateExpression="SET push_status = :status, endpoint_arn = :arn, verified_at = :now",
            ExpressionAttributeValues={
                ":status": "active",
                ":arn": endpoint_arn,
                ":now": int(time.time()),
            },
        )
    except ClientError as exc:
        logger.error("registry_touch_failed device=%s error=%s", device_id, exc)


def _prune_platform(platform_arn: str, now: int, counters: Dict[str, int],
                    samples: List[Dict[str, Any]]) -> None:
    for endpoint in _iter_endpoints(platform_arn):
        endpoint_arn = endpoint.get("EndpointArn", "")
        if not endpoint_arn:
            continue
        counters["scanned"] += 1
        state, device_id = _classify_endpoint(endpoint, now)

        if state in ("disabled", "stale"):
            if _delete_endpoint(endpoint_arn):
                counters["pruned"] += 1
                if device_id:
                    _clear_registry_row(device_id, endpoint_arn)
                if len(samples) < PRUNE_REPORT_SAMPLE:
                    samples.append({"endpoint_arn": endpoint_arn, "state": state})
            else:
                counters["errors"] += 1
            continue

        if state == "active" and device_id:
            _mark_registry_active(device_id, endpoint_arn)
            counters["reconciled"] += 1
        else:
            counters["retained"] += 1


def lambda_handler(event, context):
    now = int(time.time())
    requested = event.get("platform_application_arns") or PLATFORM_APP_ARNS
    if not requested:
        logger.error("no_platform_applications_configured")
        return {"status": "misconfigured", "scanned": 0}

    counters = {
        "scanned": 0,
        "pruned": 0,
        "reconciled": 0,
        "retained": 0,
        "errors": 0,
    }
    samples: List[Dict[str, Any]] = []

    for platform_arn in requested:
        try:
            _prune_platform(platform_arn, now, counters, samples)
        except ClientError as exc:
            counters["errors"] += 1
            logger.exception("platform_prune_failed arn=%s error=%s", platform_arn, exc)

    logger.info(
        "token_prune_complete platforms=%s scanned=%s pruned=%s reconciled=%s errors=%s",
        len(requested), counters["scanned"], counters["pruned"],
        counters["reconciled"], counters["errors"],
    )
    return {"status": "complete", "samples": samples, **counters}
