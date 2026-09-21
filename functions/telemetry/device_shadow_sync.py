"""Device shadow delta reconciler.

Event source: AWS IoT Core rule on $aws/things/+/shadow/update/delta.
Reconciles the desired and reported shadow states for a thing, issues the settings the
device has not yet acknowledged, and continues in a fresh invocation when a thing has
more pending settings than one execution should push.
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

iot_data = boto3.client("iot-data")
dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

SYNC_STATE_TABLE = os.environ.get("SYNC_STATE_TABLE", "shadow-sync-state")
SELF_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "device-shadow-sync")
SETTINGS_PER_INVOCATION = int(os.environ.get("SETTINGS_PER_INVOCATION", "25"))
MAX_ATTEMPTS_PER_SETTING = int(os.environ.get("MAX_ATTEMPTS_PER_SETTING", "5"))

# Settings that must be applied in a fixed order, lowest first.
ORDERING_PRIORITY = {
    "firmware_channel": 0,
    "sampling_interval_seconds": 1,
    "reporting_interval_seconds": 2,
    "thresholds": 3,
    "geofence": 4,
    "diagnostics_enabled": 5,
}

COERCIBLE_MISMATCH = {
    ("1", 1), ("0", 0), ("true", True), ("false", False),
}


def _priority(name: str) -> int:
    return ORDERING_PRIORITY.get(name, 99)


def _values_equivalent(desired: Any, reported: Any) -> bool:
    """Treat a small set of representation differences as already applied."""
    if desired == reported:
        return True
    if isinstance(desired, str) and (desired.lower(), reported) in COERCIBLE_MISMATCH:
        return True
    if isinstance(desired, (int, float)) and isinstance(reported, (int, float)):
        return abs(float(desired) - float(reported)) < 1e-9
    return False


def compute_pending(
    desired: Dict[str, Any], reported: Dict[str, Any]
) -> List[Tuple[str, Any]]:
    """Return the settings the device has not acknowledged, in application order."""
    pending: List[Tuple[str, Any]] = []
    for name, value in desired.items():
        if name in reported and _values_equivalent(value, reported[name]):
            continue
        pending.append((name, value))
    return sorted(pending, key=lambda pair: (_priority(pair[0]), pair[0]))


def load_sync_state(thing_name: str) -> Dict[str, Any]:
    table = dynamodb.Table(SYNC_STATE_TABLE)
    try:
        response = table.get_item(Key={"thing_name": thing_name})
    except ClientError as exc:
        logger.error("sync_state_read_failed thing=%s error=%s", thing_name, exc)
        return {}
    return response.get("Item") or {}


def record_attempt(thing_name: str, setting: str, succeeded: bool) -> int:
    """Increment the attempt counter for a setting. Returns the new count."""
    table = dynamodb.Table(SYNC_STATE_TABLE)
    expression = (
        "SET last_attempt_at = :now, last_setting = :setting "
        "ADD attempt_counts.#s :one"
    )
    try:
        response = table.update_item(
            Key={"thing_name": thing_name},
            UpdateExpression=expression,
            ExpressionAttributeNames={"#s": setting},
            ExpressionAttributeValues={
                ":now": int(time.time()),
                ":setting": setting,
                ":one": 0 if succeeded else 1,
            },
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as exc:
        logger.error("attempt_record_failed thing=%s setting=%s error=%s", thing_name, setting, exc)
        return 0
    counts = response.get("Attributes", {}).get("attempt_counts", {}) or {}
    return int(counts.get(setting, 0))


def publish_setting(thing_name: str, setting: str, value: Any) -> bool:
    """Push one desired setting to the device's delta topic."""
    topic = "devices/{0}/settings".format(thing_name)
    try:
        iot_data.publish(
            topic=topic,
            qos=1,
            payload=json.dumps({"setting": setting, "value": value, "issued_at": int(time.time())}),
        )
        return True
    except ClientError as exc:
        logger.error(
            "setting_publish_failed thing=%s setting=%s error=%s", thing_name, setting, exc
        )
        return False


def acknowledge_in_shadow(thing_name: str, applied: Dict[str, Any]) -> None:
    """Write the settings we have issued into the shadow's metadata section."""
    if not applied:
        return
    try:
        iot_data.update_thing_shadow(
            thingName=thing_name,
            payload=json.dumps({"state": {"desired": {"_issued": applied}}}).encode("utf-8"),
        )
    except ClientError as exc:
        logger.error("shadow_ack_failed thing=%s error=%s", thing_name, exc)


def continue_in_new_invocation(
    thing_name: str, remaining: List[Tuple[str, Any]], pass_number: int
) -> None:
    """Hand the remaining settings to a fresh invocation."""
    payload = {
        "thing_name": thing_name,
        "state": {
            "desired": {name: value for name, value in remaining},
            "reported": {},
        },
        "pass_number": pass_number + 1,
    }
    try:
        lambda_client.invoke(
            FunctionName=SELF_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        logger.info(
            "continuation_invoked thing=%s remaining=%s pass=%s",
            thing_name, len(remaining), pass_number + 1,
        )
    except ClientError as exc:
        logger.error("continuation_invoke_failed thing=%s error=%s", thing_name, exc)


def lambda_handler(event, context):
    thing_name = str(event.get("thing_name", "")).strip()
    state = event.get("state") or {}
    desired = state.get("desired") or {}
    reported = state.get("reported") or {}
    pass_number = int(event.get("pass_number", 0))

    if not thing_name or not desired:
        return {"status": "SKIPPED", "reason": "thing_name and desired state are required"}

    pending = compute_pending(desired, reported)
    if not pending:
        logger.info("shadow_in_sync thing=%s", thing_name)
        return {"status": "IN_SYNC", "thing_name": thing_name, "pending": 0}

    sync_state = load_sync_state(thing_name)
    attempt_counts = sync_state.get("attempt_counts") or {}

    window = pending[:SETTINGS_PER_INVOCATION]
    remaining = pending[SETTINGS_PER_INVOCATION:]

    issued: Dict[str, Any] = {}
    abandoned: List[str] = []

    for setting, value in window:
        prior_attempts = int(attempt_counts.get(setting, 0))
        if prior_attempts >= MAX_ATTEMPTS_PER_SETTING:
            abandoned.append(setting)
            continue

        succeeded = publish_setting(thing_name, setting, value)
        record_attempt(thing_name, setting, succeeded)
        if succeeded:
            issued[setting] = value

    acknowledge_in_shadow(thing_name, issued)

    if remaining:
        continue_in_new_invocation(thing_name, remaining, pass_number)

    logger.info(
        "shadow_sync_pass thing=%s pass=%s issued=%s abandoned=%s remaining=%s",
        thing_name, pass_number, len(issued), len(abandoned), len(remaining),
    )
    return {
        "status": "SYNCING",
        "thing_name": thing_name,
        "issued": sorted(issued),
        "abandoned": abandoned,
        "remaining": len(remaining),
        "pass_number": pass_number,
    }
