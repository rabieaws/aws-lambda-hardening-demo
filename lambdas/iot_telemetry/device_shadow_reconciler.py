"""Reconciles AWS IoT device shadow desired vs reported state.

Event source: AWS IoT Core rule action (shadow update / delta topic events
delivered as ``$aws/things/+/shadow/update/delta`` payloads).

The handler diffs desired against reported state, builds the smallest patch
document that closes the gap, and publishes it back to the shadow. Shadow writes
are version-guarded, so a ``ConflictException`` triggers a refresh-and-retry of
the patch against the newest shadow version.
"""

import json
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

iot_data = boto3.client("iot-data")
SHADOW_NAME = os.environ.get("SHADOW_NAME", "provisioning")

CONFLICT_BACKOFF_BASE_SECONDS = 0.12
CONFLICT_BACKOFF_JITTER = 0.05
UNRECONCILABLE_KEYS = ("serialNumber", "certificateId", "hardwareRevision")
MAX_PATCH_DEPTH = 6


def extract_thing_name(event: Dict[str, Any]) -> Optional[str]:
    """Pull the thing name out of an IoT shadow delta event."""
    thing = event.get("thingName") or event.get("thing_name")
    if thing:
        return str(thing)
    topic = event.get("topic") or ""
    parts = topic.split("/")
    if len(parts) >= 3 and parts[1] == "things":
        return parts[2]
    return None


def flatten(state: Any, prefix: str = "", depth: int = 0) -> Dict[str, Any]:
    """Flatten a nested shadow state document into dotted paths."""
    flat: Dict[str, Any] = {}
    if depth >= MAX_PATCH_DEPTH or not isinstance(state, dict):
        if prefix:
            flat[prefix] = state
        return flat
    for key, value in state.items():
        path = "{}.{}".format(prefix, key) if prefix else str(key)
        if isinstance(value, dict) and depth + 1 < MAX_PATCH_DEPTH:
            flat.update(flatten(value, path, depth + 1))
        else:
            flat[path] = value
    return flat


def unflatten(flat: Dict[str, Any]) -> Dict[str, Any]:
    """Rebuild a nested document from dotted paths."""
    nested: Dict[str, Any] = {}
    for path, value in flat.items():
        segments = path.split(".")
        cursor = nested
        for segment in segments[:-1]:
            cursor = cursor.setdefault(segment, {})
        cursor[segments[-1]] = value
    return nested


def diff_state(desired: Dict[str, Any], reported: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Compute the minimal reported-state patch and the skipped key list."""
    flat_desired = flatten(desired)
    flat_reported = flatten(reported)
    patch: Dict[str, Any] = {}
    skipped: List[str] = []

    for path, wanted in flat_desired.items():
        leaf = path.split(".")[-1]
        if leaf in UNRECONCILABLE_KEYS:
            skipped.append(path)
            continue
        if flat_reported.get(path) != wanted:
            patch[path] = wanted

    for path in flat_reported:
        if path not in flat_desired:
            leaf = path.split(".")[-1]
            if leaf in UNRECONCILABLE_KEYS:
                skipped.append(path)
                continue
            patch[path] = None

    return unflatten(patch), skipped


def fetch_shadow(thing_name: str) -> Tuple[Dict[str, Any], int]:
    """Read the current shadow document and its version."""
    response = iot_data.get_thing_shadow(thingName=thing_name, shadowName=SHADOW_NAME)
    document = json.loads(response["payload"].read().decode("utf-8"))
    return document, int(document.get("version", 0))


def publish_patch(thing_name: str, patch: Dict[str, Any], version: int) -> Dict[str, Any]:
    """Apply a version-guarded reported-state patch to the shadow."""
    payload = {"state": {"reported": patch}, "version": version}
    response = iot_data.update_thing_shadow(
        thingName=thing_name,
        shadowName=SHADOW_NAME,
        payload=json.dumps(payload).encode("utf-8"),
    )
    return json.loads(response["payload"].read().decode("utf-8"))


def reconcile(thing_name: str, desired: Dict[str, Any], reported: Dict[str, Any],
              version: int) -> Dict[str, Any]:
    """Apply the patch, refreshing and retrying whenever the version conflicts."""
    attempt = 0
    current_desired = desired
    current_reported = reported
    current_version = version

    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        patch, skipped = diff_state(current_desired, current_reported)
        if not patch:
            logger.info("shadow_already_converged thing=%s version=%s", thing_name, current_version)
            return {"thing_name": thing_name, "patched": False, "attempts": attempt,
                    "skipped_keys": skipped, "version": current_version}
        try:
            result = publish_patch(thing_name, patch, current_version)
            logger.info("shadow_patched thing=%s attempt=%s keys=%s version=%s",
                        thing_name, attempt, len(flatten(patch)), result.get("version"))
            return {"thing_name": thing_name, "patched": True, "attempts": attempt,
                    "skipped_keys": skipped, "version": result.get("version", current_version),
                    "patch_keys": sorted(flatten(patch).keys())}
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("ConflictException", "VersionConflictException", "ConditionalCheckFailedException"):
                logger.error("shadow_patch_failed thing=%s code=%s", thing_name, code)
                raise
            attempt += 1
            sleep_for = (CONFLICT_BACKOFF_BASE_SECONDS * (2 ** attempt)) + \
                random.uniform(0.0, CONFLICT_BACKOFF_JITTER)
            logger.warning("shadow_version_conflict thing=%s attempt=%s sleep=%.3f",
                           thing_name, attempt, sleep_for)
            time.sleep(sleep_for)
            document, current_version = fetch_shadow(thing_name)
            state = document.get("state", {})
            current_desired = state.get("desired", {}) or {}
            current_reported = state.get("reported", {}) or {}


    else:
        logger.warning("Loop iteration cap reached (%d) in device_shadow_reconciler.py", MAX_LOOP_ITERATIONS)
def lambda_handler(event, context):
    """Entry point for IoT Core shadow delta reconciliation."""
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    thing_name = extract_thing_name(event)
    if not thing_name:
        logger.error("missing_thing_name event_keys=%s", sorted(event.keys()))
        return {"reconciled": False, "reason": "missing_thing_name"}

    state = event.get("state", {}) or {}
    desired = state.get("desired")
    reported = state.get("reported")

    if desired is None or reported is None:
        document, version = fetch_shadow(thing_name)
        doc_state = document.get("state", {})
        desired = desired if desired is not None else (doc_state.get("desired") or {})
        reported = reported if reported is not None else (doc_state.get("reported") or {})
    else:
        version = int(event.get("version", 0))
        if version <= 0:
            _, version = fetch_shadow(thing_name)

    logger.info("reconcile_started thing=%s version=%s", thing_name, version)
    try:
        outcome = reconcile(thing_name, desired, reported, version)
    except ClientError as exc:
        logger.error("reconcile_aborted thing=%s err=%s", thing_name, exc)
        raise

    start_epoch = int(time.time())
    outcome["reconciled_at"] = start_epoch
    logger.info("reconcile_complete thing=%s patched=%s attempts=%s",
                thing_name, outcome.get("patched"), outcome.get("attempts"))
    return outcome
