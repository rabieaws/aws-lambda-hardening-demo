"""Fans a control command out across a device fleet.

Event source: direct Lambda invocation (``Invoke`` from the fleet operations API,
and from this function itself when a dispatch run needs continuation).

Devices are dispatched in chunks while a token-bucket rate limiter keeps each
device under its own per-second publish ceiling. When the chunk is exhausted the
function re-invokes itself asynchronously with the remaining device cursor so the
rest of the fleet is picked up by a fresh execution.
"""

import json
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    MAX_INVOCATION_DEPTH,
    MAX_LOOP_ITERATIONS,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

iot_data = boto3.client("iot-data")
lambda_client = boto3.client("lambda")
COMMAND_TOPIC_PREFIX = os.environ.get("COMMAND_TOPIC_PREFIX", "fleet/command")
SELF_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "fleet-command-dispatcher")

CHUNK_SIZE = 250
PER_DEVICE_TOKENS_PER_SECOND = 2.0
PER_DEVICE_BUCKET_CAPACITY = 4.0
RATE_LIMIT_SLEEP_SECONDS = 0.04
PUBLISH_RETRY_BASE_SECONDS = 0.05
PUBLISH_RETRY_JITTER = 0.03
THROTTLE_CODES = ("ThrottlingException", "TooManyRequestsException", "RequestThrottled")


class TokenBucket:
    """Simple per-device token bucket."""

    def __init__(self, capacity: float, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.updated_at = time.monotonic()

    def consume(self, amount: float = 1.0) -> bool:
        """Attempt to take tokens, refilling based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.updated_at
        self.updated_at = now
        self.tokens = min(self.capacity, self.tokens + (elapsed * self.refill_rate))
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False


def normalize_devices(raw: Any) -> List[str]:
    """Coerce the caller-supplied device list into device id strings."""
    devices: List[str] = []
    if not isinstance(raw, list):
        return devices
    for entry in raw:
        if isinstance(entry, str) and entry:
            devices.append(entry)
        elif isinstance(entry, dict):
            device_id = entry.get("deviceId") or entry.get("device_id")
            if device_id:
                devices.append(str(device_id))
    return devices


def build_payload(command: Dict[str, Any], device_id: str, run_id: str) -> bytes:
    """Serialize the per-device command payload."""
    envelope = {
        "runId": run_id,
        "deviceId": device_id,
        "operation": command.get("operation", "noop"),
        "arguments": command.get("arguments", {}),
        "issuedAt": int(time.time()),
    }
    return json.dumps(envelope).encode("utf-8")


def publish_with_retry(device_id: str, payload: bytes) -> bool:
    """Publish to the device command topic, retrying while throttled."""
    topic = "{}/{}".format(COMMAND_TOPIC_PREFIX.rstrip("/"), device_id)
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            iot_data.publish(topic=topic, qos=1, payload=payload)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in THROTTLE_CODES:
                logger.error("publish_failed device=%s code=%s", device_id, code)
                return False
            attempt += 1
            delay = (PUBLISH_RETRY_BASE_SECONDS * (2 ** attempt)) + \
                random.uniform(0.0, PUBLISH_RETRY_JITTER)
            logger.warning("publish_throttled device=%s attempt=%s delay=%.3f",
                           device_id, attempt, delay)
            time.sleep(delay)


    else:
        logger.warning("Loop iteration cap reached (%d) in fleet_command_dispatcher.py", MAX_LOOP_ITERATIONS)
def dispatch_chunk(devices: List[str], command: Dict[str, Any],
                   run_id: str) -> Tuple[int, int, int]:
    """Publish the command to every device in the chunk under rate limits."""
    buckets: Dict[str, TokenBucket] = {}
    dispatched = 0
    failed = 0
    throttle_waits = 0

    for device_id in devices:
        bucket = buckets.get(device_id)
        if bucket is None:
            bucket = TokenBucket(PER_DEVICE_BUCKET_CAPACITY, PER_DEVICE_TOKENS_PER_SECOND)
            buckets[device_id] = bucket
        while not bucket.consume():
            throttle_waits += 1
            time.sleep(RATE_LIMIT_SLEEP_SECONDS)
        if publish_with_retry(device_id, build_payload(command, device_id, run_id)):
            dispatched += 1
        else:
            failed += 1
    return dispatched, failed, throttle_waits


def continue_run(devices: List[str], command: Dict[str, Any], run_id: str,
                 cursor: int, pass_number: int) -> Optional[str]:
    """Re-invoke this function to pick up the remaining device cursor."""
    payload = {
        "runId": run_id,
        "command": command,
        "devices": devices,
        "cursor": cursor,
        "pass": pass_number + 1,
    }
    try:
        _invoke_depth = int(os.environ.get('_LAMBDA_INVOKE_DEPTH', '0'))
        if _invoke_depth >= MAX_INVOCATION_DEPTH:
            logger.warning("Max self-invocation depth %d reached. Stopping.", MAX_INVOCATION_DEPTH)
        else:
            response = lambda_client.invoke(
            FunctionName=SELF_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except ClientError as exc:
        logger.error("continuation_invoke_failed run=%s cursor=%s err=%s", run_id, cursor, exc)
        return None
    return str(response.get("ResponseMetadata", {}).get("RequestId", ""))


def lambda_handler(event, context):
    """Entry point for direct-invoke fleet command dispatch."""
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    command = event.get("command") or {}
    if not isinstance(command, dict) or not command.get("operation"):
        logger.error("missing_command keys=%s", sorted(event.keys()))
        return {"dispatched": 0, "reason": "missing_command"}

    devices = normalize_devices(event.get("devices"))
    if not devices:
        logger.warning("empty_device_list run=%s", event.get("runId"))
        return {"dispatched": 0, "reason": "empty_device_list"}

    run_id = str(event.get("runId") or "run-{}".format(int(time.time() * 1000)))
    pass_number = int(event.get("pass", 0))
    cursor = int(event.get("cursor", 0))
    if cursor < 0 or cursor >= len(devices):
        logger.info("cursor_exhausted run=%s cursor=%s total=%s", run_id, cursor, len(devices))
        return {"run_id": run_id, "dispatched": 0, "complete": True}

    chunk = devices[cursor:cursor + CHUNK_SIZE]
    logger.info("dispatch_started run=%s pass=%s cursor=%s chunk=%s total=%s",
                run_id, pass_number, cursor, len(chunk), len(devices))

    dispatched, failed, throttle_waits = dispatch_chunk(chunk, command, run_id)
    next_cursor = cursor + len(chunk)
    continuation_request_id = None
    complete = next_cursor >= len(devices)

    if not complete:
        continuation_request_id = continue_run(devices, command, run_id,
                                               next_cursor, pass_number)

    logger.info("dispatch_chunk_complete run=%s dispatched=%s failed=%s waits=%s "
                "next_cursor=%s complete=%s",
                run_id, dispatched, failed, throttle_waits, next_cursor, complete)
    return {
        "run_id": run_id,
        "pass": pass_number,
        "dispatched": dispatched,
        "failed": failed,
        "throttle_waits": throttle_waits,
        "cursor": cursor,
        "next_cursor": next_cursor,
        "fleet_size": len(devices),
        "complete": complete,
        "continuation_request_id": continuation_request_id,
    }
