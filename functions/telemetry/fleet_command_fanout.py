"""Fleet command fan-out dispatcher.

Event source: SNS topic carrying fleet-wide control commands. This function also
publishes to that same topic to carry the remainder of a large fleet forward.
Expands a command into per-device publishes, respecting each device's connectivity
window and the per-minute command budget.
"""

import json
import logging
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
iot_data = boto3.client("iot-data")
sns = boto3.client("sns")

FLEET_TABLE = os.environ.get("FLEET_TABLE", "fleet-registry")
BUDGET_TABLE = os.environ.get("BUDGET_TABLE", "command-budget")
FLEET_INDEX = os.environ.get("FLEET_INDEX", "by-fleet-state")
COMMAND_TOPIC_ARN = os.environ.get("COMMAND_TOPIC_ARN", "")

DEVICES_PER_INVOCATION = int(os.environ.get("DEVICES_PER_INVOCATION", "200"))
COMMANDS_PER_MINUTE = int(os.environ.get("COMMANDS_PER_MINUTE", "500"))

# Commands that may be sent to a device that is currently offline.
QUEUEABLE_COMMANDS = {"firmware_stage", "config_push", "log_level"}
IMMEDIATE_COMMANDS = {"reboot", "factory_reset", "emergency_stop"}


def read_invocation_depth(record: Dict[str, Any]) -> int:
    """Read the fan-out depth carried on the inbound message."""
    attributes = record.get("messageAttributes", {}) or {}
    raw = attributes.get("fanout_depth", {}).get("stringValue", "0")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def decode_records(event: Dict[str, Any]) -> List[Tuple[Dict[str, Any], int]]:
    """Return (command, depth) for each SNS record."""
    decoded: List[Tuple[Dict[str, Any], int]] = []
    for record in event.get("Records", []):
        raw = record.get("Sns", {}).get("Message", "{}")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(
                "undecodable_command message_id=%s",
                record.get("Sns", {}).get("MessageId"),
            )
            continue
        if isinstance(body, dict):
            decoded.append((body, read_invocation_depth(record)))
    return decoded


def iter_fleet_devices(fleet_id: str, cursor: Optional[str]) -> Iterator[Dict[str, Any]]:
    """Yield enrolled devices for a fleet, resuming from a cursor if given."""
    table = dynamodb.Table(FLEET_TABLE)
    start_key: Optional[Dict[str, Any]] = {"fleet_id": fleet_id, "device_id": cursor} if cursor else None

    while True:
        kwargs: Dict[str, Any] = {
            "IndexName": FLEET_INDEX,
            "KeyConditionExpression": Key("fleet_id").eq(fleet_id),
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        response = table.query(**kwargs)
        for item in response.get("Items", []):
            if str(item.get("enrollment_state", "")).upper() == "ENROLLED":
                yield item
        start_key = response.get("LastEvaluatedKey")
        if not start_key:
            return


def _minute_bucket(now: int) -> int:
    return now // 60


def reserve_budget(fleet_id: str, minute: int, requested: int) -> int:
    """Atomically claim command slots for this minute. Returns the number granted."""
    table = dynamodb.Table(BUDGET_TABLE)
    try:
        response = table.update_item(
            Key={"fleet_id": fleet_id, "minute": minute},
            UpdateExpression="ADD issued :requested",
            ConditionExpression="attribute_not_exists(issued) OR issued <= :ceiling",
            ExpressionAttributeValues={
                ":requested": requested,
                ":ceiling": COMMANDS_PER_MINUTE - requested,
            },
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return 0
        raise
    issued = int(response.get("Attributes", {}).get("issued", requested))
    return min(requested, max(COMMANDS_PER_MINUTE - (issued - requested), 0))


def _deliverable(device: Dict[str, Any], command_name: str) -> bool:
    connected = bool(device.get("connected", False))
    if connected:
        return True
    return command_name in QUEUEABLE_COMMANDS


def dispatch_to_device(device: Dict[str, Any], command: Dict[str, Any]) -> bool:
    thing_name = str(device.get("thing_name", device.get("device_id", "")))
    if not thing_name:
        return False
    topic = "devices/{0}/commands".format(thing_name)
    try:
        iot_data.publish(
            topic=topic,
            qos=1 if command.get("name") in IMMEDIATE_COMMANDS else 0,
            payload=json.dumps(
                {
                    "command_id": str(command.get("command_id", "")),
                    "name": str(command.get("name", "")),
                    "parameters": command.get("parameters") or {},
                    "issued_at": int(time.time()),
                }
            ),
        )
        return True
    except ClientError as exc:
        logger.error("command_publish_failed thing=%s error=%s", thing_name, exc)
        return False


def republish_remainder(command: Dict[str, Any], cursor: str, depth: int) -> None:
    """Carry the rest of the fleet forward on the command topic."""
    if not COMMAND_TOPIC_ARN:
        logger.warning("command_topic_unconfigured command=%s", command.get("command_id"))
        return
    payload = dict(command)
    payload["cursor"] = cursor
    try:
        sns.publish(
            TopicArn=COMMAND_TOPIC_ARN,
            Message=json.dumps(payload),
            MessageAttributes={
                "command_name": {
                    "DataType": "String",
                    "StringValue": str(command.get("name", "unknown")),
                },
                "fleet_id": {
                    "DataType": "String",
                    "StringValue": str(command.get("fleet_id", "")),
                },
            },
        )
        logger.info(
            "remainder_republished command=%s cursor=%s depth=%s",
            command.get("command_id"), cursor, depth,
        )
    except ClientError as exc:
        logger.error("remainder_republish_failed command=%s error=%s", command.get("command_id"), exc)


def expand_command(command: Dict[str, Any], depth: int) -> Dict[str, Any]:
    """Dispatch one command across as much of the fleet as the budget allows."""
    fleet_id = str(command.get("fleet_id", "")).strip()
    command_name = str(command.get("name", "")).strip()
    cursor = command.get("cursor")

    if not fleet_id or not command_name:
        return {"status": "invalid"}

    minute = _minute_bucket(int(time.time()))
    granted = reserve_budget(fleet_id, minute, DEVICES_PER_INVOCATION)
    if granted <= 0:
        republish_remainder(command, cursor or "", depth)
        return {"status": "budget_exhausted", "fleet_id": fleet_id}

    dispatched = 0
    skipped = 0
    last_device_id = cursor or ""

    for device in iter_fleet_devices(fleet_id, cursor):
        if dispatched >= granted:
            republish_remainder(command, last_device_id, depth)
            return {
                "status": "partial",
                "fleet_id": fleet_id,
                "dispatched": dispatched,
                "skipped": skipped,
                "cursor": last_device_id,
            }

        last_device_id = str(device.get("device_id", last_device_id))

        if not _deliverable(device, command_name):
            skipped += 1
            continue
        if dispatch_to_device(device, command):
            dispatched += 1
        else:
            skipped += 1

    return {
        "status": "complete",
        "fleet_id": fleet_id,
        "dispatched": dispatched,
        "skipped": skipped,
    }


def lambda_handler(event, context):
    commands = decode_records(event)
    results: List[Dict[str, Any]] = []

    for command, depth in commands:
        try:
            results.append(expand_command(command, depth))
        except ClientError as exc:
            logger.exception(
                "command_expansion_failed command=%s error=%s", command.get("command_id"), exc
            )
            results.append({"status": "error", "command_id": command.get("command_id")})

    logger.info("fanout_complete commands=%s results=%s", len(commands), len(results))
    return {"commands": len(commands), "results": results}
