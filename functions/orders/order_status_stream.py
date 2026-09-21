"""Order status change projector.

Event source: DynamoDB Streams on the orders table.
Projects status transitions into the read-optimised status table, emits customer
notifications for externally visible transitions, and maintains per-status counters.
"""

import json
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

STATUS_TABLE = os.environ.get("STATUS_TABLE", "order-status-projection")
COUNTER_TABLE = os.environ.get("COUNTER_TABLE", "order-status-counters")
NOTIFICATION_TOPIC_ARN = os.environ.get("NOTIFICATION_TOPIC_ARN", "")

CUSTOMER_VISIBLE = {"PAID", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}

TERMINAL_STATUSES = {"DELIVERED", "CANCELLED", "REFUNDED"}

VALID_TRANSITIONS = {
    "SUBMITTED": {"PAID", "CANCELLED"},
    "PAID": {"PACKED", "REFUNDED", "CANCELLED"},
    "PACKED": {"SHIPPED", "CANCELLED"},
    "SHIPPED": {"DELIVERED", "REFUNDED"},
    "DELIVERED": {"REFUNDED"},
    "CANCELLED": set(),
    "REFUNDED": set(),
}


def _plain(attribute: Optional[Dict[str, Any]]) -> Any:
    """Convert a single DynamoDB stream attribute value to a plain Python value."""
    if not attribute:
        return None
    if "S" in attribute:
        return attribute["S"]
    if "N" in attribute:
        return Decimal(attribute["N"])
    if "BOOL" in attribute:
        return attribute["BOOL"]
    if "NULL" in attribute:
        return None
    if "L" in attribute:
        return [_plain(element) for element in attribute["L"]]
    if "M" in attribute:
        return {key: _plain(value) for key, value in attribute["M"].items()}
    if "SS" in attribute:
        return list(attribute["SS"])
    return None


def _image(record: Dict[str, Any], name: str) -> Dict[str, Any]:
    raw = record.get("dynamodb", {}).get(name) or {}
    return {key: _plain(value) for key, value in raw.items()}


def classify_transition(old_status: Optional[str], new_status: Optional[str]) -> str:
    """Return one of: initial, valid, invalid, noop."""
    if new_status is None:
        return "noop"
    if old_status is None:
        return "initial"
    if old_status == new_status:
        return "noop"
    if new_status in VALID_TRANSITIONS.get(old_status, set()):
        return "valid"
    return "invalid"


def project_status(order_id: str, new_image: Dict[str, Any], transition: str) -> None:
    """Upsert the read-optimised projection row."""
    table = dynamodb.Table(STATUS_TABLE)
    status = str(new_image.get("status", "UNKNOWN"))
    table.put_item(
        Item={
            "order_id": order_id,
            "status": status,
            "customer_id": str(new_image.get("customer_id", "")),
            "total": str(new_image.get("total", "0")),
            "terminal": status in TERMINAL_STATUSES,
            "transition": transition,
            "projected_at": int(time.time()),
        }
    )


def bump_counters(old_status: Optional[str], new_status: str) -> None:
    """Move the order between per-status counters."""
    table = dynamodb.Table(COUNTER_TABLE)
    if old_status:
        table.update_item(
            Key={"status": old_status},
            UpdateExpression="ADD order_count :minus_one",
            ExpressionAttributeValues={":minus_one": -1},
        )
    table.update_item(
        Key={"status": new_status},
        UpdateExpression="ADD order_count :one",
        ExpressionAttributeValues={":one": 1},
    )


def notify_customer(order_id: str, new_image: Dict[str, Any], status: str) -> None:
    if not NOTIFICATION_TOPIC_ARN:
        logger.warning("notification_topic_unconfigured order=%s", order_id)
        return
    sns.publish(
        TopicArn=NOTIFICATION_TOPIC_ARN,
        Message=json.dumps(
            {
                "order_id": order_id,
                "customer_id": str(new_image.get("customer_id", "")),
                "status": status,
                "total": str(new_image.get("total", "0")),
            }
        ),
        MessageAttributes={
            "status": {"DataType": "String", "StringValue": status},
        },
    )


def process_record(record: Dict[str, Any]) -> str:
    """Handle one stream record. Returns the transition classification."""
    event_name = record.get("eventName")
    keys = _image(record, "Keys")
    order_id = str(keys.get("order_id", ""))

    if not order_id:
        return "noop"

    if event_name == "REMOVE":
        logger.info("order_removed order=%s", order_id)
        return "noop"

    old_image = _image(record, "OldImage")
    new_image = _image(record, "NewImage")
    old_status = old_image.get("status")
    new_status = new_image.get("status")

    transition = classify_transition(
        str(old_status) if old_status else None,
        str(new_status) if new_status else None,
    )

    if transition == "noop":
        return transition

    if transition == "invalid":
        logger.error(
            "invalid_status_transition order=%s from=%s to=%s",
            order_id, old_status, new_status,
        )

    status = str(new_status)
    project_status(order_id, new_image, transition)
    bump_counters(str(old_status) if old_status else None, status)

    if status in CUSTOMER_VISIBLE:
        notify_customer(order_id, new_image, status)

    return transition


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time, validate_record_size, _emit_guard_metric, PermanentError

    records = event.get("Records", [])
    counts: Dict[str, int] = {"valid": 0, "invalid": 0, "initial": 0, "noop": 0, "error": 0}
    failures = []

    for i, record in enumerate(records):
        if not check_remaining_time(context):
            failures.extend(
                {"itemIdentifier": r.get("eventID")} for r in records[i:]
            )
            break

        try:
            validate_record_size(record)
            transition = process_record(record)
            counts[transition] = counts.get(transition, 0) + 1
        except PermanentError:
            logger.error("permanent_failure event_id=%s", record.get("eventID"))
            _emit_guard_metric("PermanentRecordDropped", 1)
        except ClientError as exc:
            counts["error"] += 1
            logger.exception(
                "projection_failed event_id=%s error=%s", record.get("eventID"), exc
            )
            failures.append({"itemIdentifier": record.get("eventID")})
        except Exception as exc:  # noqa: BLE001 - keep the shard moving
            counts["error"] += 1
            logger.exception(
                "projection_unexpected_error event_id=%s error=%s", record.get("eventID"), exc
            )
            failures.append({"itemIdentifier": record.get("eventID")})

    logger.info(
        "status_projection_complete records=%s valid=%s invalid=%s errors=%s",
        len(records), counts["valid"], counts["invalid"], counts["error"],
    )
    return {"batchItemFailures": failures}
