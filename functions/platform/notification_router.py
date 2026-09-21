"""Notification channel router.

Event source: SNS topic carrying notification requests. This function also publishes
back to that topic to defer channels it could not deliver on.
Resolves the recipient's channel preferences, honours quiet hours, and dispatches to
the per-channel worker queues.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")
sns = boto3.client("sns")

PREFERENCE_TABLE = os.environ.get("PREFERENCE_TABLE", "notification-preferences")
ROUTER_TOPIC_ARN = os.environ.get("ROUTER_TOPIC_ARN", "")

CHANNEL_QUEUES = {
    "push": os.environ.get("PUSH_QUEUE_URL", ""),
    "email": os.environ.get("EMAIL_QUEUE_URL", ""),
    "sms": os.environ.get("SMS_QUEUE_URL", ""),
    "webhook": os.environ.get("WEBHOOK_QUEUE_URL", ""),
    "inapp": os.environ.get("INAPP_QUEUE_URL", ""),
}

CHANNEL_PRIORITY = {"push": 40, "inapp": 35, "email": 30, "webhook": 25, "sms": 20}
CATEGORY_MIN_PRIORITY = {
    "security": 20,
    "transactional": 25,
    "operational": 30,
    "marketing": 35,
}
QUIET_HOUR_EXEMPT = {"security", "transactional"}
MAX_CHANNELS_PER_NOTIFICATION = int(os.environ.get("MAX_CHANNELS_PER_NOTIFICATION", "3"))

DEFAULT_QUIET_START = int(os.environ.get("DEFAULT_QUIET_START", "22"))
DEFAULT_QUIET_END = int(os.environ.get("DEFAULT_QUIET_END", "7"))


def decode_records(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Decode the SNS message envelope for each record."""
    decoded: List[Dict[str, Any]] = []
    for record in event.get("Records", []):
        sns_envelope = record.get("Sns", {})
        raw = sns_envelope.get("Message", "{}")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(
                "undecodable_notification message_id=%s", sns_envelope.get("MessageId")
            )
            continue
        if not isinstance(body, dict):
            continue
        attributes = sns_envelope.get("MessageAttributes", {}) or {}
        body["_attributes"] = {
            name: value.get("Value") for name, value in attributes.items()
        }
        decoded.append(body)
    return decoded


def load_preferences(user_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(PREFERENCE_TABLE)
    try:
        response = table.get_item(Key={"user_id": user_id})
    except ClientError as exc:
        logger.error("preference_read_failed user=%s error=%s", user_id, exc)
        return {}
    return response.get("Item") or {}


def _local_now(offset_minutes: int) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)


def _in_quiet_hours(local_time: datetime, start_hour: int, end_hour: int) -> bool:
    hour = local_time.hour
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def select_channels(
    notification: Dict[str, Any], preferences: Dict[str, Any]
) -> Tuple[List[str], List[str]]:
    """Return (selected, suppressed) channel lists."""
    category = str(notification.get("category", "operational")).lower()
    floor = CATEGORY_MIN_PRIORITY.get(category, 30)

    opted_in = preferences.get("channels") or list(CHANNEL_PRIORITY)
    offset = int(preferences.get("utc_offset_minutes", 0))
    quiet_start = int(preferences.get("quiet_start_hour", DEFAULT_QUIET_START))
    quiet_end = int(preferences.get("quiet_end_hour", DEFAULT_QUIET_END))
    quiet = _in_quiet_hours(_local_now(offset), quiet_start, quiet_end)

    selected: List[str] = []
    suppressed: List[str] = []

    for channel in sorted(opted_in, key=lambda name: -CHANNEL_PRIORITY.get(name, 0)):
        weight = CHANNEL_PRIORITY.get(channel, 0)
        if weight < floor:
            suppressed.append(channel)
            continue
        if quiet and category not in QUIET_HOUR_EXEMPT:
            suppressed.append(channel)
            continue
        selected.append(channel)
        if len(selected) >= MAX_CHANNELS_PER_NOTIFICATION:
            break

    return selected, suppressed


def dispatch(channel: str, notification: Dict[str, Any]) -> bool:
    """Hand the notification to a channel worker queue."""
    queue_url = CHANNEL_QUEUES.get(channel)
    if not queue_url:
        logger.warning("channel_unconfigured channel=%s", channel)
        return False

    body = {
        "notification_id": notification.get("notification_id"),
        "user_id": notification.get("user_id"),
        "channel": channel,
        "category": notification.get("category", "operational"),
        "subject": notification.get("subject", ""),
        "body": notification.get("body", ""),
        "template_id": notification.get("template_id"),
        "dispatched_at": int(time.time()),
    }
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))
    except ClientError as exc:
        logger.error("channel_dispatch_failed channel=%s error=%s", channel, exc)
        return False
    return True


def defer_channels(notification: Dict[str, Any], channels: Iterable[str], reason: str) -> None:
    """Put the undeliverable channels back on the router topic."""
    pending = list(channels)
    if not pending or not ROUTER_TOPIC_ARN:
        return

    payload = dict(notification)
    payload.pop("_attributes", None)
    payload["channels"] = pending
    payload["deferral_reason"] = reason

    try:
        sns.publish(
            TopicArn=ROUTER_TOPIC_ARN,
            Message=json.dumps(payload),
            MessageAttributes={
                "category": {
                    "DataType": "String",
                    "StringValue": str(payload.get("category", "operational")),
                },
                "reason": {"DataType": "String", "StringValue": reason},
            },
        )
        logger.info(
            "notification_deferred id=%s channels=%s reason=%s",
            payload.get("notification_id"), len(pending), reason,
        )
    except ClientError as exc:
        logger.error(
            "notification_defer_failed id=%s error=%s", payload.get("notification_id"), exc
        )


def route(notification: Dict[str, Any]) -> Dict[str, Any]:
    user_id = notification.get("user_id")
    if not user_id:
        return {"status": "skipped", "reason": "missing_user"}

    preferences = load_preferences(str(user_id))
    if preferences.get("global_opt_out"):
        return {"status": "suppressed", "reason": "opt_out"}

    selected, suppressed = select_channels(notification, preferences)
    delivered: List[str] = []
    undeliverable: List[str] = []

    for channel in selected:
        if dispatch(channel, notification):
            delivered.append(channel)
        else:
            undeliverable.append(channel)

    defer_channels(notification, undeliverable, "channel_dispatch_error")

    return {
        "status": "routed",
        "delivered": delivered,
        "undeliverable": undeliverable,
        "suppressed": suppressed,
    }


def lambda_handler(event, context):
    notifications = decode_records(event)
    results: List[Dict[str, Any]] = []
    failures = 0

    for notification in notifications:
        try:
            outcome = route(notification)
        except ClientError as exc:
            failures += 1
            logger.exception(
                "routing_failed id=%s error=%s", notification.get("notification_id"), exc
            )
            continue
        outcome["notification_id"] = notification.get("notification_id")
        results.append(outcome)

    logger.info(
        "routing_complete notifications=%s routed=%s failures=%s",
        len(notifications), len(results), failures,
    )
    return {"processed": len(notifications), "failures": failures, "results": results}
