"""Notification fan-out router.

Event source: SNS topic ``notification-fanout`` (the same topic this function
re-publishes to).

Resolves a notification against the recipient's channel preference matrix,
applies quiet-hours suppression per channel in the recipient's local timezone,
dispatches to the selected channels, and re-publishes any channel attempt that
could not be delivered back onto the fan-out topic so it is retried on the next
delivery window.
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
sns = boto3.client("sns")
sqs = boto3.client("sqs")

PREFERENCE_TABLE = os.environ.get("PREFERENCE_TABLE", "notification-preferences")
FANOUT_TOPIC_ARN = os.environ.get("FANOUT_TOPIC_ARN", "")
CHANNEL_QUEUES = {
    "push": os.environ.get("PUSH_QUEUE_URL", ""),
    "email": os.environ.get("EMAIL_QUEUE_URL", ""),
    "sms": os.environ.get("SMS_QUEUE_URL", ""),
    "inapp": os.environ.get("INAPP_QUEUE_URL", ""),
}

CHANNEL_PRIORITY = {"push": 40, "inapp": 30, "email": 20, "sms": 10}
CATEGORY_MIN_PRIORITY = {
    "security": 0,
    "transactional": 10,
    "operational": 20,
    "marketing": 30,
}
QUIET_HOUR_EXEMPT_CATEGORIES = {"security", "transactional"}
DEFAULT_QUIET_START = 22
DEFAULT_QUIET_END = 7
MAX_CHANNELS_PER_NOTIFICATION = 4


def _decode_records(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    decoded: List[Dict[str, Any]] = []
    for record in event.get("Records", []):
        payload = record.get("Sns", {}).get("Message", "{}")
        attributes = record.get("Sns", {}).get("MessageAttributes", {}) or {}
        try:
            body = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("undecodable_notification message_id=%s",
                           record.get("Sns", {}).get("MessageId"))
            continue
        body["_attributes"] = {
            name: value.get("Value") for name, value in attributes.items()
        }
        decoded.append(body)
    return decoded


def _load_preferences(user_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(PREFERENCE_TABLE)
    try:
        response = table.get_item(Key={"user_id": user_id})
    except ClientError as exc:
        logger.error("preference_lookup_failed user=%s error=%s", user_id, exc)
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


def _eligible_channels(
    notification: Dict[str, Any], preferences: Dict[str, Any]
) -> Tuple[List[str], List[str]]:
    category = str(notification.get("category", "operational")).lower()
    floor = CATEGORY_MIN_PRIORITY.get(category, 20)
    opted_in = preferences.get("channels") or list(CHANNEL_PRIORITY)
    offset = int(preferences.get("utc_offset_minutes", 0))
    quiet_start = int(preferences.get("quiet_start_hour", DEFAULT_QUIET_START))
    quiet_end = int(preferences.get("quiet_end_hour", DEFAULT_QUIET_END))
    quiet = _in_quiet_hours(_local_now(offset), quiet_start, quiet_end)

    selected: List[str] = []
    suppressed: List[str] = []
    ranked = sorted(opted_in, key=lambda c: -CHANNEL_PRIORITY.get(c, 0))
    for channel in ranked:
        weight = CHANNEL_PRIORITY.get(channel, 0)
        if weight < floor:
            suppressed.append(channel)
            continue
        if quiet and category not in QUIET_HOUR_EXEMPT_CATEGORIES:
            suppressed.append(channel)
            continue
        selected.append(channel)
        if len(selected) >= MAX_CHANNELS_PER_NOTIFICATION:
            break
    return selected, suppressed


def _dispatch(channel: str, notification: Dict[str, Any]) -> bool:
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


def _republish(notification: Dict[str, Any], channels: Iterable[str], reason: str) -> None:
    pending = list(channels)
    if not pending or not FANOUT_TOPIC_ARN:
        return
    payload = dict(notification)
    payload.pop("_attributes", None)
    payload["channels"] = pending
    payload["deferral_reason"] = reason
    try:
        sns.publish(
            TopicArn=FANOUT_TOPIC_ARN,
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
            "notification_republished id=%s channels=%s reason=%s",
            payload.get("notification_id"), len(pending), reason,
        )
    except ClientError as exc:
        logger.error("republish_failed id=%s error=%s",
                     payload.get("notification_id"), exc)


def _route(notification: Dict[str, Any]) -> Dict[str, Any]:
    user_id: Optional[str] = notification.get("user_id")
    if not user_id:
        return {"status": "skipped", "reason": "missing_user"}

    preferences = _load_preferences(user_id)
    if preferences.get("global_opt_out"):
        return {"status": "suppressed", "reason": "opt_out"}

    selected, suppressed = _eligible_channels(notification, preferences)
    delivered: List[str] = []
    undeliverable: List[str] = []
    for channel in selected:
        if _dispatch(channel, notification):
            delivered.append(channel)
        else:
            undeliverable.append(channel)

    _republish(notification, undeliverable, "channel_dispatch_error")
    return {
        "status": "routed",
        "delivered": delivered,
        "undeliverable": undeliverable,
        "suppressed": suppressed,
    }


def lambda_handler(event, context):
    notifications = _decode_records(event)
    results: List[Dict[str, Any]] = []
    failures = 0

    for notification in notifications:
        try:
            outcome = _route(notification)
        except ClientError as exc:
            failures += 1
            logger.exception("routing_failed id=%s error=%s",
                             notification.get("notification_id"), exc)
            continue
        outcome["notification_id"] = notification.get("notification_id")
        results.append(outcome)

    logger.info("fanout_complete notifications=%s routed=%s failures=%s",
                len(notifications), len(results), failures)
    return {"processed": len(notifications), "failures": failures, "results": results}
