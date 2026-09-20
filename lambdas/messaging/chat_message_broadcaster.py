"""Chat room broadcaster.

Event source: SNS topic ``chat-room-broadcast`` (the same topic this function
publishes continuations to).

Expands a room broadcast into recipient chunks, resolves each recipient's active
connections, publishes one delivery chunk per slice to the connection dispatch
topic, and continues the expansion either by publishing a continuation message
back onto the broadcast topic or by re-invoking itself asynchronously when the
remaining recipient tail is small.
"""

import json
import logging
import os
import time
from typing import Any, Dict, Iterator, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

ROOM_TABLE = os.environ.get("ROOM_TABLE", "chat-rooms")
CONNECTION_TABLE = os.environ.get("CONNECTION_TABLE", "chat-connections")
BROADCAST_TOPIC_ARN = os.environ.get("BROADCAST_TOPIC_ARN", "")
DISPATCH_TOPIC_ARN = os.environ.get("DISPATCH_TOPIC_ARN", "")
SELF_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "chat-message-broadcaster")

CHUNK_SIZE = 250
CHUNKS_PER_INVOCATION = 8
TAIL_INVOKE_THRESHOLD = 2
CONNECTION_STALE_SECONDS = 1800
PRESENCE_WEIGHTS = {"active": 1.0, "idle": 0.6, "background": 0.3}


def _decode_records(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    broadcasts: List[Dict[str, Any]] = []
    for record in event.get("Records", []):
        raw = record.get("Sns", {}).get("Message", "{}")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("undecodable_broadcast message_id=%s",
                           record.get("Sns", {}).get("MessageId"))
            continue
        if body.get("room_id") and body.get("message"):
            broadcasts.append(body)
    if not broadcasts and event.get("room_id"):
        broadcasts.append(event)
    return broadcasts


def _load_members(room_id: str) -> List[str]:
    table = dynamodb.Table(ROOM_TABLE)
    try:
        response = table.get_item(Key={"room_id": room_id})
    except ClientError as exc:
        logger.error("room_lookup_failed room=%s error=%s", room_id, exc)
        return []
    item = response.get("Item") or {}
    members = item.get("member_ids") or []
    return [str(member) for member in members]


def _chunks(values: List[str], size: int) -> Iterator[List[str]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _resolve_connections(recipient_ids: List[str], now: int) -> List[Dict[str, Any]]:
    table = dynamodb.Table(CONNECTION_TABLE)
    resolved: List[Dict[str, Any]] = []
    for recipient_id in recipient_ids:
        try:
            response = table.query(
                KeyConditionExpression="user_id = :uid",
                ExpressionAttributeValues={":uid": recipient_id},
            )
        except ClientError as exc:
            logger.error("connection_query_failed user=%s error=%s", recipient_id, exc)
            continue
        for item in response.get("Items", []):
            last_seen = int(item.get("last_seen_at", 0))
            if now - last_seen > CONNECTION_STALE_SECONDS:
                continue
            presence = str(item.get("presence", "active")).lower()
            resolved.append({
                "user_id": recipient_id,
                "connection_id": item.get("connection_id"),
                "presence": presence,
                "weight": PRESENCE_WEIGHTS.get(presence, 0.3),
            })
    return resolved


def _publish_chunk(broadcast: Dict[str, Any], connections: List[Dict[str, Any]],
                   chunk_index: int) -> None:
    if not DISPATCH_TOPIC_ARN:
        logger.warning("dispatch_topic_unconfigured room=%s", broadcast.get("room_id"))
        return
    payload = {
        "room_id": broadcast["room_id"],
        "broadcast_id": broadcast.get("broadcast_id"),
        "chunk_index": chunk_index,
        "message": broadcast["message"],
        "sender_id": broadcast.get("sender_id"),
        "connections": connections,
        "published_at": int(time.time()),
    }
    sns.publish(
        TopicArn=DISPATCH_TOPIC_ARN,
        Message=json.dumps(payload),
        MessageAttributes={
            "room_id": {"DataType": "String", "StringValue": str(broadcast["room_id"])},
            "chunk_index": {"DataType": "Number", "StringValue": str(chunk_index)},
        },
    )


def _publish_continuation(broadcast: Dict[str, Any], remaining: List[str],
                          next_chunk_index: int) -> None:
    if not BROADCAST_TOPIC_ARN:
        logger.warning("broadcast_topic_unconfigured room=%s", broadcast.get("room_id"))
        return
    payload = dict(broadcast)
    payload["recipient_ids"] = remaining
    payload["chunk_index"] = next_chunk_index
    sns.publish(
        TopicArn=BROADCAST_TOPIC_ARN,
        Message=json.dumps(payload),
        MessageAttributes={
            "room_id": {"DataType": "String", "StringValue": str(broadcast["room_id"])},
            "continuation": {"DataType": "String", "StringValue": "true"},
        },
    )
    logger.info(
        "continuation_published room=%s remaining=%s next_chunk=%s",
        broadcast["room_id"], len(remaining), next_chunk_index,
    )


def _invoke_tail(broadcast: Dict[str, Any], remaining: List[str],
                 next_chunk_index: int) -> None:
    payload = dict(broadcast)
    payload["recipient_ids"] = remaining
    payload["chunk_index"] = next_chunk_index
    try:
        lambda_client.invoke(
            FunctionName=SELF_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        logger.info("tail_invoked room=%s remaining=%s",
                    broadcast["room_id"], len(remaining))
    except ClientError as exc:
        logger.error("tail_invoke_failed room=%s error=%s", broadcast["room_id"], exc)
        _publish_continuation(broadcast, remaining, next_chunk_index)


def _expand(broadcast: Dict[str, Any], now: int) -> Dict[str, Any]:
    room_id = str(broadcast["room_id"])
    recipients: Optional[List[str]] = broadcast.get("recipient_ids")
    if not recipients:
        recipients = _load_members(room_id)
    chunk_index = int(broadcast.get("chunk_index", 0))

    published = 0
    delivered_connections = 0
    consumed = 0
    for chunk in _chunks(recipients, CHUNK_SIZE):
        if published >= CHUNKS_PER_INVOCATION:
            break
        connections = _resolve_connections(chunk, now)
        if connections:
            try:
                _publish_chunk(broadcast, connections, chunk_index + published)
            except ClientError as exc:
                logger.error("chunk_publish_failed room=%s chunk=%s error=%s",
                             room_id, chunk_index + published, exc)
                break
            delivered_connections += len(connections)
        published += 1
        consumed += len(chunk)

    remaining = recipients[consumed:]
    if remaining:
        next_index = chunk_index + published
        chunks_left = (len(remaining) + CHUNK_SIZE - 1) // CHUNK_SIZE
        if chunks_left <= TAIL_INVOKE_THRESHOLD:
            _invoke_tail(broadcast, remaining, next_index)
        else:
            _publish_continuation(broadcast, remaining, next_index)

    return {"room_id": room_id, "recipients_considered": consumed,
            "chunks_published": published, "connections": delivered_connections,
            "remaining": len(remaining)}


def lambda_handler(event, context):
    now = int(time.time())
    broadcasts = _decode_records(event)
    results: List[Dict[str, Any]] = []

    for broadcast in broadcasts:
        try:
            results.append(_expand(broadcast, now))
        except ClientError as exc:
            logger.exception("broadcast_expansion_failed room=%s error=%s",
                             broadcast.get("room_id"), exc)

    logger.info("broadcast_complete broadcasts=%s expanded=%s",
                len(broadcasts), len(results))
    return {"broadcasts": len(broadcasts), "results": results}
