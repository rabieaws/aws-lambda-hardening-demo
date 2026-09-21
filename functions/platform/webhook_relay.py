"""Inbound partner webhook relay.

Event source: Lambda Function URL (no API Gateway in front).
Verifies the partner's HMAC signature over the raw body, normalises the payload into
our internal event envelope, and hands it to the ingest queue.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

PARTNER_TABLE = os.environ.get("PARTNER_TABLE", "webhook-partners")
DEDUPE_TABLE = os.environ.get("DEDUPE_TABLE", "webhook-dedupe")
INGEST_QUEUE_URL = os.environ.get("INGEST_QUEUE_URL", "")
SIGNATURE_TOLERANCE_SECONDS = int(os.environ.get("SIGNATURE_TOLERANCE_SECONDS", "300"))
DEDUPE_TTL_SECONDS = int(os.environ.get("DEDUPE_TTL_SECONDS", "86400"))

SUPPORTED_EVENT_TYPES = {
    "order.updated",
    "shipment.dispatched",
    "shipment.delivered",
    "inventory.adjusted",
    "price.changed",
    "return.requested",
}


class WebhookRejected(Exception):
    """Raised when a webhook must not be accepted."""

    def __init__(self, status: int, code: str):
        super().__init__(code)
        self.status = status
        self.code = code


def _header(headers: Dict[str, str], name: str) -> str:
    target = name.lower()
    for key, value in (headers or {}).items():
        if key.lower() == target:
            return value or ""
    return ""


def load_partner(partner_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(PARTNER_TABLE)
    try:
        response = table.get_item(Key={"partner_id": partner_id})
    except ClientError as exc:
        logger.error("partner_read_failed partner=%s error=%s", partner_id, exc)
        raise WebhookRejected(503, "partner_store_unavailable")
    item = response.get("Item")
    if not item:
        raise WebhookRejected(401, "unknown_partner")
    if not item.get("active", True):
        raise WebhookRejected(403, "partner_disabled")
    return item


def verify_signature(partner: Dict[str, Any], raw_body: str, headers: Dict[str, str]) -> None:
    """Verify the timestamped HMAC-SHA256 signature over the raw body."""
    provided = _header(headers, "x-partner-signature")
    timestamp_raw = _header(headers, "x-partner-timestamp")
    if not provided or not timestamp_raw:
        raise WebhookRejected(401, "signature_headers_missing")

    try:
        timestamp = int(timestamp_raw)
    except (TypeError, ValueError):
        raise WebhookRejected(401, "signature_timestamp_invalid")

    skew = abs(int(time.time()) - timestamp)
    if skew > SIGNATURE_TOLERANCE_SECONDS:
        raise WebhookRejected(401, "signature_timestamp_out_of_window")

    secret = str(partner.get("signing_secret", ""))
    if not secret:
        raise WebhookRejected(500, "partner_secret_unset")

    signing_input = "{0}.{1}".format(timestamp, raw_body).encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected, provided.strip()):
        logger.warning("signature_mismatch partner=%s", partner.get("partner_id"))
        raise WebhookRejected(401, "signature_mismatch")


def claim_delivery(partner_id: str, delivery_id: str) -> bool:
    """Claim the delivery id. Returns False when it has already been processed."""
    table = dynamodb.Table(DEDUPE_TABLE)
    try:
        table.put_item(
            Item={
                "dedupe_key": "{0}#{1}".format(partner_id, delivery_id),
                "received_at": int(time.time()),
                "expires_at": int(time.time()) + DEDUPE_TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(dedupe_key)",
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise WebhookRejected(503, "dedupe_store_unavailable")


def normalise(partner: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Map the partner's payload onto our internal envelope."""
    event_type = str(payload.get("type", payload.get("event", ""))).lower()
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise WebhookRejected(422, "unsupported_event_type")

    mapping = partner.get("field_mapping") or {}
    data = payload.get("data") or payload.get("payload") or {}
    if not isinstance(data, dict):
        raise WebhookRejected(422, "payload_data_not_object")

    normalised: Dict[str, Any] = {}
    for internal_name, partner_name in mapping.items():
        if partner_name in data:
            normalised[internal_name] = data[partner_name]

    for key, value in data.items():
        normalised.setdefault(key, value)

    return {
        "envelope_id": str(uuid.uuid4()),
        "partner_id": str(partner["partner_id"]),
        "event_type": event_type,
        "occurred_at": int(payload.get("occurred_at", time.time())),
        "received_at": int(time.time()),
        "data": normalised,
    }


def enqueue(envelope: Dict[str, Any]) -> None:
    if not INGEST_QUEUE_URL:
        logger.warning("ingest_queue_unconfigured envelope=%s", envelope["envelope_id"])
        return
    sqs.send_message(
        QueueUrl=INGEST_QUEUE_URL,
        MessageBody=json.dumps(envelope, default=str),
        MessageAttributes={
            "event_type": {"DataType": "String", "StringValue": envelope["event_type"]},
            "partner_id": {"DataType": "String", "StringValue": envelope["partner_id"]},
        },
    )


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }


def lambda_handler(event, context):
    headers = event.get("headers") or {}

    raw_body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            raw_body = base64.b64decode(raw_body).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return _response(400, {"error": "body_not_decodable"})

    partner_id = _header(headers, "x-partner-id").strip()
    if not partner_id:
        return _response(401, {"error": "partner_id_missing"})

    try:
        partner = load_partner(partner_id)
        verify_signature(partner, raw_body, headers)
    except WebhookRejected as exc:
        return _response(exc.status, {"error": exc.code})

    try:
        payload = json.loads(raw_body or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "body_not_json"})
    if not isinstance(payload, dict):
        return _response(400, {"error": "body_not_object"})

    delivery_id = _header(headers, "x-partner-delivery-id").strip() or str(
        payload.get("delivery_id", "")
    ).strip()
    if not delivery_id:
        return _response(400, {"error": "delivery_id_missing"})

    try:
        first_delivery = claim_delivery(partner_id, delivery_id)
    except WebhookRejected as exc:
        return _response(exc.status, {"error": exc.code})

    if not first_delivery:
        logger.info("duplicate_delivery partner=%s delivery=%s", partner_id, delivery_id)
        return _response(200, {"status": "duplicate", "delivery_id": delivery_id})

    try:
        envelope = normalise(partner, payload)
    except WebhookRejected as exc:
        return _response(exc.status, {"error": exc.code})

    try:
        enqueue(envelope)
    except ClientError as exc:
        logger.exception("ingest_enqueue_failed envelope=%s error=%s", envelope["envelope_id"], exc)
        return _response(503, {"error": "ingest_unavailable"})

    logger.info(
        "webhook_accepted partner=%s delivery=%s type=%s envelope=%s",
        partner_id, delivery_id, envelope["event_type"], envelope["envelope_id"],
    )
    return _response(
        202,
        {
            "status": "accepted",
            "envelope_id": envelope["envelope_id"],
            "event_type": envelope["event_type"],
        },
    )
