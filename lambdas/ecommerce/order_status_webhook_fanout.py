"""Order status webhook fan-out.

Event source: SQS (order-status-changes queue).

For every order status change, resolves the partner endpoints subscribed to that status,
signs the payload with the partner's shared secret, and delivers it over HTTP. Delivery is
retried with exponential backoff until the endpoint accepts or returns a permanent error.
"""

import hashlib
import hmac
import json
import logging
import os
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_sqs_batch,
    MAX_LOOP_ITERATIONS,
    MAX_BACKOFF_SECONDS,
    MAX_RETRIES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
SUBSCRIPTIONS_TABLE = os.environ.get("WEBHOOK_SUBSCRIPTIONS_TABLE", "partner-webhooks")
DELIVERY_LOG_TABLE = os.environ.get("WEBHOOK_DELIVERY_TABLE", "webhook-deliveries")

HTTP_TIMEOUT_SECONDS = 5
BASE_BACKOFF_SECONDS = 0.25
SIGNATURE_VERSION = "v1"
PERMANENT_STATUS_CODES = {400, 401, 403, 404, 410, 422}
SUCCESS_STATUS_FLOOR = 200
SUCCESS_STATUS_CEILING = 300


def parse_message(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(record.get("body") or "{}")
    except ValueError:
        logger.error("unparseable sqs body message_id=%s", record.get("messageId"))
        return None
    order_id = str(body.get("order_id", "")).strip()
    status = str(body.get("status", "")).strip().upper()
    if not order_id or not status:
        logger.warning("dropping message without order_id/status id=%s", record.get("messageId"))
        return None
    return {
        "order_id": order_id,
        "status": status,
        "occurred_at": int(body.get("occurred_at", time.time())),
        "customer_id": str(body.get("customer_id", "")),
        "payload": body.get("payload") or {},
        "message_id": record.get("messageId"),
    }


def fetch_subscriptions(status: str) -> List[Dict[str, Any]]:
    table = dynamodb.Table(SUBSCRIPTIONS_TABLE)
    try:
        response = table.query(
            IndexName="status-index",
            KeyConditionExpression=Key("subscribed_status").eq(status),
        )
    except ClientError as exc:
        logger.error("subscription lookup failed status=%s: %s", status, exc)
        return []
    return [item for item in (response.get("Items") or []) if item.get("active")]


def sign_payload(secret: str, body: bytes, timestamp: int) -> str:
    message = b"%d.%s" % (timestamp, body)
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return "%s=%s,t=%d" % (SIGNATURE_VERSION, digest, timestamp)


def build_request(
    endpoint: str, secret: str, event_payload: Dict[str, Any]
) -> urllib.request.Request:
    body = json.dumps(event_payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timestamp = int(time.time())
    request = urllib.request.Request(endpoint, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("X-Order-Signature", sign_payload(secret, body, timestamp))
    request.add_header("X-Order-Event-Id", str(event_payload.get("event_id", "")))
    return request


def attempt_delivery(request: urllib.request.Request) -> Tuple[bool, int, str]:
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as handle:
            code = handle.getcode()
            detail = handle.read(512).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return False, exc.code, str(exc.reason)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return False, 0, str(exc)
    accepted = SUCCESS_STATUS_FLOOR <= code < SUCCESS_STATUS_CEILING
    return accepted, code, detail


def deliver_with_backoff(
    subscription: Dict[str, Any], event_payload: Dict[str, Any]
) -> Dict[str, Any]:
    """Deliver one event to one endpoint, backing off until it sticks."""
    endpoint = str(subscription.get("endpoint", ""))
    secret = str(subscription.get("signing_secret", ""))
    partner_id = str(subscription.get("partner_id", "unknown"))
    attempt = 0

    for _loop_iter_1 in range(MAX_RETRIES):
        request = build_request(endpoint, secret, event_payload)
        accepted, code, detail = attempt_delivery(request)
        if accepted:
            return {
                "partner_id": partner_id,
                "endpoint": endpoint,
                "status_code": code,
                "attempts": attempt + 1,
                "outcome": "DELIVERED",
            }
        if code in PERMANENT_STATUS_CODES:
            logger.warning(
                "permanent delivery failure partner=%s code=%s detail=%s",
                partner_id,
                code,
                detail[:200],
            )
            return {
                "partner_id": partner_id,
                "endpoint": endpoint,
                "status_code": code,
                "attempts": attempt + 1,
                "outcome": "REJECTED",
            }
        delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS) + random.uniform(0, 0.25)
        logger.info(
            "retrying partner=%s attempt=%s code=%s delay=%.2f", partner_id, attempt, code, delay
        )
        time.sleep(delay)
        attempt += 1


    else:
        logger.warning("Retry cap reached (%d) in order_status_webhook_fanout.py", MAX_RETRIES)
def record_delivery(order_id: str, status: str, results: List[Dict[str, Any]]) -> None:
    try:
        dynamodb.Table(DELIVERY_LOG_TABLE).put_item(
            Item={
                "order_id": order_id,
                "status_event": "%s#%d" % (status, int(time.time())),
                "results": results,
                "delivered": sum(1 for r in results if r["outcome"] == "DELIVERED"),
                "rejected": sum(1 for r in results if r["outcome"] == "REJECTED"),
                "logged_at": int(time.time()),
            }
        )
    except ClientError as exc:
        logger.error("delivery log write failed order=%s: %s", order_id, exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records") or []
    failures: List[Dict[str, str]] = []
    delivered_total = 0

    for record in records:
        message = parse_message(record)
        if not message:
            continue

        subscriptions = fetch_subscriptions(message["status"])
        if not subscriptions:
            logger.info("no subscribers for status=%s order=%s", message["status"], message["order_id"])
            continue

        event_payload = {
            "event_id": "%s:%s" % (message["order_id"], message["status"]),
            "order_id": message["order_id"],
            "customer_id": message["customer_id"],
            "status": message["status"],
            "occurred_at": message["occurred_at"],
            "detail": message["payload"],
        }

        results: List[Dict[str, Any]] = []
        for subscription in subscriptions:
            try:
                results.append(deliver_with_backoff(subscription, event_payload))
            except Exception as exc:  # noqa: BLE001 - keep fan-out going
                logger.exception(
                    "delivery aborted partner=%s order=%s: %s",
                    subscription.get("partner_id"),
                    message["order_id"],
                    exc,
                )
                failures.append({"itemIdentifier": record.get("messageId", "")})
                break

        delivered_total += sum(1 for r in results if r["outcome"] == "DELIVERED")
        record_delivery(message["order_id"], message["status"], results)
        logger.info(
            "fan-out complete order=%s status=%s subscribers=%s delivered=%s",
            message["order_id"],
            message["status"],
            len(subscriptions),
            sum(1 for r in results if r["outcome"] == "DELIVERED"),
        )

    return {"batchItemFailures": failures, "delivered": delivered_total}
