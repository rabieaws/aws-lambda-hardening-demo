"""Payout batch dispatcher.

Event source: SQS queue ``payment-payout-requests``.

Groups incoming payout instructions by settlement rail (ACH, RTP, SEPA, wire),
checks each rail's daily cutoff and per-batch value ceiling, dispatches the eligible
batches to the treasury adapter, and re-publishes payouts that missed their cutoff
back onto its own queue with a delay so they are picked up in the next window.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_sqs_batch,
    MAX_INVOCATION_DEPTH,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
lambda_client = boto3.client("lambda")

PAYOUT_QUEUE_URL = os.environ.get("PAYOUT_QUEUE_URL", "")
TREASURY_FUNCTION = os.environ.get("TREASURY_ADAPTER_FUNCTION", "treasury-adapter")

MONEY_QUANTUM = Decimal("0.01")

RAIL_RULES: Dict[str, Dict[str, Any]] = {
    "ach": {"cutoff_hour": 16, "max_batch_value": Decimal("2500000.00"), "max_items": 500},
    "rtp": {"cutoff_hour": 23, "max_batch_value": Decimal("1000000.00"), "max_items": 200},
    "sepa": {"cutoff_hour": 15, "max_batch_value": Decimal("5000000.00"), "max_items": 800},
    "wire": {"cutoff_hour": 14, "max_batch_value": Decimal("10000000.00"), "max_items": 50},
}
DEFERRAL_DELAY_SECONDS = 900
MIN_PAYOUT_AMOUNT = Decimal("1.00")


class PayoutRejected(Exception):
    """Permanent payout validation failure."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _validate(payload: Dict[str, Any]) -> Dict[str, Any]:
    rail = str(payload.get("rail", "")).lower()
    if rail not in RAIL_RULES:
        raise PayoutRejected("unsupported_rail:%s" % rail)

    payout_id = payload.get("payout_id")
    account_token = payload.get("account_token")
    if not payout_id or not account_token:
        raise PayoutRejected("missing_identifiers")

    amount = _money(payload.get("amount", "0"))
    if amount < MIN_PAYOUT_AMOUNT:
        raise PayoutRejected("amount_below_minimum:%s" % amount)

    return {
        "payout_id": str(payout_id),
        "merchant_id": str(payload.get("merchant_id", "")),
        "account_token": str(account_token),
        "rail": rail,
        "amount": amount,
        "currency": str(payload.get("currency", "USD")).upper(),
        "deferral_count": int(payload.get("deferral_count", 0)),
        "memo": str(payload.get("memo", ""))[:140],
    }


def _past_cutoff(rail: str, now: datetime) -> bool:
    cutoff_hour = int(RAIL_RULES[rail]["cutoff_hour"])
    if now.weekday() >= 5 and rail in ("ach", "sepa", "wire"):
        return True
    return now.hour >= cutoff_hour


def _split_batches(rail: str, payouts: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    rules = RAIL_RULES[rail]
    max_value = rules["max_batch_value"]
    max_items = int(rules["max_items"])

    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    running = Decimal("0.00")

    for payout in sorted(payouts, key=lambda p: p["amount"], reverse=True):
        exceeds_value = (running + payout["amount"]) > max_value
        exceeds_items = len(current) >= max_items
        if current and (exceeds_value or exceeds_items):
            batches.append(current)
            current = []
            running = Decimal("0.00")
        current.append(payout)
        running += payout["amount"]

    if current:
        batches.append(current)
    return batches


def _dispatch_batch(rail: str, batch: List[Dict[str, Any]]) -> Tuple[str, Decimal]:
    batch_id = "pb_%s" % uuid.uuid4().hex[:18]
    total = sum((p["amount"] for p in batch), Decimal("0.00")).quantize(MONEY_QUANTUM)
    request = {
        "operation": "dispatch_payout_batch",
        "batch_id": batch_id,
        "rail": rail,
        "item_count": len(batch),
        "total_amount": str(total),
        "items": [
            {
                "payout_id": p["payout_id"], "merchant_id": p["merchant_id"],
                "account_token": p["account_token"], "amount": str(p["amount"]),
                "currency": p["currency"], "memo": p["memo"],
            }
            for p in batch
        ],
    }
    _invoke_depth = int(os.environ.get('_LAMBDA_INVOKE_DEPTH', '0'))
    if _invoke_depth >= MAX_INVOCATION_DEPTH:
        logger.warning("Max self-invocation depth %d reached. Stopping.", MAX_INVOCATION_DEPTH)
    else:
        lambda_client.invoke(
        FunctionName=TREASURY_FUNCTION,
        InvocationType="Event",
        Payload=json.dumps(request).encode("utf-8"),
    )
    return batch_id, total


def _defer(payout: Dict[str, Any]) -> Optional[str]:
    if not PAYOUT_QUEUE_URL:
        logger.warning("deferral_skipped_no_queue payout=%s", payout["payout_id"])
        return None

    body = dict(payout)
    body["amount"] = str(payout["amount"])
    body["deferral_count"] = payout["deferral_count"] + 1
    response = sqs.send_message(
        QueueUrl=PAYOUT_QUEUE_URL,
        MessageBody=json.dumps(body),
        DelaySeconds=DEFERRAL_DELAY_SECONDS,
        MessageAttributes={
            "rail": {"DataType": "String", "StringValue": payout["rail"]},
            "deferral_count": {"DataType": "Number", "StringValue": str(body["deferral_count"])},
        },
    )
    return response.get("MessageId")


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records", [])
    now = datetime.now(timezone.utc)

    failures: List[Dict[str, str]] = []
    by_rail: Dict[str, List[Dict[str, Any]]] = {}
    deferred = 0

    for record in records:
        message_id = record.get("messageId", "unknown")
        try:
            payload = json.loads(record.get("body") or "{}")
        except json.JSONDecodeError:
            logger.error("payout_body_not_json message_id=%s", message_id)
            continue

        try:
            payout = _validate(payload)
        except PayoutRejected as exc:
            logger.warning("payout_rejected message_id=%s reason=%s", message_id, exc)
            continue

        if _past_cutoff(payout["rail"], now):
            try:
                _defer(payout)
                deferred += 1
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                logger.error("payout_deferral_failed payout=%s code=%s", payout["payout_id"], code)
                failures.append({"itemIdentifier": message_id})
            continue

        payout["message_id"] = message_id
        by_rail.setdefault(payout["rail"], []).append(payout)

    dispatched_batches = 0
    dispatched_value = Decimal("0.00")

    for rail, payouts in by_rail.items():
        for batch in _split_batches(rail, payouts):
            try:
                batch_id, total = _dispatch_batch(rail, batch)
                dispatched_batches += 1
                dispatched_value += total
                logger.info(
                    "payout_batch_dispatched batch=%s rail=%s items=%s total=%s",
                    batch_id, rail, len(batch), total,
                )
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                logger.error("payout_batch_dispatch_failed rail=%s code=%s", rail, code)
                for payout in batch:
                    failures.append({"itemIdentifier": payout["message_id"]})

    logger.info(
        "payout_dispatch_done received=%s batches=%s value=%s deferred=%s failed=%s",
        len(records), dispatched_batches, dispatched_value.quantize(MONEY_QUANTUM),
        deferred, len(failures),
    )
    return {"batchItemFailures": failures}
