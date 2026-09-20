"""Chargeback dispute intake.

Event source: API Gateway REST API, ``POST /payments/disputes``.

Validates the inbound network reason code, derives the representment evidence
deadline from the card network's response-window rules and the notification date,
assembles the representment evidence packet skeleton, and persists the dispute with
its liability posture and compelling-evidence requirements.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_api_gateway_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

DISPUTE_TABLE = os.environ.get("DISPUTE_TABLE", "payment-disputes")
EVIDENCE_QUEUE_URL = os.environ.get("EVIDENCE_QUEUE_URL", "")

MONEY_QUANTUM = Decimal("0.01")

REASON_CODES: Dict[str, Dict[str, Any]] = {
    "10.4": {"network": "visa", "category": "fraud", "window_days": 30, "liability": "issuer"},
    "12.5": {"network": "visa", "category": "processing", "window_days": 30, "liability": "acquirer"},
    "13.1": {"network": "visa", "category": "consumer", "window_days": 30, "liability": "merchant"},
    "13.3": {"network": "visa", "category": "consumer", "window_days": 30, "liability": "merchant"},
    "4853": {"network": "mastercard", "category": "consumer", "window_days": 45, "liability": "merchant"},
    "4837": {"network": "mastercard", "category": "fraud", "window_days": 45, "liability": "issuer"},
    "4834": {"network": "mastercard", "category": "processing", "window_days": 45, "liability": "acquirer"},
    "F24": {"network": "amex", "category": "processing", "window_days": 20, "liability": "acquirer"},
    "C08": {"network": "amex", "category": "consumer", "window_days": 20, "liability": "merchant"},
}

EVIDENCE_BY_CATEGORY: Dict[str, List[str]] = {
    "fraud": ["avs_cvv_result", "device_fingerprint", "3ds_authentication_log", "prior_undisputed_txns"],
    "consumer": ["proof_of_delivery", "terms_acceptance", "refund_policy", "customer_correspondence"],
    "processing": ["authorization_record", "settlement_record", "currency_conversion_detail"],
}
WEEKEND_DAYS = (5, 6)
PREPARATION_BUFFER_DAYS = 5
HIGH_VALUE_ESCALATION = Decimal("2500.00")


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _parse_notified_at(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    cleaned = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shift_off_weekend(moment: datetime) -> datetime:
    while moment.weekday() in WEEKEND_DAYS:
        moment = moment - timedelta(days=1)
    return moment


def _deadlines(notified_at: datetime, window_days: int) -> Tuple[datetime, datetime]:
    network_deadline = notified_at + timedelta(days=window_days)
    internal_target = _shift_off_weekend(
        network_deadline - timedelta(days=PREPARATION_BUFFER_DAYS)
    )
    return network_deadline, internal_target


def _required_evidence(category: str, amount: Decimal) -> List[str]:
    required = list(EVIDENCE_BY_CATEGORY.get(category, []))
    if amount >= HIGH_VALUE_ESCALATION:
        required.append("signed_merchant_rebuttal")
        required.append("chargeback_analyst_review")
    return required


def _representment_packet(
    dispute_id: str, body: Dict[str, Any], rule: Dict[str, Any], evidence: List[str]
) -> Dict[str, Any]:
    return {
        "dispute_id": dispute_id,
        "network": rule["network"],
        "reason_code": body["reason_code"],
        "category": rule["category"],
        "liability_posture": rule["liability"],
        "sections": [
            {"name": "cover_letter", "status": "pending", "generated": False},
            {"name": "transaction_summary", "status": "pending", "generated": False},
            {"name": "evidence_index", "status": "pending", "items": evidence},
        ],
        "evidence_required": evidence,
        "evidence_collected": [],
    }


def _persist(record: Dict[str, Any]) -> None:
    table = dynamodb.Table(DISPUTE_TABLE)
    table.put_item(Item=record, ConditionExpression="attribute_not_exists(dispute_id)")


def _enqueue_evidence_collection(packet: Dict[str, Any]) -> None:
    if not EVIDENCE_QUEUE_URL:
        return
    try:
        sqs.send_message(
            QueueUrl=EVIDENCE_QUEUE_URL,
            MessageBody=json.dumps(packet, default=str),
            MessageAttributes={"category": {"DataType": "String", "StringValue": packet["category"]}},
        )
    except ClientError as exc:
        logger.warning("evidence_enqueue_failed dispute=%s error=%s", packet["dispute_id"], exc)


def lambda_handler(event, context):
    try:
        validate_payload_size(event)
    except ValueError:
        return {"statusCode": 413, "body": json.dumps({"error": "Payload too large"})}

    validation_error = validate_api_gateway_event(event)
    if validation_error:
        return validation_error

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": json.dumps({"error": "Insufficient execution time"})}

    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    query = event.get("queryStringParameters") or {}

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    reason_code = str(body.get("reason_code", "")).strip()
    rule = REASON_CODES.get(reason_code)
    if not rule:
        return _response(422, {"error": "unknown_reason_code", "reason_code": reason_code})

    for required in ("capture_id", "merchant_id", "disputed_amount"):
        if not body.get(required):
            return _response(400, {"error": "missing_field", "field": required})

    try:
        disputed_amount = _money(body["disputed_amount"])
    except (InvalidOperation, ValueError):
        return _response(400, {"error": "invalid_disputed_amount"})
    if disputed_amount <= 0:
        return _response(400, {"error": "invalid_disputed_amount"})

    try:
        notified_at = _parse_notified_at(body.get("notified_at"))
    except ValueError:
        return _response(400, {"error": "invalid_notified_at"})

    network_deadline, internal_target = _deadlines(notified_at, int(rule["window_days"]))
    evidence = _required_evidence(str(rule["category"]), disputed_amount)

    dispute_id = body.get("dispute_id") or "dsp_%s" % uuid.uuid4().hex[:20]
    packet = _representment_packet(dispute_id, body, rule, evidence)

    record = {
        "dispute_id": dispute_id,
        "capture_id": body["capture_id"],
        "merchant_id": body["merchant_id"],
        "network": rule["network"],
        "reason_code": reason_code,
        "category": rule["category"],
        "liability_posture": rule["liability"],
        "disputed_amount": disputed_amount,
        "currency": str(body.get("currency", "USD")).upper(),
        "state": "evidence_collection",
        "accept_liability": bool(query.get("accept_liability") == "true"),
        "notified_at": notified_at.isoformat(),
        "network_deadline": network_deadline.isoformat(),
        "internal_target": internal_target.isoformat(),
        "evidence_required": evidence,
        "submitted_by": headers.get("x-operator-id", "api"),
        "representment_packet": packet,
    }

    try:
        _persist(record)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("dispute_already_exists dispute=%s", dispute_id)
            return _response(409, {"error": "dispute_exists", "dispute_id": dispute_id})
        logger.exception("dispute_persist_failed dispute=%s", dispute_id)
        return _response(503, {"error": "persistence_unavailable"})

    _enqueue_evidence_collection(packet)
    logger.info(
        "dispute_accepted dispute=%s network=%s reason=%s amount=%s deadline=%s",
        dispute_id,
        rule["network"],
        reason_code,
        disputed_amount,
        network_deadline.isoformat(),
    )
    return _response(201, {
        "dispute_id": dispute_id,
        "state": record["state"],
        "network_deadline": record["network_deadline"],
        "internal_target": record["internal_target"],
        "evidence_required": evidence,
    })
