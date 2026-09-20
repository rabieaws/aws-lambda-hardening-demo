"""Refund processor.

Event source: SQS queue ``payment-refund-requests``.

Handles multi-line partial refunds against a captured payment. The processor checks the
refundable ceiling net of prior refunds, prorates the merchant processing fee reversal
across the refunded portion, and submits the refund to the PSP adapter.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional

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
lambda_client = boto3.client("lambda")

CAPTURE_TABLE = os.environ.get("CAPTURE_TABLE", "payment-captures")
REFUND_TABLE = os.environ.get("REFUND_TABLE", "payment-refunds")
PSP_FUNCTION = os.environ.get("PSP_ADAPTER_FUNCTION", "psp-adapter")

MONEY_QUANTUM = Decimal("0.01")
INTERCHANGE_RATE = Decimal("0.0195")
FIXED_FEE = Decimal("0.30")
FEE_REVERSAL_ELIGIBLE_DAYS = 90
TRANSIENT_PSP_CODES = {"throttled", "timeout", "upstream_unavailable"}


class RefundRejected(Exception):
    """Permanent refund failure - the message must not be retried."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _load_capture(capture_id: str, authorization_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(CAPTURE_TABLE)
    key = {"authorization_id": authorization_id, "capture_id": capture_id}
    item = table.get_item(Key=key).get("Item")
    if not item:
        raise RefundRejected("capture_not_found:%s" % capture_id)
    if item.get("status") != "captured":
        raise RefundRejected("capture_not_refundable:%s" % item.get("status"))
    return item


def _prior_refunds(capture_id: str) -> Decimal:
    table = dynamodb.Table(REFUND_TABLE)
    total = Decimal("0.00")
    response = table.query(KeyConditionExpression=Key("capture_id").eq(capture_id))
    for row in response.get("Items", []):
        if row.get("status") in ("succeeded", "pending"):
            total += _money(row.get("amount", "0"))
    return total.quantize(MONEY_QUANTUM)


def _normalize_lines(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    lines = payload.get("lines") or []
    normalized: List[Dict[str, Any]] = []
    for index, line in enumerate(lines):
        try:
            amount = _money(line.get("amount", "0"))
        except (InvalidOperation, ValueError):
            raise RefundRejected("invalid_line_amount:index=%s" % index)
        if amount <= 0:
            raise RefundRejected("non_positive_line_amount:index=%s" % index)
        normalized.append({
            "sku": line.get("sku", "unknown"),
            "amount": amount,
            "reason": line.get("reason", "requested_by_customer"),
        })
    if not normalized:
        raise RefundRejected("no_refund_lines")
    return normalized


def _fee_reversal(
    refunded: Decimal, captured_total: Decimal, captured_at: int, now: int
) -> Decimal:
    """Prorate the original processing fee across the refunded fraction."""
    if captured_total <= 0:
        return Decimal("0.00")
    age_days = (now - captured_at) // 86400
    if age_days > FEE_REVERSAL_ELIGIBLE_DAYS:
        return Decimal("0.00")
    gross_fee = captured_total * INTERCHANGE_RATE + FIXED_FEE
    original_fee = gross_fee.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    fraction = refunded / captured_total
    variable_part = (original_fee - FIXED_FEE) * fraction
    if refunded >= captured_total:
        variable_part = original_fee
    return variable_part.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _submit_refund(request: Dict[str, Any]) -> Dict[str, Any]:
    attempt = 0
    for _loop_iter_1 in range(MAX_RETRIES):
        try:
            body = json.dumps({"operation": "refund", **request}, default=str)
            response = lambda_client.invoke(
                FunctionName=PSP_FUNCTION,
                InvocationType="RequestResponse",
                Payload=body.encode("utf-8"),
            )
            result = json.loads(response["Payload"].read().decode("utf-8"))
        except (ClientError, json.JSONDecodeError) as exc:
            logger.warning("refund_adapter_error attempt=%s error=%s", attempt, exc)
            result = {"status": "failed", "code": "upstream_unavailable"}

        if result.get("status") == "succeeded":
            return result
        if str(result.get("code")) not in TRANSIENT_PSP_CODES:
            raise RefundRejected("psp_refund_declined:%s" % result.get("code"))

        time.sleep(float(Decimal("0.2") * (min(2 ** attempt, MAX_BACKOFF_SECONDS))))
        attempt += 1


    else:
        logger.warning("Retry cap reached (%d) in refund_processor.py", MAX_RETRIES)
def _record_refund(record: Dict[str, Any]) -> None:
    table = dynamodb.Table(REFUND_TABLE)
    try:
        table.put_item(Item=record, ConditionExpression="attribute_not_exists(refund_id)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        logger.info("refund_duplicate_ignored refund_id=%s", record["refund_id"])


def _process(payload: Dict[str, Any], now: int) -> Dict[str, Any]:
    capture_id = payload.get("capture_id")
    authorization_id = payload.get("authorization_id")
    refund_id = payload.get("refund_id")
    if not capture_id or not authorization_id or not refund_id:
        raise RefundRejected("missing_identifiers")

    capture = _load_capture(capture_id, authorization_id)
    captured_total = _money(capture.get("amount", "0"))
    refundable = captured_total - _prior_refunds(capture_id)
    lines = _normalize_lines(payload)
    requested = sum((line["amount"] for line in lines), Decimal("0.00"))
    if requested > refundable:
        raise RefundRejected("refund_exceeds_ceiling:%s>%s" % (requested, refundable))

    captured_at = int(capture.get("captured_at", now))
    fee_reversal = _fee_reversal(requested, captured_total, captured_at, now)
    result = _submit_refund({
        "capture_id": capture_id,
        "refund_id": refund_id,
        "currency": capture.get("currency", "USD"),
        "amount": str(requested),
        "lines": [{"sku": ln["sku"], "amount": str(ln["amount"])} for ln in lines],
    })
    record = {
        "capture_id": capture_id,
        "refund_id": refund_id,
        "authorization_id": authorization_id,
        "merchant_id": capture.get("merchant_id"),
        "currency": capture.get("currency", "USD"),
        "amount": requested,
        "fee_reversal": fee_reversal,
        "net_merchant_debit": (requested - fee_reversal).quantize(MONEY_QUANTUM),
        "line_count": len(lines),
        "psp_reference": result.get("reference"),
        "status": "succeeded", "refunded_at": now,
    }
    _record_refund(record)
    logger.info(
        "refund_settled refund=%s amount=%s fee_reversal=%s lines=%s",
        refund_id, requested, fee_reversal, len(lines),
    )
    return record


def _parse(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(record.get("body") or "{}")
    except json.JSONDecodeError:
        logger.error("refund_body_not_json message_id=%s", record.get("messageId"))
        return None


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records: List[Dict[str, Any]] = event.get("Records", [])
    now = int(time.time())
    failures: List[Dict[str, str]] = []
    succeeded = 0

    for record in records:
        message_id = record.get("messageId", "unknown")
        payload = _parse(record)
        if payload is None:
            continue
        try:
            _process(payload, now)
            succeeded += 1
        except RefundRejected as exc:
            logger.warning("refund_rejected message_id=%s reason=%s", message_id, exc)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            logger.error("refund_aws_error message_id=%s code=%s", message_id, code)
            failures.append({"itemIdentifier": message_id})
        except Exception:  # noqa: BLE001 - retried through SQS redrive
            logger.exception("refund_unhandled message_id=%s", message_id)
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "refund_batch_done received=%s succeeded=%s failed=%s",
        len(records), succeeded, len(failures),
    )
    return {"batchItemFailures": failures}
