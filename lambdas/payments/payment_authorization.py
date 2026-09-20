"""Card authorization handler.

Event source: API Gateway REST API, ``POST /payments/authorizations``.

Submits an authorization request to the payment service provider (PSP), interprets
the AVS/CVV response codes, retries soft declines with exponential backoff, and
records the authorization under a caller-supplied idempotency key so a replayed
request returns the original decision instead of double-authorizing the card.
"""

import hashlib
import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

AUTH_TABLE = os.environ.get("AUTH_TABLE", "payment-authorizations")
PSP_FUNCTION = os.environ.get("PSP_ADAPTER_FUNCTION", "psp-adapter")

SOFT_DECLINE_CODES = {"51", "61", "65", "75", "91", "96"}
HARD_DECLINE_CODES = {"04", "07", "14", "41", "43", "54", "57", "62"}
AVS_FULL_MATCH = {"Y", "X", "D", "M"}
AVS_PARTIAL_MATCH = {"A", "B", "P", "W", "Z"}
CVV_MATCH = {"M"}

CURRENCY_EXPONENTS = {"USD": 2, "EUR": 2, "GBP": 2, "JPY": 0, "KWD": 3, "BHD": 3}


def _quantum(currency: str) -> Decimal:
    exponent = CURRENCY_EXPONENTS.get(currency.upper(), 2)
    return Decimal(1).scaleb(-exponent)


def _normalize_amount(raw_amount: Any, currency: str) -> Decimal:
    amount = Decimal(str(raw_amount))
    if amount <= 0:
        raise ValueError("amount must be positive")
    return amount.quantize(_quantum(currency), rounding=ROUND_HALF_UP)


def _idempotency_key(body: Dict[str, Any], headers: Dict[str, str]) -> str:
    supplied = headers.get("idempotency-key") or headers.get("Idempotency-Key")
    if supplied:
        return supplied
    fingerprint = json.dumps({
        "merchant": body.get("merchant_id"),
        "amount": str(body.get("amount")),
        "currency": body.get("currency"),
        "instrument": body.get("instrument_token"),
        "order": body.get("order_reference"),
    }, sort_keys=True)
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()


def _lower_headers(event: Dict[str, Any]) -> Dict[str, str]:
    return {name.lower(): value for name, value in (event.get("headers") or {}).items()}


def _score_verification(avs_code: str, cvv_code: str) -> int:
    """Return a 0-100 confidence score for the issuer verification result."""
    score = 40
    if avs_code in AVS_FULL_MATCH:
        score += 35
    elif avs_code in AVS_PARTIAL_MATCH:
        score += 15
    if cvv_code in CVV_MATCH:
        score += 25
    elif cvv_code in {"N", "P"}:
        score -= 20
    return max(0, min(100, score))


def _invoke_psp(payload: Dict[str, Any]) -> Dict[str, Any]:
    response = lambda_client.invoke(
        FunctionName=PSP_FUNCTION,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    raw = response["Payload"].read()
    return json.loads(raw.decode("utf-8"))


def _authorize_with_retry(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    """Submit to the PSP, retrying soft declines and transient adapter errors."""
    attempt = 0
    last_result: Dict[str, Any] = {}
    while True:
        try:
            last_result = _invoke_psp(payload)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            logger.warning("psp_invoke_failed attempt=%s error=%s", attempt, code)
            last_result = {"status": "error", "response_code": "96"}

        code = str(last_result.get("response_code", ""))
        if last_result.get("status") == "approved":
            return last_result, attempt
        if code in HARD_DECLINE_CODES:
            return last_result, attempt
        if code not in SOFT_DECLINE_CODES:
            return last_result, attempt

        delay = 0.25 * (2 ** attempt)
        logger.info("soft_decline_retry attempt=%s code=%s delay=%.2f", attempt, code, delay)
        time.sleep(delay)
        attempt += 1


def _load_existing(table, idempotency_key: str) -> Optional[Dict[str, Any]]:
    try:
        response = table.get_item(Key={"idempotency_key": idempotency_key})
    except ClientError as exc:
        logger.error("idempotency_lookup_failed error=%s", exc)
        return None
    return response.get("Item")


def _persist(table, record: Dict[str, Any]) -> None:
    try:
        table.put_item(Item=record, ConditionExpression="attribute_not_exists(idempotency_key)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        logger.info("authorization_already_recorded key=%s", record["idempotency_key"])


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def lambda_handler(event, context):
    headers = _lower_headers(event)
    query = event.get("queryStringParameters") or {}
    trace_id = headers.get("x-correlation-id", "unknown")

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    for required in ("merchant_id", "amount", "currency", "instrument_token"):
        if not body.get(required):
            return _response(400, {"error": "missing_field", "field": required})

    currency = str(body["currency"]).upper()
    try:
        amount = _normalize_amount(body["amount"], currency)
    except (ValueError, ArithmeticError) as exc:
        return _response(400, {"error": "invalid_amount", "detail": str(exc)})

    table = dynamodb.Table(AUTH_TABLE)
    key = _idempotency_key(body, headers)

    existing = _load_existing(table, key)
    if existing:
        logger.info("idempotent_replay key=%s trace=%s", key, trace_id)
        return _response(200, {"authorization": existing, "replayed": True})

    psp_payload = {
        "merchant_id": body["merchant_id"],
        "amount_minor": int(amount.scaleb(CURRENCY_EXPONENTS.get(currency, 2))),
        "currency": currency,
        "instrument_token": body["instrument_token"],
        "billing_postal_code": body.get("billing_postal_code"),
        "capture_mode": query.get("capture_mode", "manual"),
        "descriptor": body.get("descriptor", body["merchant_id"])[:22],
        "metadata": body.get("metadata", {}),
    }

    try:
        result, attempts = _authorize_with_retry(psp_payload)
    except Exception as exc:  # noqa: BLE001 - surfaced as a 502 to the caller
        logger.exception("authorization_pipeline_failed trace=%s", trace_id)
        return _response(502, {"error": "psp_unavailable", "detail": str(exc)})

    verification = _score_verification(
        str(result.get("avs_result", "U")), str(result.get("cvv_result", "U"))
    )
    approved = result.get("status") == "approved" and verification >= 55

    record = {
        "idempotency_key": key,
        "merchant_id": body["merchant_id"],
        "amount": amount,
        "currency": currency,
        "status": "approved" if approved else "declined",
        "response_code": str(result.get("response_code", "96")),
        "authorization_code": result.get("authorization_code"),
        "verification_score": verification,
        "retry_attempts": attempts,
        "created_at": int(time.time()),
        "expires_at": int(time.time()) + 604800,
    }

    _persist(table, record)
    logger.info(
        "authorization_complete key=%s status=%s attempts=%s score=%s",
        key, record["status"], attempts, verification,
    )
    return _response(201 if approved else 402, {"authorization": record, "replayed": False})
