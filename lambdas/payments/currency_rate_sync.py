"""FX rate synchronisation job.

Event source: EventBridge scheduled rule ``payments-fx-rate-sync`` (every 15 minutes).

Pulls the latest mid-market rates from the treasury FX provider's paginated REST API,
derives missing cross rates through the USD base pair, rejects quotes that fall
outside the sanity band relative to the previously stored rate, and upserts the
accepted rates into DynamoDB with a provider quote timestamp.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, DivisionByZero, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.client("dynamodb")
ssm = boto3.client("ssm")

RATE_TABLE = os.environ.get("FX_RATE_TABLE", "fx-rates")
PROVIDER_BASE_URL = os.environ.get("FX_PROVIDER_URL", "https://fx.internal.example.com/v2/rates")
PROVIDER_KEY_PARAM = os.environ.get("FX_PROVIDER_KEY_PARAM", "/payments/fx/api-key")

RATE_QUANTUM = Decimal("0.00000001")
BASE_CURRENCY = "USD"
SANITY_BAND = Decimal("0.15")
STALE_QUOTE_SECONDS = 3600
HTTP_TIMEOUT_SECONDS = 8
TRANSIENT_STATUS = {429, 500, 502, 503, 504}
REQUIRED_PAIRS = [("EUR", "GBP"), ("EUR", "CHF"), ("GBP", "JPY"), ("CAD", "MXN"), ("AUD", "NZD")]


def _rate(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(RATE_QUANTUM, rounding=ROUND_HALF_UP)


def _provider_key() -> str:
    try:
        response = ssm.get_parameter(Name=PROVIDER_KEY_PARAM, WithDecryption=True)
        return response["Parameter"]["Value"]
    except ClientError as exc:
        logger.error("fx_key_lookup_failed param=%s error=%s", PROVIDER_KEY_PARAM, exc)
        raise


def _http_get(url: str, api_key: str) -> Dict[str, Any]:
    """GET a provider page, retrying transient responses with exponential backoff."""
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        request = urllib.request.Request(url, method="GET")
        request.add_header("Authorization", "Bearer %s" % api_key)
        request.add_header("Accept", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                payload = response.read().decode("utf-8")
            return json.loads(payload)
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_STATUS:
                raise
            logger.warning("fx_http_transient status=%s attempt=%s", exc.code, attempt)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            logger.warning("fx_http_error attempt=%s error=%s", attempt, exc)

        time.sleep(0.5 * (2 ** attempt))
        attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in currency_rate_sync.py", MAX_LOOP_ITERATIONS)
def _fetch_all_pages(api_key: str) -> List[Dict[str, Any]]:
    quotes: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    for _loop_iter_2 in range(MAX_LOOP_ITERATIONS):
        params = {"base": BASE_CURRENCY, "page_size": "250"}
        if next_token:
            params["page_token"] = next_token
        url = "%s?%s" % (PROVIDER_BASE_URL, urllib.parse.urlencode(params))

        page = _http_get(url, api_key)
        quotes.extend(page.get("quotes", []))
        next_token = page.get("next_page_token")
        if not next_token:
            break

    else:
        logger.warning("Loop iteration cap reached (%d) in currency_rate_sync.py", MAX_LOOP_ITERATIONS)
    return quotes


def _index_quotes(quotes: List[Dict[str, Any]], now: int) -> Dict[str, Decimal]:
    indexed: Dict[str, Decimal] = {}
    for quote in quotes:
        currency = str(quote.get("quote_currency", "")).upper()
        if not currency or currency == BASE_CURRENCY:
            continue
        quoted_at = int(quote.get("quoted_at", now))
        if now - quoted_at > STALE_QUOTE_SECONDS:
            logger.info("fx_quote_stale currency=%s age=%s", currency, now - quoted_at)
            continue
        try:
            rate = _rate(quote.get("mid"))
        except (InvalidOperation, TypeError):
            logger.warning("fx_quote_unparseable currency=%s", currency)
            continue
        if rate <= 0:
            continue
        indexed[currency] = rate
    return indexed


def _derive_cross_rates(base_rates: Dict[str, Decimal]) -> Dict[Tuple[str, str], Decimal]:
    derived: Dict[Tuple[str, str], Decimal] = {}
    for left, right in REQUIRED_PAIRS:
        left_rate = base_rates.get(left)
        right_rate = base_rates.get(right)
        if not left_rate or not right_rate:
            continue
        try:
            cross = right_rate / left_rate
            derived[(left, right)] = cross.quantize(RATE_QUANTUM, rounding=ROUND_HALF_UP)
        except (DivisionByZero, InvalidOperation):
            logger.warning("cross_rate_failed pair=%s/%s", left, right)
    return derived


def _previous_rate(pair: str) -> Optional[Decimal]:
    try:
        response = dynamodb.get_item(TableName=RATE_TABLE, Key={"pair": {"S": pair}})
    except ClientError as exc:
        logger.warning("previous_rate_lookup_failed pair=%s error=%s", pair, exc)
        return None
    item = response.get("Item")
    if not item:
        return None
    raw = item.get("rate", {}).get("N")
    return _rate(raw) if raw is not None else None


def _within_sanity_band(pair: str, candidate: Decimal) -> bool:
    previous = _previous_rate(pair)
    if previous is None or previous == 0:
        return True
    drift = ((candidate - previous) / previous).copy_abs()
    if drift > SANITY_BAND:
        logger.warning(
            "fx_rate_outside_band pair=%s previous=%s candidate=%s drift=%s",
            pair, previous, candidate, drift,
        )
        return False
    return True


def _upsert(pair: str, rate: Decimal, source: str, now: int) -> None:
    dynamodb.update_item(
        TableName=RATE_TABLE,
        Key={"pair": {"S": pair}},
        UpdateExpression="SET #rate = :rate, source = :source, synced_at = :now ADD revision :one",
        ExpressionAttributeNames={"#rate": "rate"},
        ExpressionAttributeValues={
            ":rate": {"N": str(rate)}, ":source": {"S": source},
            ":now": {"N": str(now)}, ":one": {"N": "1"},
        },
    )


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = int(time.time())

    try:
        api_key = _provider_key()
    except ClientError as exc:
        return {"status": "error", "reason": exc.response.get("Error", {}).get("Code")}

    try:
        quotes = _fetch_all_pages(api_key)
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        logger.exception("fx_provider_unavailable")
        return {"status": "error", "reason": "provider_unavailable", "detail": str(exc)}

    base_rates = _index_quotes(quotes, now)
    crosses = _derive_cross_rates(base_rates)

    candidates: List[Tuple[str, Decimal, str]] = [
        ("%s/%s" % (BASE_CURRENCY, currency), rate, "provider")
        for currency, rate in base_rates.items()
    ]
    candidates.extend(
        ("%s/%s" % (left, right), rate, "derived") for (left, right), rate in crosses.items()
    )

    written = 0
    rejected = 0
    for pair, rate, source in candidates:
        if not _within_sanity_band(pair, rate):
            rejected += 1
            continue
        try:
            _upsert(pair, rate, source, now)
            written += 1
        except ClientError as exc:
            logger.error("fx_upsert_failed pair=%s error=%s", pair, exc)

    logger.info(
        "fx_sync_done quotes=%s base_pairs=%s crosses=%s written=%s rejected=%s",
        len(quotes), len(base_rates), len(crosses), written, rejected,
    )
    return {
        "status": "complete",
        "quotes_received": len(quotes),
        "pairs_written": written,
        "pairs_rejected": rejected,
        "synced_at": now,
    }
