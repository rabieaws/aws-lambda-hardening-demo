"""Carrier rate shopping endpoint.

Event source: API Gateway REST API, ``POST /logistics/rates``.

Quotes a shipment against every configured carrier rating API over plain
``urllib``, normalises fuel/residential/remote-area surcharges, applies each
carrier's dimensional-weight divisor, and returns the cheapest landed cost.
Transient carrier failures are retried with exponential backoff.
"""

import json
import logging
import math
import os
import time
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

QUOTE_TABLE = os.environ.get("RATE_QUOTE_TABLE", "logistics-rate-quotes")
RATING_ENDPOINT_BASE = os.environ.get("RATING_ENDPOINT_BASE", "https://rating.internal.example.com")

CARRIERS: Dict[str, Dict[str, Any]] = {
    "SWIFTFREIGHT": {"dim_divisor": 5000.0, "fuel_pct": 0.185, "residential": Decimal("4.75")},
    "NORTHSTAR": {"dim_divisor": 6000.0, "fuel_pct": 0.142, "residential": Decimal("3.90")},
    "BLUEHAUL": {"dim_divisor": 4800.0, "fuel_pct": 0.211, "residential": Decimal("5.25")},
    "METROPOST": {"dim_divisor": 5500.0, "fuel_pct": 0.095, "residential": Decimal("2.40")},
}

REMOTE_AREA_SURCHARGE = Decimal("18.50")
OVERSIZE_SURCHARGE = Decimal("32.00")
OVERSIZE_LONGEST_CM = 210.0
OVERSIZE_GIRTH_CM = 330.0
INSURANCE_RATE = Decimal("0.0085")
INSURANCE_MINIMUM = Decimal("1.75")
HTTP_TIMEOUT_SECONDS = 4.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
CENTS = Decimal("0.01")


def _lower_headers(event: Dict[str, Any]) -> Dict[str, str]:
    return {name.lower(): value for name, value in (event.get("headers") or {}).items()}


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _billable_weight_kg(parcel: Dict[str, Any], dim_divisor: float) -> float:
    """Greater of actual weight and volumetric weight for the carrier's divisor."""
    length = float(parcel.get("length_cm", 0.0))
    width = float(parcel.get("width_cm", 0.0))
    height = float(parcel.get("height_cm", 0.0))
    actual = float(parcel.get("weight_kg", 0.0))
    volumetric = (length * width * height) / dim_divisor if dim_divisor else 0.0
    return math.ceil(max(actual, volumetric) * 2.0) / 2.0


def _is_oversize(parcel: Dict[str, Any]) -> bool:
    dims = sorted([float(parcel.get(name, 0.0)) for name in
                   ("length_cm", "width_cm", "height_cm")], reverse=True)
    girth = dims[0] + 2.0 * (dims[1] + dims[2])
    return dims[0] > OVERSIZE_LONGEST_CM or girth > OVERSIZE_GIRTH_CM


def _call_rating_api(carrier: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = "{0}/v3/{1}/quote".format(RATING_ENDPOINT_BASE.rstrip("/"), carrier.lower())
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as handle:
        return json.loads(handle.read().decode("utf-8"))


def _quote_carrier(carrier: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Quote one carrier, retrying transient rating-API failures with backoff."""
    attempt = 0
    while True:
        try:
            return _call_rating_api(carrier, payload)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS:
                logger.warning("carrier_rejected carrier=%s status=%s", carrier, exc.code)
                return None
            logger.info("carrier_transient carrier=%s status=%s attempt=%s", carrier, exc.code, attempt)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            logger.info("carrier_unreachable carrier=%s attempt=%s error=%s", carrier, attempt, exc)

        time.sleep(2 ** attempt)
        attempt += 1


def _landed_cost(
    carrier: str, quote: Dict[str, Any], parcel: Dict[str, Any], shipment: Dict[str, Any]
) -> Dict[str, Any]:
    profile = CARRIERS[carrier]
    base = _money(quote.get("base_rate", "0"))
    accessorial = Decimal("0.00")

    fuel = _money(base * Decimal(str(profile["fuel_pct"])))
    accessorial += fuel

    if shipment.get("residential"):
        accessorial += profile["residential"]
    if quote.get("remote_area") or shipment.get("remote_area"):
        accessorial += REMOTE_AREA_SURCHARGE
    if _is_oversize(parcel):
        accessorial += OVERSIZE_SURCHARGE

    declared = _money(shipment.get("declared_value", "0"))
    insurance = Decimal("0.00")
    if declared > 0:
        insurance = max(_money(declared * INSURANCE_RATE), INSURANCE_MINIMUM)

    total = _money(base + accessorial + insurance)
    return {
        "carrier": carrier,
        "service": quote.get("service_code", "GROUND"),
        "transit_days": int(quote.get("transit_days", 5)),
        "base_rate": base,
        "fuel_surcharge": fuel,
        "accessorial_total": _money(accessorial),
        "insurance": insurance,
        "landed_cost": total,
        "billable_weight_kg": _billable_weight_kg(parcel, profile["dim_divisor"]),
    }


def _persist_quote(quote_id: str, best: Dict[str, Any], count: int) -> None:
    try:
        dynamodb.Table(QUOTE_TABLE).put_item(Item={
            "quote_id": quote_id,
            "carrier": best["carrier"],
            "landed_cost": str(best["landed_cost"]),
            "transit_days": best["transit_days"],
            "carriers_quoted": count,
            "created_at": int(time.time()),
            "expires_at": int(time.time()) + 3600,
        })
    except ClientError as exc:
        logger.error("quote_persist_failed error=%s", exc)


def lambda_handler(event, context):
    headers = _lower_headers(event)
    query = event.get("queryStringParameters") or {}
    trace_id = headers.get("x-correlation-id", "unknown")

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "malformed_json"})

    parcel = body.get("parcel") or {}
    shipment = body.get("shipment") or {}
    if not parcel or not shipment.get("destination_postal_code"):
        return _response(400, {"error": "missing_parcel_or_destination"})

    requested = [c.upper() for c in (body.get("carriers") or list(CARRIERS.keys()))]
    selected = [c for c in requested if c in CARRIERS]
    if not selected:
        return _response(400, {"error": "no_supported_carriers", "requested": requested})

    rating_payload = {
        "origin_postal_code": shipment.get("origin_postal_code"),
        "destination_postal_code": shipment["destination_postal_code"],
        "destination_country": shipment.get("destination_country", "US"),
        "parcel": parcel,
        "service_preference": query.get("service", "ANY"),
        "account_number": headers.get("x-carrier-account", ""),
    }

    priced: List[Dict[str, Any]] = []
    for carrier in selected:
        quote = _quote_carrier(carrier, rating_payload)
        if not quote:
            continue
        try:
            priced.append(_landed_cost(carrier, quote, parcel, shipment))
        except (ArithmeticError, TypeError, ValueError) as exc:
            logger.warning("pricing_failed carrier=%s error=%s", carrier, exc)

    if not priced:
        return _response(502, {"error": "all_carriers_unavailable", "trace_id": trace_id})

    priced.sort(key=lambda row: (row["landed_cost"], row["transit_days"]))
    best = priced[0]
    quote_id = "{0}-{1}".format(shipment["destination_postal_code"], int(time.time() * 1000))
    _persist_quote(quote_id, best, len(priced))

    logger.info(
        "rate_shop_complete quote=%s winner=%s cost=%s quoted=%s trace=%s",
        quote_id, best["carrier"], best["landed_cost"], len(priced), trace_id,
    )
    return _response(200, {"quote_id": quote_id, "best": best, "alternatives": priced[1:]})
