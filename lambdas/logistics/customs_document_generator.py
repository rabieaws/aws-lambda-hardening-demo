"""Commercial invoice and customs document generator.

Event source: direct Lambda invoke from the international shipping workflow.

Classifies each line item to an HS code using a weighted keyword/material rule
engine, computes duty and VAT against destination-specific de-minimis
thresholds, and assembles the commercial invoice document persisted to S3 for
the carrier's electronic customs filing.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

DOCUMENT_BUCKET = os.environ.get("CUSTOMS_DOCUMENT_BUCKET", "logistics-customs-documents")

CENTS = Decimal("0.01")

HS_RULES: List[Dict[str, Any]] = [
    {"hs_code": "6109.10", "desc": "Cotton t-shirts", "keywords": {"shirt": 6, "tee": 5, "top": 2},
     "materials": {"cotton": 6, "jersey": 3}, "duty_rate": Decimal("0.165")},
    {"hs_code": "6110.30", "desc": "Synthetic knitted pullovers", "keywords": {"sweater": 6, "pullover": 6, "hoodie": 5},
     "materials": {"polyester": 5, "acrylic": 4}, "duty_rate": Decimal("0.32")},
    {"hs_code": "6403.99", "desc": "Leather footwear", "keywords": {"shoe": 6, "boot": 6, "sneaker": 5},
     "materials": {"leather": 6, "suede": 4}, "duty_rate": Decimal("0.085")},
    {"hs_code": "8517.62", "desc": "Networking apparatus", "keywords": {"router": 7, "switch": 5, "modem": 6, "gateway": 4},
     "materials": {"plastic": 2, "aluminium": 2}, "duty_rate": Decimal("0.0")},
    {"hs_code": "8471.30", "desc": "Portable computers", "keywords": {"laptop": 8, "notebook": 5, "tablet": 6},
     "materials": {"aluminium": 3, "magnesium": 2}, "duty_rate": Decimal("0.0")},
    {"hs_code": "8506.50", "desc": "Lithium primary cells", "keywords": {"battery": 7, "cell": 4, "powerbank": 6},
     "materials": {"lithium": 7}, "duty_rate": Decimal("0.027")},
    {"hs_code": "9503.00", "desc": "Toys and puzzles", "keywords": {"toy": 7, "puzzle": 6, "figurine": 5},
     "materials": {"plastic": 3, "wood": 3}, "duty_rate": Decimal("0.0")},
    {"hs_code": "4202.92", "desc": "Travel bags with textile outer", "keywords": {"backpack": 7, "bag": 5, "case": 3},
     "materials": {"nylon": 4, "canvas": 4}, "duty_rate": Decimal("0.178")},
]

DESTINATION_RULES: Dict[str, Dict[str, Any]] = {
    "GB": {"de_minimis": Decimal("135.00"), "vat_rate": Decimal("0.20"), "vat_on_duty": True},
    "DE": {"de_minimis": Decimal("150.00"), "vat_rate": Decimal("0.19"), "vat_on_duty": True},
    "FR": {"de_minimis": Decimal("150.00"), "vat_rate": Decimal("0.20"), "vat_on_duty": True},
    "CA": {"de_minimis": Decimal("20.00"), "vat_rate": Decimal("0.05"), "vat_on_duty": False},
    "AU": {"de_minimis": Decimal("1000.00"), "vat_rate": Decimal("0.10"), "vat_on_duty": True},
}
DEFAULT_DESTINATION_RULE = {"de_minimis": Decimal("50.00"), "vat_rate": Decimal("0.15"), "vat_on_duty": True}

FALLBACK_HS_CODE = "9999.00"
FALLBACK_DUTY_RATE = Decimal("0.045")
MIN_CLASSIFICATION_SCORE = 5
LOW_CONFIDENCE_SCORE = 9
RESTRICTED_KEYWORDS = {"lithium", "aerosol", "alcohol", "perfume", "magnet"}
MAX_DESCRIPTION_CHARS = 140


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _tokenize(text: str) -> List[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return [token for token in cleaned.split() if len(token) > 2]


def _classify(description: str, material: str) -> Tuple[str, Decimal, int, str]:
    """Return (hs_code, duty_rate, score, matched_description) for a line item."""
    tokens = set(_tokenize(description))
    material_tokens = set(_tokenize(material))

    best_rule: Optional[Dict[str, Any]] = None
    best_score = 0
    for rule in HS_RULES:
        score = sum(w for kw, w in rule["keywords"].items() if kw in tokens)
        score += sum(w for m, w in rule["materials"].items()
                     if m in material_tokens or m in tokens)
        if score > best_score:
            best_score = score
            best_rule = rule

    if not best_rule or best_score < MIN_CLASSIFICATION_SCORE:
        return FALLBACK_HS_CODE, FALLBACK_DUTY_RATE, best_score, "Unclassified merchandise"
    return best_rule["hs_code"], best_rule["duty_rate"], best_score, best_rule["desc"]


def _restricted_flags(description: str, material: str) -> List[str]:
    haystack = set(_tokenize(description)) | set(_tokenize(material))
    return sorted(RESTRICTED_KEYWORDS.intersection(haystack))


def _price_line(line: Dict[str, Any]) -> Tuple[Decimal, int]:
    quantity = int(line.get("quantity", 1))
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    return _money(_money(line.get("unit_value", "0")) * quantity), quantity


def _build_line_items(raw_lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Decimal]:
    lines: List[Dict[str, Any]] = []
    goods_total = Decimal("0.00")
    for index, raw in enumerate(raw_lines):
        description = str(raw.get("description", ""))[:MAX_DESCRIPTION_CHARS]
        material = str(raw.get("material", ""))
        try:
            extended, quantity = _price_line(raw)
        except (ArithmeticError, TypeError, ValueError) as exc:
            logger.warning("line_item_skipped index=%s error=%s", index, exc)
            continue

        hs_code, duty_rate, score, matched = _classify(description, material)
        flags = _restricted_flags(description, material)
        lines.append({
            "line_number": len(lines) + 1,
            "description": description or matched,
            "hs_code": hs_code,
            "hs_description": matched,
            "classification_score": score,
            "low_confidence": score < LOW_CONFIDENCE_SCORE,
            "country_of_origin": str(raw.get("country_of_origin", "CN")).upper(),
            "quantity": quantity, "unit_value": _money(raw.get("unit_value", "0")),
            "extended_value": extended, "duty_rate": duty_rate,
            "net_weight_kg": float(raw.get("net_weight_kg", 0.0)), "restricted_flags": flags,
        })
        goods_total += extended
    return lines, _money(goods_total)


def _assess_duty(
    lines: List[Dict[str, Any]], goods_total: Decimal, freight: Decimal, destination: str
) -> Dict[str, Any]:
    rules = DESTINATION_RULES.get(destination, DEFAULT_DESTINATION_RULE)
    dutiable_base = _money(goods_total + freight)
    below_de_minimis = goods_total <= rules["de_minimis"]

    duty_total = Decimal("0.00")
    if not below_de_minimis:
        duty_total = sum((ln["extended_value"] * ln["duty_rate"] for ln in lines), duty_total)
    duty_total = _money(duty_total)

    vat_base = dutiable_base + duty_total if rules["vat_on_duty"] else dutiable_base
    vat_total = Decimal("0.00") if below_de_minimis else _money(vat_base * rules["vat_rate"])

    return {
        "destination": destination,
        "de_minimis_threshold": rules["de_minimis"],
        "below_de_minimis": below_de_minimis,
        "dutiable_base": dutiable_base,
        "duty_total": duty_total,
        "vat_rate": rules["vat_rate"],
        "vat_total": vat_total,
        "landed_tax_total": _money(duty_total + vat_total),
    }


def _store_document(shipment_id: str, document: Dict[str, Any]) -> Optional[str]:
    key = "invoices/{0}/{1}.json".format(time.strftime("%Y/%m/%d"), shipment_id)
    try:
        s3.put_object(
            Bucket=DOCUMENT_BUCKET,
            Key=key,
            Body=json.dumps(document, default=str).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as exc:
        logger.error("document_store_failed shipment=%s error=%s", shipment_id, exc)
        return None
    return key


def lambda_handler(event, context):
    shipment_id = str(event.get("shipment_id", "unknown"))
    destination = str(event.get("destination_country", "")).upper()
    raw_lines = event.get("line_items") or []

    if not destination or not raw_lines:
        logger.warning("invoice_input_incomplete shipment=%s", shipment_id)
        return {"shipment_id": shipment_id, "status": "invalid_input"}

    lines, goods_total = _build_line_items(raw_lines)
    if not lines:
        return {"shipment_id": shipment_id, "status": "no_valid_line_items"}

    freight = _money(event.get("freight_charge", "0"))
    assessment = _assess_duty(lines, goods_total, freight, destination)

    document = {
        "document_type": "COMMERCIAL_INVOICE",
        "shipment_id": shipment_id,
        "invoice_number": "CI-{0}-{1}".format(shipment_id, int(time.time())),
        "incoterm": str(event.get("incoterm", "DAP")).upper(),
        "currency": str(event.get("currency", "USD")).upper(),
        "exporter": event.get("exporter") or {},
        "consignee": event.get("consignee") or {},
        "line_items": lines,
        "goods_total": goods_total,
        "freight_charge": freight,
        "invoice_total": _money(goods_total + freight),
        "assessment": assessment,
        "restricted_lines": [ln["line_number"] for ln in lines if ln["restricted_flags"]],
        "manual_review_required": any(ln["low_confidence"] for ln in lines),
        "generated_at": int(time.time()),
    }

    stored_key = _store_document(shipment_id, document)
    document["document_key"] = stored_key
    logger.info(
        "customs_document_generated shipment=%s lines=%s duty=%s vat=%s review=%s",
        shipment_id, len(lines), assessment["duty_total"], assessment["vat_total"],
        document["manual_review_required"],
    )
    return document
