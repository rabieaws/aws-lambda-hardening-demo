"""Settlement file reconciliation.

Event source: S3 ``ObjectCreated:*`` notification on the settlement landing bucket.

Parses the acquirer settlement CSV, matches every settlement row against the internal
capture records using a DynamoDB query paginator, classifies unmatched rows as missing,
amount-mismatch or duplicate breaks, and writes a CSV exception report back into the
settlement bucket for the finance operations team.
"""

import csv
import io
import json
import logging
import os
import time
import urllib.parse
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    check_s3_recursive_invocation,
    MAX_PAGINATION_PAGES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

CAPTURE_TABLE = os.environ.get("CAPTURE_TABLE", "payment-captures")
CAPTURE_INDEX = os.environ.get("CAPTURE_PSP_INDEX", "psp_reference-index")
REPORT_PREFIX = os.environ.get("REPORT_PREFIX", "reconciliation/exceptions")

MONEY_QUANTUM = Decimal("0.01")
AMOUNT_TOLERANCE = Decimal("0.02")
REQUIRED_COLUMNS = ("psp_reference", "merchant_id", "gross_amount", "currency", "settled_at")
BREAK_MISSING = "MISSING_INTERNAL"
BREAK_AMOUNT = "AMOUNT_MISMATCH"
BREAK_DUPLICATE = "DUPLICATE_SETTLEMENT"


def _money(value: Any) -> Decimal:
    return Decimal(str(value).strip() or "0").quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _read_object(bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8-sig")


def _parse_settlement_csv(raw: str) -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(raw))
    if not reader.fieldnames:
        raise ValueError("settlement_file_empty")
    missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
    if missing:
        raise ValueError("settlement_columns_missing:%s" % ",".join(missing))

    rows: List[Dict[str, str]] = []
    for row in reader:
        reference = (row.get("psp_reference") or "").strip()
        if not reference:
            continue
        rows.append(row)
    return rows


def _query_internal(psp_reference: str) -> List[Dict[str, Any]]:
    paginator = dynamodb.get_paginator("query")
    pages = paginator.paginate(
        TableName=CAPTURE_TABLE,
        IndexName=CAPTURE_INDEX,
        KeyConditionExpression="psp_reference = :ref",
        ExpressionAttributeValues={":ref": {"S": psp_reference}},
    )
    items: List[Dict[str, Any]] = []
    for _page_num, page in enumerate(pages, 1):
        items.extend(page.get("Items", []))
        if _page_num >= MAX_PAGINATION_PAGES:
            break
    return items


def _internal_amount(item: Dict[str, Any]) -> Decimal:
    amount = item.get("amount", {})
    if "N" in amount:
        return _money(amount["N"])
    return Decimal("0.00")


def _classify(row: Dict[str, str], internal: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    settled_amount = _money(row.get("gross_amount", "0"))

    if not internal:
        return BREAK_MISSING, {"settled_amount": settled_amount, "internal_amount": None}

    settled_matches = [i for i in internal if i.get("status", {}).get("S") == "captured"]
    if len(settled_matches) > 1:
        return BREAK_DUPLICATE, {
            "settled_amount": settled_amount,
            "internal_amount": _internal_amount(settled_matches[0]),
            "match_count": len(settled_matches),
        }

    candidate = settled_matches[0] if settled_matches else internal[0]
    internal_amount = _internal_amount(candidate)
    delta = (settled_amount - internal_amount).copy_abs()
    if delta > AMOUNT_TOLERANCE:
        detail = {"settled_amount": settled_amount, "internal_amount": internal_amount}
        detail["delta"] = delta
        return BREAK_AMOUNT, detail
    return "MATCHED", {"settled_amount": settled_amount, "internal_amount": internal_amount}


def _reconcile(rows: Iterable[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    breaks: List[Dict[str, Any]] = []
    matched = 0
    settled_total = Decimal("0.00")
    internal_total = Decimal("0.00")

    for row in rows:
        reference = row["psp_reference"].strip()
        try:
            internal = _query_internal(reference)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            logger.error("internal_lookup_failed reference=%s code=%s", reference, code)
            continue

        try:
            classification, detail = _classify(row, internal)
        except (InvalidOperation, ValueError) as exc:
            logger.warning("row_unparseable reference=%s error=%s", reference, exc)
            continue

        settled_total += detail.get("settled_amount") or Decimal("0.00")
        internal_total += detail.get("internal_amount") or Decimal("0.00")

        if classification == "MATCHED":
            matched += 1
            continue

        breaks.append({
            "psp_reference": reference,
            "merchant_id": row.get("merchant_id", ""),
            "currency": row.get("currency", ""),
            "break_type": classification,
            "settled_amount": str(detail.get("settled_amount", "")),
            "internal_amount": str(detail.get("internal_amount", "")),
            "delta": str(detail.get("delta", "")),
            "settled_at": row.get("settled_at", ""),
        })

    summary = {
        "matched": matched, "breaks": len(breaks),
        "settled_total": str(settled_total.quantize(MONEY_QUANTUM)),
        "internal_total": str(internal_total.quantize(MONEY_QUANTUM)),
        "net_difference": str((settled_total - internal_total).quantize(MONEY_QUANTUM)),
    }
    return breaks, summary


def _write_report(bucket: str, source_key: str, breaks: List[Dict[str, Any]], summary: Dict[str, Any]) -> str:
    buffer = io.StringIO()
    fieldnames = [
        "psp_reference", "merchant_id", "currency", "break_type",
        "settled_amount", "internal_amount", "delta", "settled_at",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in breaks:
        writer.writerow(row)

    base = os.path.basename(source_key).rsplit(".", 1)[0]
    report_key = "%s/%s-exceptions-%d.csv" % (REPORT_PREFIX, base, int(time.time()))
    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
        Metadata={"source-key": source_key, "matched": str(summary["matched"])},
    )
    return report_key


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    results: List[Dict[str, Any]] = []

    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))
        if not bucket or not key:
            continue

        try:
            raw = _read_object(bucket, key)
            rows = _parse_settlement_csv(raw)
        except ClientError as exc:
            logger.error("settlement_read_failed bucket=%s key=%s error=%s", bucket, key, exc)
            continue
        except (ValueError, UnicodeDecodeError) as exc:
            logger.error("settlement_parse_failed key=%s error=%s", key, exc)
            continue

        breaks, summary = _reconcile(rows)

        try:
            report_key = _write_report(bucket, key, breaks, summary)
        except ClientError as exc:
            logger.error("report_write_failed key=%s error=%s", key, exc)
            continue

        logger.info(
            "reconciliation_done key=%s rows=%s matched=%s breaks=%s report=%s",
            key, len(rows), summary["matched"], summary["breaks"], report_key,
        )
        results.append({"source_key": key, "report_key": report_key, "summary": summary})

    return {"processed": len(results), "results": json.loads(json.dumps(results, default=str))}
