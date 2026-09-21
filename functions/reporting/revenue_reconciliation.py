"""Settlement file reconciliation.

Event source: S3 ObjectCreated on the settlement landing bucket.
Parses the acquirer's settlement CSV, matches every settled line against our internal
capture records, and writes an exceptions report for anything that does not reconcile.
The exceptions report drives the finance team's month-end adjustments.
"""

import csv
import io
import json
import logging
import os
import time
import urllib.parse
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb_client = boto3.client("dynamodb")

CAPTURE_TABLE = os.environ.get("CAPTURE_TABLE", "captures")
CAPTURE_INDEX = os.environ.get("CAPTURE_INDEX", "by-psp-reference")
REPORT_BUCKET = os.environ.get("REPORT_BUCKET", "")
REPORT_PREFIX = os.environ.get("REPORT_PREFIX", "reconciliation/exceptions")
QUERY_PAGE_SIZE = int(os.environ.get("QUERY_PAGE_SIZE", "100"))

CENTS = Decimal("0.01")
AMOUNT_TOLERANCE = Decimal("0.01")

BREAK_MISSING = "MISSING_INTERNAL"
BREAK_DUPLICATE = "DUPLICATE_CAPTURE"
BREAK_AMOUNT = "AMOUNT_MISMATCH"
BREAK_CURRENCY = "CURRENCY_MISMATCH"
BREAK_STATUS = "UNEXPECTED_STATUS"

REQUIRED_COLUMNS = ("psp_reference", "gross_amount", "currency", "settled_at")


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def read_settlement_object(bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8-sig")


def parse_settlement_csv(raw: str) -> List[Dict[str, str]]:
    """Parse and validate the settlement CSV."""
    reader = csv.DictReader(io.StringIO(raw))
    if reader.fieldnames is None:
        raise ValueError("settlement file has no header row")

    missing = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
    if missing:
        raise ValueError("settlement file missing columns: {0}".format(", ".join(missing)))

    rows: List[Dict[str, str]] = []
    for line_number, row in enumerate(reader, start=2):
        reference = (row.get("psp_reference") or "").strip()
        if not reference:
            logger.warning("settlement_row_no_reference line=%s", line_number)
            continue
        row["_line_number"] = str(line_number)
        rows.append(row)
    return rows


def query_internal_captures(psp_reference: str) -> List[Dict[str, Any]]:
    """Read every internal capture row carrying this acquirer reference."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("query")

    items: List[Dict[str, Any]] = []
    for page in safe_paginate(paginator, fail_on_cap=True,
        TableName=CAPTURE_TABLE,
        IndexName=CAPTURE_INDEX,
        KeyConditionExpression="psp_reference = :ref",
        ExpressionAttributeValues={":ref": {"S": psp_reference}},
        PaginationConfig={"PageSize": QUERY_PAGE_SIZE},
    ):
        items.extend(page.get("Items", []))
    return items


def _internal_amount(item: Dict[str, Any]) -> Decimal:
    raw = item.get("amount", {}).get("N")
    return _money(raw) if raw is not None else Decimal("0.00")


def _internal_status(item: Dict[str, Any]) -> str:
    return item.get("status", {}).get("S", "")


def classify(row: Dict[str, str], internal: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    """Compare one settled line against the internal capture set."""
    settled_amount = _money(row.get("gross_amount", "0"))
    settled_currency = (row.get("currency") or "").upper()

    if not internal:
        return BREAK_MISSING, {
            "settled_amount": settled_amount,
            "internal_amount": None,
        }

    captured = [item for item in internal if _internal_status(item) == "CAPTURED"]
    if len(captured) > 1:
        return BREAK_DUPLICATE, {
            "settled_amount": settled_amount,
            "internal_amount": _internal_amount(captured[0]),
            "match_count": len(captured),
        }

    candidate = captured[0] if captured else internal[0]
    internal_status = _internal_status(candidate)
    if not captured:
        return BREAK_STATUS, {
            "settled_amount": settled_amount,
            "internal_amount": _internal_amount(candidate),
            "internal_status": internal_status,
        }

    internal_currency = candidate.get("currency", {}).get("S", "").upper()
    if settled_currency and internal_currency and settled_currency != internal_currency:
        return BREAK_CURRENCY, {
            "settled_amount": settled_amount,
            "internal_amount": _internal_amount(candidate),
            "settled_currency": settled_currency,
            "internal_currency": internal_currency,
        }

    internal_amount = _internal_amount(candidate)
    delta = (settled_amount - internal_amount).copy_abs()
    if delta > AMOUNT_TOLERANCE:
        return BREAK_AMOUNT, {
            "settled_amount": settled_amount,
            "internal_amount": internal_amount,
            "delta": delta,
        }

    return "MATCHED", {"settled_amount": settled_amount, "internal_amount": internal_amount}


def reconcile(rows: Iterable[Dict[str, str]], context=None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Reconcile every settled line. Returns (breaks, summary)."""
    from lambda_guards import check_remaining_time
    breaks: List[Dict[str, Any]] = []
    matched = 0
    settled_total = Decimal("0.00")
    internal_total = Decimal("0.00")
    by_kind: Dict[str, int] = {}

    for row in rows:
        if context and not check_remaining_time(context):
            logger.warning("reconciliation_time_remaining_low, stopping early")
            break
        reference = (row.get("psp_reference") or "").strip()
        try:
            internal = query_internal_captures(reference)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            logger.error("internal_lookup_failed reference=%s code=%s", reference, code)
            continue

        classification, detail = classify(row, internal)
        settled_total += detail["settled_amount"]
        if detail.get("internal_amount") is not None:
            internal_total += detail["internal_amount"]

        if classification == "MATCHED":
            matched += 1
            continue

        by_kind[classification] = by_kind.get(classification, 0) + 1
        breaks.append(
            {
                "psp_reference": reference,
                "line_number": row.get("_line_number"),
                "kind": classification,
                **detail,
            }
        )

    summary = {
        "matched": matched,
        "breaks": len(breaks),
        "breaks_by_kind": by_kind,
        "settled_total": settled_total,
        "internal_total": internal_total,
        "net_variance": (settled_total - internal_total),
    }
    return breaks, summary


def write_exceptions_report(
    source_key: str, breaks: List[Dict[str, Any]], summary: Dict[str, Any]
) -> Optional[str]:
    """Write the exceptions CSV that finance works from."""
    if not REPORT_BUCKET:
        logger.warning("report_bucket_unconfigured source=%s", source_key)
        return None

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["psp_reference", "line_number", "kind", "settled_amount", "internal_amount", "delta"]
    )
    for entry in breaks:
        writer.writerow(
            [
                entry.get("psp_reference", ""),
                entry.get("line_number", ""),
                entry.get("kind", ""),
                entry.get("settled_amount", ""),
                entry.get("internal_amount", ""),
                entry.get("delta", ""),
            ]
        )

    base = source_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    report_key = "{0}/{1}-exceptions-{2}.csv".format(REPORT_PREFIX, base, int(time.time()))

    s3.put_object(
        Bucket=REPORT_BUCKET,
        Key=report_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
        Metadata={
            "matched": str(summary["matched"]),
            "breaks": str(summary["breaks"]),
            "net-variance": str(summary["net_variance"]),
        },
    )
    return report_key


def lambda_handler(event, context):
    from lambda_guards import check_s3_recursive_invocation, validate_payload_size

    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"processed": 0, "results": [], "reason": "recursive_invocation_blocked"}

    results: List[Dict[str, Any]] = []

    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))
        if not bucket or not key:
            continue

        try:
            raw = read_settlement_object(bucket, key)
            rows = parse_settlement_csv(raw)
        except ClientError as exc:
            logger.error("settlement_read_failed bucket=%s key=%s error=%s", bucket, key, exc)
            continue
        except (ValueError, UnicodeDecodeError) as exc:
            logger.error("settlement_parse_failed key=%s error=%s", key, exc)
            continue

        breaks, summary = reconcile(rows, context)

        try:
            report_key = write_exceptions_report(key, breaks, summary)
        except ClientError as exc:
            logger.error("report_write_failed key=%s error=%s", key, exc)
            continue

        logger.info(
            "reconciliation_complete key=%s rows=%s matched=%s breaks=%s variance=%s report=%s",
            key, len(rows), summary["matched"], summary["breaks"],
            summary["net_variance"], report_key,
        )
        results.append(
            {
                "source_key": key,
                "report_key": report_key,
                "summary": json.loads(json.dumps(summary, default=str)),
            }
        )

    return {"processed": len(results), "results": results}
