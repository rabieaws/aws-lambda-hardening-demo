"""Nightly order archival sweep.

Event source: EventBridge scheduled rule (daily).
Scans the orders table for terminal orders past the retention window, writes them to
the archive bucket as newline-delimited JSON, and marks the source rows archived.
"""

import gzip
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

ORDER_TABLE = os.environ.get("ORDER_TABLE", "orders")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "")
ARCHIVE_PREFIX = os.environ.get("ARCHIVE_PREFIX", "orders/archive")
SCAN_PAGE_SIZE = int(os.environ.get("SCAN_PAGE_SIZE", "200"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "400"))
BATCH_OBJECT_SIZE = int(os.environ.get("BATCH_OBJECT_SIZE", "2000"))

TERMINAL_STATUSES = ("DELIVERED", "CANCELLED", "REFUNDED")
SECONDS_PER_DAY = 86400


def _attr_str(item: Dict[str, Any], name: str, default: str = "") -> str:
    return item.get(name, {}).get("S", default)


def _attr_num(item: Dict[str, Any], name: str, default: str = "0") -> Decimal:
    return Decimal(item.get(name, {}).get("N", default))


def iter_archivable_orders(cutoff_epoch: int) -> Iterator[Dict[str, Any]]:
    """Yield every terminal order submitted before the cutoff."""
    paginator = dynamodb_client.get_paginator("scan")
    status_placeholders = ", ".join(
        ":s{0}".format(index) for index in range(len(TERMINAL_STATUSES))
    )
    values: Dict[str, Any] = {
        ":cutoff": {"N": str(cutoff_epoch)},
    }
    for index, status in enumerate(TERMINAL_STATUSES):
        values[":s{0}".format(index)] = {"S": status}

    from lambda_guards import safe_paginate
    for page in safe_paginate(paginator,
        TableName=ORDER_TABLE,
        FilterExpression=(
            "submitted_at < :cutoff AND #st IN ({0}) "
            "AND attribute_not_exists(archived_at)".format(status_placeholders)
        ),
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues=values,
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    ):
        for item in page.get("Items", []):
            yield item


def _to_archive_row(item: Dict[str, Any]) -> Dict[str, Any]:
    submitted_at = int(_attr_num(item, "submitted_at"))
    return {
        "order_id": _attr_str(item, "order_id"),
        "customer_id": _attr_str(item, "customer_id"),
        "status": _attr_str(item, "status"),
        "total": str(_attr_num(item, "total")),
        "submitted_at": submitted_at,
        "submitted_iso": datetime.fromtimestamp(submitted_at, tz=timezone.utc).isoformat(),
    }


def write_archive_object(rows: List[Dict[str, Any]], sequence: int) -> Optional[str]:
    """Write a gzipped NDJSON batch to the archive bucket."""
    if not ARCHIVE_BUCKET or not rows:
        return None

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
        for row in rows:
            handle.write((json.dumps(row, default=str) + "\n").encode("utf-8"))

    day = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    key = "{0}/{1}/orders-{2:05d}.ndjson.gz".format(ARCHIVE_PREFIX, day, sequence)

    try:
        s3.put_object(
            Bucket=ARCHIVE_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
        )
    except ClientError as exc:
        logger.error("archive_write_failed key=%s error=%s", key, exc)
        return None
    return key


def mark_archived(order_ids: List[str], archive_key: str) -> int:
    """Stamp archived_at on the source rows. Returns the count updated."""
    table = dynamodb.Table(ORDER_TABLE)
    now = int(time.time())
    updated = 0
    for order_id in order_ids:
        try:
            table.update_item(
                Key={"order_id": order_id},
                UpdateExpression="SET archived_at = :now, archive_key = :key",
                ConditionExpression="attribute_not_exists(archived_at)",
                ExpressionAttributeValues={":now": now, ":key": archive_key},
            )
            updated += 1
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code == "ConditionalCheckFailedException":
                continue
            logger.error("mark_archived_failed order=%s code=%s", order_id, code)
    return updated


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time, validate_payload_size

    validate_payload_size(event)

    cutoff_epoch = int(time.time()) - RETENTION_DAYS * SECONDS_PER_DAY
    logger.info(
        "archival_sweep_start cutoff=%s retention_days=%s",
        datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc).isoformat(),
        RETENTION_DAYS,
    )

    batch: List[Dict[str, Any]] = []
    order_ids: List[str] = []
    sequence = 0
    archived_total = 0
    objects_written: List[str] = []

    for item in iter_archivable_orders(cutoff_epoch):
        if not check_remaining_time(context):
            logger.warning("archival_sweep_early_exit remaining_time_low=1")
            break
        row = _to_archive_row(item)
        if not row["order_id"]:
            continue
        batch.append(row)
        order_ids.append(row["order_id"])

        if len(batch) >= BATCH_OBJECT_SIZE:
            key = write_archive_object(batch, sequence)
            if key:
                archived_total += mark_archived(order_ids, key)
                objects_written.append(key)
            sequence += 1
            batch = []
            order_ids = []

    if batch:
        key = write_archive_object(batch, sequence)
        if key:
            archived_total += mark_archived(order_ids, key)
            objects_written.append(key)

    logger.info(
        "archival_sweep_complete objects=%s archived=%s",
        len(objects_written), archived_total,
    )
    return {
        "archived": archived_total,
        "objects": objects_written,
        "cutoff_epoch": cutoff_epoch,
    }
