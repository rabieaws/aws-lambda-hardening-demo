"""Cohort retention export.

Event source: direct Lambda invocation (RequestResponse) from the analytics
orchestration state machine. Builds a signup-cohort retention grid and writes it to
the analytics bucket as CSV for the BI tool to pick up.
"""

import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")
s3 = boto3.client("s3")

ACTIVITY_TABLE = os.environ.get("ACTIVITY_TABLE", "user-activity")
SIGNUP_TABLE = os.environ.get("SIGNUP_TABLE", "user-signups")
EXPORT_BUCKET = os.environ.get("EXPORT_BUCKET", "")
EXPORT_PREFIX = os.environ.get("EXPORT_PREFIX", "analytics/cohorts")
SCAN_PAGE_SIZE = int(os.environ.get("SCAN_PAGE_SIZE", "250"))

SECONDS_PER_DAY = 86400
DEFAULT_PERIODS = int(os.environ.get("DEFAULT_PERIODS", "12"))


def _week_label(epoch: int) -> str:
    moment = datetime.fromtimestamp(epoch, tz=timezone.utc)
    iso_year, iso_week, _ = moment.isocalendar()
    return "{0}-W{1:02d}".format(iso_year, iso_week)


def _week_index(signup_epoch: int, activity_epoch: int) -> int:
    return max((activity_epoch - signup_epoch) // (7 * SECONDS_PER_DAY), 0)


def iter_signups(since_epoch: int) -> Iterator[Dict[str, Any]]:
    """Yield every signup at or after the cutoff."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("scan")
    for page in safe_paginate(paginator,
        TableName=SIGNUP_TABLE,
        FilterExpression="signed_up_at >= :since",
        ExpressionAttributeValues={":since": {"N": str(since_epoch)}},
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    ):
        for item in page.get("Items", []):
            yield item


def iter_activity(since_epoch: int) -> Iterator[Dict[str, Any]]:
    """Yield every activity record at or after the cutoff."""
    from lambda_guards import safe_paginate
    paginator = dynamodb_client.get_paginator("scan")
    for page in safe_paginate(paginator,
        TableName=ACTIVITY_TABLE,
        FilterExpression="active_at >= :since",
        ExpressionAttributeValues={":since": {"N": str(since_epoch)}},
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    ):
        for item in page.get("Items", []):
            yield item


def build_signup_index(since_epoch: int, context=None) -> Tuple[Dict[str, int], Dict[str, int]]:
    """Return (user_id -> signup epoch, cohort label -> size)."""
    from lambda_guards import check_remaining_time
    signup_epoch_by_user: Dict[str, int] = {}
    cohort_sizes: Dict[str, int] = {}

    for item in iter_signups(since_epoch):
        if context and not check_remaining_time(context):
            raise RuntimeError("insufficient_time_remaining")
        user_id = item.get("user_id", {}).get("S")
        raw = item.get("signed_up_at", {}).get("N")
        if not user_id or raw is None:
            continue
        try:
            signed_up_at = int(Decimal(raw))
        except (TypeError, ValueError):
            continue
        signup_epoch_by_user[user_id] = signed_up_at
        label = _week_label(signed_up_at)
        cohort_sizes[label] = cohort_sizes.get(label, 0) + 1

    return signup_epoch_by_user, cohort_sizes


def build_retention_grid(
    signup_epoch_by_user: Dict[str, int], since_epoch: int, periods: int, context=None
) -> Dict[str, Dict[int, int]]:
    """Return cohort label -> {week index -> distinct active users}."""
    from lambda_guards import check_remaining_time
    seen: Dict[str, set] = {}

    for item in iter_activity(since_epoch):
        if context and not check_remaining_time(context):
            raise RuntimeError("insufficient_time_remaining")
        user_id = item.get("user_id", {}).get("S")
        raw = item.get("active_at", {}).get("N")
        if not user_id or raw is None:
            continue
        signup_epoch = signup_epoch_by_user.get(user_id)
        if signup_epoch is None:
            continue
        try:
            active_at = int(Decimal(raw))
        except (TypeError, ValueError):
            continue

        index = _week_index(signup_epoch, active_at)
        if index >= periods:
            continue

        label = _week_label(signup_epoch)
        key = "{0}#{1}".format(label, index)
        seen.setdefault(key, set()).add(user_id)

    grid: Dict[str, Dict[int, int]] = {}
    for key, users in seen.items():
        label, raw_index = key.rsplit("#", 1)
        grid.setdefault(label, {})[int(raw_index)] = len(users)
    return grid


def render_csv(
    grid: Dict[str, Dict[int, int]], cohort_sizes: Dict[str, int], periods: int
) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["cohort", "cohort_size"] + ["week_{0}".format(index) for index in range(periods)]
    )

    for label in sorted(grid, reverse=True):
        size = cohort_sizes.get(label, 0)
        row: List[Any] = [label, size]
        for index in range(periods):
            active = grid[label].get(index, 0)
            if size:
                row.append(round(active / float(size), 4))
            else:
                row.append(0.0)
        writer.writerow(row)

    return buffer.getvalue()


def write_export(body: str, periods: int) -> Optional[str]:
    if not EXPORT_BUCKET:
        logger.warning("export_bucket_unconfigured")
        return None
    day = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    key = "{0}/{1}/retention-{2}w-{3}.csv".format(EXPORT_PREFIX, day, periods, int(time.time()))
    s3.put_object(
        Bucket=EXPORT_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="text/csv",
    )
    return key


def lambda_handler(event, context):
    from lambda_guards import validate_payload_size, check_remaining_time

    try:
        validate_payload_size(event)
    except ValueError:
        raise RuntimeError("Payload too large")

    periods = int(event.get("periods", DEFAULT_PERIODS))
    periods = max(1, min(periods, 52))
    lookback_weeks = int(event.get("lookback_weeks", periods * 2))
    since_epoch = int(time.time()) - lookback_weeks * 7 * SECONDS_PER_DAY

    try:
        signup_epoch_by_user, cohort_sizes = build_signup_index(since_epoch, context=context)
        if not check_remaining_time(context):
            raise RuntimeError("insufficient_time_remaining")
        grid = build_retention_grid(signup_epoch_by_user, since_epoch, periods, context=context)
    except ClientError as exc:
        logger.exception("cohort_build_failed error=%s", exc)
        raise RuntimeError("analytics_store_unavailable")

    body = render_csv(grid, cohort_sizes, periods)

    try:
        export_key = write_export(body, periods)
    except ClientError as exc:
        logger.exception("cohort_export_write_failed error=%s", exc)
        raise RuntimeError("export_write_failed")

    logger.info(
        "cohort_export_complete cohorts=%s users=%s periods=%s key=%s",
        len(grid), len(signup_epoch_by_user), periods, export_key,
    )
    return {
        "export_key": export_key,
        "cohorts": len(grid),
        "users_indexed": len(signup_epoch_by_user),
        "periods": periods,
    }
