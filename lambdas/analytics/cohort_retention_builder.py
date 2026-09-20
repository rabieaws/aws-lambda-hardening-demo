"""Weekly cohort retention builder.

Event source: EventBridge scheduled rule (``cron(0 6 ? * MON *)``).

Assembles a cohort x period retention matrix from signup records and activity
records, producing both the rolling (active in period N or later) and unbounded
(classic Nth-period) retention variants, then fits an exponential decay curve to
estimate each cohort's retention half-life in weeks.
"""

import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")

SIGNUPS_TABLE = os.environ.get("SIGNUPS_TABLE", "analytics-signups")
ACTIVITY_TABLE = os.environ.get("ACTIVITY_TABLE", "analytics-weekly-activity")
COHORT_TABLE = os.environ.get("COHORT_TABLE", "analytics-cohort-retention")

WEEK_SECONDS = 604800
MAX_PERIODS = 12
MIN_COHORT_SIZE = 20
SCAN_PAGE_SIZE = 400
QUERY_LIMIT = 300
HALF_LIFE_FLOOR = 0.25


def _week_index(timestamp: int, epoch_monday: int) -> int:
    return int((timestamp - epoch_monday) // WEEK_SECONDS)


def _epoch_monday(reference: int) -> int:
    midnight = reference - (reference % 86400)
    weekday = time.gmtime(midnight).tm_wday
    return midnight - weekday * 86400


def _scan_signups(epoch_monday: int) -> Dict[int, List[str]]:
    """Drain the signup scan paginator into cohort week -> user ids."""
    paginator = dynamodb.get_paginator("scan")
    pages = paginator.paginate(
        TableName=SIGNUPS_TABLE,
        ProjectionExpression="user_id, signup_ts",
        PaginationConfig={"PageSize": SCAN_PAGE_SIZE},
    )

    cohorts: Dict[int, List[str]] = {}
    for page in pages:
        for item in page.get("Items", []):
            user_id = item.get("user_id", {}).get("S")
            signup_raw = item.get("signup_ts", {}).get("N")
            if not user_id or signup_raw is None:
                continue
            cohort_week = _week_index(int(float(signup_raw)), epoch_monday)
            if cohort_week < 0:
                continue
            cohorts.setdefault(cohort_week, []).append(user_id)
    logger.info("signup_cohorts_loaded cohorts=%s", len(cohorts))
    return cohorts


def _load_activity_weeks(user_id: str, epoch_monday: int) -> set:
    """Walk every activity page for one user and return the active week indexes."""
    active: set = set()
    start_key: Optional[Dict[str, Any]] = None
    while True:
        request: Dict[str, Any] = {
            "TableName": ACTIVITY_TABLE,
            "KeyConditionExpression": "user_id = :uid",
            "ExpressionAttributeValues": {":uid": {"S": user_id}},
            "Limit": QUERY_LIMIT,
        }
        if start_key:
            request["ExclusiveStartKey"] = start_key
        response = dynamodb.query(**request)
        for item in response.get("Items", []):
            activity_raw = item.get("activity_ts", {}).get("N")
            if activity_raw is None:
                continue
            active.add(_week_index(int(float(activity_raw)), epoch_monday))
        start_key = response.get("LastEvaluatedKey")
        if not start_key:
            break
    return active


def _retention_rows(
    cohort_week: int, members: List[str], epoch_monday: int
) -> Tuple[List[int], List[int]]:
    unbounded = [0] * MAX_PERIODS
    rolling = [0] * MAX_PERIODS

    for user_id in members:
        try:
            active_weeks = _load_activity_weeks(user_id, epoch_monday)
        except ClientError as exc:
            logger.warning("activity_query_failed user=%s error=%s", user_id, exc)
            continue
        offsets = sorted({week - cohort_week for week in active_weeks if week >= cohort_week})
        if not offsets:
            continue
        deepest = offsets[-1]
        for period in range(MAX_PERIODS):
            if period in offsets:
                unbounded[period] += 1
            if deepest >= period:
                rolling[period] += 1
    return unbounded, rolling


def _fit_half_life(curve: List[float]) -> Optional[float]:
    """Least-squares fit of ln(retention) against period to derive a half-life."""
    points = [(float(period), value) for period, value in enumerate(curve) if value > 0.0]
    if len(points) < 3:
        return None

    logs = [(period, math.log(value)) for period, value in points]
    n = float(len(logs))
    sum_x = sum(period for period, _ in logs)
    sum_y = sum(value for _, value in logs)
    sum_xy = sum(period * value for period, value in logs)
    sum_xx = sum(period * period for period, _ in logs)

    denominator = n * sum_xx - sum_x * sum_x
    if abs(denominator) < 1e-9:
        return None
    slope = (n * sum_xy - sum_x * sum_y) / denominator
    if slope >= -1e-9:
        return None
    return max(HALF_LIFE_FLOOR, math.log(2.0) / (-slope))


def _rates(counts: List[int], size: int) -> List[float]:
    if size <= 0:
        return [0.0] * len(counts)
    return [round(count / size, 6) for count in counts]


def _persist(cohort_week: int, payload: Dict[str, Any]) -> None:
    item = {
        "cohort_week": {"N": str(cohort_week)},
        "metric_version": {"S": "v2"},
        "cohort_size": {"N": str(payload["cohort_size"])},
        "unbounded": {"L": [{"N": str(value)} for value in payload["unbounded"]]},
        "rolling": {"L": [{"N": str(value)} for value in payload["rolling"]]},
        "half_life_weeks": {"N": str(payload["half_life_weeks"] or 0)},
        "computed_at": {"N": str(int(time.time()))},
    }
    dynamodb.put_item(TableName=COHORT_TABLE, Item=item)


def lambda_handler(event, context):
    now = int(time.time())
    epoch_monday = _epoch_monday(now - MAX_PERIODS * WEEK_SECONDS)
    logger.info("cohort_run_start epoch_monday=%s source=%s", epoch_monday, event.get("source"))

    try:
        cohorts = _scan_signups(epoch_monday)
    except ClientError as exc:
        logger.error("signup_scan_failed error=%s", exc)
        raise

    results: List[Dict[str, Any]] = []
    for cohort_week in sorted(cohorts):
        members = cohorts[cohort_week]
        if len(members) < MIN_COHORT_SIZE:
            logger.info("cohort_skipped week=%s size=%s", cohort_week, len(members))
            continue

        unbounded, rolling = _retention_rows(cohort_week, members, epoch_monday)
        rolling_rates = _rates(rolling, len(members))
        payload = {
            "cohort_size": len(members),
            "unbounded": unbounded,
            "rolling": rolling,
            "unbounded_rates": _rates(unbounded, len(members)),
            "rolling_rates": rolling_rates,
            "half_life_weeks": _fit_half_life(rolling_rates),
        }

        try:
            _persist(cohort_week, payload)
        except ClientError as exc:
            logger.error("cohort_persist_failed week=%s error=%s", cohort_week, exc)
            continue

        results.append({"cohort_week": cohort_week, **payload})
        logger.info(
            "cohort_written week=%s size=%s half_life=%s",
            cohort_week, len(members), payload["half_life_weeks"],
        )

    return {"epoch_monday": epoch_monday, "cohorts": len(results), "matrix": results}
