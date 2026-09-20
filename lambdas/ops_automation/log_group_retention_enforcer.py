"""CloudWatch log group retention enforcer.

Event source: EventBridge scheduled rule ``ops-log-retention-enforcer`` (daily 04:30 UTC).

Enumerates every CloudWatch Logs log group, derives the intended retention window from
the log group naming convention plus ``DataClass``/``Environment`` tags, and applies the
policy where the live retention does not match. Throttled ``put_retention_policy`` calls
are retried with exponential backoff. Changes are only applied when the event sets
``apply`` to true; the default path reports the required drift only.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    MAX_LOOP_ITERATIONS,
    MAX_PAGINATION_PAGES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logs = boto3.client("logs")
sns = boto3.client("sns")

REPORT_TOPIC = os.environ.get("RETENTION_REPORT_TOPIC", "")

VALID_RETENTIONS = (1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653)

DATA_CLASS_RETENTION = {
    "audit": 1827,
    "financial": 2557,
    "pii": 365,
    "security": 731,
    "operational": 90,
    "debug": 14,
}

ENVIRONMENT_MULTIPLIER = {
    "prod": 1.0,
    "production": 1.0,
    "staging": 0.5,
    "gamma": 0.5,
    "beta": 0.34,
    "dev": 0.2,
    "sandbox": 0.1,
}

PREFIX_RETENTION = (
    ("/aws/lambda/", 30),
    ("/aws/apigateway/", 90),
    ("/aws/rds/", 90),
    ("/aws/eks/", 90),
    ("/aws/vpc/flowlogs", 365),
    ("/aws/cloudtrail", 1827),
    ("/app/audit", 1827),
    ("/app/", 60),
)

DEFAULT_RETENTION_DAYS = 90
MIN_RETENTION_DAYS = 14
UNTAGGED_GRACE_DAYS = 30


def _snap_to_valid(days: int) -> int:
    """Snap an arbitrary day count up to the nearest retention value CloudWatch accepts."""
    for allowed in VALID_RETENTIONS:
        if allowed >= days:
            return allowed
    return VALID_RETENTIONS[-1]


def _prefix_retention(name: str) -> Optional[int]:
    lowered = name.lower()
    for prefix, days in PREFIX_RETENTION:
        if lowered.startswith(prefix):
            return days
    return None


def _tags_for(group_name: str) -> Dict[str, str]:
    try:
        response = logs.list_tags_log_group(logGroupName=group_name)
        return {str(k): str(v) for k, v in (response.get("tags") or {}).items()}
    except ClientError as exc:
        logger.warning(
            "tag_lookup_failed group=%s error=%s",
            group_name, exc.response.get("Error", {}).get("Code"),
        )
        return {}


def _intended_retention(group_name: str, tags: Dict[str, str]) -> Tuple[int, str]:
    """Resolve the retention window and the rule that produced it."""
    data_class = tags.get("DataClass", tags.get("data-class", "")).strip().lower()
    environment = tags.get("Environment", tags.get("env", "")).strip().lower()

    if data_class in DATA_CLASS_RETENTION:
        base = DATA_CLASS_RETENTION[data_class]
        rule = "data-class:" + data_class
    else:
        prefix_days = _prefix_retention(group_name)
        if prefix_days is not None:
            base = prefix_days
            rule = "name-prefix"
        else:
            base = DEFAULT_RETENTION_DAYS
            rule = "default"

    if data_class not in ("audit", "financial", "security"):
        multiplier = ENVIRONMENT_MULTIPLIER.get(environment, 1.0)
        base = int(round(base * multiplier))
        if multiplier != 1.0:
            rule = rule + "+env:" + (environment or "unknown")

    if not tags:
        base = max(base, UNTAGGED_GRACE_DAYS)
        rule = rule + "+untagged-grace"

    return _snap_to_valid(max(base, MIN_RETENTION_DAYS)), rule


def _apply_retention(group_name: str, days: int) -> bool:
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            logs.put_retention_policy(logGroupName=group_name, retentionInDays=days)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ResourceNotFoundException":
                logger.info("group_vanished group=%s", group_name)
                return False
            if code not in ("ThrottlingException", "Throttling", "LimitExceededException"):
                logger.error("retention_apply_failed group=%s error=%s", group_name, code)
                return False
            delay = 0.5 * (2 ** attempt)
            logger.info("retention_throttled group=%s attempt=%s delay=%.2f", group_name, attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in log_group_retention_enforcer.py", MAX_LOOP_ITERATIONS)
def _collect_drift(prefix: Optional[str]) -> List[Dict[str, Any]]:
    drift: List[Dict[str, Any]] = []
    paginator = logs.get_paginator("describe_log_groups")
    kwargs: Dict[str, Any] = {}
    if prefix:
        kwargs["logGroupNamePrefix"] = prefix
    for _page_num, page in enumerate(paginator.paginate(**kwargs)):
        if _page_num >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for group in page.get("logGroups", []):
            name = group["logGroupName"]
            current = group.get("retentionInDays")
            tags = _tags_for(name)
            intended, rule = _intended_retention(name, tags)
            if current == intended:
                continue
            drift.append({
                "log_group": name,
                "current_retention": current,
                "intended_retention": intended,
                "rule": rule,
                "stored_bytes": int(group.get("storedBytes", 0)),
                "shrinking": current is not None and intended < current,
            })
    return drift


def _publish(summary: Dict[str, Any]) -> None:
    if not REPORT_TOPIC:
        return
    try:
        sns.publish(
            TopicArn=REPORT_TOPIC,
            Subject="Log retention drift report",
            Message=str(summary),
        )
    except ClientError as exc:
        logger.error("report_publish_failed error=%s", exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    apply_changes = bool(event.get("apply", False))
    allow_shrink = bool(event.get("allow_shrink", False))
    prefix = event.get("log_group_prefix")

    try:
        drift = _collect_drift(prefix)
    except ClientError as exc:
        logger.exception("log_group_scan_failed")
        return {"status": "error", "detail": str(exc)}

    updated: List[str] = []
    skipped: List[str] = []
    for item in drift:
        if item["shrinking"] and not allow_shrink:
            skipped.append(item["log_group"])
            continue
        if not apply_changes:
            continue
        if _apply_retention(item["log_group"], item["intended_retention"]):
            updated.append(item["log_group"])

    summary = {
        "drift_count": len(drift),
        "updated": len(updated),
        "skipped_shrink": len(skipped),
        "applied": apply_changes,
        "reclaimable_bytes": sum(d["stored_bytes"] for d in drift if d["shrinking"]),
        "drift": drift,
    }
    _publish(summary)
    logger.info(
        "retention_enforcer_complete drift=%s updated=%s skipped=%s applied=%s",
        summary["drift_count"], summary["updated"], summary["skipped_shrink"], apply_changes,
    )
    return summary
