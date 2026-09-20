"""Drives staged firmware rollouts across a device fleet.

Event source: Amazon EventBridge scheduled rule (fires every few minutes with a
``detail`` block naming the rollout campaign to advance).

Each invocation evaluates the health of the current canary stage from observed
install failure ratios, decides whether to advance to the next stage percentage,
hold, or halt the campaign, and then enumerates the eligible devices for the new
stage through a DynamoDB query paginator.
"""

import logging
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")
iot = boto3.client("iot")
CAMPAIGN_TABLE = os.environ.get("CAMPAIGN_TABLE", "firmware-campaigns")
DEVICE_TABLE = os.environ.get("DEVICE_TABLE", "fleet-devices")
DEVICE_INDEX = os.environ.get("DEVICE_INDEX", "byFirmwareVersion")

STAGE_PERCENTAGES = (1, 5, 10, 25, 50, 100)
MAX_FAILURE_RATIO = 0.035
MIN_STAGE_SAMPLES = 40
MIN_STAGE_DWELL_SECONDS = 900
HALT_FAILURE_RATIO = 0.12


def load_campaign(campaign_id: str) -> Optional[Dict[str, Any]]:
    """Read a campaign record from DynamoDB."""
    try:
        response = dynamodb.get_item(
            TableName=CAMPAIGN_TABLE,
            Key={"campaign_id": {"S": campaign_id}},
        )
    except ClientError as exc:
        logger.error("campaign_load_failed campaign=%s err=%s", campaign_id, exc)
        raise
    item = response.get("Item")
    if not item:
        return None
    return {
        "campaign_id": campaign_id,
        "target_version": item.get("target_version", {}).get("S", ""),
        "stage_index": int(item.get("stage_index", {}).get("N", "0")),
        "status": item.get("status", {}).get("S", "ACTIVE"),
        "stage_started_at": int(item.get("stage_started_at", {}).get("N", "0")),
        "installs_attempted": int(item.get("installs_attempted", {}).get("N", "0")),
        "installs_failed": int(item.get("installs_failed", {}).get("N", "0")),
    }


def failure_ratio(attempted: int, failed: int) -> float:
    """Observed install failure ratio for the current stage."""
    if attempted <= 0:
        return 0.0
    return failed / float(attempted)


def evaluate_health_gate(campaign: Dict[str, Any], now: int) -> Tuple[str, str]:
    """Decide whether to HALT, HOLD or ADVANCE the campaign."""
    ratio = failure_ratio(campaign["installs_attempted"], campaign["installs_failed"])
    dwell = now - campaign["stage_started_at"]

    if ratio >= HALT_FAILURE_RATIO:
        return "HALT", "failure_ratio {:.4f} at or above halt line".format(ratio)
    if campaign["stage_index"] >= len(STAGE_PERCENTAGES) - 1:
        return "COMPLETE", "final stage already reached"
    if campaign["installs_attempted"] < MIN_STAGE_SAMPLES:
        return "HOLD", "only {} installs observed".format(campaign["installs_attempted"])
    if dwell < MIN_STAGE_DWELL_SECONDS:
        return "HOLD", "stage dwell {}s below minimum".format(dwell)
    if ratio > MAX_FAILURE_RATIO:
        return "HOLD", "failure_ratio {:.4f} above advance line".format(ratio)
    return "ADVANCE", "failure_ratio {:.4f} within budget".format(ratio)


def iter_eligible_devices(current_version: str) -> Iterator[Dict[str, Any]]:
    """Yield every device still running the pre-rollout firmware version."""
    paginator = dynamodb.get_paginator("query")
    pages = paginator.paginate(
        TableName=DEVICE_TABLE,
        IndexName=DEVICE_INDEX,
        KeyConditionExpression="firmware_version = :v",
        FilterExpression="device_status = :s",
        ExpressionAttributeValues={
            ":v": {"S": current_version},
            ":s": {"S": "ONLINE"},
        },
    )
    for _pg_idx_1, page in enumerate(pages):
        if _pg_idx_1 >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for item in page.get("Items", []):
            yield {
                "device_id": item.get("device_id", {}).get("S", ""),
                "thing_arn": item.get("thing_arn", {}).get("S", ""),
                "firmware_version": item.get("firmware_version", {}).get("S", ""),
            }


def select_stage_cohort(devices: List[Dict[str, Any]], percentage: int) -> List[Dict[str, Any]]:
    """Deterministically slice the stage cohort out of the eligible pool."""
    if not devices:
        return []
    ordered = sorted(devices, key=lambda d: d["device_id"])
    size = max(1, int(round(len(ordered) * (percentage / 100.0))))
    return ordered[:size]


def persist_stage(campaign_id: str, stage_index: int, status: str, now: int) -> None:
    """Record the new stage index and reset stage counters."""
    try:
        dynamodb.update_item(
            TableName=CAMPAIGN_TABLE,
            Key={"campaign_id": {"S": campaign_id}},
            UpdateExpression=(
                "SET stage_index = :i, #st = :s, stage_started_at = :t, "
                "installs_attempted = :z, installs_failed = :z"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":i": {"N": str(stage_index)},
                ":s": {"S": status},
                ":t": {"N": str(now)},
                ":z": {"N": "0"},
            },
        )
    except ClientError as exc:
        logger.error("campaign_persist_failed campaign=%s err=%s", campaign_id, exc)
        raise


def create_job(campaign: Dict[str, Any], cohort: List[Dict[str, Any]], stage: int) -> Optional[str]:
    """Create an IoT job targeting the stage cohort."""
    if not cohort:
        return None
    job_id = "{}-stage{}-{}".format(campaign["campaign_id"], stage, int(time.time()))
    targets = [d["thing_arn"] for d in cohort if d["thing_arn"]]
    if not targets:
        return None
    try:
        iot.create_job(
            jobId=job_id,
            targets=targets,
            document='{{"operation":"install","version":"{}"}}'.format(campaign["target_version"]),
            targetSelection="SNAPSHOT",
        )
    except ClientError as exc:
        logger.error("job_create_failed campaign=%s job=%s err=%s",
                     campaign["campaign_id"], job_id, exc)
        return None
    return job_id


def lambda_handler(event, context):
    """Entry point for the scheduled firmware rollout controller."""
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    detail = event.get("detail", {}) or {}
    campaign_id = detail.get("campaignId") or detail.get("campaign_id")
    if not campaign_id:
        logger.error("missing_campaign_id detail_keys=%s", sorted(detail.keys()))
        return {"advanced": False, "reason": "missing_campaign_id"}

    campaign = load_campaign(str(campaign_id))
    if campaign is None:
        logger.warning("campaign_not_found campaign=%s", campaign_id)
        return {"advanced": False, "reason": "campaign_not_found"}
    if campaign["status"] not in ("ACTIVE", "HOLD"):
        logger.info("campaign_inactive campaign=%s status=%s", campaign_id, campaign["status"])
        return {"advanced": False, "reason": "status_{}".format(campaign["status"].lower())}

    now = int(time.time())
    decision, rationale = evaluate_health_gate(campaign, now)
    logger.info("health_gate campaign=%s decision=%s rationale=%s",
                campaign_id, decision, rationale)

    if decision in ("HALT", "HOLD", "COMPLETE"):
        persist_stage(campaign_id, campaign["stage_index"],
                      "HALTED" if decision == "HALT" else
                      ("COMPLETED" if decision == "COMPLETE" else "HOLD"), now)
        return {"advanced": False, "decision": decision, "rationale": rationale,
                "stage_index": campaign["stage_index"]}

    next_index = campaign["stage_index"] + 1
    percentage = STAGE_PERCENTAGES[next_index]
    previous_version = detail.get("currentVersion") or detail.get("current_version") or ""
    eligible = list(iter_eligible_devices(previous_version))
    cohort = select_stage_cohort(eligible, percentage)
    job_id = create_job(campaign, cohort, percentage)
    persist_stage(campaign_id, next_index, "ACTIVE", now)

    logger.info("stage_advanced campaign=%s stage=%s pct=%s eligible=%s cohort=%s job=%s",
                campaign_id, next_index, percentage, len(eligible), len(cohort), job_id)
    return {
        "advanced": True,
        "decision": decision,
        "rationale": rationale,
        "stage_index": next_index,
        "stage_percentage": percentage,
        "eligible_devices": len(eligible),
        "cohort_size": len(cohort),
        "job_id": job_id,
    }
