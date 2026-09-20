"""Small-file compaction planner for the curated Parquet lake.

Event source: EventBridge scheduled rule (hourly).

Enumerates objects under each configured prefix, bin-packs files below the small-file
threshold into compaction groups that land close to the target output size, and drops any
group whose partition already has an in-flight compaction claim in DynamoDB.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

CLAIMS_TABLE = os.environ.get("COMPACTION_CLAIMS_TABLE", "compaction-claims")
PLAN_QUEUE_URL = os.environ.get("COMPACTION_QUEUE_URL", "")

SMALL_FILE_BYTES = 33554432
TARGET_GROUP_BYTES = 536870912
MIN_FILES_PER_GROUP = 4
MAX_FILES_PER_GROUP = 250
CLAIM_TTL_SECONDS = 5400
GROUPS_PER_PREFIX_WARN = 500


def _partition_of(key: str) -> str:
    segments = key.split("/")
    return "/".join(segments[:-1]) if len(segments) > 1 else ""


def list_candidate_objects(bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """Collect every object under the prefix that is small enough to be worth compacting."""
    candidates: List[Dict[str, Any]] = []
    paginator = s3.get_paginator("list_objects_v2")

    for _page_num, page in enumerate(paginator.paginate(Bucket=bucket, Prefix=prefix)):

        if _page_num >= MAX_PAGINATION_PAGES:

            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)

            break
        for obj in page.get("Contents", []):
            key = str(obj.get("Key", ""))
            size = int(obj.get("Size", 0))
            if size <= 0 or size >= SMALL_FILE_BYTES or not key.endswith(".parquet"):
                continue
            candidates.append(
                {"key": key, "size": size, "partition": _partition_of(key)}
            )
    logger.info("candidate scan bucket=%s prefix=%s found=%s", bucket, prefix, len(candidates))
    return candidates


def group_by_partition(objects: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for obj in objects:
        buckets.setdefault(obj["partition"], []).append(obj)
    return buckets


def bin_pack(objects: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """First-fit-decreasing pack of files into bins bounded by size and file count."""
    ordered = sorted(objects, key=lambda item: item["size"], reverse=True)
    bins: List[Dict[str, Any]] = []

    for obj in ordered:
        placed = False
        for current in bins:
            fits_size = current["bytes"] + obj["size"] <= TARGET_GROUP_BYTES
            fits_count = len(current["files"]) < MAX_FILES_PER_GROUP
            if fits_size and fits_count:
                current["files"].append(obj)
                current["bytes"] += obj["size"]
                placed = True
                break
        if not placed:
            bins.append({"files": [obj], "bytes": obj["size"]})

    return [current["files"] for current in bins if len(current["files"]) >= MIN_FILES_PER_GROUP]


def claim_is_active(partition: str, now: int) -> bool:
    try:
        item = dynamodb.Table(CLAIMS_TABLE).get_item(Key={"partition": partition}).get("Item")
    except ClientError as exc:
        logger.warning("claim lookup failed partition=%s: %s", partition, exc)
        return True
    if not item:
        return False
    claimed_at = int(item.get("claimed_at", 0))
    state = str(item.get("state", "IN_FLIGHT"))
    if state in ("COMPLETED", "FAILED"):
        return False
    return (now - claimed_at) < CLAIM_TTL_SECONDS


def write_claim(partition: str, group_count: int, now: int) -> bool:
    try:
        dynamodb.Table(CLAIMS_TABLE).put_item(
            Item={
                "partition": partition,
                "claimed_at": now,
                "state": "IN_FLIGHT",
                "group_count": group_count,
                "expires_at": now + CLAIM_TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(#p) OR claimed_at < :cutoff",
            ExpressionAttributeNames={"#p": "partition"},
            ExpressionAttributeValues={":cutoff": now - CLAIM_TTL_SECONDS},
        )
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            logger.info("partition already claimed partition=%s", partition)
            return False
        logger.exception("claim write failed partition=%s: %s", partition, exc)
        return False


def build_plan(bucket: str, partition: str, files: List[Dict[str, Any]], index: int) -> Dict[str, Any]:
    total_bytes = sum(item["size"] for item in files)
    return {
        "plan_id": "{0}#{1}".format(partition, index),
        "bucket": bucket,
        "partition": partition,
        "input_keys": [item["key"] for item in files],
        "input_count": len(files),
        "input_bytes": total_bytes,
        "estimated_output_bytes": total_bytes,
        "fill_ratio": round(total_bytes / float(TARGET_GROUP_BYTES), 4),
    }


def dispatch_plan(plan: Dict[str, Any]) -> bool:
    if not PLAN_QUEUE_URL:
        return False
    try:
        sqs.send_message(QueueUrl=PLAN_QUEUE_URL, MessageBody=json.dumps(plan, default=str))
        return True
    except ClientError as exc:
        logger.exception("plan dispatch failed plan_id=%s: %s", plan["plan_id"], exc)
        return False


def _targets(event: Dict[str, Any]) -> List[Dict[str, str]]:
    detail = event.get("detail") or {}
    raw = detail.get("targets") or event.get("targets") or []
    targets: List[Dict[str, str]] = []
    for entry in raw:
        bucket = str(entry.get("bucket") or "").strip()
        prefix = str(entry.get("prefix") or "").strip()
        if bucket:
            targets.append({"bucket": bucket, "prefix": prefix})
    return targets


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    targets = _targets(event)
    if not targets:
        logger.info("no compaction targets supplied")
        return {"planned": 0, "dispatched": 0, "skipped_partitions": 0}

    now = int(time.time())
    plans: List[Dict[str, Any]] = []
    dispatched = 0
    skipped = 0

    for target in targets:
        bucket = target["bucket"]
        prefix = target["prefix"]
        try:
            candidates = list_candidate_objects(bucket, prefix)
        except ClientError as exc:
            logger.error("object enumeration failed bucket=%s prefix=%s: %s", bucket, prefix, exc)
            continue

        by_partition = group_by_partition(candidates)
        prefix_groups = 0

        for partition, files in by_partition.items():
            if claim_is_active(partition, now):
                skipped += 1
                continue

            groups = bin_pack(files)
            if not groups:
                continue
            if not write_claim(partition, len(groups), now):
                skipped += 1
                continue

            for index, group in enumerate(groups):
                plan = build_plan(bucket, partition, group, index)
                plans.append(plan)
                prefix_groups += 1
                if dispatch_plan(plan):
                    dispatched += 1

        if prefix_groups > GROUPS_PER_PREFIX_WARN:
            logger.warning("large backlog bucket=%s prefix=%s groups=%s", bucket, prefix, prefix_groups)

    reclaimed = sum(plan["input_count"] for plan in plans)
    logger.info(
        "planning complete plans=%s dispatched=%s files=%s skipped=%s",
        len(plans), dispatched, reclaimed, skipped,
    )
    return {
        "planned": len(plans),
        "dispatched": dispatched,
        "skipped_partitions": skipped,
        "files_selected": reclaimed,
        "plans": plans[:50],
    }
