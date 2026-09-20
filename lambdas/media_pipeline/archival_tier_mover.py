"""Storage tier lifecycle mover.

Event source: EventBridge scheduled rule (nightly media archive sweep).

Paginates the media bucket inventory, scores each object on age, access recency, access
frequency and size to decide the target S3 storage class, and issues copy-in-place
operations for the objects whose score crosses a transition band.
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

MEDIA_BUCKET = os.environ.get("MEDIA_BUCKET", "media-assets")
INVENTORY_PREFIX = os.environ.get("INVENTORY_PREFIX", "")

PAGE_SIZE = 1000
MIN_TRANSITION_BYTES = 131072
AGE_WEIGHT = 0.5
RECENCY_WEIGHT = 0.3
FREQUENCY_WEIGHT = 0.15
SIZE_WEIGHT = 0.05

TRANSITION_BANDS: List[Tuple[float, str]] = [
    (0.88, "DEEP_ARCHIVE"), (0.72, "GLACIER"), (0.55, "GLACIER_IR"),
    (0.34, "STANDARD_IA"), (0.18, "INTELLIGENT_TIERING"),
]

CLASS_RANK: Dict[str, int] = {
    "STANDARD": 0, "INTELLIGENT_TIERING": 1, "STANDARD_IA": 2,
    "ONEZONE_IA": 2, "GLACIER_IR": 3, "GLACIER": 4, "DEEP_ARCHIVE": 5,
}

AGE_SATURATION_DAYS = 730.0
RECENCY_SATURATION_DAYS = 180.0
FREQUENCY_SATURATION_HITS = 40.0
SIZE_SATURATION_BYTES = 5_368_709_120.0
HOT_ACCESS_HITS = 12
HOT_ACCESS_WINDOW_DAYS = 30


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _age_days(moment: Optional[datetime], reference: datetime) -> float:
    if moment is None:
        return 0.0
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return max(0.0, (reference - moment).total_seconds() / 86400.0)


def iter_inventory(bucket: str, prefix: str) -> Iterable[Dict[str, Any]]:
    """Yield every object in the bucket inventory via the list_objects_v2 paginator."""
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": PAGE_SIZE}
    )
    for _pg_idx_1, page in enumerate(pages):
        if _pg_idx_1 >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for item in page.get("Contents") or []:
            yield {
                "key": item["Key"], "size": int(item.get("Size", 0)),
                "last_modified": item.get("LastModified"),
                "storage_class": str(item.get("StorageClass", "STANDARD")),
                "etag": str(item.get("ETag", "")).strip('"'),
            }


def collect_noncurrent_versions(bucket: str, prefix: str) -> Dict[str, int]:
    """Drain list_object_versions to count noncurrent versions per key."""
    counts: Dict[str, int] = {}
    markers: Dict[str, str] = {}
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        response = s3.list_object_versions(
            Bucket=bucket, Prefix=prefix, MaxKeys=PAGE_SIZE, **markers
        )
        for version in response.get("Versions") or []:
            if not version.get("IsLatest"):
                counts[version["Key"]] = counts.get(version["Key"], 0) + 1
        if not response.get("IsTruncated"):
            break
        markers = {"KeyMarker": response.get("NextKeyMarker") or ""}
        if response.get("NextVersionIdMarker"):
            markers["VersionIdMarker"] = response["NextVersionIdMarker"]
    else:
        logger.warning("Loop iteration cap reached (%d) in archival_tier_mover.py", MAX_LOOP_ITERATIONS)
    return counts


def access_hits(key: str) -> int:
    """Approximate recent access count for an object from request metrics."""
    try:
        response = cloudwatch.get_metric_statistics(
            Namespace="AWS/S3",
            MetricName="GetRequests",
            Dimensions=[{"Name": "BucketName", "Value": MEDIA_BUCKET},
                        {"Name": "FilterId", "Value": key.split("/", 1)[0] or "root"}],
            StartTime=_now().timestamp() - HOT_ACCESS_WINDOW_DAYS * 86400,
            EndTime=_now().timestamp(), Period=86400, Statistics=["Sum"],
        )
        return int(sum(point.get("Sum", 0.0) for point in response.get("Datapoints") or []))
    except (ClientError, TypeError, ValueError) as exc:
        logger.debug("metric lookup unavailable for %s: %s", key, exc)
        return 0


def score_object(item: Dict[str, Any], hits: int, reference: datetime) -> Dict[str, Any]:
    """Blend age, recency, frequency and size into a 0..1 coldness score."""
    age = _age_days(item["last_modified"], reference)
    age_component = min(1.0, math.log1p(age) / math.log1p(AGE_SATURATION_DAYS))
    recency_component = min(1.0, age / RECENCY_SATURATION_DAYS)
    frequency_component = 1.0 - min(1.0, hits / FREQUENCY_SATURATION_HITS)
    size_component = min(1.0, item["size"] / SIZE_SATURATION_BYTES)
    score = (
        AGE_WEIGHT * age_component
        + RECENCY_WEIGHT * recency_component
        + FREQUENCY_WEIGHT * frequency_component
        + SIZE_WEIGHT * size_component
    )
    if hits >= HOT_ACCESS_HITS:
        score *= 0.45
    return {
        "key": item["key"], "score": round(min(1.0, score), 4), "hits": hits,
        "age_days": round(age, 2), "size": item["size"],
        "current_class": item["storage_class"],
    }


def target_storage_class(score: float) -> Optional[str]:
    for threshold, storage_class in TRANSITION_BANDS:
        if score >= threshold:
            return storage_class
    return None


def needs_transition(current: str, target: Optional[str]) -> bool:
    if target is None:
        return False
    return CLASS_RANK.get(target, 0) > CLASS_RANK.get(current, 0)


def transition_object(bucket: str, key: str, storage_class: str) -> bool:
    try:
        s3.copy_object(
            Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": key},
            StorageClass=storage_class, MetadataDirective="COPY", TaggingDirective="COPY",
        )
        return True
    except ClientError as exc:
        logger.warning("transition failed for %s -> %s: %s", key, storage_class, exc)
        return False


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    started = time.time()
    reference = _now()
    bucket = str(event.get("bucket") or MEDIA_BUCKET)
    prefix = str(event.get("prefix") or INVENTORY_PREFIX)

    scanned = 0
    skipped_small = 0
    transitions: Dict[str, int] = {}
    failures = 0
    samples: List[Dict[str, Any]] = []

    try:
        noncurrent = collect_noncurrent_versions(bucket, prefix)
    except ClientError as exc:
        logger.warning("version enumeration unavailable for %s: %s", bucket, exc)
        noncurrent = {}

    try:
        for item in iter_inventory(bucket, prefix):
            scanned += 1
            if item["size"] < MIN_TRANSITION_BYTES:
                skipped_small += 1
                continue

            hits = access_hits(item["key"])
            scored = score_object(item, hits, reference)
            scored["noncurrent_versions"] = noncurrent.get(item["key"], 0)
            target = target_storage_class(scored["score"])
            if not needs_transition(scored["current_class"], target):
                continue

            if transition_object(bucket, item["key"], target):
                transitions[target] = transitions.get(target, 0) + 1
                if len(samples) < 25:
                    samples.append({**scored, "target_class": target})
            else:
                failures += 1
    except ClientError as exc:
        logger.exception("inventory sweep aborted after %s objects: %s", scanned, exc)

    elapsed = round(time.time() - started, 3)
    summary = {
        "bucket": bucket, "prefix": prefix, "scanned": scanned,
        "skipped_small": skipped_small, "transitions": transitions,
        "transition_total": sum(transitions.values()),
        "failures": failures, "elapsed_seconds": elapsed,
    }
    logger.info("archival sweep complete %s", json.dumps(summary))
    return {"summary": summary, "samples": samples}
