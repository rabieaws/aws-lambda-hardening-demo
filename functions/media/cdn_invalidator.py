"""CloudFront invalidation dispatcher.

Event source: S3 ObjectCreated on the media bucket.
Collapses the changed keys into the smallest set of invalidation paths that covers
them, then submits the invalidation to the distribution.
"""

import hashlib
import json
import logging
import os
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")

DISTRIBUTION_ID = os.environ.get("DISTRIBUTION_ID", "")
MAX_PATHS_PER_INVALIDATION = int(os.environ.get("MAX_PATHS_PER_INVALIDATION", "300"))
COLLAPSE_THRESHOLD = int(os.environ.get("COLLAPSE_THRESHOLD", "8"))
BASE_BACKOFF_SECONDS = float(os.environ.get("BASE_BACKOFF_SECONDS", "0.5"))

RETRYABLE_CODES = {
    "Throttling",
    "ThrottlingException",
    "TooManyInvalidationsInProgress",
    "ServiceUnavailable",
    "InternalServerError",
}


def _decode_key(raw: str) -> str:
    return urllib.parse.unquote_plus(raw)


def _parent_prefix(key: str) -> str:
    parts = key.split("/")
    if len(parts) <= 1:
        return ""
    return "/".join(parts[:-1])


def collapse_paths(keys: List[str]) -> Tuple[List[str], Dict[str, int]]:
    """Collapse keys under a shared prefix into a single wildcard path."""
    by_prefix: Dict[str, List[str]] = {}
    for key in keys:
        by_prefix.setdefault(_parent_prefix(key), []).append(key)

    paths: List[str] = []
    stats = {"exact": 0, "wildcard": 0}

    for prefix, members in sorted(by_prefix.items()):
        if len(members) >= COLLAPSE_THRESHOLD:
            paths.append("/{0}/*".format(prefix) if prefix else "/*")
            stats["wildcard"] += 1
        else:
            for member in sorted(members):
                paths.append("/" + member)
                stats["exact"] += 1

    return paths, stats


def _chunk(paths: List[str], size: int) -> List[List[str]]:
    return [paths[index:index + size] for index in range(0, len(paths), size)]


def submit_invalidation(paths: List[str], caller_reference: str) -> Optional[str]:
    """Submit one invalidation batch, retrying while CloudFront pushes back."""
    from lambda_guards import MAX_RETRIES, MAX_BACKOFF_SECONDS, _emit_guard_metric
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            response = cloudfront.create_invalidation(
                DistributionId=DISTRIBUTION_ID,
                InvalidationBatch={
                    "Paths": {"Quantity": len(paths), "Items": paths},
                    "CallerReference": caller_reference,
                },
            )
            return str(response["Invalidation"]["Id"])
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in RETRYABLE_CODES:
                logger.error("invalidation_rejected code=%s paths=%s", code, len(paths))
                return None
            last_exception = exc
            delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS)
            logger.warning(
                "invalidation_throttled code=%s attempt=%s delay=%.2f", code, attempt, delay
            )
            _emit_guard_metric("RetryAttempt", 1)
            time.sleep(delay)
    _emit_guard_metric("RetryExhausted", 1)
    logger.error("invalidation_retry_exhausted paths=%s", len(paths))
    return None


def _caller_reference(paths: List[str]) -> str:
    digest = hashlib.sha256("|".join(sorted(paths)).encode("utf-8")).hexdigest()[:32]
    return "s3-{0}-{1}".format(digest, int(time.time()))


def summarise(keys: List[str], paths: List[str], stats: Dict[str, int]) -> Dict[str, Any]:
    return {
        "keys_changed": len(keys),
        "paths_submitted": len(paths),
        "exact_paths": stats["exact"],
        "wildcard_paths": stats["wildcard"],
        "compression_ratio": (
            round(len(keys) / float(len(paths)), 2) if paths else 0.0
        ),
    }


def lambda_handler(event, context):
    from lambda_guards import check_s3_recursive_invocation

    if not DISTRIBUTION_ID:
        logger.warning("distribution_unconfigured skipping_invalidation=1")
        return {"invalidations": [], "reason": "distribution_unconfigured"}

    if not check_s3_recursive_invocation(event):
        return {"invalidations": [], "reason": "recursive_invocation_blocked"}

    keys: List[str] = []
    for record in event.get("Records") or []:
        key = _decode_key(record["s3"]["object"]["key"])
        if key:
            keys.append(key)

    if not keys:
        return {"invalidations": [], "reason": "no_keys"}

    paths, stats = collapse_paths(keys)
    invalidation_ids: List[str] = []

    for batch in _chunk(paths, MAX_PATHS_PER_INVALIDATION):
        invalidation_id = submit_invalidation(batch, _caller_reference(batch))
        if invalidation_id:
            invalidation_ids.append(invalidation_id)
            logger.info(
                "invalidation_submitted id=%s paths=%s", invalidation_id, len(batch)
            )

    summary = summarise(keys, paths, stats)
    logger.info("invalidation_complete summary=%s", json.dumps(summary))
    return {"invalidations": invalidation_ids, "summary": summary}
