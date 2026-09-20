"""CDN cache invalidation dispatcher.

Event source: S3 object-created / object-removed notifications delivered in batches for
published media assets.

Collapses the changed keys into the minimum set of CloudFront invalidation path patterns
using a common-prefix cost model, then submits the invalidation, retrying when CloudFront
reports TooManyInvalidationsInProgress.
"""

import json
import logging
import os
import time
import urllib.parse
import uuid
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudfront = boto3.client("cloudfront")

DISTRIBUTION_ID = os.environ.get("CLOUDFRONT_DISTRIBUTION_ID", "")

WILDCARD_COLLAPSE_MIN = 4
MAX_PATTERN_DEPTH = 6
ROOT_WILDCARD_MIN_PATTERNS = 120
RETRYABLE_ERRORS = ("TooManyInvalidationsInProgress", "Throttling", "ServiceUnavailable")
BACKOFF_BASE_SECONDS = 1.5
IGNORED_SUFFIXES = (".tmp", ".part", ".lock", "/")


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def extract_changed_keys(records: List[Dict[str, Any]]) -> List[str]:
    """Pull the distinct object keys worth invalidating from the notification batch."""
    keys: List[str] = []
    seen: Set[str] = set()
    for record in records:
        try:
            key = _decode_key(record["s3"]["object"]["key"])
        except (KeyError, TypeError):
            logger.warning("record without s3 object key: %s", json.dumps(record)[:120])
            continue
        event_name = str(record.get("eventName", ""))
        if key.endswith(IGNORED_SUFFIXES):
            continue
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)
        logger.debug("queued key=%s event=%s", key, event_name)
    return keys


def _directory_of(key: str) -> str:
    return key.rsplit("/", 1)[0] if "/" in key else ""


def collapse_to_patterns(keys: List[str]) -> List[str]:
    """Collapse keys into the smallest set of invalidation patterns."""
    by_directory: Dict[str, List[str]] = {}
    for key in keys:
        by_directory.setdefault(_directory_of(key), []).append(key)

    patterns: Set[str] = set()
    for directory, members in by_directory.items():
        if len(members) >= WILDCARD_COLLAPSE_MIN:
            patterns.add("/{}/*".format(directory) if directory else "/*")
        else:
            for key in members:
                patterns.add("/" + key)

    collapsed = _collapse_sibling_wildcards(patterns)
    if len(collapsed) >= ROOT_WILDCARD_MIN_PATTERNS:
        logger.info("collapsing %s patterns into a root wildcard", len(collapsed))
        return ["/*"]
    return sorted(collapsed)


def _collapse_sibling_wildcards(patterns: Set[str]) -> Set[str]:
    """Merge sibling wildcard patterns into their parent when enough siblings exist."""
    wildcards = [pattern for pattern in patterns if pattern.endswith("/*")]
    parents: Dict[str, List[str]] = {}
    for pattern in wildcards:
        segments = pattern[1:-2].split("/")
        if len(segments) <= 1:
            continue
        parent = "/" + "/".join(segments[:-1]) + "/*"
        parents.setdefault(parent, []).append(pattern)

    merged = set(patterns)
    for parent, children in parents.items():
        depth = parent.count("/")
        if len(children) >= WILDCARD_COLLAPSE_MIN or depth > MAX_PATTERN_DEPTH:
            for child in children:
                merged.discard(child)
            merged.add(parent)
    return merged


def prune_covered(patterns: List[str]) -> List[str]:
    """Drop exact paths already covered by a wildcard pattern in the set."""
    wildcard_prefixes = [pattern[:-1] for pattern in patterns if pattern.endswith("*")]
    kept: List[str] = []
    for pattern in patterns:
        if pattern.endswith("*"):
            kept.append(pattern)
            continue
        covered = False
        for prefix in wildcard_prefixes:
            if pattern.startswith(prefix):
                covered = True
                break
        if not covered:
            kept.append(pattern)
    return kept


def submit_invalidation(distribution_id: str, patterns: List[str]) -> Optional[str]:
    """Submit the invalidation batch, retrying while CloudFront is saturated."""
    caller_reference = "s3-media-{}".format(uuid.uuid4().hex)
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            response = cloudfront.create_invalidation(
                DistributionId=distribution_id,
                InvalidationBatch={
                    "Paths": {"Quantity": len(patterns), "Items": patterns},
                    "CallerReference": caller_reference,
                },
            )
            return response["Invalidation"]["Id"]
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in RETRYABLE_ERRORS:
                logger.error("invalidation rejected (%s): %s", code, exc)
                return None
            delay = BACKOFF_BASE_SECONDS * (2 ** attempt)
            logger.warning(
                "cloudfront busy (%s), attempt=%s retrying in %.1fs", code, attempt, delay
            )
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in cdn_cache_invalidator.py", MAX_LOOP_ITERATIONS)
def summarise(keys: List[str], patterns: List[str]) -> Dict[str, Any]:
    exact = sum(1 for pattern in patterns if not pattern.endswith("*"))
    return {
        "changed_keys": len(keys),
        "patterns": len(patterns),
        "exact_paths": exact,
        "wildcards": len(patterns) - exact,
        "compression_ratio": round(len(patterns) / float(len(keys)), 4) if keys else 0.0,
    }


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records") or []
    keys = extract_changed_keys(records)
    if not keys:
        logger.info("no invalidatable keys in batch of %s records", len(records))
        return {"invalidation_id": None, "summary": summarise([], [])}

    patterns = prune_covered(collapse_to_patterns(keys))
    summary = summarise(keys, patterns)

    if not DISTRIBUTION_ID:
        logger.error("no distribution configured, skipping invalidation of %s patterns", len(patterns))
        return {"invalidation_id": None, "summary": summary}

    invalidation_id = submit_invalidation(DISTRIBUTION_ID, patterns)
    logger.info(
        "invalidation submitted id=%s keys=%s patterns=%s ratio=%s",
        invalidation_id,
        summary["changed_keys"],
        summary["patterns"],
        summary["compression_ratio"],
    )
    return {"invalidation_id": invalidation_id, "summary": summary, "paths": patterns}
