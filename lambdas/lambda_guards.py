"""Shared Lambda security guards for DDoS, DoS, and Denial of Wallet protection.

All thresholds are configurable via environment variables with sensible defaults.
Only uses the Python standard library (no additional dependencies required).
"""

import json
import logging
import os
import sys
import time

logger = logging.getLogger()

# ---------------------------------------------------------------------------
# Configurable thresholds (all overridable via environment variables)
# ---------------------------------------------------------------------------
MAX_PAYLOAD_SIZE_BYTES = int(os.environ.get("MAX_PAYLOAD_SIZE_BYTES", str(256 * 1024)))
MAX_INVOCATION_DEPTH = int(os.environ.get("MAX_INVOCATION_DEPTH", "3"))
MAX_LOOP_ITERATIONS = int(os.environ.get("MAX_LOOP_ITERATIONS", "1000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
MAX_BACKOFF_SECONDS = int(os.environ.get("MAX_BACKOFF_SECONDS", "10"))
MAX_PAGINATION_PAGES = int(os.environ.get("MAX_PAGINATION_PAGES", "100"))
MAX_QUERY_PARAMS = int(os.environ.get("MAX_QUERY_PARAMS", "20"))
MAX_HEADER_SIZE = int(os.environ.get("MAX_HEADER_SIZE", "8192"))
MAX_BODY_SIZE = int(os.environ.get("MAX_BODY_SIZE", str(128 * 1024)))
MAX_BATCH_SIZE = int(os.environ.get("MAX_BATCH_SIZE", "10"))
MAX_MESSAGE_SIZE = int(os.environ.get("MAX_MESSAGE_SIZE", str(256 * 1024)))
EXPECTED_SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "input/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
MIN_REMAINING_MS = int(os.environ.get("MIN_REMAINING_MS", "5000"))


# ---------------------------------------------------------------------------
# Phase 2: Payload size validation
# ---------------------------------------------------------------------------
def validate_payload_size(event):
    """Reject oversized payloads early to prevent resource exhaustion."""
    payload_size = sys.getsizeof(json.dumps(event) if not isinstance(event, str) else event)
    if payload_size > MAX_PAYLOAD_SIZE_BYTES:
        logger.warning(
            "Payload rejected: size %d bytes exceeds limit %d bytes",
            payload_size,
            MAX_PAYLOAD_SIZE_BYTES,
        )
        raise ValueError(
            "Payload size %d bytes exceeds maximum allowed %d bytes"
            % (payload_size, MAX_PAYLOAD_SIZE_BYTES)
        )


# ---------------------------------------------------------------------------
# Phase 3: Recursive invocation detection
# ---------------------------------------------------------------------------
def check_s3_recursive_invocation(event):
    """Prevent recursive invocation from S3 events by validating key prefix."""
    for record in event.get("Records", []):
        key = record.get("s3", {}).get("object", {}).get("key", "")
        if not key.startswith(EXPECTED_SOURCE_PREFIX):
            logger.warning(
                "Potential recursive invocation detected: S3 key '%s' "
                "does not match expected source prefix '%s'",
                key,
                EXPECTED_SOURCE_PREFIX,
            )
            return False
    return True


def check_invocation_depth(event):
    """Prevent recursive loops by tracking invocation depth via message attributes."""
    for record in event.get("Records", []):
        attributes = record.get("messageAttributes", {})
        depth = int(
            attributes.get("invocation_depth", {}).get("stringValue", "0")
        )
        if depth >= MAX_INVOCATION_DEPTH:
            logger.warning(
                "Max invocation depth %d reached. Stopping to prevent recursive loop.",
                MAX_INVOCATION_DEPTH,
            )
            return False
    return True


def get_invocation_depth(event):
    """Extract the current invocation depth from event message attributes."""
    for record in event.get("Records", []):
        attributes = record.get("messageAttributes", {})
        return int(
            attributes.get("invocation_depth", {}).get("stringValue", "0")
        )
    return 0


def increment_invocation_depth(current_depth):
    """Increment depth counter when sending messages that may re-trigger this function."""
    return {
        "invocation_depth": {
            "DataType": "Number",
            "StringValue": str(current_depth + 1),
        }
    }


# ---------------------------------------------------------------------------
# Phase 4: Remaining time check
# ---------------------------------------------------------------------------
def check_remaining_time(context, min_remaining_ms=None):
    """Ensure sufficient execution time remains to prevent timeout-driven retries."""
    threshold = min_remaining_ms or MIN_REMAINING_MS
    remaining = context.get_remaining_time_in_millis()
    if remaining < threshold:
        logger.warning(
            "Insufficient time remaining: %d ms. Exiting early to prevent retry.",
            remaining,
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Phase 4: Unbounded loop and retry protection
# ---------------------------------------------------------------------------
def safe_iterate(iterable, max_items=None):
    """Iterate with a hard cap to prevent unbounded loops."""
    limit = max_items or MAX_LOOP_ITERATIONS
    for i, item in enumerate(iterable):
        if i >= limit:
            logger.warning("Iteration cap reached at %d items. Breaking loop.", limit)
            break
        yield item


def retry_with_limit(func, max_retries=None, max_backoff=None):
    """Execute function with bounded retries and capped exponential backoff."""
    retries = max_retries or MAX_RETRIES
    backoff_cap = max_backoff or MAX_BACKOFF_SECONDS
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt == retries - 1:
                logger.error("Max retries (%d) exhausted: %s", retries, str(e))
                raise
            wait = min(2 ** attempt, backoff_cap)
            logger.warning(
                "Attempt %d failed, retrying in %ds: %s",
                attempt + 1,
                wait,
                str(e),
            )
            time.sleep(wait)


def safe_paginate(paginator, max_pages=None, **kwargs):
    """Paginate with a cap on number of pages to prevent runaway API calls."""
    limit = max_pages or MAX_PAGINATION_PAGES
    for i, page in enumerate(paginator.paginate(**kwargs)):
        if i >= limit:
            logger.warning("Pagination cap reached at %d pages.", limit)
            break
        yield page


# ---------------------------------------------------------------------------
# Phase 6: Event source-specific guards
# ---------------------------------------------------------------------------
def validate_api_gateway_event(event):
    """Validate API Gateway event structure to reject suspicious requests.

    Returns an error response dict if validation fails, or None if valid.
    """
    params = event.get("queryStringParameters") or {}
    if len(params) > MAX_QUERY_PARAMS:
        return {"statusCode": 400, "body": json.dumps({"error": "Too many parameters"})}

    body = event.get("body") or ""
    if len(body) > MAX_BODY_SIZE:
        return {"statusCode": 413, "body": json.dumps({"error": "Request body too large"})}

    headers = event.get("headers") or {}
    total_header_size = sum(len(k) + len(v) for k, v in headers.items())
    if total_header_size > MAX_HEADER_SIZE:
        return {"statusCode": 431, "body": json.dumps({"error": "Headers too large"})}

    return None


def validate_sqs_batch(event):
    """Validate SQS batch event to prevent oversized batch processing."""
    records = event.get("Records", [])
    if len(records) > MAX_BATCH_SIZE:
        logger.warning(
            "SQS batch size %d exceeds maximum %d", len(records), MAX_BATCH_SIZE
        )
        return records[:MAX_BATCH_SIZE]
    return records
