"""Shared guard functions for Lambda security hardening.

Provides payload validation, iteration caps, retry limits, pagination caps,
remaining-time checks, recursive invocation detection, and EMF metric emission.
"""

import json
import logging
import os
import sys
import time

logger = logging.getLogger()

_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")


# ---------------------------------------------------------------------------
# Belt-and-braces threshold parsing
# ---------------------------------------------------------------------------

def parse_int_env(env_var, default):
    """Parse int from env var, falling back to default on any parse error."""
    try:
        return int(os.environ.get(env_var, str(default)))
    except (ValueError, TypeError):
        logger.warning("Malformed env var %s, falling back to default %d", env_var, default)
        return default


def parse_float_env(env_var, default):
    """Parse float from env var, falling back to default on any parse error."""
    try:
        return float(os.environ.get(env_var, str(default)))
    except (ValueError, TypeError):
        logger.warning("Malformed env var %s, falling back to default %s", env_var, default)
        return default


MAX_PAYLOAD_SIZE_BYTES = parse_int_env("MAX_PAYLOAD_SIZE_BYTES", 256 * 1024)
MAX_INVOCATION_DEPTH = parse_int_env("MAX_INVOCATION_DEPTH", 3)
MAX_LOOP_ITERATIONS = parse_int_env("MAX_LOOP_ITERATIONS", 1000)
MAX_RETRIES = max(1, parse_int_env("MAX_RETRIES", 3))
MAX_BACKOFF_SECONDS = parse_float_env("MAX_BACKOFF_SECONDS", 10.0)
MAX_PAGINATION_PAGES = parse_int_env("MAX_PAGINATION_PAGES", 100)
MIN_REMAINING_MS = parse_int_env("MIN_REMAINING_MS", 5000)


# ---------------------------------------------------------------------------
# EMF metric emission
# ---------------------------------------------------------------------------

def _emit_guard_metric(metric_name, value, unit="Count"):
    """Emit a CloudWatch metric via EMF. Zero external dependencies."""
    metric_payload = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "LambdaGuards",
                "Dimensions": [["FunctionName", "GuardType"]],
                "Metrics": [{"Name": "GuardActivation", "Unit": unit}]
            }]
        },
        "FunctionName": _FUNCTION_NAME,
        "GuardType": metric_name,
        "GuardActivation": value
    }
    print(json.dumps(metric_payload))


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class IterationCapExceeded(Exception):
    """Raised when a decision-driving loop hits its iteration cap."""
    pass


class PermanentError(Exception):
    """Raised for records that will never succeed (oversized, depth-exceeded, malformed)."""
    pass


# ---------------------------------------------------------------------------
# Payload size validation
# ---------------------------------------------------------------------------

def validate_payload_size(event):
    """Reject oversized payloads early. For single-event sources only."""
    payload_size = sys.getsizeof(json.dumps(event) if not isinstance(event, str) else event)
    if payload_size > MAX_PAYLOAD_SIZE_BYTES:
        logger.warning(
            "Payload rejected: size %d bytes exceeds limit %d bytes",
            payload_size, MAX_PAYLOAD_SIZE_BYTES
        )
        _emit_guard_metric("PayloadRejected", 1)
        raise ValueError(
            "Payload size %d bytes exceeds maximum allowed %d bytes"
            % (payload_size, MAX_PAYLOAD_SIZE_BYTES)
        )


def validate_record_size(record, max_size=None):
    """Validate individual record size for batch sources."""
    import base64
    limit = max_size or MAX_PAYLOAD_SIZE_BYTES
    if "kinesis" in record:
        body = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
    else:
        body = record.get("body", "")
    size = len(body.encode("utf-8") if isinstance(body, str) else body)
    if size > limit:
        _emit_guard_metric("RecordPayloadRejected", 1)
        raise PermanentError("Record size %d exceeds limit %d" % (size, limit))


# ---------------------------------------------------------------------------
# Remaining time check
# ---------------------------------------------------------------------------

def check_remaining_time(context, min_remaining_ms=None):
    """Check execution time remaining. NEVER place at handler entry."""
    threshold = min_remaining_ms if min_remaining_ms is not None else MIN_REMAINING_MS
    remaining = context.get_remaining_time_in_millis()
    if remaining < threshold:
        logger.warning("Insufficient time remaining: %d ms", remaining)
        _emit_guard_metric("TimeoutEarlyExit", 1)
        return False
    return True


# ---------------------------------------------------------------------------
# Safe iteration
# ---------------------------------------------------------------------------

def safe_iterate(iterable, max_items=None, fail_on_cap=False):
    """Iterate with a hard cap to prevent unbounded loops."""
    limit = max_items or MAX_LOOP_ITERATIONS
    for i, item in enumerate(iterable):
        if i >= limit:
            _emit_guard_metric("IterationCapReached", 1)
            if fail_on_cap:
                raise IterationCapExceeded(
                    "Iteration cap %d reached on decision-driving operation. "
                    "Refusing to act on partial data." % limit
                )
            logger.warning("Iteration cap reached at %d items. Breaking loop.", limit)
            break
        yield item


# ---------------------------------------------------------------------------
# Safe paginator
# ---------------------------------------------------------------------------

def safe_paginate(paginator, max_pages=None, fail_on_cap=False, **kwargs):
    """Paginate with a cap on number of pages."""
    limit = max_pages or MAX_PAGINATION_PAGES
    for i, page in enumerate(paginator.paginate(**kwargs)):
        if i >= limit:
            _emit_guard_metric("PaginationCapReached", 1)
            if fail_on_cap:
                raise IterationCapExceeded(
                    "Pagination cap %d reached on decision-driving query. "
                    "Refusing to act on partial data." % limit
                )
            logger.warning("Pagination cap reached at %d pages.", limit)
            break
        yield page


# ---------------------------------------------------------------------------
# Retry with limit
# ---------------------------------------------------------------------------

def retry_with_limit(func, max_retries=None, max_backoff=None):
    """Execute function with bounded retries and capped exponential backoff."""
    retries = MAX_RETRIES if max_retries is None else max_retries
    backoff_cap = MAX_BACKOFF_SECONDS if max_backoff is None else max_backoff
    last_exception = None
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            last_exception = e
            wait = min(2 ** attempt, backoff_cap)
            logger.warning(
                "Attempt %d/%d failed, retrying in %ds: %s",
                attempt + 1, retries, wait, str(e)
            )
            _emit_guard_metric("RetryAttempt", 1)
            time.sleep(wait)
    logger.error("Max retries (%d) exhausted: %s", retries, str(last_exception))
    _emit_guard_metric("RetryExhausted", 1)
    raise last_exception


# ---------------------------------------------------------------------------
# Recursive invocation detection -- S3
# ---------------------------------------------------------------------------

EXPECTED_SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "input/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")


def check_s3_recursive_invocation(event):
    """Prevent recursive invocation from S3 events."""
    for record in event.get("Records", []):
        key = record.get("s3", {}).get("object", {}).get("key", "")
        if not key.startswith(EXPECTED_SOURCE_PREFIX):
            logger.warning(
                "Potential recursive invocation: S3 key '%s' does not match prefix '%s'",
                key, EXPECTED_SOURCE_PREFIX
            )
            _emit_guard_metric("RecursiveInvocationBlocked", 1)
            return False
    return True


# ---------------------------------------------------------------------------
# Recursive invocation detection -- SQS depth tracking
# ---------------------------------------------------------------------------

def check_sqs_invocation_depth(record):
    """Check invocation depth for an SQS record."""
    attributes = record.get("messageAttributes", {})
    depth = int(attributes.get("invocation_depth", {}).get("stringValue", "0"))
    if depth >= MAX_INVOCATION_DEPTH:
        logger.warning("Max invocation depth %d reached on SQS record.", MAX_INVOCATION_DEPTH)
        _emit_guard_metric("DepthLimitReached", 1)
        return False, depth
    return True, depth


def increment_sqs_depth(current_depth):
    """Increment depth for outbound SQS send_message."""
    return {
        "invocation_depth": {
            "DataType": "Number",
            "StringValue": str(current_depth + 1)
        }
    }


# ---------------------------------------------------------------------------
# Recursive invocation detection -- SNS depth tracking
# ---------------------------------------------------------------------------

def check_sns_invocation_depth(record):
    """Check invocation depth for an SNS record.

    SNS path: record["Sns"]["MessageAttributes"]["invocation_depth"]["Value"]
    NOT the SQS path.
    """
    attributes = record.get("Sns", {}).get("MessageAttributes", {})
    depth = int(attributes.get("invocation_depth", {}).get("Value", "0"))
    if depth >= MAX_INVOCATION_DEPTH:
        logger.warning("Max invocation depth %d reached on SNS record.", MAX_INVOCATION_DEPTH)
        _emit_guard_metric("DepthLimitReached", 1)
        return False, depth
    return True, depth


def increment_sns_depth(current_depth):
    """Increment depth for outbound SNS publish."""
    return {
        "invocation_depth": {
            "DataType": "Number",
            "StringValue": str(current_depth + 1)
        }
    }


# ---------------------------------------------------------------------------
# Recursive invocation detection -- EventBridge depth tracking
# ---------------------------------------------------------------------------

def check_eventbridge_invocation_depth(detail):
    """Check invocation depth from EventBridge detail."""
    depth = int(detail.get("_invocation_depth", 0))
    if depth >= MAX_INVOCATION_DEPTH:
        logger.warning("Max invocation depth %d reached on EventBridge event.", MAX_INVOCATION_DEPTH)
        _emit_guard_metric("DepthLimitReached", 1)
        return False, depth
    return True, depth


# ---------------------------------------------------------------------------
# Recursive invocation detection -- Lambda self-invoke depth tracking
# ---------------------------------------------------------------------------

def check_invoke_depth(event):
    """Check invocation depth from event payload for Lambda self-invoke chains."""
    depth = event.get("_invocation_depth", 0)
    if depth >= MAX_INVOCATION_DEPTH:
        logger.warning("Max self-invoke depth %d reached.", MAX_INVOCATION_DEPTH)
        _emit_guard_metric("SelfInvokeDepthLimitReached", 1)
        return False, depth
    return True, depth
