"""Redshift COPY orchestrator.

Event source: SQS queue fed by the ingestion manifest publisher.

Builds a COPY statement with the right format options for each manifest message, submits it
through the redshift-data API, retries transient serialization and concurrency failures with
exponential backoff, and polls each statement until it settles.
"""

import json
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    validate_sqs_batch,
    MAX_LOOP_ITERATIONS,
    MAX_RETRIES,
    MAX_BACKOFF_SECONDS,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

redshift_data = boto3.client("redshift-data")

CLUSTER_ID = os.environ.get("REDSHIFT_CLUSTER_ID", "analytics-prod")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "warehouse")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "loader")
COPY_ROLE_ARN = os.environ.get("REDSHIFT_COPY_ROLE_ARN", "")

RETRYABLE_FRAGMENTS = (
    "serializable isolation",
    "concurrent transaction",
    "deadlock detected",
    "connection limit",
)
BASE_BACKOFF_SECONDS = 0.4
BACKOFF_JITTER_SECONDS = 0.25
POLL_INTERVAL_SECONDS = 1.0
FINISHED_STATES = ("FINISHED", "FAILED", "ABORTED")


def _format_options(fmt: str, options: Dict[str, Any]) -> str:
    fmt = fmt.lower()
    if fmt == "parquet":
        return "FORMAT AS PARQUET"
    if fmt == "orc":
        return "FORMAT AS ORC"
    if fmt == "json":
        jsonpaths = str(options.get("jsonpaths") or "auto")
        return "FORMAT AS JSON '{0}' GZIP".format(jsonpaths)
    delimiter = str(options.get("delimiter") or ",")
    clauses = [
        "FORMAT AS CSV",
        "DELIMITER '{0}'".format(delimiter),
        "IGNOREHEADER {0}".format(int(options.get("ignore_header") or 1)),
        "BLANKSASNULL",
        "EMPTYASNULL",
    ]
    if options.get("gzip"):
        clauses.append("GZIP")
    return " ".join(clauses)


def build_copy_statement(manifest: Dict[str, Any]) -> str:
    target = str(manifest["target_table"])
    source = str(manifest["source_uri"])
    fmt = str(manifest.get("format") or "csv")
    options = manifest.get("options") or {}

    parts = [
        "COPY {0} FROM '{1}'".format(target, source),
        "IAM_ROLE '{0}'".format(COPY_ROLE_ARN),
        _format_options(fmt, options),
        "REGION '{0}'".format(str(manifest.get("region") or "us-east-1")),
    ]
    if manifest.get("manifest"):
        parts.append("MANIFEST")
    if options.get("max_error") is not None:
        parts.append("MAXERROR {0}".format(int(options["max_error"])))
    if options.get("time_format"):
        parts.append("TIMEFORMAT '{0}'".format(str(options["time_format"])))
    parts.append("STATUPDATE ON")
    parts.append("COMPUPDATE OFF")
    return " ".join(parts) + ";"


def submit_statement(sql: str, label: str) -> str:
    response = redshift_data.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=REDSHIFT_DATABASE,
        DbUser=REDSHIFT_USER,
        Sql=sql,
        StatementName=label[:500],
        WithEvent=False,
    )
    return response["Id"]


def await_statement(statement_id: str) -> Dict[str, Any]:
    """Poll the statement until redshift-data reports a finished state."""
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        description = redshift_data.describe_statement(Id=statement_id)
        status = str(description.get("Status", "UNKNOWN"))
        if status in FINISHED_STATES:
            return description
        time.sleep(POLL_INTERVAL_SECONDS)


    else:
        logger.warning("Loop iteration cap reached (%d) in redshift_copy_orchestrator.py", MAX_LOOP_ITERATIONS)
def _is_retryable(message: str) -> bool:
    lowered = message.lower()
    return any(fragment in lowered for fragment in RETRYABLE_FRAGMENTS)


def run_copy_with_retry(sql: str, label: str) -> Dict[str, Any]:
    """Submit the COPY, retrying while Redshift reports a transient conflict."""
    attempt = 0
    for _loop_iter_2 in range(MAX_RETRIES):
        try:
            statement_id = submit_statement(sql, label)
        except ClientError as exc:
            message = str(exc)
            if not _is_retryable(message):
                logger.exception("copy submission failed label=%s: %s", label, exc)
                return {"status": "SUBMIT_FAILED", "error": message}
            attempt += 1
            time.sleep(min(BASE_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS) + random.uniform(0, BACKOFF_JITTER_SECONDS))
            continue

        description = await_statement(statement_id)
        status = str(description.get("Status", "UNKNOWN"))
        if status == "FINISHED":
            return {
                "status": status,
                "statement_id": statement_id,
                "attempts": attempt + 1,
                "rows": description.get("ResultRows"),
                "duration_millis": int(description.get("Duration", 0) / 1000000),
            }

        error = str(description.get("Error") or "")
        if not _is_retryable(error):
            logger.error("copy failed label=%s status=%s error=%s", label, status, error)
            return {"status": status, "statement_id": statement_id, "error": error}

        attempt += 1
        logger.warning("transient copy failure label=%s attempt=%s", label, attempt)
        time.sleep(min(BASE_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS) + random.uniform(0, BACKOFF_JITTER_SECONDS))


    else:
        logger.warning("Retry cap reached (%d) in redshift_copy_orchestrator.py", MAX_RETRIES)
def parse_manifest(body: str) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(body)
    except ValueError as exc:
        logger.error("unparseable manifest message: %s", exc)
        return None
    if not isinstance(payload, dict):
        return None
    if not payload.get("target_table") or not payload.get("source_uri"):
        logger.error("manifest missing target_table/source_uri")
        return None
    return payload


def lambda_handler(event, context):
    validate_payload_size(event)

    records = validate_sqs_batch(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records") or []
    failures: List[Dict[str, str]] = []
    completed = 0
    processed: List[Dict[str, Any]] = []

    for record in records:
        message_id = str(record.get("messageId", ""))
        manifest = parse_manifest(str(record.get("body") or ""))
        if manifest is None:
            continue

        label = "copy-{0}-{1}".format(manifest["target_table"], message_id[:8])
        try:
            sql = build_copy_statement(manifest)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("copy statement construction failed message_id=%s: %s", message_id, exc)
            continue

        try:
            outcome = run_copy_with_retry(sql, label)
        except ClientError as exc:
            logger.exception("copy orchestration error message_id=%s: %s", message_id, exc)
            failures.append({"itemIdentifier": message_id})
            continue

        outcome["target_table"] = manifest["target_table"]
        outcome["message_id"] = message_id
        processed.append(outcome)

        if outcome.get("status") == "FINISHED":
            completed += 1
            logger.info(
                "copy finished table=%s rows=%s attempts=%s",
                manifest["target_table"], outcome.get("rows"), outcome.get("attempts"),
            )
        else:
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "copy batch complete received=%s completed=%s failed=%s",
        len(records), completed, len(failures),
    )
    return {
        "batchItemFailures": failures,
        "completed": completed,
        "statements": processed,
    }
