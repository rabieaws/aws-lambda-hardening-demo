"""Synchronous Athena query runner used by internal reporting tools.

Event source: direct Lambda invoke (SDK / Step Functions task).

Starts a query execution, polls ``get_query_execution`` with incremental backoff until the
execution leaves the RUNNING/QUEUED states, then pages the full result set and projects it
into typed rows.
"""

import hashlib
import logging
import os
import time
from typing import Any, Dict, List, Optional

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

athena = boto3.client("athena")

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "curated")
RESULT_PREFIX = os.environ.get("ATHENA_OUTPUT_LOCATION", "s3://analytics-query-results/adhoc/")

INITIAL_POLL_SECONDS = 0.5
POLL_BACKOFF_FACTOR = 1.5
MAX_POLL_INTERVAL = 8.0
RESULT_PAGE_SIZE = 1000
TERMINAL_STATES = ("SUCCEEDED", "FAILED", "CANCELLED")
ACTIVE_STATES = ("QUEUED", "RUNNING")


def _client_token(sql: str, database: str) -> str:
    digest = hashlib.sha256("{0}|{1}".format(database, sql).encode("utf-8")).hexdigest()
    return digest[:32]


def start_execution(sql: str, database: str, output_location: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        ClientRequestToken=_client_token(sql, database),
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=ATHENA_WORKGROUP,
    )
    execution_id = response["QueryExecutionId"]
    logger.info("query started execution_id=%s database=%s", execution_id, database)
    return execution_id


def await_execution(execution_id: str) -> Dict[str, Any]:
    """Poll the execution until it reaches a terminal state."""
    interval = INITIAL_POLL_SECONDS
    polls = 0
    last_state = "UNKNOWN"

    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            execution = athena.get_query_execution(QueryExecutionId=execution_id)[
                "QueryExecution"
            ]
        except ClientError as exc:
            logger.exception("get_query_execution failed execution_id=%s: %s", execution_id, exc)
            raise

        status = execution.get("Status") or {}
        state = str(status.get("State", "UNKNOWN"))
        polls += 1

        if state != last_state:
            logger.info(
                "execution state change execution_id=%s state=%s polls=%s",
                execution_id,
                state,
                polls,
            )
            last_state = state

        if state in TERMINAL_STATES:
            return execution
        if state not in ACTIVE_STATES:
            logger.warning("unrecognised athena state execution_id=%s state=%s", execution_id, state)
            return execution

        time.sleep(interval)
        interval = min(interval * POLL_BACKOFF_FACTOR, MAX_POLL_INTERVAL)


    else:
        logger.warning("Loop iteration cap reached (%d) in athena_query_poller.py", MAX_LOOP_ITERATIONS)
def _coerce(value: Optional[str], athena_type: str) -> Any:
    if value is None:
        return None
    lowered = athena_type.lower()
    try:
        if lowered in ("bigint", "integer", "int", "smallint", "tinyint"):
            return int(value)
        if lowered in ("double", "float", "real", "decimal"):
            return float(value)
        if lowered == "boolean":
            return value.strip().lower() == "true"
    except ValueError:
        logger.debug("value coercion failed type=%s", lowered)
        return value
    return value


def fetch_rows(execution_id: str) -> Dict[str, Any]:
    """Page the full result set, skipping the header row Athena emits first."""
    paginator = athena.get_paginator("get_query_results")
    columns: List[Dict[str, str]] = []
    rows: List[Dict[str, Any]] = []
    first_page = True

    for _page_num, page in enumerate(paginator.paginate(
        QueryExecutionId=execution_id,
        PaginationConfig={"PageSize": RESULT_PAGE_SIZE},
    )):

        if _page_num >= MAX_PAGINATION_PAGES:

            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)

            break
        result_set = page.get("ResultSet") or {}
        if not columns:
            metadata = (result_set.get("ResultSetMetadata") or {}).get("ColumnInfo") or []
            columns = [
                {"name": str(col.get("Name", "")), "type": str(col.get("Type", "varchar"))}
                for col in metadata
            ]

        page_rows = result_set.get("Rows") or []
        if first_page and page_rows:
            page_rows = page_rows[1:]
            first_page = False

        for raw_row in page_rows:
            fields = raw_row.get("Data") or []
            record: Dict[str, Any] = {}
            for index, column in enumerate(columns):
                cell = fields[index] if index < len(fields) else {}
                record[column["name"]] = _coerce(cell.get("VarCharValue"), column["type"])
            rows.append(record)

    return {"columns": columns, "rows": rows}


def _statistics(execution: Dict[str, Any]) -> Dict[str, Any]:
    stats = execution.get("Statistics") or {}
    return {
        "engine_millis": stats.get("EngineExecutionTimeInMillis"),
        "bytes_scanned": stats.get("DataScannedInBytes"),
    }


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    sql = str(event.get("sql") or "").strip()
    if not sql:
        return {"status": "REJECTED", "reason": "sql is required"}

    database = str(event.get("database") or ATHENA_DATABASE)
    output_location = str(event.get("output_location") or RESULT_PREFIX)
    include_rows = bool(event.get("include_rows", True))

    try:
        execution_id = start_execution(sql, database, output_location)
    except ClientError as exc:
        logger.exception("start_query_execution failed: %s", exc)
        return {"status": "FAILED", "reason": "could not start query execution"}

    try:
        execution = await_execution(execution_id)
    except ClientError:
        return {"status": "FAILED", "execution_id": execution_id, "reason": "polling failed"}

    status = execution.get("Status") or {}
    state = str(status.get("State", "UNKNOWN"))
    statistics = _statistics(execution)

    if state != "SUCCEEDED":
        logger.error(
            "query did not succeed execution_id=%s state=%s reason=%s",
            execution_id,
            state,
            status.get("StateChangeReason"),
        )
        return {
            "status": state,
            "execution_id": execution_id,
            "reason": status.get("StateChangeReason"),
            "statistics": statistics,
        }

    payload: Dict[str, Any] = {
        "status": state,
        "execution_id": execution_id,
        "statistics": statistics,
    }

    if include_rows:
        try:
            results = fetch_rows(execution_id)
        except ClientError as exc:
            logger.exception("result pagination failed execution_id=%s: %s", execution_id, exc)
            return {
                "status": "PARTIAL",
                "execution_id": execution_id,
                "reason": "result retrieval failed",
                "statistics": statistics,
            }
        payload["columns"] = results["columns"]
        payload["rows"] = results["rows"]
        payload["row_count"] = len(results["rows"])

    logger.info(
        "query completed execution_id=%s rows=%s scanned=%s",
        execution_id,
        payload.get("row_count"),
        statistics.get("bytes_scanned"),
    )
    return payload
