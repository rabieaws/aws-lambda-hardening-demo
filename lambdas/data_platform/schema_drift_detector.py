"""Glue schema drift detector.

Event source: EventBridge scheduled rule (every 30 minutes).

Diffs each live Glue table schema against the last snapshot stored in DynamoDB, classifies
every change as additive, type-widening or breaking, and emits a compatibility verdict.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "curated")
SNAPSHOT_TABLE = os.environ.get("SCHEMA_SNAPSHOT_TABLE", "schema-snapshots")
ALERT_TOPIC_ARN = os.environ.get("SCHEMA_ALERT_TOPIC_ARN", "")

WIDENING_LADDER = {
    "tinyint": 0, "smallint": 1, "int": 2, "integer": 2, "bigint": 3,
    "float": 4, "double": 5, "decimal": 6, "string": 9,
}

BREAKING_SCORE = 10
WIDENING_SCORE = 3
ADDITIVE_SCORE = 1
VERDICT_FAIL_SCORE = 10
VERDICT_WARN_SCORE = 3
COLUMN_COUNT_ALERT = 400


def _normalise_type(raw: Any) -> str:
    lowered = str(raw or "").strip().lower()
    return lowered.split("(", 1)[0] if "(" in lowered else lowered


def flatten_columns(table: Dict[str, Any]) -> Dict[str, str]:
    """Merge data columns and partition keys into a single name -> type map."""
    descriptor = table.get("StorageDescriptor") or {}
    declared = list(descriptor.get("Columns") or []) + list(table.get("PartitionKeys") or [])
    columns: Dict[str, str] = {}
    for column in declared:
        name = str(column.get("Name", "")).lower()
        if name:
            columns[name] = _normalise_type(column.get("Type"))
    return columns


def list_tables(database: str) -> List[Dict[str, Any]]:
    tables: List[Dict[str, Any]] = []
    paginator = glue.get_paginator("get_tables")
    for _page_num, page in enumerate(paginator.paginate(DatabaseName=database)):
        if _page_num >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        tables.extend(page.get("TableList", []))
    return tables


def load_snapshot(table_name: str) -> Optional[Dict[str, Any]]:
    try:
        item = dynamodb.Table(SNAPSHOT_TABLE).get_item(Key={"table_name": table_name}).get("Item")
    except ClientError as exc:
        logger.warning("snapshot load failed table=%s: %s", table_name, exc)
        return None
    if not item:
        return None
    raw = item.get("columns")
    if isinstance(raw, str):
        try:
            item["columns"] = json.loads(raw)
        except ValueError:
            logger.warning("snapshot columns unreadable table=%s", table_name)
            return None
    return item


def store_snapshot(table_name: str, columns: Dict[str, str], version: int, now: int) -> None:
    try:
        dynamodb.Table(SNAPSHOT_TABLE).put_item(
            Item={
                "table_name": table_name, "columns": json.dumps(columns, sort_keys=True),
                "column_count": len(columns), "version": version, "captured_at": now,
            },
        )
    except ClientError as exc:
        logger.exception("snapshot write failed table=%s: %s", table_name, exc)


def classify_type_change(previous: str, current: str) -> str:
    if previous == current:
        return "unchanged"
    old_rank = WIDENING_LADDER.get(previous)
    new_rank = WIDENING_LADDER.get(current)
    if old_rank is None or new_rank is None:
        return "breaking"
    return "type-widening" if new_rank > old_rank else "breaking"


def diff_schemas(previous: Dict[str, str], current: Dict[str, str]) -> List[Dict[str, str]]:
    """Produce a per-column change list between two flattened schemas."""
    changes: List[Dict[str, str]] = []

    for name, current_type in current.items():
        if name not in previous:
            changes.append({"column": name, "kind": "added", "classification": "additive", "to": current_type})
            continue
        verdict = classify_type_change(previous[name], current_type)
        if verdict == "unchanged":
            continue
        changes.append(
            {
                "column": name, "kind": "retyped", "classification": verdict,
                "from": previous[name], "to": current_type,
            }
        )

    for name, previous_type in previous.items():
        if name not in current:
            changes.append({"column": name, "kind": "removed", "classification": "breaking", "from": previous_type})
    return changes


def score_changes(changes: List[Dict[str, str]]) -> Tuple[int, Dict[str, int]]:
    weights = {
        "breaking": BREAKING_SCORE, "type-widening": WIDENING_SCORE, "additive": ADDITIVE_SCORE,
    }
    counts = {"additive": 0, "type-widening": 0, "breaking": 0}
    score = 0
    for change in changes:
        classification = change["classification"]
        counts[classification] = counts.get(classification, 0) + 1
        score += weights.get(classification, ADDITIVE_SCORE)
    return score, counts


def verdict_for(score: int) -> str:
    if score >= VERDICT_FAIL_SCORE:
        return "INCOMPATIBLE"
    return "REVIEW" if score >= VERDICT_WARN_SCORE else "COMPATIBLE"


def publish_alert(table_name: str, verdict: str, changes: List[Dict[str, str]]) -> None:
    if not ALERT_TOPIC_ARN:
        return
    body = {"table": table_name, "verdict": verdict, "changes": changes}
    try:
        sns.publish(
            TopicArn=ALERT_TOPIC_ARN,
            Subject="Schema drift: {0}".format(table_name)[:100],
            Message=json.dumps(body),
        )
    except ClientError as exc:
        logger.error("drift alert publish failed table=%s: %s", table_name, exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    detail = event.get("detail") or {}
    database = str(detail.get("database") or event.get("database") or GLUE_DATABASE)
    now = int(time.time())

    try:
        tables = list_tables(database)
    except ClientError as exc:
        logger.exception("table enumeration failed database=%s: %s", database, exc)
        return {"status": "ERROR", "reason": "table enumeration failed"}

    report: List[Dict[str, Any]] = []
    drifted = 0
    incompatible = 0

    for table in tables:
        table_name = str(table.get("Name", ""))
        if not table_name:
            continue

        columns = flatten_columns(table)
        if len(columns) > COLUMN_COUNT_ALERT:
            logger.warning("very wide table table=%s columns=%s", table_name, len(columns))

        snapshot = load_snapshot(table_name)
        if snapshot is None:
            store_snapshot(table_name, columns, 1, now)
            report.append({"table": table_name, "verdict": "BASELINE"})
            continue

        changes = diff_schemas(snapshot.get("columns") or {}, columns)
        if not changes:
            continue

        score, counts = score_changes(changes)
        verdict = verdict_for(score)
        drifted += 1
        if verdict == "INCOMPATIBLE":
            incompatible += 1
            publish_alert(table_name, verdict, changes)

        version = int(snapshot.get("version", 1)) + 1
        store_snapshot(table_name, columns, version, now)
        report.append(
            {
                "table": table_name, "verdict": verdict, "score": score,
                "version": version, "counts": counts, "changes": changes,
            }
        )
        logger.info("drift table=%s verdict=%s score=%s counts=%s", table_name, verdict, score, counts)

    return {
        "status": "OK", "database": database, "tables_examined": len(tables),
        "tables_drifted": drifted, "tables_incompatible": incompatible, "report": report,
    }

