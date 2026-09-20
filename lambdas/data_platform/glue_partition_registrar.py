"""Hive partition registrar for the curated data lake.

Event source: S3 ObjectCreated notifications on the curated bucket.

Derives Hive partition values from the object key path, reconciles them against the
partitions already registered in the Glue Data Catalog, and batch-registers whatever is
missing. Storage descriptors are cloned from the table definition so every partition keeps
the table's serde and columns.
"""

import logging
import os
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "curated")
CATALOG_ID = os.environ.get("GLUE_CATALOG_ID")

BATCH_REGISTER_SIZE = 100
PARTITION_DEPTH_LIMIT = 6
RECONCILE_WARN_THRESHOLD = 20000


def parse_partition_spec(key: str) -> Tuple[Optional[str], Dict[str, str]]:
    """Split ``table/k=v/k=v/file.parquet`` into a table name and ordered partition map."""
    segments = [segment for segment in key.split("/") if segment]
    if len(segments) < 2:
        return None, {}

    table_name = segments[0]
    spec: Dict[str, str] = {}
    for segment in segments[1:-1]:
        if "=" not in segment:
            continue
        name, _, value = segment.partition("=")
        name = name.strip().lower()
        value = urllib.parse.unquote(value.strip())
        if not name or not value:
            continue
        spec[name] = value
        if len(spec) >= PARTITION_DEPTH_LIMIT:
            break
    return table_name, spec


def partition_location(bucket: str, key: str, spec: Dict[str, str]) -> str:
    prefix = "/".join(key.split("/")[: 1 + len(spec)])
    return "s3://{0}/{1}/".format(bucket, prefix)


def describe_table(table_name: str) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"DatabaseName": GLUE_DATABASE, "Name": table_name}
    if CATALOG_ID:
        kwargs["CatalogId"] = CATALOG_ID
    return glue.get_table(**kwargs)["Table"]


def existing_partition_values(table_name: str) -> set:
    """Enumerate every partition currently registered for the table."""
    kwargs: Dict[str, Any] = {"DatabaseName": GLUE_DATABASE, "TableName": table_name}
    if CATALOG_ID:
        kwargs["CatalogId"] = CATALOG_ID

    known = set()
    paginator = glue.get_paginator("get_partitions")
    for _page_num, page in enumerate(paginator.paginate(**kwargs)):
        if _page_num >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for partition in page.get("Partitions", []):
            known.add(tuple(str(value) for value in partition.get("Values", [])))
    if len(known) > RECONCILE_WARN_THRESHOLD:
        logger.warning("wide partition set table=%s partitions=%s", table_name, len(known))
    return known


def ordered_values(spec: Dict[str, str], partition_keys: List[Dict[str, Any]]) -> Optional[List[str]]:
    """Project the parsed spec onto the table's declared partition key order."""
    values: List[str] = []
    for column in partition_keys:
        name = str(column.get("Name", "")).lower()
        if name not in spec:
            return None
        values.append(spec[name])
    return values


def build_partition_input(
    values: List[str], location: str, descriptor: Dict[str, Any]
) -> Dict[str, Any]:
    storage = dict(descriptor)
    storage["Location"] = location
    storage.pop("Parameters", None)
    return {"Values": values, "StorageDescriptor": storage}


def _chunk(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    return (items[start : start + size] for start in range(0, len(items), size))


def register_partitions(table_name: str, inputs: List[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    already_present = 0
    failed = 0

    for batch in _chunk(inputs, BATCH_REGISTER_SIZE):
        kwargs: Dict[str, Any] = {
            "DatabaseName": GLUE_DATABASE,
            "TableName": table_name,
            "PartitionInputList": batch,
        }
        if CATALOG_ID:
            kwargs["CatalogId"] = CATALOG_ID
        try:
            response = glue.batch_create_partition(**kwargs)
        except ClientError as exc:
            logger.exception("batch_create_partition failed table=%s: %s", table_name, exc)
            failed += len(batch)
            continue

        errors = response.get("Errors") or []
        for error in errors:
            code = (error.get("ErrorDetail") or {}).get("ErrorCode")
            if code == "AlreadyExistsException":
                already_present += 1
            else:
                logger.error(
                    "partition registration error table=%s values=%s code=%s",
                    table_name,
                    error.get("PartitionValues"),
                    code,
                )
                failed += 1
        created += len(batch) - len(errors)

    return {"created": created, "already_present": already_present, "failed": failed}


def _extract_records(event: Dict[str, Any]) -> List[Dict[str, str]]:
    extracted: List[Dict[str, str]] = []
    for record in event.get("Records", []):
        s3_block = record.get("s3") or {}
        bucket = ((s3_block.get("bucket") or {}).get("name")) or ""
        key = ((s3_block.get("object") or {}).get("key")) or ""
        if not bucket or not key or key.endswith("/"):
            continue
        extracted.append({"bucket": bucket, "key": urllib.parse.unquote_plus(key)})
    return extracted


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = _extract_records(event)
    if not records:
        logger.info("no usable s3 records in notification")
        return {"tables": 0, "registered": 0}

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    unparsed = 0

    for record in records:
        table_name, spec = parse_partition_spec(record["key"])
        if not table_name or not spec:
            unparsed += 1
            continue
        grouped.setdefault(table_name, []).append(
            {
                "spec": spec,
                "location": partition_location(record["bucket"], record["key"], spec),
            }
        )

    summary: Dict[str, Any] = {"tables": 0, "registered": 0, "skipped": unparsed, "details": {}}

    for table_name, candidates in grouped.items():
        try:
            table = describe_table(table_name)
        except ClientError as exc:
            logger.error("table lookup failed table=%s: %s", table_name, exc)
            continue

        partition_keys = table.get("PartitionKeys") or []
        descriptor = table.get("StorageDescriptor") or {}
        if not partition_keys:
            logger.warning("table is not partitioned table=%s", table_name)
            continue

        known = existing_partition_values(table_name)
        inputs: List[Dict[str, Any]] = []
        seen = set()

        for candidate in candidates:
            values = ordered_values(candidate["spec"], partition_keys)
            if values is None:
                unparsed += 1
                continue
            fingerprint = tuple(values)
            if fingerprint in known or fingerprint in seen:
                continue
            seen.add(fingerprint)
            inputs.append(build_partition_input(values, candidate["location"], descriptor))

        if not inputs:
            continue

        result = register_partitions(table_name, inputs)
        summary["tables"] += 1
        summary["registered"] += result["created"]
        summary["details"][table_name] = result
        logger.info("partitions registered table=%s result=%s", table_name, result)

    summary["skipped"] = unparsed
    return summary
