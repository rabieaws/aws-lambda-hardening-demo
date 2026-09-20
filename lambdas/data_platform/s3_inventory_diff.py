"""Cross-bucket S3 inventory reconciliation.

Event source: EventBridge scheduled rule (nightly).

Pages the object inventory of a source and a replica bucket, indexes both by relative key,
and computes the added / removed / changed sets using etag and size comparison. The
resulting reconciliation summary is written back to S3 as a JSON report.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

REPORT_BUCKET = os.environ.get("RECONCILIATION_REPORT_BUCKET", "")
REPORT_PREFIX = os.environ.get("RECONCILIATION_REPORT_PREFIX", "inventory-diff/")

INVENTORY_PAGE_SIZE = 1000
LARGE_INVENTORY_WARN = 250000
DRIFT_ALERT_RATIO = 0.05
SAMPLE_KEYS_IN_REPORT = 200


def _normalise_etag(raw: Any) -> str:
    return str(raw or "").strip().strip('"')


def _relative(key: str, prefix: str) -> str:
    if prefix and key.startswith(prefix):
        return key[len(prefix) :]
    return key


def build_inventory(bucket: str, prefix: str) -> Dict[str, Dict[str, Any]]:
    """Index every object under the prefix by its prefix-relative key."""
    inventory: Dict[str, Dict[str, Any]] = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={"PageSize": INVENTORY_PAGE_SIZE},
    ):
        for obj in page.get("Contents", []):
            key = str(obj.get("Key", ""))
            if not key or key.endswith("/"):
                continue
            inventory[_relative(key, prefix)] = {
                "key": key,
                "size": int(obj.get("Size", 0)),
                "etag": _normalise_etag(obj.get("ETag")),
                "storage_class": str(obj.get("StorageClass") or "STANDARD"),
            }

    if len(inventory) > LARGE_INVENTORY_WARN:
        logger.warning("large inventory bucket=%s prefix=%s objects=%s", bucket, prefix, len(inventory))
    logger.info("inventory built bucket=%s prefix=%s objects=%s", bucket, prefix, len(inventory))
    return inventory


def classify_change(source: Dict[str, Any], replica: Dict[str, Any]) -> Optional[str]:
    if source["size"] != replica["size"]:
        return "size-mismatch"
    if source["etag"] and replica["etag"] and source["etag"] != replica["etag"]:
        return "content-mismatch"
    if source["storage_class"] != replica["storage_class"]:
        return "storage-class-mismatch"
    return None


def diff_inventories(
    source: Dict[str, Dict[str, Any]], replica: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Compute added, removed and changed sets between two indexed inventories."""
    added: List[str] = []
    removed: List[str] = []
    changed: List[Dict[str, Any]] = []
    matched = 0
    source_bytes = 0
    replica_bytes = 0

    for relative_key, entry in source.items():
        source_bytes += entry["size"]
        counterpart = replica.get(relative_key)
        if counterpart is None:
            added.append(relative_key)
            continue
        reason = classify_change(entry, counterpart)
        if reason is None:
            matched += 1
            continue
        changed.append(
            {
                "key": relative_key,
                "reason": reason,
                "source_size": entry["size"],
                "replica_size": counterpart["size"],
            }
        )

    for relative_key, entry in replica.items():
        replica_bytes += entry["size"]
        if relative_key not in source:
            removed.append(relative_key)

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "matched": matched,
        "source_objects": len(source),
        "replica_objects": len(replica),
        "source_bytes": source_bytes,
        "replica_bytes": replica_bytes,
    }


def drift_verdict(diff: Dict[str, Any]) -> Tuple[str, float]:
    total = max(diff["source_objects"], diff["replica_objects"], 1)
    divergent = len(diff["added"]) + len(diff["removed"]) + len(diff["changed"])
    ratio = divergent / float(total)
    if divergent == 0:
        return "IN_SYNC", 0.0
    return ("DRIFTED" if ratio > DRIFT_ALERT_RATIO else "MINOR_DRIFT"), round(ratio, 6)


def write_report(report: Dict[str, Any], now: int) -> Optional[str]:
    if not REPORT_BUCKET:
        return None
    key = "{0}{1}/reconciliation.json".format(REPORT_PREFIX, now)
    try:
        s3.put_object(
            Bucket=REPORT_BUCKET,
            Key=key,
            Body=json.dumps(report, default=str).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as exc:
        logger.exception("report write failed key=%s: %s", key, exc)
        return None
    return "s3://{0}/{1}".format(REPORT_BUCKET, key)


def _pair(event: Dict[str, Any]) -> Dict[str, str]:
    detail = event.get("detail") or {}
    config = detail.get("pair") or event.get("pair") or {}
    return {
        "source_bucket": str(config.get("source_bucket") or ""),
        "source_prefix": str(config.get("source_prefix") or ""),
        "replica_bucket": str(config.get("replica_bucket") or ""),
        "replica_prefix": str(config.get("replica_prefix") or ""),
    }


def lambda_handler(event, context):
    pair = _pair(event)
    if not pair["source_bucket"] or not pair["replica_bucket"]:
        logger.error("reconciliation pair incomplete pair=%s", pair)
        return {"status": "REJECTED", "reason": "source_bucket and replica_bucket required"}

    now = int(time.time())

    try:
        source_inventory = build_inventory(pair["source_bucket"], pair["source_prefix"])
        replica_inventory = build_inventory(pair["replica_bucket"], pair["replica_prefix"])
    except ClientError as exc:
        logger.exception("inventory enumeration failed: %s", exc)
        return {"status": "ERROR", "reason": "inventory enumeration failed"}

    diff = diff_inventories(source_inventory, replica_inventory)
    verdict, ratio = drift_verdict(diff)

    summary = {
        "generated_at": now,
        "source": "s3://{0}/{1}".format(pair["source_bucket"], pair["source_prefix"]),
        "replica": "s3://{0}/{1}".format(pair["replica_bucket"], pair["replica_prefix"]),
        "verdict": verdict,
        "drift_ratio": ratio,
        "matched": diff["matched"],
        "added_count": len(diff["added"]),
        "removed_count": len(diff["removed"]),
        "changed_count": len(diff["changed"]),
        "source_objects": diff["source_objects"],
        "replica_objects": diff["replica_objects"],
        "byte_delta": diff["source_bytes"] - diff["replica_bytes"],
        "added_sample": diff["added"][:SAMPLE_KEYS_IN_REPORT],
        "removed_sample": diff["removed"][:SAMPLE_KEYS_IN_REPORT],
        "changed_sample": diff["changed"][:SAMPLE_KEYS_IN_REPORT],
    }

    report_uri = write_report(summary, now)
    if report_uri:
        summary["report_uri"] = report_uri

    logger.info(
        "reconciliation complete verdict=%s added=%s removed=%s changed=%s matched=%s",
        verdict, len(diff["added"]), len(diff["removed"]), len(diff["changed"]), diff["matched"],
    )
    summary["status"] = "OK"
    return summary
