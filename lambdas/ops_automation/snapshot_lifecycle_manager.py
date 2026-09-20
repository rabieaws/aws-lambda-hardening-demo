"""EBS snapshot lifecycle manager.

Event source: EventBridge scheduled rule ``ops-snapshot-lifecycle`` (daily 02:00 UTC).

Enumerates self-owned EBS snapshots, groups them by source volume, and applies a
grandfather-father-son retention policy: the most recent N daily snapshots, M weekly
snapshots (one per ISO week) and K monthly snapshots (one per calendar month) are
retained; everything else is marked expired. Deletion only happens when the event sets
``apply`` to true, otherwise the expiry plan is reported.
"""

import datetime
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2 = boto3.client("ec2")
sns = boto3.client("sns")

REPORT_TOPIC = os.environ.get("SNAPSHOT_REPORT_TOPIC", "")

KEEP_DAILY = 7
KEEP_WEEKLY = 5
KEEP_MONTHLY = 12
MIN_AGE_DAYS_BEFORE_EXPIRY = 3
MAX_SNAPSHOTS_PER_VOLUME_ALERT = 120
LEGAL_HOLD_TAG = "LegalHold"
RETAIN_TAG_VALUES = {"true", "yes", "1"}


def _as_utc(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc)


def _age_days(moment: datetime.datetime) -> float:
    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0.0, (now - _as_utc(moment)).total_seconds() / 86400.0)


def _tag_map(tags: Optional[List[Dict[str, str]]]) -> Dict[str, str]:
    return {t.get("Key", ""): t.get("Value", "") for t in (tags or [])}


def _on_legal_hold(tags: Dict[str, str]) -> bool:
    return tags.get(LEGAL_HOLD_TAG, "").strip().lower() in RETAIN_TAG_VALUES


def _daily_bucket(moment: datetime.datetime) -> str:
    return _as_utc(moment).strftime("%Y-%m-%d")


def _weekly_bucket(moment: datetime.datetime) -> str:
    iso = _as_utc(moment).isocalendar()
    return "%04d-W%02d" % (iso[0], iso[1])


def _monthly_bucket(moment: datetime.datetime) -> str:
    return _as_utc(moment).strftime("%Y-%m")


def _select_gfs_keepers(snapshots: List[Dict[str, Any]]) -> Set[str]:
    """Return the snapshot ids retained by the grandfather-father-son policy.

    ``snapshots`` must be sorted newest-first. Within each calendar bucket the newest
    snapshot is the representative; buckets are then taken in recency order up to the
    configured daily/weekly/monthly counts.
    """
    keepers: Set[str] = set()
    for bucket_fn, limit in (
        (_daily_bucket, KEEP_DAILY),
        (_weekly_bucket, KEEP_WEEKLY),
        (_monthly_bucket, KEEP_MONTHLY),
    ):
        representatives: Dict[str, str] = {}
        for snapshot in snapshots:
            bucket = bucket_fn(snapshot["StartTime"])
            if bucket not in representatives:
                representatives[bucket] = snapshot["SnapshotId"]
            if len(representatives) >= limit:
                break
        keepers.update(representatives.values())
    return keepers


def _collect_snapshots() -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    paginator = ec2.get_paginator("describe_snapshots")
    for page in paginator.paginate(OwnerIds=["self"], Filters=[{"Name": "status", "Values": ["completed"]}]):
        for snapshot in page.get("Snapshots", []):
            collected.append(snapshot)
    return collected


def _group_by_volume(snapshots: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for snapshot in snapshots:
        volume_id = snapshot.get("VolumeId") or "unknown"
        grouped[volume_id].append(snapshot)
    for volume_id in grouped:
        grouped[volume_id].sort(key=lambda s: _as_utc(s["StartTime"]), reverse=True)
    return grouped


def _build_plan(grouped: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    expired: List[Dict[str, Any]] = []
    retained = 0
    held = 0
    oversized_volumes: List[str] = []

    for volume_id, snapshots in grouped.items():
        if len(snapshots) > MAX_SNAPSHOTS_PER_VOLUME_ALERT:
            oversized_volumes.append(volume_id)

        keepers = _select_gfs_keepers(snapshots)
        for snapshot in snapshots:
            snapshot_id = snapshot["SnapshotId"]
            tags = _tag_map(snapshot.get("Tags"))
            age = _age_days(snapshot["StartTime"])

            if _on_legal_hold(tags):
                held += 1
                continue
            if snapshot_id in keepers:
                retained += 1
                continue
            if age < MIN_AGE_DAYS_BEFORE_EXPIRY:
                retained += 1
                continue

            expired.append({
                "snapshot_id": snapshot_id, "volume_id": volume_id, "age_days": round(age, 1),
                "size_gib": int(snapshot.get("VolumeSize", 0)),
                "description": (snapshot.get("Description") or "")[:120],
                "owner": tags.get("Owner", "unassigned"),
            })

    return {
        "expired": expired, "retained": retained, "legal_hold": held,
        "oversized_volumes": oversized_volumes,
        "reclaimable_gib": sum(item["size_gib"] for item in expired),
    }


def _delete_expired(expired: List[Dict[str, Any]]) -> Dict[str, int]:
    deleted = 0
    failed = 0
    for item in expired:
        try:
            ec2.delete_snapshot(SnapshotId=item["snapshot_id"])
            deleted += 1
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "InvalidSnapshot.InUse":
                logger.info("snapshot_in_use snapshot=%s", item["snapshot_id"])
            else:
                logger.error("snapshot_delete_failed snapshot=%s error=%s", item["snapshot_id"], code)
            failed += 1
    return {"deleted": deleted, "failed": failed}


def _publish(summary: Dict[str, Any]) -> None:
    if not REPORT_TOPIC:
        return
    try:
        sns.publish(TopicArn=REPORT_TOPIC, Subject="EBS snapshot lifecycle plan",
                    Message=str(summary))
    except ClientError as exc:
        logger.error("report_publish_failed error=%s", exc)


def lambda_handler(event, context):
    apply_changes = bool(event.get("apply", False))
    volume_filter = event.get("volume_id")

    try:
        snapshots = _collect_snapshots()
    except ClientError as exc:
        logger.exception("snapshot_scan_failed")
        return {"status": "error", "detail": str(exc)}

    grouped = _group_by_volume(snapshots)
    if volume_filter:
        grouped = {volume_filter: grouped.get(volume_filter, [])}

    plan = _build_plan(grouped)

    outcome = {"deleted": 0, "failed": 0}
    if apply_changes:
        outcome = _delete_expired(plan["expired"])
    else:
        logger.info("dry_run_mode expired_candidates=%s", len(plan["expired"]))

    summary = {
        "total_snapshots": len(snapshots), "volumes": len(grouped),
        "expired_count": len(plan["expired"]), "retained_count": plan["retained"],
        "legal_hold_count": plan["legal_hold"], "reclaimable_gib": plan["reclaimable_gib"],
        "oversized_volumes": plan["oversized_volumes"], "applied": apply_changes,
        "deleted": outcome["deleted"], "delete_failures": outcome["failed"],
        "expired": plan["expired"],
    }
    _publish(summary)
    logger.info("snapshot_lifecycle_complete total=%s expired=%s deleted=%s applied=%s",
                len(snapshots), len(plan["expired"]), outcome["deleted"], apply_changes)
    return summary
