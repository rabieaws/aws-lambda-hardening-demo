"""SSM patch compliance reporter.

Event source: EventBridge scheduled rule ``ops-patch-compliance-reporter`` (daily 08:45 UTC).

Enumerates SSM managed instances, pulls their patch compliance items, rolls the results up
into per-account and per-patch-group compliance percentages weighted by patch severity,
and flags instances whose missing patches are past the severity-specific remediation SLA.
The reporter is read-only and publishes its rollup to SNS.
"""

import datetime
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
sns = boto3.client("sns")

COMPLIANCE_TOPIC = os.environ.get("PATCH_COMPLIANCE_TOPIC", "")

SEVERITY_WEIGHTS = {
    "CRITICAL": 10.0, "HIGH": 6.0, "MEDIUM": 3.0,
    "LOW": 1.0, "INFORMATIONAL": 0.5, "UNSPECIFIED": 1.0,
}
SEVERITY_SLA_DAYS = {
    "CRITICAL": 7, "HIGH": 14, "MEDIUM": 30,
    "LOW": 90, "INFORMATIONAL": 180, "UNSPECIFIED": 30,
}
NON_COMPLIANT_STATES = {"MISSING", "FAILED", "NOT_APPLICABLE_PENDING"}
COMPLIANT_STATES = {"INSTALLED", "INSTALLED_OTHER", "INSTALLED_PENDING_REBOOT", "INSTALLED_REJECTED"}
GROUP_COMPLIANCE_TARGET = 95.0
GROUP_WARNING_FLOOR = 85.0
MAX_FLAGGED_INSTANCES = 250
UNKNOWN_PATCH_GROUP = "unassigned"


def _as_utc(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc)


def _age_days(moment: Optional[datetime.datetime]) -> float:
    if moment is None:
        return 0.0
    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0.0, (now - _as_utc(moment)).total_seconds() / 86400.0)


def _normalize_severity(raw: Any) -> str:
    severity = str(raw or "UNSPECIFIED").upper()
    return severity if severity in SEVERITY_WEIGHTS else "UNSPECIFIED"


def _list_instances() -> List[Dict[str, Any]]:
    instances: List[Dict[str, Any]] = []
    paginator = ssm.get_paginator("describe_instance_patch_states")
    inventory = ssm.get_paginator("describe_instance_information")

    patch_groups: Dict[str, str] = {}
    for page in inventory.paginate():
        for info in page.get("InstanceInformationList", []):
            patch_groups[info["InstanceId"]] = info.get("ComputerName", "")

    instance_ids = list(patch_groups.keys())
    for offset in range(0, len(instance_ids), 50):
        chunk = instance_ids[offset:offset + 50]
        for page in paginator.paginate(InstanceIds=chunk):
            for state in page.get("InstancePatchStates", []):
                state["_computer_name"] = patch_groups.get(state["InstanceId"], "")
                instances.append(state)
    return instances


def _compliance_items(instance_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    paginator = ssm.get_paginator("list_compliance_items")
    try:
        pages = paginator.paginate(
            ResourceIds=[instance_id], ResourceTypes=["ManagedInstance"],
            Filters=[{"Key": "ComplianceType", "Values": ["Patch"], "Type": "EQUAL"}])
        for page in pages:
            items.extend(page.get("ComplianceItems", []))
    except ClientError as exc:
        logger.warning("compliance_items_failed instance=%s error=%s", instance_id,
                       exc.response.get("Error", {}).get("Code"))
    return items


def _weighted_instance_score(items: List[Dict[str, Any]]) -> Tuple[float, float, List[Dict[str, Any]]]:
    """Return ``(earned_weight, total_weight, sla_breaches)`` for one instance."""
    earned = 0.0
    total = 0.0
    breaches: List[Dict[str, Any]] = []

    for item in items:
        severity = _normalize_severity(item.get("Severity"))
        weight = SEVERITY_WEIGHTS[severity]
        status = str(item.get("Status", "")).upper()
        state = str((item.get("Details") or {}).get("PatchState", status)).upper()
        total += weight

        if status == "COMPLIANT" or state in COMPLIANT_STATES:
            earned += weight
            continue

        if state in NON_COMPLIANT_STATES or status == "NON_COMPLIANT":
            age = _age_days(item.get("ExecutionSummary", {}).get("ExecutionTime"))
            sla = SEVERITY_SLA_DAYS[severity]
            if age > sla:
                breaches.append({
                    "patch_id": item.get("Id", "unknown"), "severity": severity,
                    "title": (item.get("Title") or "")[:120], "state": state,
                    "age_days": round(age, 1), "sla_days": sla,
                    "days_over_sla": round(age - sla, 1),
                })

    return earned, total, breaches


def _rollup(records: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"earned": 0.0, "total": 0.0, "instances": 0.0, "breaching": 0.0})
    for record in records:
        bucket = grouped[record[key]]
        bucket["earned"] += record["earned_weight"]
        bucket["total"] += record["total_weight"]
        bucket["instances"] += 1
        if record["sla_breaches"]:
            bucket["breaching"] += 1

    rollup: List[Dict[str, Any]] = []
    for name, totals in grouped.items():
        pct = (totals["earned"] / totals["total"] * 100.0) if totals["total"] > 0 else 100.0
        if pct >= GROUP_COMPLIANCE_TARGET:
            status = "on-target"
        elif pct >= GROUP_WARNING_FLOOR:
            status = "warning"
        else:
            status = "breach"
        rollup.append({
            key: name, "weighted_compliance_pct": round(pct, 2), "status": status,
            "instance_count": int(totals["instances"]),
            "instances_past_sla": int(totals["breaching"]),
        })
    rollup.sort(key=lambda r: r["weighted_compliance_pct"])
    return rollup


def _evaluate(instances: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for state in instances:
        instance_id = state["InstanceId"]
        items = _compliance_items(instance_id)
        earned, total, breaches = _weighted_instance_score(items)
        pct = (earned / total * 100.0) if total > 0 else 100.0
        records.append({
            "instance_id": instance_id, "baseline_id": state.get("BaselineId", ""),
            "patch_group": state.get("PatchGroup") or UNKNOWN_PATCH_GROUP,
            "account_id": str(state.get("OwnerInformation") or state.get("AccountId") or "self"),
            "computer_name": state.get("_computer_name", ""),
            "missing_count": int(state.get("MissingCount", 0)),
            "failed_count": int(state.get("FailedCount", 0)),
            "installed_count": int(state.get("InstalledCount", 0)),
            "last_operation_days": round(_age_days(state.get("OperationEndTime")), 1),
            "earned_weight": round(earned, 2), "total_weight": round(total, 2),
            "weighted_compliance_pct": round(pct, 2), "sla_breaches": breaches,
        })
    return records


def _publish(summary: Dict[str, Any]) -> None:
    if not COMPLIANCE_TOPIC:
        return
    try:
        sns.publish(TopicArn=COMPLIANCE_TOPIC, Subject="Patch compliance rollup",
                    Message=str(summary))
    except ClientError as exc:
        logger.error("compliance_publish_failed error=%s", exc)


def lambda_handler(event, context):
    notify = bool(event.get("notify", True))
    patch_group_filter = event.get("patch_group")

    try:
        instances = _list_instances()
    except ClientError as exc:
        logger.exception("instance_enumeration_failed")
        return {"status": "error", "detail": str(exc)}

    records = _evaluate(instances)
    if patch_group_filter:
        records = [r for r in records if r["patch_group"] == patch_group_filter]

    flagged = [r for r in records if r["sla_breaches"]]
    flagged.sort(key=lambda r: len(r["sla_breaches"]), reverse=True)

    fleet_total = sum(r["total_weight"] for r in records)
    fleet_earned = sum(r["earned_weight"] for r in records)
    summary = {
        "instances_evaluated": len(records), "instances_past_sla": len(flagged),
        "by_patch_group": _rollup(records, "patch_group"),
        "by_account": _rollup(records, "account_id"),
        "flagged_instances": flagged[:MAX_FLAGGED_INSTANCES],
        "fleet_weighted_compliance_pct": round(
            (fleet_earned / fleet_total * 100.0) if fleet_total > 0 else 100.0, 2),
    }
    if notify:
        _publish(summary)
    logger.info("patch_compliance_complete instances=%s past_sla=%s fleet_pct=%s", len(records),
                len(flagged), summary["fleet_weighted_compliance_pct"])
    return summary
