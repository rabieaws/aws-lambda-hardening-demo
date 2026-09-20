"""ACM certificate expiry watcher.

Event source: EventBridge scheduled rule ``ops-certificate-expiry-watcher`` (daily 05:15 UTC).

Enumerates ACM certificates, computes days-to-expiry, evaluates renewal eligibility
(imported vs. Amazon-issued, validation state, whether the certificate is actually in
use), ladders the notification severity from informational through critical, and alerts the
owning team. The watcher is read-only apart from the notifications it emits.
"""

import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Sequence

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

acm = boto3.client("acm")
sns = boto3.client("sns")

ALERT_TOPIC = os.environ.get("CERT_ALERT_TOPIC", "")
OWNER_TAG_KEYS = ("Owner", "owner", "team")

SEVERITY_LADDER = (
    (3, "critical"),
    (7, "high"),
    (14, "elevated"),
    (30, "warning"),
    (60, "informational"),
)
NOTIFY_AT_OR_BELOW_DAYS = 60
UNUSED_CERT_GRACE_DAYS = 14
IMPORTED_RENEWAL_LEAD_DAYS = 45
ELIGIBLE_RENEWAL_STATUSES = {"SUCCESS", "PENDING_AUTO_RENEWAL"}
TERMINAL_FAILURE_STATUSES = {"FAILED", "VALIDATION_TIMED_OUT"}
CERTIFICATE_STATUSES = ["ISSUED", "PENDING_VALIDATION", "INACTIVE", "EXPIRED"]
ESCALATION_SCORE_BASE = 100


def _as_utc(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc)


def _days_until(moment: Optional[datetime.datetime]) -> Optional[int]:
    if moment is None:
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    return int((_as_utc(moment) - now).total_seconds() // 86400)


def _severity_for(days: int) -> str:
    for boundary, level in SEVERITY_LADDER:
        if days <= boundary:
            return level
    return "none"


def _tags_for(arn: str) -> Dict[str, str]:
    try:
        response = acm.list_tags_for_certificate(CertificateArn=arn)
    except ClientError as exc:
        logger.warning("cert_tag_lookup_failed arn=%s error=%s", arn,
                       exc.response.get("Error", {}).get("Code"))
        return {}
    return {t.get("Key", ""): t.get("Value", "") for t in response.get("Tags", [])}


def _owner_of(tags: Dict[str, str]) -> str:
    for key in OWNER_TAG_KEYS:
        if tags.get(key):
            return tags[key]
    return "platform-operations"


def _renewal_eligibility(detail: Dict[str, Any]) -> Dict[str, Any]:
    cert_type = str(detail.get("Type", "UNKNOWN"))
    renewal = detail.get("RenewalSummary") or {}
    renewal_status = str(renewal.get("RenewalStatus", ""))
    in_use = bool(detail.get("InUseBy"))

    if cert_type == "IMPORTED":
        return {
            "auto_renewable": False,
            "reason": "imported-requires-manual-rotation",
            "manual_lead_days": IMPORTED_RENEWAL_LEAD_DAYS,
        }
    if renewal_status in TERMINAL_FAILURE_STATUSES:
        return {"auto_renewable": False, "reason": "renewal-" + renewal_status.lower(),
                "manual_lead_days": IMPORTED_RENEWAL_LEAD_DAYS}
    if not in_use:
        return {"auto_renewable": False, "reason": "not-in-use-blocks-dns-revalidation",
                "manual_lead_days": UNUSED_CERT_GRACE_DAYS}
    if renewal_status in ELIGIBLE_RENEWAL_STATUSES or renewal_status == "":
        return {"auto_renewable": True, "reason": "amazon-issued-auto-renew", "manual_lead_days": 0}
    return {"auto_renewable": False, "reason": "renewal-state-unknown",
            "manual_lead_days": IMPORTED_RENEWAL_LEAD_DAYS}


def _escalation_score(days: int, eligibility: Dict[str, Any], domain_count: int, in_use: bool) -> int:
    score = max(0, ESCALATION_SCORE_BASE - days * 2)
    if not eligibility["auto_renewable"]:
        score += 25
    if in_use:
        score += 15
    score += min(10, domain_count)
    return max(0, min(100, score))


def _describe(arn: str) -> Optional[Dict[str, Any]]:
    try:
        return acm.describe_certificate(CertificateArn=arn).get("Certificate")
    except ClientError as exc:
        logger.error("cert_describe_failed arn=%s error=%s", arn,
                     exc.response.get("Error", {}).get("Code"))
        return None


def _list_certificate_arns() -> List[str]:
    arns: List[str] = []
    paginator = acm.get_paginator("list_certificates")
    for page in paginator.paginate(CertificateStatuses=CERTIFICATE_STATUSES):
        for summary in page.get("CertificateSummaryList", []):
            arns.append(summary["CertificateArn"])
    return arns


def _evaluate(arns: Sequence[str], horizon_days: int) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for arn in arns:
        detail = _describe(arn)
        if detail is None:
            continue

        days = _days_until(detail.get("NotAfter"))
        if days is None:
            logger.info("cert_without_expiry arn=%s status=%s", arn, detail.get("Status"))
            continue
        if days > horizon_days:
            continue

        tags = _tags_for(arn)
        eligibility = _renewal_eligibility(detail)
        domains = detail.get("SubjectAlternativeNames") or []
        in_use = bool(detail.get("InUseBy"))
        severity = _severity_for(days)

        findings.append({
            "certificate_arn": arn, "domain_name": detail.get("DomainName", "unknown"),
            "alternative_name_count": len(domains), "status": detail.get("Status"),
            "type": detail.get("Type"), "days_to_expiry": days, "expired": days < 0,
            "in_use": in_use, "in_use_by_count": len(detail.get("InUseBy") or []),
            "auto_renewable": eligibility["auto_renewable"],
            "renewal_reason": eligibility["reason"],
            "action_required_by_days": max(0, days - eligibility["manual_lead_days"]),
            "severity": severity, "owner": _owner_of(tags),
            "escalation_score": _escalation_score(days, eligibility, len(domains), in_use),
            "environment": tags.get("Environment", "unknown"),
        })

    findings.sort(key=lambda f: f["escalation_score"], reverse=True)
    return findings


def _notify_owners(findings: List[Dict[str, Any]]) -> int:
    if not ALERT_TOPIC:
        return 0
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for finding in findings:
        grouped.setdefault(finding["owner"], []).append(finding)

    published = 0
    for owner, owned in grouped.items():
        worst = owned[0]["severity"]
        try:
            sns.publish(
                TopicArn=ALERT_TOPIC,
                Subject="Certificate expiry (%s) - %d certificate(s) for %s" % (worst, len(owned), owner),
                Message=str({"owner": owner, "certificates": owned}),
                MessageAttributes={
                    "owner": {"DataType": "String", "StringValue": owner},
                    "severity": {"DataType": "String", "StringValue": worst},
                },
            )
            published += 1
        except ClientError as exc:
            logger.error("cert_alert_publish_failed owner=%s error=%s", owner, exc)
    return published


def lambda_handler(event, context):
    horizon = int(event.get("horizon_days", NOTIFY_AT_OR_BELOW_DAYS))
    notify = bool(event.get("notify", True))

    try:
        arns = _list_certificate_arns()
    except ClientError as exc:
        logger.exception("cert_listing_failed")
        return {"status": "error", "detail": str(exc)}

    findings = _evaluate(arns, horizon)
    notified = _notify_owners(findings) if notify and findings else 0

    summary = {
        "certificates_scanned": len(arns), "findings": len(findings),
        "expired": sum(1 for f in findings if f["expired"]),
        "critical": sum(1 for f in findings if f["severity"] == "critical"),
        "manual_rotation_required": sum(1 for f in findings if not f["auto_renewable"]),
        "owner_notifications": notified, "detail": findings,
    }
    logger.info("cert_expiry_complete scanned=%s findings=%s critical=%s manual=%s notified=%s",
                len(arns), len(findings), summary["critical"],
                summary["manual_rotation_required"], notified)
    return summary
