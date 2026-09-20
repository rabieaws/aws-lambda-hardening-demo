"""Security group drift auditor.

Event source: EventBridge scheduled rule ``ops-sg-drift-auditor`` (every 6 hours).

Enumerates every security group in the account, flattens live ingress and egress
permissions into comparable rule tuples, diffs them against an approved baseline stored in
DynamoDB, and scores each finding by port sensitivity and CIDR breadth. The result is a
ranked drift report; the auditor never mutates security groups.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import ipaddress
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2 = boto3.client("ec2")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

BASELINE_TABLE = os.environ.get("SG_BASELINE_TABLE", "sg-approved-baseline")
REPORT_TOPIC = os.environ.get("DRIFT_REPORT_TOPIC", "")

SENSITIVE_PORTS = {
    22: 40, 3389: 40, 3306: 35, 5432: 35, 1433: 35,
    27017: 35, 6379: 30, 9200: 30, 2375: 45, 445: 45, 23: 45,
}
WEB_PORTS = {80: 5, 443: 5, 8080: 10, 8443: 10}
CIDR_BREADTH_SCORE = ((0, 45), (8, 35), (16, 25), (24, 12), (32, 3))
RANGE_SOURCE_KEYS = (("IpRanges", "CidrIp"), ("Ipv6Ranges", "CidrIpv6"),
                     ("UserIdGroupPairs", "GroupId"), ("PrefixListIds", "PrefixListId"))

ALL_PROTOCOL_PENALTY = 25
WIDE_RANGE_PORT_COUNT = 64
WIDE_RANGE_PENALTY = 20
EGRESS_DAMPENING = 0.6
SEVERITY_CRITICAL = 80
SEVERITY_HIGH = 55
SEVERITY_MEDIUM = 30
MISSING_RULE_SCORE = 10

RuleTuple = Tuple[str, int, int, str]


def _cidr_breadth_score(cidr: str) -> int:
    try:
        network = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return 20
    prefix = network.prefixlen - 96 if network.version == 6 else network.prefixlen
    for boundary, value in CIDR_BREADTH_SCORE:
        if max(0, prefix) <= boundary:
            return value
    return CIDR_BREADTH_SCORE[-1][1]


def _port_sensitivity(from_port: int, to_port: int, protocol: str) -> int:
    if protocol == "-1":
        return max(SENSITIVE_PORTS.values())
    best = max((w for p, w in SENSITIVE_PORTS.items() if from_port <= p <= to_port), default=0)
    if best == 0:
        best = max((w for p, w in WEB_PORTS.items() if from_port <= p <= to_port), default=0)
    if best == 0:
        best = 15 if to_port > from_port else 10
    return best


def _score_rule(rule: RuleTuple, direction: str) -> int:
    protocol, from_port, to_port, source = rule
    score = _port_sensitivity(from_port, to_port, protocol)
    score += _cidr_breadth_score(source) if source.count("/") == 1 else 5
    if protocol == "-1":
        score += ALL_PROTOCOL_PENALTY
    if (to_port - from_port) >= WIDE_RANGE_PORT_COUNT:
        score += WIDE_RANGE_PENALTY
    if direction == "egress":
        score = int(score * EGRESS_DAMPENING)
    return max(0, min(100, score))


def _severity(score: int) -> str:
    if score >= SEVERITY_CRITICAL:
        return "critical"
    if score >= SEVERITY_HIGH:
        return "high"
    return "medium" if score >= SEVERITY_MEDIUM else "low"


def _flatten(permissions: List[Dict[str, Any]]) -> Set[RuleTuple]:
    flattened: Set[RuleTuple] = set()
    for permission in permissions:
        protocol = str(permission.get("IpProtocol", "-1"))
        from_port = int(permission.get("FromPort", 0))
        to_port = int(permission.get("ToPort", 65535 if protocol == "-1" else from_port))
        for collection, source_key in RANGE_SOURCE_KEYS:
            for entry in permission.get(collection, []):
                flattened.add((protocol, from_port, to_port, entry.get(source_key, "")))
    return flattened


def _finding(group_id: str, direction: str, drift_type: str, rule: RuleTuple, score: int) -> Dict[str, Any]:
    return {
        "group_id": group_id, "direction": direction, "drift_type": drift_type,
        "protocol": rule[0], "from_port": rule[1], "to_port": rule[2], "source": rule[3],
        "score": score, "severity": _severity(score),
    }


def _parse_baseline(raw: Any) -> Set[RuleTuple]:
    parsed: Set[RuleTuple] = set()
    for entry in raw or []:
        try:
            parsed.add((str(entry["protocol"]), int(entry["from_port"]),
                        int(entry["to_port"]), str(entry["source"])))
        except (KeyError, TypeError, ValueError):
            logger.warning("baseline_entry_malformed entry=%s", entry)
    return parsed


def _load_baseline(group_id: str) -> Optional[Dict[str, Set[RuleTuple]]]:
    table = dynamodb.Table(BASELINE_TABLE)
    try:
        item = table.get_item(Key={"group_id": group_id}).get("Item")
    except ClientError as exc:
        logger.error("baseline_lookup_failed group=%s error=%s", group_id, exc)
        return None
    if not item:
        return None
    return {"ingress": _parse_baseline(item.get("ingress")),
            "egress": _parse_baseline(item.get("egress"))}


def _diff_direction(
    group_id: str, direction: str, live: Set[RuleTuple], approved: Set[RuleTuple]
) -> List[Dict[str, Any]]:
    findings = [_finding(group_id, direction, "unapproved", rule, _score_rule(rule, direction))
                for rule in sorted(live - approved)]
    findings.extend(_finding(group_id, direction, "missing", rule, MISSING_RULE_SCORE)
                    for rule in sorted(approved - live))
    return findings


def _audit_groups(vpc_id: Optional[str]) -> Tuple[List[Dict[str, Any]], int, int]:
    findings: List[Dict[str, Any]] = []
    audited = 0
    unbaselined = 0
    paginator = ec2.get_paginator("describe_security_groups")
    kwargs: Dict[str, Any] = {}
    if vpc_id:
        kwargs["Filters"] = [{"Name": "vpc-id", "Values": [vpc_id]}]

    for _page_num, page in enumerate(paginator.paginate(**kwargs)):

        if _page_num >= MAX_PAGINATION_PAGES:

            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)

            break
        for group in page.get("SecurityGroups", []):
            group_id = group["GroupId"]
            audited += 1
            baseline = _load_baseline(group_id)
            live_ingress = _flatten(group.get("IpPermissions", []))
            live_egress = _flatten(group.get("IpPermissionsEgress", []))

            if baseline is None:
                unbaselined += 1
                for rule in sorted(live_ingress):
                    score = _score_rule(rule, "ingress")
                    if score >= SEVERITY_MEDIUM:
                        findings.append(_finding(group_id, "ingress", "no-baseline", rule, score))
                continue

            findings.extend(_diff_direction(group_id, "ingress", live_ingress, baseline["ingress"]))
            findings.extend(_diff_direction(group_id, "egress", live_egress, baseline["egress"]))

    return findings, audited, unbaselined


def _publish(summary: Dict[str, Any]) -> None:
    if not REPORT_TOPIC:
        return
    try:
        sns.publish(TopicArn=REPORT_TOPIC, Subject="Security group drift report",
                    Message=str(summary))
    except ClientError as exc:
        logger.error("report_publish_failed error=%s", exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    vpc_id = event.get("vpc_id")
    min_severity = str(event.get("min_severity", "medium")).lower()
    floor = {"low": 0, "medium": SEVERITY_MEDIUM, "high": SEVERITY_HIGH,
             "critical": SEVERITY_CRITICAL}.get(min_severity, SEVERITY_MEDIUM)

    try:
        findings, audited, unbaselined = _audit_groups(vpc_id)
    except ClientError as exc:
        logger.exception("sg_audit_failed")
        return {"status": "error", "detail": str(exc)}

    reportable = [f for f in findings if f["score"] >= floor]
    reportable.sort(key=lambda f: f["score"], reverse=True)

    summary = {
        "groups_audited": audited, "groups_without_baseline": unbaselined,
        "total_findings": len(findings), "reported_findings": len(reportable),
        "critical": sum(1 for f in reportable if f["severity"] == "critical"),
        "high": sum(1 for f in reportable if f["severity"] == "high"),
        "findings": reportable,
    }
    _publish(summary)
    logger.info("sg_drift_complete audited=%s findings=%s critical=%s high=%s",
                audited, len(findings), summary["critical"], summary["high"])
    return summary
