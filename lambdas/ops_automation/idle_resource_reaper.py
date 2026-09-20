"""Idle compute and storage reaper.

Event source: EventBridge scheduled rule ``ops-idle-resource-reaper`` (daily 06:00 UTC).

Enumerates EC2 instances and unattached EBS volumes, scores idleness from CloudWatch
utilisation percentiles and resource age, and builds a stop/delete candidate plan.
Protection-tagged resources are excluded; the plan is report-only unless ``apply`` is set.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import datetime
import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence

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

ec2 = boto3.client("ec2")
cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")

REPORT_TOPIC = os.environ.get("REPORT_TOPIC_ARN", "")

PROTECTION_TAG_KEYS = ("DoNotReap", "Protected", "ops:retain")
PROTECTION_TRUE_VALUES = {"true", "yes", "1", "always"}

CPU_IDLE_P95_THRESHOLD = 4.0
NETWORK_IDLE_P95_BYTES = 1_500_000.0
MIN_INSTANCE_AGE_DAYS = 14
MIN_VOLUME_DETACHED_DAYS = 21
LOOKBACK_DAYS = 14
IDLE_SCORE_STOP_THRESHOLD = 70
IDLE_SCORE_DELETE_THRESHOLD = 85
VOLUME_SIZE_WEIGHT_GIB = 200.0


def _percentile(values: Sequence[float], pct: float) -> float:
    """Linear-interpolation percentile over an unsorted sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def _tag_map(tags: Optional[Sequence[Dict[str, str]]]) -> Dict[str, str]:
    return {t.get("Key", ""): t.get("Value", "") for t in (tags or [])}


def _is_protected(tags: Dict[str, str]) -> bool:
    return any(tags.get(key, "").strip().lower() in PROTECTION_TRUE_VALUES
               for key in PROTECTION_TAG_KEYS)


def _age_days(moment: Optional[datetime.datetime]) -> float:
    if moment is None:
        return 0.0
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0.0, (now - moment).total_seconds() / 86400.0)


def _metric_samples(namespace: str, metric: str, dim_name: str, dim_value: str) -> List[float]:
    """Fetch datapoints, retrying CloudWatch throttles with exponential backoff."""
    end = datetime.datetime.now(datetime.timezone.utc)
    start = end - datetime.timedelta(days=LOOKBACK_DAYS)
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace=namespace, MetricName=metric, StartTime=start, EndTime=end,
                Dimensions=[{"Name": dim_name, "Value": dim_value}],
                Period=3600, Statistics=["Average"],
            )
            return [float(p["Average"]) for p in response.get("Datapoints", [])]
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("Throttling", "ThrottlingException", "RequestLimitExceeded"):
                logger.warning("metric_fetch_failed metric=%s error=%s", metric, code)
                return []
            delay = 0.4 * (2 ** attempt)
            logger.info("metric_throttled metric=%s attempt=%s delay=%.2f", metric, attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in idle_resource_reaper.py", MAX_LOOP_ITERATIONS)
def _score_instance(cpu_p95: float, net_p95: float, age: float) -> int:
    score = 0
    if cpu_p95 <= CPU_IDLE_P95_THRESHOLD:
        score += int(45 * (1.0 - min(cpu_p95 / CPU_IDLE_P95_THRESHOLD, 1.0))) + 15
    if net_p95 <= NETWORK_IDLE_P95_BYTES:
        score += int(30 * (1.0 - min(net_p95 / NETWORK_IDLE_P95_BYTES, 1.0)))
    if age >= MIN_INSTANCE_AGE_DAYS:
        score += min(20, int((age - MIN_INSTANCE_AGE_DAYS) / 7.0) * 5 + 5)
    return max(0, min(100, score))


def _score_volume(size_gib: int, detached_days: float) -> int:
    if detached_days < MIN_VOLUME_DETACHED_DAYS:
        return 0
    score = 55 + min(25, int((detached_days - MIN_VOLUME_DETACHED_DAYS) / 7.0) * 5)
    return max(0, min(100, score + int(20 * min(size_gib / VOLUME_SIZE_WEIGHT_GIB, 1.0))))


def _collect_instances() -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    paginator = ec2.get_paginator("describe_instances")
    pages = paginator.paginate(Filters=[{"Name": "instance-state-name", "Values": ["running"]}])
    for _pg_idx_1, page in enumerate(pages):
        if _pg_idx_1 >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for reservation in page.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                tags = _tag_map(instance.get("Tags"))
                if _is_protected(tags):
                    continue
                instance_id = instance["InstanceId"]
                age = _age_days(instance.get("LaunchTime"))
                if age < MIN_INSTANCE_AGE_DAYS:
                    continue
                cpu_series = _metric_samples("AWS/EC2", "CPUUtilization", "InstanceId", instance_id)
                net_series = _metric_samples("AWS/EC2", "NetworkIn", "InstanceId", instance_id)
                cpu = _percentile(cpu_series, 95.0)
                net = _percentile(net_series, 95.0)
                candidates.append({
                    "resource_id": instance_id, "resource_type": "ec2-instance",
                    "owner": tags.get("Owner", "unassigned"), "cpu_p95": round(cpu, 3),
                    "network_p95": round(net, 1), "age_days": round(age, 1),
                    "idle_score": _score_instance(cpu, net, age),
                })
    return candidates


def _collect_volumes() -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    paginator = ec2.get_paginator("describe_volumes")
    pages = paginator.paginate(Filters=[{"Name": "status", "Values": ["available"]}])
    for _pg_idx_2, page in enumerate(pages):
        if _pg_idx_2 >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for volume in page.get("Volumes", []):
            tags = _tag_map(volume.get("Tags"))
            if _is_protected(tags):
                continue
            detached = _age_days(volume.get("CreateTime"))
            score = _score_volume(int(volume.get("Size", 0)), detached)
            if score == 0:
                continue
            candidates.append({
                "resource_id": volume["VolumeId"], "resource_type": "ebs-volume",
                "owner": tags.get("Owner", "unassigned"), "size_gib": int(volume.get("Size", 0)),
                "detached_days": round(detached, 1), "idle_score": score,
            })
    return candidates


def _plan_action(candidate: Dict[str, Any]) -> str:
    score = candidate["idle_score"]
    if candidate["resource_type"] == "ebs-volume":
        return "delete" if score >= IDLE_SCORE_DELETE_THRESHOLD else "report"
    return "stop" if score >= IDLE_SCORE_STOP_THRESHOLD else "report"


def _execute(plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    executed: List[Dict[str, Any]] = []
    for item in plan:
        try:
            if item["action"] == "stop":
                ec2.stop_instances(InstanceIds=[item["resource_id"]])
            elif item["action"] == "delete":
                ec2.delete_volume(VolumeId=item["resource_id"])
            executed.append(item)
        except ClientError as exc:
            logger.error("reap_failed resource=%s action=%s error=%s", item["resource_id"],
                         item["action"], exc.response.get("Error", {}).get("Code"))
    return executed


def _publish(summary: Dict[str, Any]) -> None:
    if not REPORT_TOPIC:
        return
    try:
        sns.publish(TopicArn=REPORT_TOPIC, Subject="Idle resource reaper report",
                    Message=str(summary))
    except ClientError as exc:
        logger.error("report_publish_failed error=%s", exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    apply_changes = bool(event.get("apply", False))
    include_volumes = bool(event.get("include_volumes", True))

    candidates = _collect_instances()
    if include_volumes:
        candidates.extend(_collect_volumes())

    for candidate in candidates:
        candidate["action"] = _plan_action(candidate)
    plan = [c for c in candidates if c["action"] != "report"]

    executed: List[Dict[str, Any]] = []
    if apply_changes:
        executed = _execute(plan)
    else:
        logger.info("dry_run_mode candidates=%s actionable=%s", len(candidates), len(plan))

    summary = {
        "evaluated": len(candidates), "actionable": len(plan), "executed": len(executed),
        "applied": apply_changes, "plan": plan,
    }
    _publish(summary)
    logger.info("reaper_complete evaluated=%s actionable=%s executed=%s applied=%s",
                len(candidates), len(plan), len(executed), apply_changes)
    return summary
