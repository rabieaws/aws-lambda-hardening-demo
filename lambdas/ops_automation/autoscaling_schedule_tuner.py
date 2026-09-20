"""Auto Scaling scheduled-action tuner.

Event source: EventBridge scheduled rule ``ops-asg-schedule-tuner`` (weekly, Sunday 23:00 UTC).

Enumerates Auto Scaling groups, pulls four weeks of hourly ``GroupInServiceInstances`` and
``CPUUtilization`` history, computes per-hour load percentiles split into weekday and weekend
profiles, and derives a scheduled-scaling plan with capacity headroom and a ramp lead time.
Scheduled actions are written only when the event sets ``apply``.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import datetime
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_PAGINATION_PAGES

logger = logging.getLogger()
logger.setLevel(logging.INFO)

autoscaling = boto3.client("autoscaling")
cloudwatch = boto3.client("cloudwatch")

PLAN_BUCKET = os.environ.get("PLAN_BUCKET", "")

HISTORY_DAYS = 28
DEMAND_PERCENTILE = 92.0
TROUGH_PERCENTILE = 25.0
HEADROOM_FACTOR = 1.25
RAMP_LEAD_MINUTES = 20
TARGET_CPU_PER_INSTANCE = 55.0
MIN_CAPACITY_FLOOR = 2
MAX_CAPACITY_CEILING = 200
MIN_SAMPLES_PER_HOUR = 6
SIGNIFICANT_CHANGE_RATIO = 0.15
WEEKEND_WEEKDAYS = (5, 6)


def _percentile(values: Sequence[float], pct: float) -> float:
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


def _hourly_samples(metric: str, namespace: str, group_name: str) -> List[Tuple[datetime.datetime, float]]:
    end = datetime.datetime.now(datetime.timezone.utc)
    start = end - datetime.timedelta(days=HISTORY_DAYS)
    try:
        response = cloudwatch.get_metric_statistics(
            Namespace=namespace, MetricName=metric, StartTime=start, EndTime=end,
            Dimensions=[{"Name": "AutoScalingGroupName", "Value": group_name}],
            Period=3600, Statistics=["Average", "Maximum"],
        )
    except ClientError as exc:
        logger.warning("metric_fetch_failed group=%s metric=%s error=%s", group_name, metric,
                       exc.response.get("Error", {}).get("Code"))
        return []
    return [(p["Timestamp"], float(p.get("Maximum", p.get("Average", 0.0))))
            for p in response.get("Datapoints", [])]


def _profile_buckets(
    samples: List[Tuple[datetime.datetime, float]]
) -> Dict[str, Dict[int, List[float]]]:
    buckets: Dict[str, Dict[int, List[float]]] = {
        "weekday": defaultdict(list),
        "weekend": defaultdict(list),
    }
    for timestamp, value in samples:
        moment = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=datetime.timezone.utc)
        profile = "weekend" if moment.weekday() in WEEKEND_WEEKDAYS else "weekday"
        buckets[profile][moment.hour].append(value)
    return buckets


def _required_capacity(instance_p: float, cpu_p: float) -> int:
    """Blend observed capacity with CPU-implied capacity, then add headroom."""
    cpu_implied = 0.0
    if instance_p > 0 and cpu_p > 0:
        cpu_implied = instance_p * (cpu_p / TARGET_CPU_PER_INSTANCE)
    demand = max(instance_p, cpu_implied)
    with_headroom = demand * HEADROOM_FACTOR
    capacity = int(with_headroom + 0.999) if with_headroom > 0 else MIN_CAPACITY_FLOOR
    return max(MIN_CAPACITY_FLOOR, min(MAX_CAPACITY_CEILING, capacity))


def _ramp_cron(hour: int, profile: str) -> str:
    total_minutes = (hour * 60 - RAMP_LEAD_MINUTES) % (24 * 60)
    day_spec = "6,0" if profile == "weekend" else "1-5"
    return "%d %d ? * %s *" % (total_minutes % 60, (total_minutes // 60) % 24, day_spec)


def _build_profile_plan(
    group_name: str,
    profile: str,
    instance_hours: Dict[int, List[float]],
    cpu_hours: Dict[int, List[float]],
    current_desired: int,
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    previous_capacity: Optional[int] = None

    for hour in range(24):
        instance_samples = instance_hours.get(hour, [])
        cpu_samples = cpu_hours.get(hour, [])
        if len(instance_samples) < MIN_SAMPLES_PER_HOUR:
            continue

        instance_p = _percentile(instance_samples, DEMAND_PERCENTILE)
        trough = _percentile(instance_samples, TROUGH_PERCENTILE)
        cpu_p = _percentile(cpu_samples, DEMAND_PERCENTILE) if cpu_samples else 0.0
        capacity = _required_capacity(instance_p, cpu_p)

        reference = previous_capacity if previous_capacity is not None else current_desired
        if reference > 0 and abs(capacity - reference) / float(reference) < SIGNIFICANT_CHANGE_RATIO:
            previous_capacity = previous_capacity or capacity
            continue

        actions.append({
            "action_name": "%s-%s-h%02d" % (group_name[:40], profile, hour),
            "profile": profile, "effective_hour_utc": hour,
            "recurrence": _ramp_cron(hour, profile), "desired_capacity": capacity,
            "min_size": max(MIN_CAPACITY_FLOOR, int(trough)),
            "max_size": min(MAX_CAPACITY_CEILING, max(capacity, int(capacity * 1.5))),
            "demand_p92_instances": round(instance_p, 2), "demand_p92_cpu": round(cpu_p, 2),
            "sample_count": len(instance_samples),
        })
        previous_capacity = capacity

    return actions


def _write_actions(group_name: str, actions: List[Dict[str, Any]]) -> int:
    written = 0
    for action in actions:
        try:
            autoscaling.put_scheduled_update_group_action(
                AutoScalingGroupName=group_name, ScheduledActionName=action["action_name"],
                Recurrence=action["recurrence"], MinSize=action["min_size"],
                MaxSize=action["max_size"], DesiredCapacity=action["desired_capacity"],
            )
            written += 1
        except ClientError as exc:
            logger.error("scheduled_action_write_failed group=%s action=%s error=%s", group_name,
                         action["action_name"], exc.response.get("Error", {}).get("Code"))
    return written


def _tune_groups(name_filter: Optional[str]) -> List[Dict[str, Any]]:
    plans: List[Dict[str, Any]] = []
    paginator = autoscaling.get_paginator("describe_auto_scaling_groups")
    kwargs: Dict[str, Any] = {}
    if name_filter:
        kwargs["AutoScalingGroupNames"] = [name_filter]

    for _page_num, page in enumerate(paginator.paginate(**kwargs)):

        if _page_num >= MAX_PAGINATION_PAGES:

            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)

            break
        for group in page.get("AutoScalingGroups", []):
            group_name = group["AutoScalingGroupName"]
            current_desired = int(group.get("DesiredCapacity", MIN_CAPACITY_FLOOR))

            instance_samples = _hourly_samples("GroupInServiceInstances", "AWS/AutoScaling", group_name)
            if not instance_samples:
                continue
            instance_profiles = _profile_buckets(instance_samples)
            cpu_profiles = _profile_buckets(_hourly_samples("CPUUtilization", "AWS/EC2", group_name))

            actions: List[Dict[str, Any]] = []
            for profile in ("weekday", "weekend"):
                actions.extend(_build_profile_plan(
                    group_name, profile, instance_profiles[profile],
                    cpu_profiles[profile], current_desired,
                ))

            plans.append({
                "group_name": group_name, "current_desired": current_desired,
                "current_min": int(group.get("MinSize", 0)),
                "current_max": int(group.get("MaxSize", 0)),
                "recommended_actions": actions,
                "peak_capacity": max((a["desired_capacity"] for a in actions), default=current_desired),
            })
    return plans


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    apply_changes = bool(event.get("apply", False))
    name_filter = event.get("auto_scaling_group_name")

    try:
        plans = _tune_groups(name_filter)
    except ClientError as exc:
        logger.exception("asg_tuning_failed")
        return {"status": "error", "detail": str(exc)}

    written_total = 0
    if apply_changes:
        for plan in plans:
            written_total += _write_actions(plan["group_name"], plan["recommended_actions"])
    else:
        logger.info("dry_run_mode groups=%s", len(plans))

    summary = {
        "groups_evaluated": len(plans), "actions_written": written_total,
        "recommended_actions": sum(len(p["recommended_actions"]) for p in plans),
        "applied": apply_changes, "plan_bucket": PLAN_BUCKET, "plans": plans,
    }
    logger.info("asg_schedule_tuner_complete groups=%s actions=%s written=%s applied=%s",
                len(plans), summary["recommended_actions"], written_total, apply_changes)
    return summary
