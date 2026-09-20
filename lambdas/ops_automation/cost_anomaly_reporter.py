"""Cost anomaly reporter.

Event source: EventBridge scheduled rule ``ops-cost-anomaly-reporter`` (weekly, Monday 07:00 UTC).

Walks Cost Explorer ``get_cost_and_usage`` results page by page, builds a per-service
daily cost series, computes week-over-week deltas against a seasonal (same-weekday)
baseline with EWMA smoothing, and ranks the resulting anomalies by absolute dollar impact.
Throttled Cost Explorer calls are retried with exponential backoff.
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
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS, MAX_RETRIES, MAX_BACKOFF_SECONDS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ce = boto3.client("ce")
sns = boto3.client("sns")

ANOMALY_TOPIC = os.environ.get("COST_ANOMALY_TOPIC", "")

ANALYSIS_WINDOW_DAYS = 56
CURRENT_WINDOW_DAYS = 7
EWMA_ALPHA = 0.35
MIN_ABSOLUTE_DELTA_USD = Decimal("25.00")
MIN_RELATIVE_DELTA = 0.20
CRITICAL_DELTA_USD = Decimal("2500.00")
HIGH_DELTA_USD = Decimal("500.00")
MAX_RANKED_ANOMALIES = 40
CENTS = Decimal("0.01")


def _window() -> Tuple[str, str]:
    today = datetime.date.today()
    start = today - datetime.timedelta(days=ANALYSIS_WINDOW_DAYS)
    return start.isoformat(), today.isoformat()


def _fetch_page(start: str, end: str, token: Optional[str]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "DAILY",
        "Metrics": ["UnblendedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    if token:
        kwargs["NextPageToken"] = token

    attempt = 0
    for _loop_iter_1 in range(MAX_RETRIES):
        try:
            return ce.get_cost_and_usage(**kwargs)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("ThrottlingException", "Throttling", "RequestLimitExceeded",
                            "LimitExceededException"):
                raise
            delay = min(0.75 * (2 ** attempt), MAX_BACKOFF_SECONDS)
            logger.info("ce_throttled attempt=%s delay=%.2f", attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Retry cap reached (%d) in cost_anomaly_reporter.py", MAX_RETRIES)
def _collect_series(start: str, end: str) -> Dict[str, Dict[datetime.date, Decimal]]:
    """Return ``{service: {day: cost}}`` across every Cost Explorer page."""
    series: Dict[str, Dict[datetime.date, Decimal]] = defaultdict(dict)
    next_token: Optional[str] = None
    pages = 0

    for _loop_iter_2 in range(MAX_LOOP_ITERATIONS):
        response = _fetch_page(start, end, next_token)
        pages += 1
        for bucket in response.get("ResultsByTime", []):
            day = datetime.date.fromisoformat(bucket["TimePeriod"]["Start"])
            for group in bucket.get("Groups", []):
                keys = group.get("Keys") or ["unknown"]
                service = keys[0]
                amount = group.get("Metrics", {}).get("UnblendedCost", {}).get("Amount", "0")
                try:
                    cost = Decimal(str(amount))
                except ArithmeticError:
                    cost = Decimal("0")
                series[service][day] = series[service].get(day, Decimal("0")) + cost

        next_token = response.get("NextPageToken")
        if not next_token:
            break

    else:
        logger.warning("Loop iteration cap reached (%d) in cost_anomaly_reporter.py", MAX_LOOP_ITERATIONS)
    logger.info("ce_pages_walked pages=%s services=%s", pages, len(series))
    return series


def _ewma(values: List[Decimal], alpha: float) -> Decimal:
    if not values:
        return Decimal("0")
    smoothed = values[0]
    factor = Decimal(str(alpha))
    for value in values[1:]:
        smoothed = factor * value + (Decimal("1") - factor) * smoothed
    return smoothed


def _seasonal_baseline(
    daily: Dict[datetime.date, Decimal], current_days: List[datetime.date]
) -> Decimal:
    """Baseline for the current window using the same weekdays from prior weeks."""
    weekday_totals: Decimal = Decimal("0")
    for day in current_days:
        history: List[Decimal] = []
        probe = day - datetime.timedelta(days=7)
        while probe in daily:
            history.append(daily[probe])
            probe = probe - datetime.timedelta(days=7)
        if history:
            history.reverse()
            weekday_totals += _ewma(history, EWMA_ALPHA)
    return weekday_totals


def _severity(delta: Decimal) -> str:
    magnitude = abs(delta)
    if magnitude >= CRITICAL_DELTA_USD:
        return "critical"
    if magnitude >= HIGH_DELTA_USD:
        return "high"
    return "medium"


def _analyse(series: Dict[str, Dict[datetime.date, Decimal]]) -> List[Dict[str, Any]]:
    today = datetime.date.today()
    current_days = [today - datetime.timedelta(days=offset)
                    for offset in range(1, CURRENT_WINDOW_DAYS + 1)]
    anomalies: List[Dict[str, Any]] = []

    for service, daily in series.items():
        current = sum((daily.get(day, Decimal("0")) for day in current_days), Decimal("0"))
        baseline = _seasonal_baseline(daily, current_days)
        if baseline <= Decimal("0"):
            if current < MIN_ABSOLUTE_DELTA_USD:
                continue
            relative = 1.0
            delta = current
        else:
            delta = current - baseline
            relative = float(delta / baseline)

        if abs(delta) < MIN_ABSOLUTE_DELTA_USD or abs(relative) < MIN_RELATIVE_DELTA:
            continue

        anomalies.append({
            "service": service,
            "current_usd": str(current.quantize(CENTS, rounding=ROUND_HALF_UP)),
            "baseline_usd": str(baseline.quantize(CENTS, rounding=ROUND_HALF_UP)),
            "delta_usd": str(delta.quantize(CENTS, rounding=ROUND_HALF_UP)),
            "delta_pct": round(relative * 100.0, 2),
            "direction": "increase" if delta > 0 else "decrease",
            "severity": _severity(delta),
            "observed_days": len(daily),
        })

    anomalies.sort(key=lambda a: abs(Decimal(a["delta_usd"])), reverse=True)
    return anomalies[:MAX_RANKED_ANOMALIES]


def _publish(summary: Dict[str, Any]) -> None:
    if not ANOMALY_TOPIC:
        return
    try:
        sns.publish(
            TopicArn=ANOMALY_TOPIC,
            Subject="Weekly cost anomaly report",
            Message=str(summary),
        )
    except ClientError as exc:
        logger.error("anomaly_publish_failed error=%s", exc)


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    notify = bool(event.get("notify", True))
    start, end = _window()
    if event.get("start") and event.get("end"):
        start, end = str(event["start"]), str(event["end"])

    try:
        series = _collect_series(start, end)
    except ClientError as exc:
        logger.exception("cost_explorer_walk_failed")
        return {"status": "error", "detail": str(exc)}

    anomalies = _analyse(series)
    increases = [a for a in anomalies if a["direction"] == "increase"]
    net_delta = sum((Decimal(a["delta_usd"]) for a in anomalies), Decimal("0"))

    summary = {
        "window_start": start,
        "window_end": end,
        "services_analysed": len(series),
        "anomaly_count": len(anomalies),
        "increase_count": len(increases),
        "net_delta_usd": str(net_delta.quantize(CENTS, rounding=ROUND_HALF_UP)),
        "critical": sum(1 for a in anomalies if a["severity"] == "critical"),
        "anomalies": anomalies,
    }

    if notify and anomalies:
        _publish(summary)

    logger.info(
        "cost_anomaly_complete services=%s anomalies=%s net_delta=%s critical=%s",
        len(series), len(anomalies), summary["net_delta_usd"], summary["critical"],
    )
    return summary
