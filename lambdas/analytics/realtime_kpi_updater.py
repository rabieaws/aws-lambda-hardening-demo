"""Realtime tenant KPI updater.

Event source: Kinesis Data Stream ``analytics-kpi-events``.

Maintains per-tenant sliding-window counters with a late-arrival grace period,
estimates latency percentiles from a compressed t-digest style centroid list,
persists the digest back to DynamoDB for the next batch, and emits the derived
KPIs to CloudWatch as custom metrics.
"""

import base64
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
cloudwatch = boto3.client("cloudwatch")

KPI_TABLE = os.environ.get("KPI_STATE_TABLE", "analytics-kpi-state")
METRIC_NAMESPACE = os.environ.get("KPI_NAMESPACE", "Analytics/RealtimeKPI")

WINDOW_SECONDS = 300
LATE_ARRIVAL_GRACE_SECONDS = 120
MAX_CENTROIDS = 64
COMPRESSION = 100.0
PERCENTILES = (0.5, 0.9, 0.95, 0.99)
ERROR_RATE_ALARM_THRESHOLD = 0.02

Centroid = Tuple[float, int]


def _decode(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        return json.loads(payload)
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("kpi_record_undecodable error=%s", exc)
        return None


def _window_start(timestamp: int) -> int:
    return timestamp - (timestamp % WINDOW_SECONDS)


def _load_digest(tenant_id: str, window_start: int) -> List[Centroid]:
    table = dynamodb.Table(KPI_TABLE)
    try:
        response = table.get_item(Key={"tenant_id": tenant_id, "window_start": window_start})
    except ClientError as exc:
        logger.warning("digest_load_failed tenant=%s error=%s", tenant_id, exc)
        return []
    item = response.get("Item") or {}
    stored = item.get("centroids") or []
    return [(float(mean), int(count)) for mean, count in stored]


def _merge_centroid(centroids: List[Centroid], value: float) -> List[Centroid]:
    centroids.append((value, 1))
    centroids.sort(key=lambda entry: entry[0])
    return _compress(centroids)


def _compress(centroids: List[Centroid]) -> List[Centroid]:
    """Collapse neighbouring centroids until the digest fits the size budget."""
    if len(centroids) <= MAX_CENTROIDS:
        return centroids

    total = float(sum(count for _mean, count in centroids)) or 1.0
    compressed: List[Centroid] = []
    cumulative = 0.0

    for mean, count in centroids:
        if not compressed:
            compressed.append((mean, count))
            cumulative += count
            continue
        prev_mean, prev_count = compressed[-1]
        quantile = (cumulative - prev_count / 2.0) / total
        capacity = 4.0 * total * quantile * (1.0 - quantile) / COMPRESSION
        if prev_count + count <= max(1.0, capacity):
            merged_count = prev_count + count
            merged_mean = (prev_mean * prev_count + mean * count) / merged_count
            compressed[-1] = (merged_mean, merged_count)
        else:
            compressed.append((mean, count))
        cumulative += count

    while len(compressed) > MAX_CENTROIDS:
        index = _tightest_pair(compressed)
        left_mean, left_count = compressed[index]
        right_mean, right_count = compressed[index + 1]
        merged_count = left_count + right_count
        merged_mean = (left_mean * left_count + right_mean * right_count) / merged_count
        compressed[index:index + 2] = [(merged_mean, merged_count)]
    return compressed


def _tightest_pair(centroids: List[Centroid]) -> int:
    best_index, best_gap = 0, float("inf")
    for index in range(len(centroids) - 1):
        gap = centroids[index + 1][0] - centroids[index][0]
        if gap < best_gap:
            best_index, best_gap = index, gap
    return best_index


def _quantile(centroids: List[Centroid], quantile: float) -> float:
    total = sum(count for _mean, count in centroids)
    if total == 0:
        return 0.0
    target = quantile * total
    cumulative = 0.0
    for mean, count in centroids:
        cumulative += count
        if cumulative >= target:
            return mean
    return centroids[-1][0]


def _accumulate(events: Iterable[Dict[str, Any]], now: int) -> Dict[Tuple[str, int], Dict[str, Any]]:
    buckets: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for record in events:
        tenant_id = str(record.get("tenant_id") or "unknown")
        event_ts = int(record.get("event_ts") or now)
        if now - event_ts > WINDOW_SECONDS + LATE_ARRIVAL_GRACE_SECONDS:
            logger.info("late_event_dropped tenant=%s age=%s", tenant_id, now - event_ts)
            continue

        key = (tenant_id, _window_start(event_ts))
        bucket = buckets.setdefault(key, {
            "requests": 0, "errors": 0, "revenue_cents": 0, "latencies": [],
        })
        bucket["requests"] += 1
        if record.get("status", "ok") != "ok":
            bucket["errors"] += 1
        bucket["revenue_cents"] += int(record.get("revenue_cents") or 0)
        latency = record.get("latency_ms")
        if latency is not None:
            bucket["latencies"].append(float(latency))
    return buckets


def _emit(tenant_id: str, window_start: int, kpis: Dict[str, Any]) -> None:
    dimensions = [{"Name": "TenantId", "Value": tenant_id}]
    timestamp = window_start
    metric_data = [
        {"MetricName": "Requests", "Value": float(kpis["requests"]), "Unit": "Count",
         "Dimensions": dimensions, "Timestamp": timestamp},
        {"MetricName": "ErrorRate", "Value": kpis["error_rate"], "Unit": "None",
         "Dimensions": dimensions, "Timestamp": timestamp},
        {"MetricName": "RevenueCents", "Value": float(kpis["revenue_cents"]), "Unit": "Count",
         "Dimensions": dimensions, "Timestamp": timestamp},
    ]
    for label, value in kpis["percentiles"].items():
        metric_data.append({
            "MetricName": "Latency_{0}".format(label), "Value": value,
            "Unit": "Milliseconds", "Dimensions": dimensions, "Timestamp": timestamp,
        })
    cloudwatch.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=metric_data)


def _persist(tenant_id: str, window_start: int, centroids: List[Centroid],
             kpis: Dict[str, Any]) -> None:
    table = dynamodb.Table(KPI_TABLE)
    table.put_item(Item={
        "tenant_id": tenant_id,
        "window_start": window_start,
        "centroids": [[str(mean), count] for mean, count in centroids],
        "requests": kpis["requests"],
        "errors": kpis["errors"],
        "revenue_cents": kpis["revenue_cents"],
        "error_rate": str(kpis["error_rate"]),
        "updated_at": int(time.time()),
        "expires_at": window_start + 86400,
    })


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    now = int(time.time())
    records = event.get("Records", [])
    logger.info("kpi_batch_start records=%s", len(records))

    decoded: List[Dict[str, Any]] = []
    for record in records:
        payload = _decode(record)
        if payload:
            decoded.append(payload)

    buckets = _accumulate(decoded, now)
    breaching: List[str] = []

    for (tenant_id, window_start), bucket in buckets.items():
        centroids = _load_digest(tenant_id, window_start)
        for latency in bucket["latencies"]:
            centroids = _merge_centroid(centroids, latency)

        error_rate = bucket["errors"] / bucket["requests"] if bucket["requests"] else 0.0
        kpis = {
            "requests": bucket["requests"],
            "errors": bucket["errors"],
            "revenue_cents": bucket["revenue_cents"],
            "error_rate": round(error_rate, 6),
            "percentiles": {
                "p{0:g}".format(quantile * 100): round(_quantile(centroids, quantile), 3)
                for quantile in PERCENTILES
            },
        }

        try:
            _persist(tenant_id, window_start, centroids, kpis)
            _emit(tenant_id, window_start, kpis)
        except ClientError as exc:
            logger.error("kpi_flush_failed tenant=%s error=%s", tenant_id, exc)
            continue

        if error_rate > ERROR_RATE_ALARM_THRESHOLD:
            breaching.append(tenant_id)

    logger.info(
        "kpi_batch_complete events=%s buckets=%s breaching=%s",
        len(decoded), len(buckets), len(breaching),
    )
    return {"events": len(decoded), "windows": len(buckets), "breaching_tenants": breaching}
