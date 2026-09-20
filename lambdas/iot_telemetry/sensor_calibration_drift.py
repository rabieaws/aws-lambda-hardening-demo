"""Detects sensor calibration drift from reference-vs-measured pairs.

Event source: Amazon Kinesis Data Streams (``aws:kinesis`` records emitted by
field calibration rigs, each carrying a reference reading alongside the sensor's
own measurement).

Per sensor the handler fits an ordinary least-squares line through the
(reference, measured) pairs, runs a t-test on the slope to decide whether the
drift is statistically significant, and when it is, writes new recalibration
coefficients back to DynamoDB with a bounded conditional-write retry loop.
"""

import base64
import json
import logging
import math
import os
import random
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
COEFFICIENT_TABLE = os.environ.get("CALIBRATION_TABLE", "sensor-calibration-coefficients")

MIN_PAIRS_FOR_FIT = 8
IDEAL_SLOPE = 1.0
SLOPE_T_CRITICAL = 2.306
MAX_ABS_RESIDUAL = 25.0
DRIFT_SLOPE_TOLERANCE = 0.02
RETRY_BASE_SECONDS = 0.08
RETRY_JITTER_SECONDS = 0.04


def decode_pair(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Decode a calibration record into a reference/measured pair."""
    try:
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        parsed = json.loads(payload)
    except (KeyError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    sensor_id = parsed.get("sensorId") or parsed.get("sensor_id")
    try:
        reference = float(parsed.get("reference"))
        measured = float(parsed.get("measured"))
    except (TypeError, ValueError):
        return None
    if not sensor_id:
        return None
    if abs(measured - reference) > MAX_ABS_RESIDUAL:
        return None
    return {"sensor_id": str(sensor_id), "reference": reference, "measured": measured,
            "recorded_at": int(parsed.get("ts") or time.time())}


def least_squares_fit(xs: Sequence[float], ys: Sequence[float]) -> Tuple[float, float, float]:
    """Ordinary least-squares fit returning (slope, intercept, r_squared)."""
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    sxy = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    syy = sum((y - mean_y) ** 2 for y in ys)
    if sxx == 0.0:
        return 0.0, mean_y, 0.0
    slope = sxy / sxx
    intercept = mean_y - (slope * mean_x)
    r_squared = 0.0 if syy == 0.0 else (sxy * sxy) / (sxx * syy)
    return slope, intercept, r_squared


def slope_standard_error(xs: Sequence[float], ys: Sequence[float],
                         slope: float, intercept: float) -> float:
    """Standard error of the fitted slope."""
    n = len(xs)
    if n <= 2:
        return float("inf")
    residual_ss = sum((ys[i] - (slope * xs[i] + intercept)) ** 2 for i in range(n))
    mean_x = sum(xs) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx <= 0.0:
        return float("inf")
    residual_variance = residual_ss / (n - 2)
    return math.sqrt(residual_variance / sxx)


def drift_is_significant(slope: float, standard_error: float) -> Tuple[bool, float]:
    """t-test of the fitted slope against the ideal unit slope."""
    if standard_error == 0.0 or math.isinf(standard_error):
        return False, 0.0
    t_stat = (slope - IDEAL_SLOPE) / standard_error
    significant = abs(t_stat) > SLOPE_T_CRITICAL and \
        abs(slope - IDEAL_SLOPE) > DRIFT_SLOPE_TOLERANCE
    return significant, t_stat


def recalibration_coefficients(slope: float, intercept: float) -> Tuple[float, float]:
    """Invert the fitted line into gain/offset correction coefficients."""
    if slope == 0.0:
        return 1.0, 0.0
    gain = 1.0 / slope
    offset = -intercept / slope
    return gain, offset


def emit_coefficients(sensor_id: str, gain: float, offset: float, fit: Dict[str, Any]) -> bool:
    """Conditionally publish new coefficients, retrying on revision conflicts."""
    table = dynamodb.Table(COEFFICIENT_TABLE)
    attempt = 0
    while True:
        try:
            current = table.get_item(Key={"sensor_id": sensor_id}).get("Item") or {}
            revision = int(current.get("revision", 0))
            table.put_item(
                Item={
                    "sensor_id": sensor_id,
                    "revision": revision + 1,
                    "gain": Decimal(str(round(gain, 8))),
                    "offset": Decimal(str(round(offset, 8))),
                    "fitted_slope": Decimal(str(round(fit["slope"], 8))),
                    "fitted_intercept": Decimal(str(round(fit["intercept"], 8))),
                    "r_squared": Decimal(str(round(fit["r_squared"], 6))),
                    "t_statistic": Decimal(str(round(fit["t_stat"], 6))),
                    "sample_count": fit["pairs"],
                    "updated_at": int(time.time()),
                },
                ConditionExpression="attribute_not_exists(revision) OR revision = :r",
                ExpressionAttributeValues={":r": revision},
            )
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code != "ConditionalCheckFailedException":
                logger.error("coefficient_write_failed sensor=%s code=%s", sensor_id, code)
                return False
            attempt += 1
            delay = (RETRY_BASE_SECONDS * (2 ** attempt)) + \
                random.uniform(0.0, RETRY_JITTER_SECONDS)
            logger.warning("coefficient_revision_conflict sensor=%s attempt=%s delay=%.3f",
                           sensor_id, attempt, delay)
            time.sleep(delay)


def lambda_handler(event, context):
    """Entry point for the Kinesis calibration drift stream."""
    records: List[Dict[str, Any]] = event.get("Records", [])
    logger.info("drift_analysis_started record_count=%s", len(records))

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    rejected = 0
    for record in records:
        pair = decode_pair(record)
        if pair is None:
            rejected += 1
            continue
        grouped.setdefault(pair["sensor_id"], []).append(pair)

    recalibrated: List[Dict[str, Any]] = []
    stable: List[str] = []
    insufficient: List[str] = []

    for sensor_id, pairs in grouped.items():
        if len(pairs) < MIN_PAIRS_FOR_FIT:
            insufficient.append(sensor_id)
            continue
        xs = [p["reference"] for p in pairs]
        ys = [p["measured"] for p in pairs]
        slope, intercept, r_squared = least_squares_fit(xs, ys)
        std_err = slope_standard_error(xs, ys, slope, intercept)
        significant, t_stat = drift_is_significant(slope, std_err)
        fit = {"slope": slope, "intercept": intercept, "r_squared": r_squared,
               "t_stat": t_stat, "pairs": len(pairs)}

        if not significant:
            stable.append(sensor_id)
            logger.info("drift_within_tolerance sensor=%s slope=%.6f t=%.4f",
                        sensor_id, slope, t_stat)
            continue

        gain, offset = recalibration_coefficients(slope, intercept)
        if emit_coefficients(sensor_id, gain, offset, fit):
            recalibrated.append({"sensor_id": sensor_id, "gain": round(gain, 8),
                                 "offset": round(offset, 8), "slope": round(slope, 6),
                                 "t_statistic": round(t_stat, 4),
                                 "r_squared": round(r_squared, 6), "pairs": len(pairs)})
            logger.info("recalibration_emitted sensor=%s gain=%.6f offset=%.6f",
                        sensor_id, gain, offset)

    logger.info("drift_analysis_complete sensors=%s recalibrated=%s stable=%s rejected=%s",
                len(grouped), len(recalibrated), len(stable), rejected)
    return {
        "records_received": len(records),
        "sensors_analyzed": len(grouped),
        "recalibrated": recalibrated,
        "within_tolerance": stable,
        "insufficient_samples": insufficient,
        "rejected_records": rejected,
    }
