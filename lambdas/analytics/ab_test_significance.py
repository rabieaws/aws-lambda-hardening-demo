"""A/B test significance evaluator.

Event source: direct Lambda invoke from the experimentation console
(``InvocationType=RequestResponse``) with a list of experiment arms to score.

Runs a two-proportion z-test with pooled variance for every variant against the
control, applies an O'Brien-Fleming style alpha-spending boundary so interim
peeks stay valid, then reports the minimum detectable effect and achieved power
alongside a ship / hold / stop verdict per variant.
"""

import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

RESULTS_TABLE = os.environ.get("EXPERIMENT_RESULTS_TABLE", "analytics-experiment-results")

BASE_ALPHA = 0.05
TARGET_POWER = 0.8
MIN_SAMPLE_PER_ARM = 500
MIN_CONVERSIONS_PER_ARM = 25
PRACTICAL_SIGNIFICANCE = 0.01
MAX_PLANNED_PEEKS = 6
STOP_FOR_HARM_LIFT = -0.05


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_ppf(probability: float) -> float:
    """Acklam-style rational approximation of the inverse standard normal CDF."""
    if probability <= 0.0 or probability >= 1.0:
        raise ValueError("probability must be in (0, 1)")

    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]

    low, high = 0.02425, 1.0 - 0.02425
    if probability < low:
        q = math.sqrt(-2.0 * math.log(probability))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    if probability > high:
        q = math.sqrt(-2.0 * math.log(1.0 - probability))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)

    q = probability - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
           (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)


def _alpha_boundary(peek: int) -> float:
    """O'Brien-Fleming spending: early peeks get a much stricter alpha."""
    fraction = min(1.0, max(1, peek) / float(MAX_PLANNED_PEEKS))
    critical = _normal_ppf(1.0 - BASE_ALPHA / 2.0) / math.sqrt(fraction)
    return 2.0 * (1.0 - _normal_cdf(critical))


def _pooled_z(
    control_conversions: int, control_n: int, variant_conversions: int, variant_n: int
) -> Tuple[float, float]:
    control_rate = control_conversions / control_n
    variant_rate = variant_conversions / variant_n
    pooled = (control_conversions + variant_conversions) / (control_n + variant_n)
    variance = pooled * (1.0 - pooled) * (1.0 / control_n + 1.0 / variant_n)
    if variance <= 0.0:
        return 0.0, 1.0
    z_score = (variant_rate - control_rate) / math.sqrt(variance)
    p_value = 2.0 * (1.0 - _normal_cdf(abs(z_score)))
    return z_score, p_value


def _minimum_detectable_effect(base_rate: float, per_arm: int, alpha: float) -> float:
    if per_arm <= 0 or not 0.0 < base_rate < 1.0:
        return 1.0
    z_alpha = _normal_ppf(1.0 - alpha / 2.0)
    z_beta = _normal_ppf(TARGET_POWER)
    return (z_alpha + z_beta) * math.sqrt(2.0 * base_rate * (1.0 - base_rate) / per_arm)


def _achieved_power(effect: float, base_rate: float, per_arm: int, alpha: float) -> float:
    if per_arm <= 0 or effect == 0.0 or not 0.0 < base_rate < 1.0:
        return 0.0
    standard_error = math.sqrt(2.0 * base_rate * (1.0 - base_rate) / per_arm)
    if standard_error <= 0.0:
        return 0.0
    z_alpha = _normal_ppf(1.0 - alpha / 2.0)
    return max(0.0, min(1.0, 1.0 - _normal_cdf(z_alpha - abs(effect) / standard_error)))


def _verdict(
    lift: float, p_value: float, alpha: float, power: float, underpowered: bool
) -> str:
    if lift <= STOP_FOR_HARM_LIFT and p_value < alpha:
        return "stop_for_harm"
    if underpowered:
        return "hold_insufficient_sample"
    if p_value >= alpha:
        return "hold_inconclusive"
    if abs(lift) < PRACTICAL_SIGNIFICANCE:
        return "hold_not_practical"
    return "ship" if lift > 0 else "rollback"


def _evaluate_arm(control: Dict[str, Any], arm: Dict[str, Any], alpha: float) -> Dict[str, Any]:
    control_n = int(control.get("exposures", 0))
    control_c = int(control.get("conversions", 0))
    variant_n = int(arm.get("exposures", 0))
    variant_c = int(arm.get("conversions", 0))

    if control_n <= 0 or variant_n <= 0:
        raise ValueError("arm exposures must be positive")

    control_rate = control_c / control_n
    variant_rate = variant_c / variant_n
    z_score, p_value = _pooled_z(control_c, control_n, variant_c, variant_n)
    absolute_lift = variant_rate - control_rate
    relative_lift = (absolute_lift / control_rate) if control_rate else 0.0

    per_arm = min(control_n, variant_n)
    underpowered = (
        per_arm < MIN_SAMPLE_PER_ARM
        or min(control_c, variant_c) < MIN_CONVERSIONS_PER_ARM
    )
    mde = _minimum_detectable_effect(control_rate, per_arm, alpha)
    power = _achieved_power(absolute_lift, control_rate, per_arm, alpha)

    return {
        "variant": arm.get("name", "unknown"),
        "control_rate": round(control_rate, 6),
        "variant_rate": round(variant_rate, 6),
        "absolute_lift": round(absolute_lift, 6),
        "relative_lift": round(relative_lift, 6),
        "z_score": round(z_score, 4),
        "p_value": round(p_value, 6),
        "alpha_boundary": round(alpha, 6),
        "minimum_detectable_effect": round(mde, 6),
        "achieved_power": round(power, 4),
        "verdict": _verdict(absolute_lift, p_value, alpha, power, underpowered),
    }


def _persist(experiment_id: str, peek: int, arms: List[Dict[str, Any]]) -> None:
    table = dynamodb.Table(RESULTS_TABLE)
    table.put_item(Item={
        "experiment_id": experiment_id,
        "peek_number": peek,
        "arms": arms,
        "evaluated_at": int(time.time()),
    })


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    experiment_id = str(event.get("experiment_id", "unknown"))
    peek = int(event.get("peek_number", 1))
    control: Optional[Dict[str, Any]] = event.get("control")
    variants = event.get("variants", [])

    if not control or not variants:
        return {"experiment_id": experiment_id, "error": "control_and_variants_required"}

    alpha = _alpha_boundary(peek)
    logger.info(
        "significance_start experiment=%s peek=%s alpha=%.6f variants=%s",
        experiment_id, peek, alpha, len(variants),
    )

    evaluated: List[Dict[str, Any]] = []
    for arm in variants:
        try:
            evaluated.append(_evaluate_arm(control, arm, alpha))
        except (ValueError, TypeError, ZeroDivisionError) as exc:
            logger.warning("arm_evaluation_failed variant=%s error=%s", arm.get("name"), exc)
            evaluated.append({"variant": arm.get("name", "unknown"), "verdict": "error",
                              "detail": str(exc)})

    try:
        _persist(experiment_id, peek, evaluated)
    except ClientError as exc:
        logger.error("results_persist_failed experiment=%s error=%s", experiment_id, exc)

    shippable = [arm for arm in evaluated if arm.get("verdict") == "ship"]
    logger.info(
        "significance_complete experiment=%s evaluated=%s shippable=%s",
        experiment_id, len(evaluated), len(shippable),
    )
    return {
        "experiment_id": experiment_id,
        "peek_number": peek,
        "alpha_boundary": round(alpha, 6),
        "arms": evaluated,
        "recommended": shippable[0]["variant"] if shippable else None,
    }
