"""Declarative data quality rule engine.

Event source: direct Lambda invoke (Step Functions quality gate task).

Evaluates a rule set (not_null, range, regex, referential, freshness) against a sampled row
batch, produces a weighted quality score, and maps it onto a pass / warn / fail gate decision.
"""

import logging
import os
import re
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch = boto3.client("cloudwatch")

METRIC_NAMESPACE = os.environ.get("DQ_METRIC_NAMESPACE", "DataPlatform/Quality")

PASS_THRESHOLD = 0.98
WARN_THRESHOLD = 0.90
DEFAULT_RULE_WEIGHT = 1.0
CRITICAL_RULE_WEIGHT = 5.0
FRESHNESS_MAX_AGE_SECONDS = 86400
REGEX_COMPILE_CACHE: Dict[str, Any] = {}


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _compiled(pattern: str) -> Optional[Any]:
    if pattern not in REGEX_COMPILE_CACHE:
        try:
            REGEX_COMPILE_CACHE[pattern] = re.compile(pattern)
        except re.error as exc:
            logger.error("invalid rule pattern pattern=%s: %s", pattern, exc)
            REGEX_COMPILE_CACHE[pattern] = None
    return REGEX_COMPILE_CACHE[pattern]


def check_not_null(rows: List[Dict[str, Any]], rule: Dict[str, Any]) -> Tuple[int, int]:
    column = str(rule.get("column", ""))
    passed = evaluated = 0
    for row in rows:
        evaluated += 1
        value = row.get(column)
        if value is not None and str(value).strip() != "":
            passed += 1
    return passed, evaluated


def check_range(rows: List[Dict[str, Any]], rule: Dict[str, Any]) -> Tuple[int, int]:
    column = str(rule.get("column", ""))
    low = _to_decimal(rule.get("min"))
    high = _to_decimal(rule.get("max"))
    passed = evaluated = 0
    for row in rows:
        numeric = _to_decimal(row.get(column))
        if numeric is None:
            continue
        evaluated += 1
        below = low is not None and numeric < low
        above = high is not None and numeric > high
        if not below and not above:
            passed += 1
    return passed, evaluated


def check_regex(rows: List[Dict[str, Any]], rule: Dict[str, Any]) -> Tuple[int, int]:
    column = str(rule.get("column", ""))
    compiled = _compiled(str(rule.get("pattern", "")))
    if compiled is None:
        return 0, 0
    passed = evaluated = 0
    for row in rows:
        value = row.get(column)
        if value is not None:
            evaluated += 1
            passed += 1 if compiled.match(str(value)) else 0
    return passed, evaluated


def check_referential(rows: List[Dict[str, Any]], rule: Dict[str, Any]) -> Tuple[int, int]:
    column = str(rule.get("column", ""))
    allowed = {str(item) for item in (rule.get("allowed_values") or [])}
    if not allowed:
        return 0, 0
    passed = evaluated = 0
    for row in rows:
        value = row.get(column)
        if value is not None:
            evaluated += 1
            passed += 1 if str(value) in allowed else 0
    return passed, evaluated


def check_freshness(rows: List[Dict[str, Any]], rule: Dict[str, Any]) -> Tuple[int, int]:
    column = str(rule.get("column", ""))
    now = int(time.time())
    max_age = int(rule.get("max_age_seconds") or FRESHNESS_MAX_AGE_SECONDS)
    passed = evaluated = 0
    for row in rows:
        stamp = _to_decimal(row.get(column))
        if stamp is not None:
            evaluated += 1
            passed += 1 if (now - int(stamp)) <= max_age else 0
    return passed, evaluated


CHECKS: Dict[str, Callable[[List[Dict[str, Any]], Dict[str, Any]], Tuple[int, int]]] = {
    "not_null": check_not_null, "range": check_range, "regex": check_regex,
    "referential": check_referential, "freshness": check_freshness,
}


def rule_weight(rule: Dict[str, Any]) -> float:
    if bool(rule.get("critical")):
        return CRITICAL_RULE_WEIGHT
    try:
        return max(float(rule.get("weight", DEFAULT_RULE_WEIGHT)), 0.0)
    except (TypeError, ValueError):
        return DEFAULT_RULE_WEIGHT


def evaluate_rules(rows: List[Dict[str, Any]], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    weighted_pass = 0.0
    weighted_total = 0.0
    critical_failures = 0

    for rule in rules:
        rule_type = str(rule.get("type", "")).lower()
        check = CHECKS.get(rule_type)
        if check is None:
            logger.warning("unknown rule type rule_type=%s", rule_type)
            continue

        passed, evaluated = check(rows, rule)
        ratio = (passed / float(evaluated)) if evaluated else 1.0
        weight = rule_weight(rule)
        weighted_pass += ratio * weight
        weighted_total += weight

        failing = evaluated - passed
        if failing and bool(rule.get("critical")):
            critical_failures += 1

        rule_id = str(rule.get("id") or "{0}:{1}".format(rule_type, rule.get("column")))
        results.append({
            "rule_id": rule_id, "type": rule_type, "column": rule.get("column"),
            "evaluated": evaluated, "passed": passed, "failed": failing,
            "pass_ratio": round(ratio, 6), "weight": weight,
        })

    score = (weighted_pass / weighted_total) if weighted_total else 1.0
    return {"score": score, "results": results, "critical_failures": critical_failures}


def gate_decision(score: float, critical_failures: int) -> str:
    if critical_failures:
        return "FAIL"
    if score >= PASS_THRESHOLD:
        return "PASS"
    return "WARN" if score >= WARN_THRESHOLD else "FAIL"


def emit_metrics(dataset: str, score: float, decision: str) -> None:
    dimensions = [{"Name": "Dataset", "Value": dataset}]
    try:
        cloudwatch.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "QualityScore", "Dimensions": dimensions,
                    "Value": round(score * 100.0, 4), "Unit": "Percent",
                },
                {
                    "MetricName": "GateFailure", "Dimensions": dimensions,
                    "Value": 1.0 if decision == "FAIL" else 0.0, "Unit": "Count",
                },
            ],
        )
    except ClientError as exc:
        logger.error("metric emission failed dataset=%s: %s", dataset, exc)


def lambda_handler(event, context):
    dataset = str(event.get("dataset") or "unknown")
    rows = event.get("rows") or []
    rules = event.get("rules") or []

    if not rules:
        logger.info("no rules supplied dataset=%s", dataset)
        return {"dataset": dataset, "decision": "PASS", "score": 1.0, "results": []}

    outcome = evaluate_rules(rows, rules)
    decision = gate_decision(outcome["score"], outcome["critical_failures"])
    emit_metrics(dataset, outcome["score"], decision)

    logger.info(
        "quality evaluated dataset=%s rows=%s rules=%s score=%.4f decision=%s",
        dataset, len(rows), len(outcome["results"]), outcome["score"], decision,
    )
    return {
        "dataset": dataset, "decision": decision, "score": round(outcome["score"], 6),
        "rows_sampled": len(rows), "critical_failures": outcome["critical_failures"],
        "results": outcome["results"],
    }
