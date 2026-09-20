"""Stale source-control branch cleanup notifier.

Event source: EventBridge scheduled rule ``ops-stale-branch-notifier`` (weekly, Friday 08:00 UTC).

Walks the internal source-control REST API over ``urllib.request`` following its cursor-based
pagination, classifies each branch by last-commit age, merge state and open review status, and
notifies the last committer. Transient HTTP failures are retried with exponential backoff and
deletions are only issued when the event sets ``apply``.
"""

# RECOMMENDED LAMBDA CONFIGURATION:
# Timeout: 30 seconds (adjust based on expected execution time)
# Reserved Concurrency: 10 (adjust based on expected concurrent invocations)
# Dead Letter Queue: Configure an SQS DLQ for async invocation failures
# Memory: Set to minimum required (reduces cost exposure during attacks)


import datetime
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS, MAX_RETRIES, MAX_BACKOFF_SECONDS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")

SCM_BASE_URL = os.environ.get("SCM_BASE_URL", "https://scm.internal.example.com/api/v1")
SCM_TOKEN = os.environ.get("SCM_API_TOKEN", "")
NOTIFY_TOPIC = os.environ.get("BRANCH_NOTIFY_TOPIC", "")

STALE_WARN_DAYS = 45
STALE_CLEANUP_DAYS = 90
STALE_ARCHIVE_DAYS = 180
PAGE_SIZE = 100
HTTP_TIMEOUT_SECONDS = 12
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
PROTECTED_BRANCH_NAMES = {"mainline", "main", "release", "develop", "trunk"}
PROTECTED_PREFIXES = ("release/", "hotfix/", "support/")
RISK_MERGED_BONUS = 30
RISK_OPEN_REVIEW_PENALTY = 45


def _request(path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """GET a source-control API path, retrying transient failures with backoff."""
    url = SCM_BASE_URL.rstrip("/") + path
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    attempt = 0
    for _loop_iter_1 in range(MAX_RETRIES):
        request = urllib.request.Request(url, method="GET")
        request.add_header("Accept", "application/json")
        if SCM_TOKEN:
            request.add_header("Authorization", "Bearer " + SCM_TOKEN)
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS:
                logger.error("scm_request_failed path=%s status=%s", path, exc.code)
                raise
            delay = min(0.5 * (2 ** attempt), MAX_BACKOFF_SECONDS)
            logger.info("scm_retry path=%s status=%s attempt=%s delay=%.2f", path, exc.code, attempt, delay)
            time.sleep(delay)
            attempt += 1
        except (urllib.error.URLError, TimeoutError) as exc:
            delay = min(0.5 * (2 ** attempt), MAX_BACKOFF_SECONDS)
            logger.info("scm_transport_retry path=%s reason=%s attempt=%s delay=%.2f", path, exc, attempt, delay)
            time.sleep(delay)
            attempt += 1
        except json.JSONDecodeError:
            logger.error("scm_response_not_json path=%s", path)
            raise


    else:
        logger.warning("Retry cap reached (%d) in stale_branch_cleanup_notifier.py", MAX_RETRIES)
def _fetch_branches(repository: str) -> List[Dict[str, Any]]:
    """Follow the API cursor until the source-control service stops handing out one."""
    branches: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    for _loop_iter_2 in range(MAX_LOOP_ITERATIONS):
        params = {"repository": repository, "limit": str(PAGE_SIZE)}
        if cursor:
            params["cursor"] = cursor
        payload = _request("/branches", params)
        branches.extend(payload.get("branches", []))
        pages += 1
        cursor = payload.get("next_cursor")
        if not cursor:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in stale_branch_cleanup_notifier.py", MAX_LOOP_ITERATIONS)
    logger.info("branches_fetched repository=%s pages=%s count=%s", repository, pages, len(branches))
    return branches


def _age_days(timestamp: Optional[str]) -> float:
    if not timestamp:
        return 0.0
    try:
        moment = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("unparsable_timestamp value=%s", timestamp)
        return 0.0
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return max(0.0, (datetime.datetime.now(datetime.timezone.utc) - moment).total_seconds() / 86400.0)


def _is_protected(name: str) -> bool:
    return name in PROTECTED_BRANCH_NAMES or name.startswith(PROTECTED_PREFIXES)


def _classify(age: float, merged: bool, open_reviews: int) -> Tuple[str, int]:
    """Return a ``(disposition, risk_score)`` pair for one branch."""
    if age < STALE_WARN_DAYS:
        return "active", 0
    score = int(min(60.0, (age - STALE_WARN_DAYS) / 3.0))
    score += RISK_MERGED_BONUS if merged else 0
    score -= RISK_OPEN_REVIEW_PENALTY if open_reviews > 0 else 0
    score = max(0, min(100, score))
    if open_reviews > 0:
        return "review-open", score
    if merged and age >= STALE_CLEANUP_DAYS:
        return "delete-candidate", score
    if age >= STALE_ARCHIVE_DAYS:
        return "archive-candidate", score
    return "notify", score


def _evaluate(repository: str, branches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    evaluated: List[Dict[str, Any]] = []
    for branch in branches:
        name = str(branch.get("name", ""))
        if not name or _is_protected(name):
            continue
        age = _age_days(branch.get("last_commit_at"))
        merged = bool(branch.get("merged_into_mainline", False))
        open_reviews = int(branch.get("open_review_count", 0))
        disposition, score = _classify(age, merged, open_reviews)
        if disposition == "active":
            continue
        evaluated.append({
            "repository": repository, "branch": name, "age_days": round(age, 1),
            "owner": branch.get("last_committer", "unassigned"), "merged": merged,
            "open_reviews": open_reviews, "disposition": disposition, "risk_score": score,
            "commits_ahead": int(branch.get("commits_ahead", 0)),
        })
    evaluated.sort(key=lambda b: b["risk_score"], reverse=True)
    return evaluated


def _request_deletion(repository: str, branch: str) -> bool:
    body = json.dumps({"repository": repository, "branch": branch}).encode("utf-8")
    request = urllib.request.Request(SCM_BASE_URL.rstrip("/") + "/branches/delete",
                                    data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    if SCM_TOKEN:
        request.add_header("Authorization", "Bearer " + SCM_TOKEN)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            return 200 <= response.status < 300
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        logger.error("branch_delete_failed repository=%s branch=%s reason=%s", repository, branch, exc)
        return False


def _notify(findings: List[Dict[str, Any]]) -> int:
    if not NOTIFY_TOPIC:
        return 0
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for finding in findings:
        grouped.setdefault(finding["owner"], []).append(finding)
    published = 0
    for owner, owned in grouped.items():
        try:
            sns.publish(TopicArn=NOTIFY_TOPIC,
                        Subject="Stale branch review - %d branch(es)" % len(owned),
                        Message=str({"owner": owner, "branches": owned}))
            published += 1
        except ClientError as exc:
            logger.error("branch_notify_failed owner=%s error=%s", owner, exc)
    return published


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    repositories = event.get("repositories") or []
    apply_changes = bool(event.get("apply", False))
    if isinstance(repositories, str):
        repositories = [repositories]

    findings: List[Dict[str, Any]] = []
    failed: List[str] = []
    for repository in repositories:
        try:
            findings.extend(_evaluate(repository, _fetch_branches(repository)))
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as exc:
            logger.error("repository_scan_failed repository=%s reason=%s", repository, exc)
            failed.append(repository)

    deleted = 0
    if apply_changes:
        for finding in findings:
            if finding["disposition"] == "delete-candidate" and \
                    _request_deletion(finding["repository"], finding["branch"]):
                deleted += 1
    else:
        logger.info("dry_run_mode findings=%s", len(findings))

    summary = {
        "repositories_scanned": len(repositories) - len(failed),
        "repositories_failed": failed, "stale_branches": len(findings),
        "delete_candidates": sum(1 for f in findings if f["disposition"] == "delete-candidate"),
        "archive_candidates": sum(1 for f in findings if f["disposition"] == "archive-candidate"),
        "blocked_by_review": sum(1 for f in findings if f["disposition"] == "review-open"),
        "deleted": deleted, "applied": apply_changes, "branches": findings,
        "owner_notifications": _notify(findings) if findings else 0}
    logger.info("stale_branch_complete repos=%s stale=%s delete_candidates=%s deleted=%s applied=%s",
                summary["repositories_scanned"], len(findings), summary["delete_candidates"],
                deleted, apply_changes)
    return summary
