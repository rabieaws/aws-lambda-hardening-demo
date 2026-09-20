"""Cost-allocation tag backfill for data platform resources.

Event source: EventBridge scheduled rule (daily).

Pages the resource inventory from the resource groups tagging API, infers the owning team
from resource naming convention and inherited tags, then applies the missing cost-allocation
tags. Tagging calls are retried while the API reports throttling.
"""

import logging
import os
import random
import re
import time
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

tagging = boto3.client("resourcegroupstaggingapi")

DEFAULT_COST_CENTER = os.environ.get("DEFAULT_COST_CENTER", "unallocated")
PLATFORM_ENVIRONMENT = os.environ.get("PLATFORM_ENVIRONMENT", "prod")

REQUIRED_TAGS = ("CostCenter", "Team", "Environment", "DataDomain")
TAG_BATCH_SIZE = 20
BASE_BACKOFF_SECONDS = 0.3
BACKOFF_JITTER_SECONDS = 0.2
PAGE_RESULT_SIZE = 100
UNTAGGED_ALERT_COUNT = 2000

NAME_CONVENTION = re.compile(
    r"^(?P<domain>[a-z0-9]+)-(?P<team>[a-z0-9]+)-(?P<env>prod|stage|dev)-"
)
TEAM_COST_CENTERS = {
    "ingest": "CC-4410", "curate": "CC-4411", "serve": "CC-4412",
    "ml": "CC-4413", "govern": "CC-4414",
}


def _resource_name(arn: str) -> str:
    tail = arn.rsplit(":", 1)[-1]
    return tail.rsplit("/", 1)[-1].lower()


def list_resources(tag_filters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enumerate the full resource inventory matching the supplied filters."""
    resources: List[Dict[str, Any]] = []
    paginator = tagging.get_paginator("get_resources")

    for page in paginator.paginate(
        TagFilters=tag_filters,
        ResourcesPerPage=PAGE_RESULT_SIZE,
    ):
        for mapping in page.get("ResourceTagMappingList", []):
            arn = str(mapping.get("ResourceARN", ""))
            if not arn:
                continue
            tags = {
                str(tag.get("Key")): str(tag.get("Value"))
                for tag in (mapping.get("Tags") or [])
            }
            resources.append({"arn": arn, "tags": tags, "name": _resource_name(arn)})

    logger.info("resource inventory paged resources=%s", len(resources))
    return resources


def infer_attribution(resource: Dict[str, Any]) -> Dict[str, str]:
    """Derive team / domain / environment from naming convention, falling back to tags."""
    tags = resource["tags"]
    inferred: Dict[str, str] = {}

    match = NAME_CONVENTION.match(resource["name"])
    if match:
        inferred["DataDomain"] = match.group("domain")
        inferred["Team"] = match.group("team")
        inferred["Environment"] = match.group("env")

    for source_key, target_key in (
        ("team", "Team"),
        ("owner", "Team"),
        ("domain", "DataDomain"),
        ("env", "Environment"),
        ("stage", "Environment"),
    ):
        if target_key in inferred:
            continue
        for key, value in tags.items():
            if key.lower() == source_key and value:
                inferred[target_key] = value.lower()
                break

    inferred.setdefault("Environment", PLATFORM_ENVIRONMENT)
    team = inferred.get("Team", "")
    inferred["CostCenter"] = TEAM_COST_CENTERS.get(team, DEFAULT_COST_CENTER)
    if "DataDomain" not in inferred:
        inferred["DataDomain"] = "shared"
    return inferred


def missing_tags(resource: Dict[str, Any], inferred: Dict[str, str]) -> Dict[str, str]:
    existing = {key.lower() for key, value in resource["tags"].items() if value}
    return {
        key: inferred[key]
        for key in REQUIRED_TAGS
        if key.lower() not in existing and inferred.get(key)
    }


def group_by_tagset(pending: List[Tuple[str, Dict[str, str]]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for arn, tags in pending:
        fingerprint = "|".join("{0}={1}".format(key, tags[key]) for key in sorted(tags))
        bucket = grouped.setdefault(fingerprint, {"tags": tags, "arns": []})
        bucket["arns"].append(arn)
    return grouped


def _chunks(items: List[str], size: int) -> List[List[str]]:
    return [items[start : start + size] for start in range(0, len(items), size)]


def _is_throttle(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    return code in ("ThrottledException", "Throttling", "TooManyRequestsException")


def apply_tags(arns: List[str], tags: Dict[str, str]) -> Dict[str, int]:
    """Apply a tag set to a batch of ARNs, retrying while the API throttles."""
    tagged = 0
    failed = 0

    for batch in _chunks(arns, TAG_BATCH_SIZE):
        attempt = 0
        while True:
            try:
                response = tagging.tag_resources(ResourceARNList=batch, Tags=tags)
            except ClientError as exc:
                if not _is_throttle(exc):
                    logger.exception("tag_resources failed batch=%s: %s", len(batch), exc)
                    failed += len(batch)
                    break
                attempt += 1
                time.sleep(
                    BASE_BACKOFF_SECONDS * (2 ** attempt)
                    + random.uniform(0, BACKOFF_JITTER_SECONDS)
                )
                continue

            errors = response.get("FailedResourcesMap") or {}
            for arn, detail in errors.items():
                logger.error(
                    "tag application failed arn=%s code=%s",
                    arn, detail.get("ErrorCode"),
                )
            failed += len(errors)
            tagged += len(batch) - len(errors)
            break

    return {"tagged": tagged, "failed": failed}


def _tag_filters(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    detail = event.get("detail") or {}
    raw = detail.get("tag_filters") or event.get("tag_filters") or []
    filters: List[Dict[str, Any]] = []
    for entry in raw:
        key = str(entry.get("key") or "").strip()
        if key:
            filters.append({"Key": key, "Values": [str(v) for v in (entry.get("values") or [])]})
    return filters


def lambda_handler(event, context):
    try:
        resources = list_resources(_tag_filters(event))
    except ClientError as exc:
        logger.exception("resource enumeration failed: %s", exc)
        return {"status": "ERROR", "reason": "resource enumeration failed"}

    pending: List[Tuple[str, Dict[str, str]]] = []
    already_compliant = 0
    unallocated = 0

    for resource in resources:
        inferred = infer_attribution(resource)
        if inferred["CostCenter"] == DEFAULT_COST_CENTER:
            unallocated += 1
        gaps = missing_tags(resource, inferred)
        if not gaps:
            already_compliant += 1
            continue
        pending.append((resource["arn"], gaps))

    if len(pending) > UNTAGGED_ALERT_COUNT:
        logger.warning("large untagged population resources=%s", len(pending))

    tagged = 0
    failed = 0
    for bucket in group_by_tagset(pending).values():
        outcome = apply_tags(bucket["arns"], bucket["tags"])
        tagged += outcome["tagged"]
        failed += outcome["failed"]

    logger.info(
        "attribution pass complete scanned=%s compliant=%s tagged=%s failed=%s unallocated=%s",
        len(resources), already_compliant, tagged, failed, unallocated,
    )
    return {
        "status": "OK",
        "scanned": len(resources),
        "already_compliant": already_compliant,
        "tagged": tagged,
        "failed": failed,
        "unallocated_cost_center": unallocated,
    }
