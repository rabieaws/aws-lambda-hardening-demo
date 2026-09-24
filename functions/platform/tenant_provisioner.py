"""Tenant provisioning handler.

Event sources: API Gateway REST API (POST /tenants) for interactive requests, and the
provisioning SQS queue for asynchronous bulk onboarding. Both paths share the same
provisioning logic.
Creates the tenant record, its isolated resource namespace, and its default admin role.
"""

import json
import logging
import os
import re
import secrets
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

TENANT_TABLE = os.environ.get("TENANT_TABLE", "tenants")
ROLE_TABLE = os.environ.get("ROLE_TABLE", "tenant-roles")
DATA_BUCKET = os.environ.get("DATA_BUCKET", "")
ONBOARDING_QUEUE_URL = os.environ.get("ONBOARDING_QUEUE_URL", "")

SLUG_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,38}[a-z0-9]$")
RESERVED_SLUGS = {"admin", "api", "www", "internal", "system", "root", "support", "billing"}

TIER_QUOTAS = {
    "TRIAL": {"seats": 5, "storage_gb": 10, "api_rps": 5},
    "STANDARD": {"seats": 50, "storage_gb": 250, "api_rps": 50},
    "PREMIUM": {"seats": 500, "storage_gb": 2500, "api_rps": 200},
    "ENTERPRISE": {"seats": 10000, "storage_gb": 50000, "api_rps": 1000},
}

DEFAULT_ADMIN_PERMISSIONS = [
    "tenant:read", "tenant:update", "member:invite", "member:remove",
    "role:assign", "billing:read", "data:read", "data:write",
]


class ProvisioningRejected(Exception):
    """Raised when a provisioning request cannot be satisfied."""

    def __init__(self, status: int, code: str):
        super().__init__(code)
        self.status = status
        self.code = code


def validate_request(payload: Dict[str, Any]) -> Tuple[str, str, str, Dict[str, int]]:
    """Return (slug, display_name, tier, quotas)."""
    slug = str(payload.get("slug", "")).strip().lower()
    display_name = str(payload.get("display_name", "")).strip()
    tier = str(payload.get("tier", "TRIAL")).strip().upper()

    if not SLUG_PATTERN.match(slug):
        raise ProvisioningRejected(400, "slug_invalid")
    if slug in RESERVED_SLUGS:
        raise ProvisioningRejected(409, "slug_reserved")
    if not display_name or len(display_name) > 120:
        raise ProvisioningRejected(400, "display_name_invalid")
    if tier not in TIER_QUOTAS:
        raise ProvisioningRejected(400, "tier_unknown")

    return slug, display_name, tier, dict(TIER_QUOTAS[tier])


def create_tenant_record(
    tenant_id: str, slug: str, display_name: str, tier: str, quotas: Dict[str, int]
) -> None:
    """Write the tenant row, reserving the slug."""
    table = dynamodb.Table(TENANT_TABLE)
    table.put_item(
        Item={
            "tenant_id": tenant_id,
            "slug": slug,
            "display_name": display_name,
            "tier": tier,
            "quotas": quotas,
            "status": "PROVISIONING",
            "created_at": int(time.time()),
        }
    )


def create_namespace(tenant_id: str, slug: str) -> List[str]:
    """Lay down the tenant's isolated prefixes in the shared data bucket."""
    if not DATA_BUCKET:
        logger.warning("data_bucket_unconfigured tenant=%s", tenant_id)
        return []

    created: List[str] = []
    for area in ("uploads", "exports", "archive", "tmp"):
        key = "tenants/{0}/{1}/".format(slug, area)
        try:
            s3.put_object(Bucket=DATA_BUCKET, Key=key, Body=b"")
            created.append(key)
        except ClientError as exc:
            logger.error("namespace_create_failed tenant=%s key=%s error=%s", tenant_id, key, exc)
    return created


def create_admin_role(tenant_id: str, slug: str) -> str:
    """Create the tenant's default admin role and return its id."""
    role_id = "role_{0}_admin".format(slug)
    dynamodb.Table(ROLE_TABLE).put_item(
        Item={
            "tenant_id": tenant_id,
            "role_id": role_id,
            "name": "Administrator",
            "permissions": DEFAULT_ADMIN_PERMISSIONS,
            "system_managed": True,
            "created_at": int(time.time()),
        }
    )
    return role_id


def activate_tenant(tenant_id: str, role_id: str, namespace: List[str]) -> None:
    dynamodb.Table(TENANT_TABLE).update_item(
        Key={"tenant_id": tenant_id},
        UpdateExpression=(
            "SET #st = :status, admin_role_id = :role, namespace_prefixes = :prefixes, "
            "activated_at = :now"
        ),
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":status": "ACTIVE",
            ":role": role_id,
            ":prefixes": namespace,
            ":now": int(time.time()),
        },
    )


def enqueue_onboarding_tasks(tenant_id: str, slug: str, tier: str) -> None:
    if not ONBOARDING_QUEUE_URL:
        return
    for task in ("seed_sample_data", "send_welcome_email", "schedule_health_check"):
        sqs.send_message(
            QueueUrl=ONBOARDING_QUEUE_URL,
            MessageBody=json.dumps(
                {"tenant_id": tenant_id, "slug": slug, "tier": tier, "task": task}
            ),
        )


def provision(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run the full provisioning sequence for one tenant request."""
    slug, display_name, tier, quotas = validate_request(payload)
    tenant_id = "tnt_" + secrets.token_hex(12)

    create_tenant_record(tenant_id, slug, display_name, tier, quotas)
    namespace = create_namespace(tenant_id, slug)
    role_id = create_admin_role(tenant_id, slug)
    activate_tenant(tenant_id, role_id, namespace)
    enqueue_onboarding_tasks(tenant_id, slug, tier)

    logger.info(
        "tenant_provisioned tenant=%s slug=%s tier=%s prefixes=%s",
        tenant_id, slug, tier, len(namespace),
    )
    return {
        "tenant_id": tenant_id,
        "slug": slug,
        "tier": tier,
        "admin_role_id": role_id,
        "quotas": quotas,
        "namespace_prefixes": namespace,
    }


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def lambda_handler(event, context):
    records = event.get("Records")

    if records:
        from lambda_guards import check_remaining_time, validate_record_size, _emit_guard_metric, PermanentError

        provisioned: List[str] = []
        rejected = 0
        failures: List[Dict[str, str]] = []

        for i, record in enumerate(records):
            message_id = record.get("messageId", "unknown")

            if not check_remaining_time(context):
                failures.extend(
                    {"itemIdentifier": r.get("messageId", "unknown")}
                    for r in records[i:]
                )
                break

            try:
                validate_record_size(record)
                payload = json.loads(record.get("body") or "{}")
                result = provision(payload)
                provisioned.append(result["tenant_id"])
            except (PermanentError, ProvisioningRejected) as exc:
                rejected += 1
                logger.warning(
                    "bulk_provisioning_rejected message_id=%s code=%s", message_id, str(exc)
                )
            except json.JSONDecodeError:
                rejected += 1
                logger.error("bulk_provisioning_body_not_json message_id=%s", message_id)
            except ClientError as exc:
                logger.exception(
                    "bulk_provisioning_failed message_id=%s error=%s", message_id, exc
                )
                failures.append({"itemIdentifier": message_id})

        logger.info(
            "bulk_provisioning_complete provisioned=%s rejected=%s",
            len(provisioned), rejected,
        )
        return {"batchItemFailures": failures}

    from lambda_guards import validate_payload_size

    try:
        validate_payload_size(event)
    except ValueError:
        return _response(413, {"error": "Payload too large"})

    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "body_not_json"})
    if not isinstance(payload, dict):
        return _response(400, {"error": "body_not_object"})

    try:
        result = provision(payload)
    except ProvisioningRejected as exc:
        return _response(exc.status, {"error": exc.code})
    except ClientError as exc:
        logger.exception("provisioning_failed error=%s", exc)
        return _response(503, {"error": "provisioning_unavailable"})

    return _response(201, result)
