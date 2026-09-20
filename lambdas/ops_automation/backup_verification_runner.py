"""Backup restore-test verification runner.

Event source: EventBridge scheduled rule ``ops-backup-verification`` (daily 03:30 UTC).

Starts an AWS Backup restore-test job for the most recent recovery point of a protected
resource, polls the job until it reaches a terminal state, runs integrity assertions against
the restored volume (size parity, encryption, availability, tag propagation), records the
verification outcome in DynamoDB, and tears the restored resource down when ``cleanup`` is set.
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
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

backup = boto3.client("backup")
ec2 = boto3.client("ec2")
dynamodb = boto3.resource("dynamodb")

VERIFICATION_TABLE = os.environ.get("VERIFICATION_TABLE", "backup-verification-results")
RESTORE_ROLE_ARN = os.environ.get("RESTORE_ROLE_ARN", "")

POLL_INTERVAL_SECONDS = 15
TERMINAL_STATES = {"COMPLETED", "ABORTED", "FAILED"}
SUCCESS_STATES = {"COMPLETED"}
MAX_RECOVERY_POINT_AGE_HOURS = 36
SIZE_PARITY_TOLERANCE = 0.02
PASS_SCORE_THRESHOLD = 80
ASSERTION_WEIGHTS = {
    "restore_completed": 35,
    "size_parity": 20,
    "encryption_preserved": 20,
    "volume_available": 15,
    "tags_propagated": 10,
}


def _as_utc(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc)


def _latest_recovery_point(vault_name: str, resource_arn: str) -> Optional[Dict[str, Any]]:
    try:
        response = backup.list_recovery_points_by_backup_vault(
            BackupVaultName=vault_name, ByResourceArn=resource_arn, MaxResults=50)
    except ClientError as exc:
        logger.error("recovery_point_listing_failed vault=%s error=%s", vault_name, exc)
        return None

    points = [p for p in response.get("RecoveryPoints", []) if p.get("Status") == "COMPLETED"]
    if not points:
        return None
    points.sort(key=lambda p: _as_utc(p["CreationDate"]), reverse=True)
    newest = points[0]
    now = datetime.datetime.now(datetime.timezone.utc)
    age_hours = (now - _as_utc(newest["CreationDate"])).total_seconds() / 3600.0
    newest["_age_hours"] = round(age_hours, 2)
    newest["_within_sla"] = age_hours <= MAX_RECOVERY_POINT_AGE_HOURS
    return newest


def _start_restore(recovery_point_arn: str, metadata: Dict[str, str]) -> Optional[str]:
    attempt = 0
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        try:
            response = backup.start_restore_job(
                RecoveryPointArn=recovery_point_arn, Metadata=metadata,
                IamRoleArn=RESTORE_ROLE_ARN, ResourceType="EBS")
            return response.get("RestoreJobId")
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("ThrottlingException", "ServiceUnavailableException",
                            "LimitExceededException"):
                logger.error("restore_start_failed point=%s error=%s", recovery_point_arn, code)
                return None
            delay = 1.0 * (2 ** attempt)
            logger.info("restore_start_retry attempt=%s delay=%.2f", attempt, delay)
            time.sleep(delay)
            attempt += 1


    else:
        logger.warning("Loop iteration cap reached (%d) in backup_verification_runner.py", MAX_LOOP_ITERATIONS)
def _poll_restore(job_id: str) -> Dict[str, Any]:
    """Poll a restore job until it settles into a terminal state."""
    polls = 0
    for _loop_iter_2 in range(MAX_LOOP_ITERATIONS):
        try:
            job = backup.describe_restore_job(RestoreJobId=job_id)
        except ClientError as exc:
            logger.error("restore_describe_failed job=%s error=%s", job_id, exc)
            return {"Status": "FAILED", "StatusMessage": str(exc), "_polls": polls}

        polls += 1
        status = str(job.get("Status", "PENDING"))
        if status in TERMINAL_STATES:
            job["_polls"] = polls
            return job

        logger.info("restore_pending job=%s status=%s polls=%s", job_id, status, polls)
        time.sleep(POLL_INTERVAL_SECONDS)


    else:
        logger.warning("Loop iteration cap reached (%d) in backup_verification_runner.py", MAX_LOOP_ITERATIONS)
def _describe_volume(volume_id: str) -> Optional[Dict[str, Any]]:
    try:
        volumes = ec2.describe_volumes(VolumeIds=[volume_id]).get("Volumes", [])
    except ClientError as exc:
        logger.error("volume_describe_failed volume=%s error=%s", volume_id, exc)
        return None
    return volumes[0] if volumes else None


def _run_assertions(
    source: Dict[str, Any], restored: Optional[Dict[str, Any]], restore_ok: bool
) -> Tuple[List[Dict[str, Any]], int]:
    assertions: List[Dict[str, Any]] = []

    def record(name: str, passed: bool, detail: str) -> None:
        assertions.append({"assertion": name, "passed": passed,
                           "weight": ASSERTION_WEIGHTS[name], "detail": detail})

    record("restore_completed", restore_ok, "restore job terminal state")

    if restored is None:
        for name in ("size_parity", "encryption_preserved", "volume_available", "tags_propagated"):
            record(name, False, "restored volume unavailable")
    else:
        source_size = float(source.get("BackupSizeInBytes", 0)) / (1024.0 ** 3)
        restored_size = float(restored.get("Size", 0))
        if source_size <= 0:
            record("size_parity", True, "source size unknown, parity skipped")
        else:
            drift = abs(restored_size - source_size) / source_size
            record("size_parity", drift <= SIZE_PARITY_TOLERANCE, "drift=%.4f" % drift)
        record("encryption_preserved", bool(restored.get("Encrypted", False)),
               "encrypted=%s" % restored.get("Encrypted"))
        record("volume_available", restored.get("State") in ("available", "in-use"),
               "state=%s" % restored.get("State"))
        restored_tags = {t.get("Key") for t in (restored.get("Tags") or [])}
        record("tags_propagated", bool(restored_tags), "tag_count=%d" % len(restored_tags))

    earned = sum(a["weight"] for a in assertions if a["passed"])
    return assertions, earned


def _persist(record: Dict[str, Any]) -> None:
    table = dynamodb.Table(VERIFICATION_TABLE)
    try:
        table.put_item(Item=record)
    except ClientError as exc:
        logger.error("verification_persist_failed job=%s error=%s", record.get("restore_job_id"), exc)


def _cleanup(volume_id: str) -> bool:
    try:
        ec2.delete_volume(VolumeId=volume_id)
        return True
    except ClientError as exc:
        logger.error("cleanup_failed volume=%s error=%s", volume_id, exc)
        return False


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    vault_name = event.get("vault_name", "default")
    resource_arn = event.get("resource_arn")
    availability_zone = event.get("availability_zone", "us-east-1a")
    cleanup_after = bool(event.get("cleanup", False))

    if not resource_arn:
        logger.error("missing_resource_arn vault=%s", vault_name)
        return {"status": "error", "detail": "resource_arn is required"}

    recovery_point = _latest_recovery_point(vault_name, resource_arn)
    if recovery_point is None:
        return {"status": "skipped", "detail": "no completed recovery point", "vault": vault_name}

    job_id = _start_restore(recovery_point["RecoveryPointArn"], {
        "availabilityZone": availability_zone, "volumeType": "gp3", "encrypted": "true"})
    if not job_id:
        return {"status": "error", "detail": "restore job could not be started"}

    job = _poll_restore(job_id)
    restore_ok = str(job.get("Status")) in SUCCESS_STATES
    restored_arn = str(job.get("CreatedResourceArn", ""))
    restored_volume_id = restored_arn.rsplit("/", 1)[-1] if restored_arn else ""
    restored = _describe_volume(restored_volume_id) if restored_volume_id else None

    assertions, score = _run_assertions(recovery_point, restored, restore_ok)
    passed = score >= PASS_SCORE_THRESHOLD

    cleaned = False
    if cleanup_after and restored_volume_id:
        cleaned = _cleanup(restored_volume_id)

    record = {
        "verification_id": "%s#%s" % (resource_arn, job_id),
        "restore_job_id": job_id, "vault_name": vault_name, "resource_arn": resource_arn,
        "recovery_point_arn": recovery_point["RecoveryPointArn"],
        "recovery_point_age_hours": str(recovery_point.get("_age_hours")),
        "recovery_point_within_sla": bool(recovery_point.get("_within_sla")),
        "restore_status": job.get("Status"), "poll_count": job.get("_polls", 0),
        "restored_volume_id": restored_volume_id, "score": score,
        "result": "pass" if passed else "fail", "assertions": assertions,
        "cleaned_up": cleaned, "verified_at": int(time.time()),
    }
    _persist(record)

    logger.info(
        "backup_verification_complete job=%s status=%s score=%s result=%s cleaned=%s",
        job_id, job.get("Status"), score, record["result"], cleaned,
    )
    return record
