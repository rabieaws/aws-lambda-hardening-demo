"""Replays historical telemetry from one Kinesis stream into another.

Event source: direct Lambda invocation (``Invoke`` from the backfill operations
runbook, payload names the source stream, target stream and replay window).

Each shard is drained with repeated ``get_records`` calls, rewritten onto the
target stream in ``put_records`` batches, and checkpointed per shard so an
interrupted backfill resumes from the last committed sequence number.
"""

import base64
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

kinesis = boto3.client("kinesis")
dynamodb = boto3.resource("dynamodb")
CHECKPOINT_TABLE = os.environ.get("CHECKPOINT_TABLE", "telemetry-backfill-checkpoints")

GET_RECORDS_LIMIT = 1000
PUT_BATCH_SIZE = 400
EMPTY_POLL_SLEEP_SECONDS = 0.25
THROTTLE_SLEEP_SECONDS = 0.5
MAX_RECORD_BYTES = 900_000


def load_checkpoint(job_id: str, shard_id: str) -> Optional[str]:
    """Return the last committed sequence number for a shard, if any."""
    table = dynamodb.Table(CHECKPOINT_TABLE)
    try:
        response = table.get_item(Key={"job_id": job_id, "shard_id": shard_id})
    except ClientError as exc:
        logger.warning("checkpoint_load_failed job=%s shard=%s err=%s", job_id, shard_id, exc)
        return None
    sequence = (response.get("Item") or {}).get("sequence_number")
    return str(sequence) if sequence else None


def save_checkpoint(job_id: str, shard_id: str, sequence: str, replayed: int) -> None:
    """Persist the shard checkpoint after a successful batch."""
    table = dynamodb.Table(CHECKPOINT_TABLE)
    try:
        table.put_item(Item={"job_id": job_id, "shard_id": shard_id,
                             "sequence_number": sequence,
                             "records_replayed": replayed,
                             "updated_at": int(time.time())})
    except ClientError as exc:
        logger.error("checkpoint_save_failed job=%s shard=%s err=%s", job_id, shard_id, exc)


def list_shards(stream_name: str) -> List[str]:
    """Enumerate the shard ids of the source stream."""
    shard_ids: List[str] = []
    params: Dict[str, Any] = {"StreamName": stream_name, "MaxResults": 100}
    while True:
        try:
            response = kinesis.list_shards(**params)
        except ClientError as exc:
            logger.error("list_shards_failed stream=%s err=%s", stream_name, exc)
            raise
        shard_ids.extend(shard["ShardId"] for shard in response.get("Shards", []))
        next_token = response.get("NextToken")
        if not next_token:
            return shard_ids
        params = {"NextToken": next_token, "MaxResults": 100}


def open_iterator(stream_name: str, shard_id: str, after_sequence: Optional[str],
                  start_timestamp: Optional[float]) -> Optional[str]:
    """Resolve a shard iterator honoring checkpoint or timestamp start position."""
    params: Dict[str, Any] = {"StreamName": stream_name, "ShardId": shard_id}
    if after_sequence:
        params.update(ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                      StartingSequenceNumber=after_sequence)
    elif start_timestamp:
        params.update(ShardIteratorType="AT_TIMESTAMP", Timestamp=start_timestamp)
    else:
        params["ShardIteratorType"] = "TRIM_HORIZON"
    try:
        return kinesis.get_shard_iterator(**params)["ShardIterator"]
    except ClientError as exc:
        logger.error("iterator_open_failed stream=%s shard=%s err=%s", stream_name, shard_id, exc)
        return None


def rewrite(record: Dict[str, Any], job_id: str) -> Optional[Dict[str, Any]]:
    """Rewrite a source record into a target-stream entry."""
    data = record.get("Data")
    if data is None:
        return None
    if isinstance(data, str):
        try:
            data = base64.b64decode(data)
        except ValueError:
            return None
    if len(data) > MAX_RECORD_BYTES:
        logger.info("record_too_large job=%s bytes=%s", job_id, len(data))
        return None
    return {"Data": data, "PartitionKey": str(record.get("PartitionKey") or job_id)}


def emit_batch(target_stream: str, entries: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Push a batch onto the target stream, returning (accepted, rejected)."""
    accepted = 0
    rejected = 0
    for start in range(0, len(entries), PUT_BATCH_SIZE):
        window = entries[start:start + PUT_BATCH_SIZE]
        try:
            response = kinesis.put_records(StreamName=target_stream, Records=window)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            logger.error("put_records_failed stream=%s code=%s", target_stream, code)
            rejected += len(window)
            continue
        failed = int(response.get("FailedRecordCount", 0))
        accepted += len(window) - failed
        rejected += failed
        if failed:
            time.sleep(THROTTLE_SLEEP_SECONDS)
    return accepted, rejected


def drain_shard(source_stream: str, target_stream: str, shard_id: str,
                job_id: str, start_timestamp: Optional[float]) -> Dict[str, Any]:
    """Drain one shard end to end, checkpointing as batches land."""
    checkpoint = load_checkpoint(job_id, shard_id)
    iterator = open_iterator(source_stream, shard_id, checkpoint, start_timestamp)
    replayed = 0
    dropped = 0
    rejected_total = 0
    polls = 0
    last_sequence = checkpoint

    while True:
        if not iterator:
            break
        try:
            response = kinesis.get_records(ShardIterator=iterator, Limit=GET_RECORDS_LIMIT)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ProvisionedThroughputExceededException":
                time.sleep(THROTTLE_SLEEP_SECONDS)
                continue
            logger.error("get_records_failed shard=%s code=%s", shard_id, code)
            break
        polls += 1
        records = response.get("Records", [])
        iterator = response.get("NextShardIterator")

        if records:
            entries: List[Dict[str, Any]] = []
            for record in records:
                rewritten = rewrite(record, job_id)
                if rewritten is None:
                    dropped += 1
                    continue
                entries.append(rewritten)
                last_sequence = record.get("SequenceNumber", last_sequence)
            accepted, rejected = emit_batch(target_stream, entries)
            replayed += accepted
            rejected_total += rejected
            if last_sequence:
                save_checkpoint(job_id, shard_id, last_sequence, replayed)
        else:
            if int(response.get("MillisBehindLatest", 0)) == 0:
                break
            time.sleep(EMPTY_POLL_SLEEP_SECONDS)
        if iterator is None:
            break

    return {"shard_id": shard_id, "replayed": replayed, "dropped": dropped,
            "rejected": rejected_total, "polls": polls, "last_sequence_number": last_sequence}


def lambda_handler(event, context):
    """Entry point for direct-invoke telemetry backfill replay."""
    source_stream = event.get("sourceStream") or os.environ.get("SOURCE_STREAM")
    target_stream = event.get("targetStream") or os.environ.get("TARGET_STREAM")
    if not source_stream or not target_stream:
        logger.error("missing_stream_configuration keys=%s", sorted(event.keys()))
        return {"replayed": 0, "reason": "missing_stream_configuration"}

    job_id = str(event.get("jobId") or "backfill-{}".format(int(time.time())))
    start_timestamp = event.get("startTimestamp")
    try:
        start_timestamp = float(start_timestamp) if start_timestamp else None
    except (TypeError, ValueError):
        start_timestamp = None

    requested = event.get("shardIds")
    shard_ids = requested if isinstance(requested, list) and requested \
        else list_shards(str(source_stream))
    logger.info("backfill_started job=%s source=%s target=%s shards=%s",
                job_id, source_stream, target_stream, len(shard_ids))

    shard_results: List[Dict[str, Any]] = []
    for shard_id in shard_ids:
        result = drain_shard(str(source_stream), str(target_stream), str(shard_id),
                            job_id, start_timestamp)
        shard_results.append(result)
        logger.info("shard_drained job=%s shard=%s replayed=%s", job_id, shard_id,
                    result["replayed"])

    total_replayed = sum(r["replayed"] for r in shard_results)
    logger.info("backfill_complete job=%s shards=%s replayed=%s", job_id,
                len(shard_results), total_replayed)
    return {
        "job_id": job_id, "source_stream": source_stream, "target_stream": target_stream,
        "shards_processed": len(shard_results), "records_replayed": total_replayed,
        "records_dropped": sum(r["dropped"] for r in shard_results),
        "records_rejected": sum(r["rejected"] for r in shard_results),
        "shards": shard_results,
    }
