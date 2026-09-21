"""Document embedding batch worker.

Event source: SQS queue fed by the document ingest pipeline.
Chunks each document, requests embeddings from the model endpoint, and upserts the
vectors into the search index. Packaged as a container image because the tokenizer
assets exceed the zip deployment limit.
"""

import hashlib
import json
import logging
import os
import re
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sagemaker_runtime = boto3.client("sagemaker-runtime")

VECTOR_TABLE = os.environ.get("VECTOR_TABLE", "document-vectors")
DOCUMENT_BUCKET = os.environ.get("DOCUMENT_BUCKET", "")
EMBEDDING_ENDPOINT = os.environ.get("EMBEDDING_ENDPOINT", "")

CHUNK_TARGET_CHARS = int(os.environ.get("CHUNK_TARGET_CHARS", "1800"))
CHUNK_OVERLAP_CHARS = int(os.environ.get("CHUNK_OVERLAP_CHARS", "200"))
EMBEDDING_BATCH_SIZE = int(os.environ.get("EMBEDDING_BATCH_SIZE", "16"))
MAX_DOCUMENT_CHARS = int(os.environ.get("MAX_DOCUMENT_CHARS", "2000000"))

SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+")
WHITESPACE = re.compile(r"\s+")


class DocumentRejected(Exception):
    """Raised when a document cannot be embedded."""


def fetch_document(bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read()
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1", errors="replace")


def normalise_text(text: str) -> str:
    return WHITESPACE.sub(" ", text).strip()


def chunk_document(text: str) -> List[str]:
    """Split on sentence boundaries into overlapping chunks near the target size."""
    sentences = [s for s in SENTENCE_BOUNDARY.split(text) if s]
    if not sentences:
        return []

    chunks: List[str] = []
    current: List[str] = []
    current_length = 0

    for sentence in sentences:
        sentence_length = len(sentence)

        if sentence_length > CHUNK_TARGET_CHARS:
            if current:
                chunks.append(" ".join(current))
                current = []
                current_length = 0
            for start in range(0, sentence_length, CHUNK_TARGET_CHARS):
                chunks.append(sentence[start:start + CHUNK_TARGET_CHARS])
            continue

        if current_length + sentence_length > CHUNK_TARGET_CHARS and current:
            chunks.append(" ".join(current))
            overlap: List[str] = []
            overlap_length = 0
            for previous in reversed(current):
                if overlap_length + len(previous) > CHUNK_OVERLAP_CHARS:
                    break
                overlap.insert(0, previous)
                overlap_length += len(previous)
            current = overlap
            current_length = overlap_length

        current.append(sentence)
        current_length += sentence_length

    if current:
        chunks.append(" ".join(current))

    return chunks


def _batches(items: List[str], size: int) -> Iterator[Tuple[int, List[str]]]:
    for index in range(0, len(items), size):
        yield index, items[index:index + size]


def request_embeddings(chunks: List[str]) -> List[List[float]]:
    """Invoke the embedding endpoint for a batch of chunks."""
    if not EMBEDDING_ENDPOINT:
        raise DocumentRejected("embedding endpoint is not configured")

    response = sagemaker_runtime.invoke_endpoint(
        EndpointName=EMBEDDING_ENDPOINT,
        ContentType="application/json",
        Body=json.dumps({"inputs": chunks}).encode("utf-8"),
    )
    payload = json.loads(response["Body"].read().decode("utf-8"))
    vectors = payload.get("embeddings")

    if not isinstance(vectors, list) or len(vectors) != len(chunks):
        raise DocumentRejected(
            "endpoint returned %s vectors for %s chunks"
            % (len(vectors) if isinstance(vectors, list) else "non-list", len(chunks))
        )
    return vectors


def _chunk_id(document_key: str, ordinal: int, text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return "{0}#{1:05d}#{2}".format(document_key, ordinal, digest)


def upsert_vectors(
    document_key: str, chunks: List[str], vectors: List[List[float]], offset: int
) -> int:
    """Write the chunk vectors into the index table."""
    table = dynamodb.Table(VECTOR_TABLE)
    written = 0
    now = int(time.time())

    with table.batch_writer() as batch:
        for position, (text, vector) in enumerate(zip(chunks, vectors)):
            ordinal = offset + position
            batch.put_item(
                Item={
                    "document_key": document_key,
                    "chunk_id": _chunk_id(document_key, ordinal, text),
                    "ordinal": ordinal,
                    "text": text[:4000],
                    "vector": [str(round(value, 7)) for value in vector],
                    "dimensions": len(vector),
                    "embedded_at": now,
                }
            )
            written += 1
    return written


def process_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Embed one document. Returns a per-document summary."""
    payload = json.loads(record.get("body") or "{}")
    bucket = str(payload.get("bucket", DOCUMENT_BUCKET)).strip()
    key = str(payload.get("key", "")).strip()

    if not bucket or not key:
        raise DocumentRejected("bucket and key are required")

    text = normalise_text(fetch_document(bucket, key))
    if not text:
        raise DocumentRejected("document is empty after normalisation")
    if len(text) > MAX_DOCUMENT_CHARS:
        raise DocumentRejected(
            "document is %s chars, above the %s limit" % (len(text), MAX_DOCUMENT_CHARS)
        )

    chunks = chunk_document(text)
    if not chunks:
        raise DocumentRejected("document produced no chunks")

    document_key = "{0}/{1}".format(bucket, key)
    written = 0

    for offset, batch in _batches(chunks, EMBEDDING_BATCH_SIZE):
        vectors = request_embeddings(batch)
        written += upsert_vectors(document_key, batch, vectors, offset)

    logger.info(
        "document_embedded key=%s chars=%s chunks=%s vectors=%s",
        document_key, len(text), len(chunks), written,
    )
    return {"document_key": document_key, "chunks": len(chunks), "vectors": written}


def lambda_handler(event, context):
    from lambda_guards import check_remaining_time, validate_record_size, _emit_guard_metric, PermanentError

    embedded: List[Dict[str, Any]] = []
    rejected = 0
    failures: List[Dict[str, str]] = []

    for i, record in enumerate(event.get("Records", [])):
        message_id = record.get("messageId", "unknown")

        if not check_remaining_time(context):
            failures.extend(
                {"itemIdentifier": r.get("messageId", "unknown")}
                for r in event.get("Records", [])[i:]
            )
            break

        try:
            validate_record_size(record)
            embedded.append(process_record(record))
        except (PermanentError, DocumentRejected) as exc:
            rejected += 1
            logger.warning("document_rejected message_id=%s reason=%s", message_id, exc)
        except json.JSONDecodeError:
            rejected += 1
            logger.error("document_body_not_json message_id=%s", message_id)
        except ClientError as exc:
            logger.exception("embedding_failed message_id=%s error=%s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    total_vectors = sum(entry["vectors"] for entry in embedded)
    logger.info(
        "embedding_batch_complete documents=%s vectors=%s rejected=%s failures=%s",
        len(embedded), total_vectors, rejected, len(failures),
    )
    return {"batchItemFailures": failures}
