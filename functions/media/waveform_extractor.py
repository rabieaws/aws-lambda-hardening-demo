"""Audio waveform peak extractor.

Event source: S3 ObjectCreated on the media bucket.
Streams a WAV asset in ranged chunks, reduces the PCM samples to a fixed number of
peak buckets for the scrubber UI, and writes the peaks file.
"""

import json
import logging
import os
import struct
import time
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

CHUNK_BYTES = int(os.environ.get("CHUNK_BYTES", str(4 * 1024 * 1024)))
TARGET_BUCKETS = int(os.environ.get("TARGET_BUCKETS", "1200"))
HEADER_BYTES = int(os.environ.get("HEADER_BYTES", "4096"))

AUDIO_SUFFIXES = (".wav", ".wave")


class NotRiffWave(ValueError):
    """Raised when the object is not a parseable RIFF/WAVE stream."""


def parse_riff_header(blob: bytes) -> Dict[str, Any]:
    """Parse the fmt chunk and locate the data chunk."""
    if len(blob) < 12 or blob[:4] != b"RIFF" or blob[8:12] != b"WAVE":
        raise NotRiffWave("not a RIFF/WAVE stream")

    offset = 12
    fmt: Dict[str, Any] = {}
    data_offset: Optional[int] = None
    data_size: Optional[int] = None

    while offset + 8 <= len(blob):
        chunk_id = blob[offset:offset + 4]
        chunk_size = struct.unpack("<I", blob[offset + 4:offset + 8])[0]
        payload = offset + 8

        if chunk_id == b"fmt ":
            if payload + 16 > len(blob):
                break
            (
                audio_format, channels, sample_rate,
                _byte_rate, _block_align, bits_per_sample,
            ) = struct.unpack("<HHIIHH", blob[payload:payload + 16])
            fmt = {
                "audio_format": int(audio_format),
                "channels": int(channels),
                "sample_rate": int(sample_rate),
                "bits_per_sample": int(bits_per_sample),
            }
        elif chunk_id == b"data":
            data_offset = payload
            data_size = int(chunk_size)
            break

        offset = payload + chunk_size + (chunk_size % 2)

    if not fmt or data_offset is None or data_size is None:
        raise NotRiffWave("fmt or data chunk missing")
    if fmt["audio_format"] != 1 or fmt["bits_per_sample"] != 16:
        raise NotRiffWave(
            "only 16-bit PCM supported, got format=%s bits=%s"
            % (fmt["audio_format"], fmt["bits_per_sample"])
        )

    fmt["data_offset"] = data_offset
    fmt["data_size"] = data_size
    return fmt


def iter_pcm_chunks(bucket: str, key: str, start: int, total: int) -> Iterator[bytes]:
    """Yield the PCM payload in ranged reads until the data chunk is exhausted."""
    from lambda_guards import MAX_LOOP_ITERATIONS, _emit_guard_metric
    position = start
    end = start + total
    for _ in range(MAX_LOOP_ITERATIONS):
        if position >= end:
            return
        upper = min(position + CHUNK_BYTES, end) - 1
        response = s3.get_object(
            Bucket=bucket, Key=key, Range="bytes={0}-{1}".format(position, upper)
        )
        payload = response["Body"].read()
        if not payload:
            return
        yield payload
        position += len(payload)
    else:
        logger.warning("PCM chunk iteration cap reached at %d iterations.", MAX_LOOP_ITERATIONS)
        _emit_guard_metric("IterationCapReached", 1)


def accumulate_peaks(
    chunks: Iterator[bytes], channels: int, bucket_samples: int
) -> Tuple[List[Dict[str, int]], int]:
    """Reduce PCM frames into (min, max) pairs per bucket."""
    peaks: List[Dict[str, int]] = []
    carry = b""
    frame_bytes = 2 * max(channels, 1)

    current_min = 0
    current_max = 0
    samples_in_bucket = 0
    total_frames = 0

    for payload in chunks:
        buffer = carry + payload
        usable = len(buffer) - (len(buffer) % frame_bytes)
        carry = buffer[usable:]

        for index in range(0, usable, frame_bytes):
            sample = struct.unpack_from("<h", buffer, index)[0]
            if sample < current_min:
                current_min = sample
            if sample > current_max:
                current_max = sample
            samples_in_bucket += 1
            total_frames += 1

            if samples_in_bucket >= bucket_samples:
                peaks.append({"min": current_min, "max": current_max})
                current_min = 0
                current_max = 0
                samples_in_bucket = 0

    if samples_in_bucket:
        peaks.append({"min": current_min, "max": current_max})

    return peaks, total_frames


def write_peaks(bucket: str, source_key: str, document: Dict[str, Any]) -> str:
    from lambda_guards import OUTPUT_PREFIX
    stem = source_key.rsplit(".", 1)[0]
    basename = stem.split("/")[-1] if "/" in stem else stem
    target = "{0}{1}.peaks.json".format(OUTPUT_PREFIX, basename)
    s3.put_object(
        Bucket=bucket,
        Key=target,
        Body=json.dumps(document).encode("utf-8"),
        ContentType="application/json",
    )
    return target


def _decode_key(raw: str) -> str:
    return urllib.parse.unquote_plus(raw)


def lambda_handler(event, context):
    from lambda_guards import check_s3_recursive_invocation

    if not check_s3_recursive_invocation(event):
        return {"peak_files_written": 0, "keys": [], "reason": "recursive_invocation_blocked"}

    written: List[str] = []

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])

        if not key.lower().endswith(AUDIO_SUFFIXES):
            continue

        try:
            header = s3.get_object(
                Bucket=bucket, Key=key, Range="bytes=0-{0}".format(HEADER_BYTES - 1)
            )["Body"].read()
            fmt = parse_riff_header(header)
        except NotRiffWave as exc:
            logger.info("waveform_skipped key=%s reason=%s", key, exc)
            continue
        except ClientError as exc:
            logger.error("header_read_failed key=%s error=%s", key, exc)
            continue

        channels = max(int(fmt["channels"]), 1)
        sample_rate = max(int(fmt["sample_rate"]), 1)
        data_size = int(fmt["data_size"])
        total_frames_expected = data_size // (2 * channels)
        bucket_samples = max(total_frames_expected // max(TARGET_BUCKETS, 1), 1)

        try:
            chunks = iter_pcm_chunks(bucket, key, int(fmt["data_offset"]), data_size)
            peaks, total_frames = accumulate_peaks(chunks, channels, bucket_samples)
        except ClientError as exc:
            logger.error("pcm_read_failed key=%s error=%s", key, exc)
            continue

        document = {
            "source_key": key,
            "sample_rate": sample_rate,
            "channels": channels,
            "duration_seconds": round(total_frames / float(sample_rate), 3),
            "bucket_samples": bucket_samples,
            "peaks": peaks,
            "extracted_at": int(time.time()),
        }

        try:
            written.append(write_peaks(bucket, key, document))
        except ClientError as exc:
            logger.error("peaks_write_failed key=%s error=%s", key, exc)
            continue

        logger.info(
            "waveform_extracted key=%s frames=%s buckets=%s duration=%ss",
            key, total_frames, len(peaks), document["duration_seconds"],
        )

    return {"peak_files_written": len(written), "keys": written}
