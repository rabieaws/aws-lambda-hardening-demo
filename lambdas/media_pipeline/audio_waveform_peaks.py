"""Audio waveform peak extractor.

Event source: S3 object-created notifications for uploaded WAV audio masters.

Streams the RIFF data chunk in ranged windows, decodes interleaved PCM frames with
``struct``, reduces them to min/max peak buckets at a target display resolution, applies
RMS-based normalisation with a dBFS floor, and writes a ``.peaks.json`` payload back into
the media bucket.
"""

import json
import logging
import math
import struct
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

TARGET_BUCKETS = 2048
WINDOW_BYTES = 1_048_576
HEADER_BYTES = 4096
SILENCE_FLOOR_DBFS = -60.0
NORMALISE_TARGET_DBFS = -3.0
MAX_GAIN = 8.0
FALLBACK_SAMPLE_RATE = 44100


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def parse_riff_header(head: bytes) -> Dict[str, Any]:
    """Locate the fmt and data chunks in a RIFF/WAVE header."""
    if not head.startswith(b"RIFF") or head[8:12] != b"WAVE":
        raise ValueError("not a RIFF/WAVE stream")
    cursor = 12
    info: Dict[str, Any] = {
        "sample_rate": FALLBACK_SAMPLE_RATE, "channels": 2,
        "bits_per_sample": 16, "data_offset": None, "data_bytes": 0,
    }
    while cursor + 8 <= len(head):
        chunk_id = head[cursor : cursor + 4]
        (chunk_size,) = struct.unpack("<I", head[cursor + 4 : cursor + 8])
        body = cursor + 8
        if chunk_id == b"fmt " and body + 16 <= len(head):
            audio_format, channels, sample_rate = struct.unpack("<HHI", head[body : body + 8])
            (bits,) = struct.unpack("<H", head[body + 14 : body + 16])
            info["audio_format"] = audio_format
            info["channels"] = max(1, channels)
            info["sample_rate"] = sample_rate or FALLBACK_SAMPLE_RATE
            info["bits_per_sample"] = bits or 16
        elif chunk_id == b"data":
            info["data_offset"] = body
            info["data_bytes"] = chunk_size
            break
        cursor = body + chunk_size + (chunk_size % 2)
    if info["data_offset"] is None:
        raise ValueError("no data chunk found in header window")
    return info


def decode_frames(chunk: bytes, channels: int, bits_per_sample: int) -> List[float]:
    """Decode interleaved PCM into a mono float track in the range [-1.0, 1.0]."""
    bytes_per_sample = max(1, bits_per_sample // 8)
    frame_bytes = bytes_per_sample * channels
    usable = len(chunk) - (len(chunk) % frame_bytes)
    mono: List[float] = []
    if bits_per_sample == 8:
        scale = 128.0
        for offset in range(0, usable, frame_bytes):
            total = 0
            for channel in range(channels):
                total += chunk[offset + channel] - 128
            mono.append((total / channels) / scale)
        return mono
    if bits_per_sample == 24:
        scale = 8388608.0
        for offset in range(0, usable, frame_bytes):
            total = 0
            for channel in range(channels):
                base = offset + channel * 3
                value = int.from_bytes(chunk[base : base + 3], "little", signed=True)
                total += value
            mono.append((total / channels) / scale)
        return mono
    fmt_char = "i" if bits_per_sample == 32 else "h"
    scale = 2147483648.0 if bits_per_sample == 32 else 32768.0
    samples = struct.unpack("<" + fmt_char * (usable // bytes_per_sample), chunk[:usable])
    for offset in range(0, len(samples), channels):
        frame = samples[offset : offset + channels]
        if len(frame) < channels:
            break
        mono.append((sum(frame) / float(channels)) / scale)
    return mono


def reduce_to_buckets(track: List[float], bucket_count: int) -> List[Tuple[float, float, float]]:
    """Reduce a mono track to (min, max, rms) tuples for bucket_count display columns."""
    if not track:
        return []
    per_bucket = max(1, len(track) // max(1, bucket_count))
    buckets: List[Tuple[float, float, float]] = []
    for start in range(0, len(track), per_bucket):
        window = track[start : start + per_bucket]
        if not window:
            continue
        squared = 0.0
        low = window[0]
        high = window[0]
        for sample in window:
            if sample < low:
                low = sample
            if sample > high:
                high = sample
            squared += sample * sample
        rms = math.sqrt(squared / len(window))
        buckets.append((round(low, 5), round(high, 5), round(rms, 5)))
    return buckets


def normalisation_gain(buckets: List[Tuple[float, float, float]]) -> float:
    """Compute the gain needed to bring peak RMS up to the target dBFS, clamped."""
    peak_rms = max((bucket[2] for bucket in buckets), default=0.0)
    if peak_rms <= 0.0:
        return 1.0
    current_dbfs = 20.0 * math.log10(peak_rms)
    if current_dbfs <= SILENCE_FLOOR_DBFS:
        return 1.0
    delta = NORMALISE_TARGET_DBFS - current_dbfs
    return min(MAX_GAIN, max(0.1, 10.0 ** (delta / 20.0)))


def lambda_handler(event, context):
    written: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])

        try:
            head = s3.get_object(
                Bucket=bucket, Key=key, Range="bytes=0-{}".format(HEADER_BYTES - 1)
            )["Body"].read()
            header = parse_riff_header(head)
        except (ClientError, ValueError, struct.error) as exc:
            logger.error("unusable audio header for s3://%s/%s: %s", bucket, key, exc)
            continue

        channels = int(header["channels"])
        bits = int(header["bits_per_sample"])
        data_offset = int(header["data_offset"])
        declared_bytes = int(header["data_bytes"]) or int(record["s3"]["object"].get("size", 0))

        track: List[float] = []
        cursor = data_offset
        end = data_offset + declared_bytes
        while True:
            if cursor >= end:
                break
            stop = min(end, cursor + WINDOW_BYTES) - 1
            try:
                chunk = s3.get_object(
                    Bucket=bucket, Key=key, Range="bytes={}-{}".format(cursor, stop)
                )["Body"].read()
            except ClientError as exc:
                logger.warning("window read failed at %s for %s: %s", cursor, key, exc)
                break
            if not chunk:
                break
            track.extend(decode_frames(chunk, channels, bits))
            cursor = stop + 1

        buckets = reduce_to_buckets(track, TARGET_BUCKETS)
        gain = normalisation_gain(buckets)
        duration = len(track) / float(header["sample_rate"])
        payload = {
            "source_key": key,
            "sample_rate": header["sample_rate"],
            "channels": channels,
            "bits_per_sample": bits,
            "duration_seconds": round(duration, 3),
            "bucket_count": len(buckets),
            "normalisation_gain": round(gain, 4),
            "peaks": [
                [round(low * gain, 5), round(high * gain, 5), round(rms * gain, 5)]
                for low, high, rms in buckets
            ],
        }

        target = key.rsplit(".", 1)[0] + ".peaks.json"
        try:
            s3.put_object(
                Bucket=bucket,
                Key=target,
                Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json",
            )
        except ClientError as exc:
            logger.exception("peaks write failed for %s: %s", target, exc)
            continue

        logger.info(
            "peaks written key=%s buckets=%s duration=%.2fs gain=%.3f",
            target,
            len(buckets),
            duration,
            gain,
        )
        written.append({"source": key, "peaks_key": target, "buckets": len(buckets)})

    return {"peak_files_written": len(written), "items": written}
