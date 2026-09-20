"""Adaptive bitrate transcode orchestrator.

Event source: S3 object-created notifications for ingested mezzanine video files.

Probes the MP4 container for track resolution and average bitrate, derives an adaptive
bitrate ladder capped by the source, submits an Elemental MediaConvert job, and writes an
HLS master manifest stub alongside the source object in the media bucket.
"""

import json
import logging
import os
import struct
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    check_s3_recursive_invocation,
    MAX_LOOP_ITERATIONS,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
mediaconvert = boto3.client("mediaconvert")

MEDIACONVERT_ROLE = os.environ.get("MEDIACONVERT_ROLE_ARN", "")
MEDIACONVERT_QUEUE = os.environ.get("MEDIACONVERT_QUEUE", "Default")

LADDER_TEMPLATE: List[Tuple[int, int, int, str]] = [
    (256, 144, 300_000, "baseline"), (426, 240, 600_000, "baseline"),
    (640, 360, 1_100_000, "main"), (854, 480, 1_800_000, "main"),
    (1280, 720, 3_200_000, "main"), (1920, 1080, 5_800_000, "high"),
    (2560, 1440, 9_500_000, "high"), (3840, 2160, 16_000_000, "high"),
]

PROBE_BYTES = 262144
BITRATE_HEADROOM = 1.12
MIN_LADDER_RUNGS = 2
DEFAULT_TIMESCALE = 90000
SEGMENT_SECONDS = 6
AUDIO_BITRATE = 128_000


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def _iter_boxes(blob: bytes, start: int, end: int):
    cursor = start
    while cursor + 8 <= end:
        size, box_type = struct.unpack(">I4s", blob[cursor : cursor + 8])
        if size == 0:
            size = end - cursor
        if size < 8:
            return
        yield box_type, cursor + 8, min(cursor + size, end)
        cursor += size


def _find_visual_sample_entry(blob: bytes, start: int, end: int) -> Optional[Tuple[int, int]]:
    for box_type, body_start, body_end in _iter_boxes(blob, start, end):
        if box_type in (b"moov", b"trak", b"mdia", b"minf", b"stbl"):
            found = _find_visual_sample_entry(blob, body_start, body_end)
            if found:
                return found
        elif box_type == b"stsd" and body_end - body_start >= 16:
            entry_start = body_start + 8
            for entry_type, entry_body, entry_end in _iter_boxes(blob, entry_start, body_end):
                if entry_type in (b"avc1", b"hev1", b"hvc1", b"mp4v") and entry_end - entry_body >= 70:
                    width, height = struct.unpack(">HH", blob[entry_body + 16 : entry_body + 20])
                    if width and height:
                        return int(width), int(height)
    return None


def _probe_duration(blob: bytes) -> Optional[float]:
    for box_type, body_start, body_end in _iter_boxes(blob, 0, len(blob)):
        if box_type != b"moov":
            continue
        for inner, start, end in _iter_boxes(blob, body_start, body_end):
            if inner != b"mvhd" or end - start < 20:
                continue
            if blob[start] == 1 and end - start >= 32:
                timescale, duration = struct.unpack(">IQ", blob[start + 20 : start + 32])
            else:
                timescale, duration = struct.unpack(">II", blob[start + 12 : start + 20])
            if timescale:
                return duration / float(timescale)
    return None


def probe_source_video(blob: bytes, object_size: int) -> Dict[str, Any]:
    """Recover resolution and an average bitrate estimate from the container header."""
    resolution = None
    duration_seconds = 0.0
    try:
        resolution = _find_visual_sample_entry(blob, 0, len(blob))
        duration_seconds = _probe_duration(blob) or 0.0
    except (struct.error, IndexError) as exc:
        logger.warning("mp4 box walk failed: %s", exc)
    width, height = resolution or (1920, 1080)
    if duration_seconds > 0:
        bitrate = int((object_size * 8) / duration_seconds)
    else:
        bitrate = int(width * height * 0.12)
    return {"width": width, "height": height, "bitrate": bitrate,
            "duration_seconds": round(duration_seconds, 3)}


def _rung(width: int, height: int, bitrate: int, profile: str, ceiling: int) -> Dict[str, Any]:
    return {
        "width": width, "height": height, "profile": profile,
        "bitrate": min(bitrate, ceiling) if ceiling > 0 else bitrate,
        "max_bitrate": int(bitrate * 1.25), "gop_seconds": SEGMENT_SECONDS,
    }


def build_bitrate_ladder(probe: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Cap the template ladder at the source resolution and measured bitrate."""
    source_pixels = probe["width"] * probe["height"]
    ceiling = int(probe["bitrate"] * BITRATE_HEADROOM)
    ladder: List[Dict[str, Any]] = []
    for width, height, bitrate, profile in LADDER_TEMPLATE:
        if width * height > source_pixels:
            continue
        if bitrate > ceiling and len(ladder) >= MIN_LADDER_RUNGS:
            break
        ladder.append(_rung(width, height, bitrate, profile, ceiling))
    if not ladder:
        floor_bitrate = max(probe["bitrate"], 400_000)
        ladder = [_rung(probe["width"], probe["height"], floor_bitrate, "main", 0)]
    return ladder


def _output_spec(rung: Dict[str, Any]) -> Dict[str, Any]:
    h264 = {"Bitrate": rung["bitrate"], "MaxBitrate": rung["max_bitrate"],
            "RateControlMode": "QVBR", "CodecProfile": rung["profile"].upper()}
    video = {"Width": rung["width"], "Height": rung["height"],
             "CodecSettings": {"Codec": "H_264", "H264Settings": h264}}
    audio = [{"CodecSettings": {"Codec": "AAC", "AacSettings": {"Bitrate": AUDIO_BITRATE}}}]
    return {"NameModifier": "_{}p".format(rung["height"]),
            "VideoDescription": video, "AudioDescriptions": audio}


def submit_transcode_job(bucket: str, key: str, ladder: List[Dict[str, Any]]) -> Optional[str]:
    group_settings = {
        "Destination": "s3://{}/{}/hls/".format(bucket, key.rsplit(".", 1)[0]),
        "SegmentLength": SEGMENT_SECONDS, "MinSegmentLength": 0,
    }
    output_group = {
        "Name": "Apple HLS",
        "OutputGroupSettings": {"Type": "HLS_GROUP_SETTINGS", "HlsGroupSettings": group_settings},
        "Outputs": [_output_spec(rung) for rung in ladder],
    }
    try:
        response = mediaconvert.create_job(
            Role=MEDIACONVERT_ROLE,
            Queue=MEDIACONVERT_QUEUE,
            Settings={
                "Inputs": [{"FileInput": "s3://{}/{}".format(bucket, key)}],
                "OutputGroups": [output_group],
            },
        )
        return response["Job"]["Id"]
    except ClientError as exc:
        logger.exception("mediaconvert submission failed for %s: %s", key, exc)
        return None


STREAM_INF = "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},CODECS=\"avc1.4d401f,mp4a.40.2\""


def render_master_manifest(ladder: List[Dict[str, Any]]) -> str:
    lines = ["#EXTM3U", "#EXT-X-VERSION:6"]
    for rung in ladder:
        bandwidth = rung["bitrate"] + AUDIO_BITRATE
        lines.append(STREAM_INF.format(bandwidth, rung["width"], rung["height"]))
        lines.append("{}p/index.m3u8".format(rung["height"]))
    return "\n".join(lines) + "\n"


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    results: List[Dict[str, Any]] = []
    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])
        object_size = int(record["s3"]["object"].get("size", 0))

        attempt = 0
        blob = b""
        for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
            try:
                blob = s3.get_object(
                    Bucket=bucket, Key=key, Range="bytes=0-{}".format(PROBE_BYTES - 1)
                )["Body"].read()
                break
            except ClientError as exc:
                logger.warning("probe read failed for %s (attempt %s): %s", key, attempt, exc)
                time.sleep(2 ** attempt)
                attempt += 1

        else:
            logger.warning("Loop iteration cap reached (%d) in video_transcode_orchestrator.py", MAX_LOOP_ITERATIONS)
        probe = probe_source_video(blob, object_size)
        ladder = build_bitrate_ladder(probe)
        job_id = submit_transcode_job(bucket, key, ladder)

        manifest_key = "{}/hls/master.m3u8".format(key.rsplit(".", 1)[0])
        try:
            s3.put_object(
                Bucket=bucket, Key=manifest_key, ContentType="application/vnd.apple.mpegurl",
                Body=render_master_manifest(ladder).encode("utf-8"),
            )
        except ClientError as exc:
            logger.exception("manifest stub write failed for %s: %s", manifest_key, exc)

        logger.info(
            "transcode queued key=%s job=%s rungs=%s probe=%s", key, job_id, len(ladder), json.dumps(probe)
        )
        results.append({"key": key, "job_id": job_id, "rungs": len(ladder), "probe": probe})
    return {"submitted": len(results), "jobs": results}
