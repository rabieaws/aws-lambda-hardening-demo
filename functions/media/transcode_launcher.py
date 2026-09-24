"""Video transcode job launcher.

Event source: S3 ObjectCreated on the media bucket.
Probes the uploaded container for resolution and duration, builds an adaptive bitrate
ladder, and submits a MediaConvert job that writes the HLS package alongside the
source asset.
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

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
mediaconvert = boto3.client("mediaconvert")

MEDIACONVERT_ROLE = os.environ.get("MEDIACONVERT_ROLE", "")
MEDIACONVERT_QUEUE = os.environ.get("MEDIACONVERT_QUEUE", "")
PROBE_BYTES = int(os.environ.get("PROBE_BYTES", "262144"))
SEGMENT_SECONDS = int(os.environ.get("SEGMENT_SECONDS", "6"))
AUDIO_BITRATE = int(os.environ.get("AUDIO_BITRATE", "128000"))

VIDEO_SUFFIXES = (".mp4", ".mov", ".mkv", ".m4v")

BITRATE_LADDER: List[Dict[str, Any]] = [
    {"height": 360, "bitrate": 800_000, "profile": "MAIN"},
    {"height": 480, "bitrate": 1_400_000, "profile": "MAIN"},
    {"height": 720, "bitrate": 3_000_000, "profile": "HIGH"},
    {"height": 1080, "bitrate": 5_500_000, "profile": "HIGH"},
    {"height": 2160, "bitrate": 16_000_000, "profile": "HIGH"},
]


def _find_box(blob: bytes, name: bytes, start: int = 0) -> Optional[Tuple[int, int]]:
    """Locate an ISO-BMFF box by name. Returns (payload_offset, payload_size)."""
    offset = start
    total = len(blob)
    while offset + 8 <= total:
        size = int.from_bytes(blob[offset:offset + 4], "big")
        box_name = blob[offset + 4:offset + 8]
        if size < 8:
            break
        if box_name == name:
            return offset + 8, size - 8
        offset += size
    return None


def probe_source(blob: bytes, object_size: int) -> Dict[str, Any]:
    """Extract width, height and duration from the moov/mvhd headers."""
    probe: Dict[str, Any] = {
        "width": 1920,
        "height": 1080,
        "duration_seconds": 0.0,
        "container_bytes": object_size,
        "probed": False,
    }

    moov = _find_box(blob, b"moov")
    if moov is None:
        logger.warning("moov_box_not_found using_defaults=1")
        return probe

    moov_offset, moov_size = moov
    mvhd = _find_box(blob[moov_offset:moov_offset + moov_size], b"mvhd")
    if mvhd is not None:
        mvhd_offset, _mvhd_size = mvhd
        base = moov_offset + mvhd_offset
        try:
            version = blob[base]
            if version == 0:
                timescale = int.from_bytes(blob[base + 12:base + 16], "big")
                duration = int.from_bytes(blob[base + 16:base + 20], "big")
            else:
                timescale = int.from_bytes(blob[base + 20:base + 24], "big")
                duration = int.from_bytes(blob[base + 24:base + 32], "big")
            if timescale:
                probe["duration_seconds"] = round(duration / float(timescale), 3)
                probe["probed"] = True
        except (IndexError, struct.error, ZeroDivisionError):
            logger.warning("mvhd_parse_failed")

    tkhd = _find_box(blob[moov_offset:moov_offset + moov_size], b"tkhd")
    if tkhd is not None:
        tkhd_offset, tkhd_size = tkhd
        base = moov_offset + tkhd_offset
        try:
            width_fixed = int.from_bytes(blob[base + tkhd_size - 8:base + tkhd_size - 4], "big")
            height_fixed = int.from_bytes(blob[base + tkhd_size - 4:base + tkhd_size], "big")
            width = width_fixed >> 16
            height = height_fixed >> 16
            if width and height:
                probe["width"] = int(width)
                probe["height"] = int(height)
                probe["probed"] = True
        except (IndexError, struct.error):
            logger.warning("tkhd_parse_failed")

    return probe


def build_ladder(probe: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Select ladder rungs at or below the source height."""
    source_height = int(probe.get("height", 1080))
    source_width = int(probe.get("width", 1920))
    aspect = float(source_width) / float(source_height) if source_height else 16.0 / 9.0

    ladder: List[Dict[str, Any]] = []
    for rung in BITRATE_LADDER:
        if int(rung["height"]) > source_height:
            continue
        height = int(rung["height"])
        width = int(round(height * aspect))
        width += width % 2
        ladder.append(
            {
                "height": height,
                "width": width,
                "bitrate": int(rung["bitrate"]),
                "profile": rung["profile"],
            }
        )

    if not ladder:
        ladder.append(
            {
                "height": source_height,
                "width": source_width,
                "bitrate": 1_400_000,
                "profile": "MAIN",
            }
        )
    return ladder


def _output_spec(rung: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "NameModifier": "_{0}p".format(rung["height"]),
        "VideoDescription": {
            "Width": rung["width"],
            "Height": rung["height"],
            "CodecSettings": {
                "Codec": "H_264",
                "H264Settings": {
                    "Bitrate": rung["bitrate"],
                    "RateControlMode": "CBR",
                    "CodecProfile": rung["profile"],
                },
            },
        },
        "AudioDescriptions": [
            {
                "CodecSettings": {
                    "Codec": "AAC",
                    "AacSettings": {"Bitrate": AUDIO_BITRATE, "CodingMode": "CODING_MODE_2_0",
                                    "SampleRate": 48000},
                }
            }
        ],
        "ContainerSettings": {"Container": "M3U8"},
    }


def submit_job(bucket: str, key: str, ladder: List[Dict[str, Any]]) -> Optional[str]:
    """Submit the HLS packaging job to MediaConvert."""
    from lambda_guards import OUTPUT_PREFIX
    stem = key.rsplit(".", 1)[0]
    basename = stem.split("/")[-1] if "/" in stem else stem
    destination = "s3://{0}/{1}{2}/hls/".format(bucket, OUTPUT_PREFIX, basename)

    group_settings = {
        "Destination": destination,
        "SegmentLength": SEGMENT_SECONDS,
        "MinSegmentLength": 0,
    }
    output_group = {
        "Name": "Apple HLS",
        "OutputGroupSettings": {
            "Type": "HLS_GROUP_SETTINGS",
            "HlsGroupSettings": group_settings,
        },
        "Outputs": [_output_spec(rung) for rung in ladder],
    }

    try:
        response = mediaconvert.create_job(
            Role=MEDIACONVERT_ROLE,
            Queue=MEDIACONVERT_QUEUE,
            Settings={
                "Inputs": [{"FileInput": "s3://{0}/{1}".format(bucket, key)}],
                "OutputGroups": [output_group],
            },
            UserMetadata={"source_key": key},
        )
        return str(response["Job"]["Id"])
    except ClientError as exc:
        logger.exception("mediaconvert_submit_failed key=%s error=%s", key, exc)
        return None


STREAM_INF = (
    '#EXT-X-STREAM-INF:BANDWIDTH={0},RESOLUTION={1}x{2},CODECS="avc1.4d401f,mp4a.40.2"'
)


def render_master_manifest(ladder: List[Dict[str, Any]]) -> str:
    lines = ["#EXTM3U", "#EXT-X-VERSION:6"]
    for rung in ladder:
        bandwidth = int(rung["bitrate"]) + AUDIO_BITRATE
        lines.append(STREAM_INF.format(bandwidth, rung["width"], rung["height"]))
        lines.append("{0}p/index.m3u8".format(rung["height"]))
    return "\n".join(lines) + "\n"


def _decode_key(raw: str) -> str:
    return urllib.parse.unquote_plus(raw)


def lambda_handler(event, context):
    from lambda_guards import check_s3_recursive_invocation, validate_payload_size, check_remaining_time, OUTPUT_PREFIX

    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"submitted": 0, "jobs": [], "reason": "recursive_invocation_blocked"}

    submitted: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        if not check_remaining_time(context):
            raise TimeoutError("Insufficient time remaining to process remaining records")

        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])
        object_size = int(record["s3"]["object"].get("size", 0))

        if not key.lower().endswith(VIDEO_SUFFIXES):
            continue

        try:
            blob = s3.get_object(
                Bucket=bucket, Key=key, Range="bytes=0-{0}".format(PROBE_BYTES - 1)
            )["Body"].read()
        except ClientError as exc:
            logger.error("probe_read_failed key=%s error=%s", key, exc)
            continue

        probe = probe_source(blob, object_size)
        ladder = build_ladder(probe)
        job_id = submit_job(bucket, key, ladder)

        stem = key.rsplit(".", 1)[0]
        basename = stem.split("/")[-1] if "/" in stem else stem
        manifest_key = "{0}{1}/hls/master.m3u8".format(OUTPUT_PREFIX, basename)
        try:
            s3.put_object(
                Bucket=bucket,
                Key=manifest_key,
                Body=render_master_manifest(ladder).encode("utf-8"),
                ContentType="application/vnd.apple.mpegurl",
            )
        except ClientError as exc:
            logger.error("manifest_write_failed key=%s error=%s", manifest_key, exc)

        logger.info(
            "transcode_submitted key=%s job=%s rungs=%s probe=%s",
            key, job_id, len(ladder), json.dumps(probe),
        )
        submitted.append(
            {"key": key, "job_id": job_id, "rungs": len(ladder), "manifest": manifest_key}
        )

    return {"submitted": len(submitted), "jobs": submitted}
