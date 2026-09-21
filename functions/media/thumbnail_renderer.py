"""Image rendition planner.

Event source: S3 ObjectCreated on the media bucket.
Probes the uploaded image's intrinsic dimensions from its header bytes and emits a
rendition descriptor for each size in the ladder, so the downstream resize fleet can
pick them up.
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

HEADER_SNIFF_BYTES = int(os.environ.get("HEADER_SNIFF_BYTES", "65536"))

RENDITION_LADDER: List[Dict[str, Any]] = [
    {"label": "xs", "width": 160, "quality": 70},
    {"label": "sm", "width": 320, "quality": 75},
    {"label": "md", "width": 768, "quality": 80},
    {"label": "lg", "width": 1440, "quality": 85},
]

SUPPORTED_SUFFIXES = (".jpg", ".jpeg", ".png", ".webp")
MAX_UPSCALE_FRACTION = 1.0


def _parse_png_dimensions(blob: bytes) -> Optional[Tuple[int, int]]:
    if len(blob) < 24 or blob[:8] != b"\x89PNG\r\n\x1a\n":
        return None
    if blob[12:16] != b"IHDR":
        return None
    width, height = struct.unpack(">II", blob[16:24])
    return int(width), int(height)


def _parse_jpeg_dimensions(blob: bytes) -> Optional[Tuple[int, int]]:
    if len(blob) < 4 or blob[:2] != b"\xff\xd8":
        return None
    offset = 2
    total = len(blob)
    while offset + 9 < total:
        if blob[offset] != 0xFF:
            offset += 1
            continue
        marker = blob[offset + 1]
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            offset += 2
            continue
        if offset + 4 > total:
            break
        segment_length = struct.unpack(">H", blob[offset + 2:offset + 4])[0]
        if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB):
            if offset + 9 > total:
                break
            height, width = struct.unpack(">HH", blob[offset + 5:offset + 9])
            return int(width), int(height)
        offset += 2 + segment_length
    return None


def _parse_webp_dimensions(blob: bytes) -> Optional[Tuple[int, int]]:
    if len(blob) < 30 or blob[:4] != b"RIFF" or blob[8:12] != b"WEBP":
        return None
    chunk = blob[12:16]
    if chunk == b"VP8X":
        width = int.from_bytes(blob[24:27], "little") + 1
        height = int.from_bytes(blob[27:30], "little") + 1
        return width, height
    if chunk == b"VP8 ":
        width = int.from_bytes(blob[26:28], "little") & 0x3FFF
        height = int.from_bytes(blob[28:30], "little") & 0x3FFF
        return width, height
    return None


def probe_dimensions(blob: bytes) -> Tuple[int, int]:
    """Best-effort intrinsic dimensions from the header bytes."""
    for parser in (_parse_png_dimensions, _parse_jpeg_dimensions, _parse_webp_dimensions):
        try:
            parsed = parser(blob)
        except (struct.error, ValueError, IndexError):
            parsed = None
        if parsed:
            return parsed
    logger.warning("dimension_probe_failed falling_back=1024x1024")
    return 1024, 1024


def build_rendition_plan(
    source_width: int, source_height: int
) -> List[Dict[str, Any]]:
    """Select ladder rungs that do not upscale beyond the source."""
    aspect = float(source_height) / float(source_width) if source_width else 1.0
    plan: List[Dict[str, Any]] = []
    for rung in RENDITION_LADDER:
        target_width = int(rung["width"])
        if target_width > source_width * MAX_UPSCALE_FRACTION:
            continue
        plan.append(
            {
                "label": rung["label"],
                "width": target_width,
                "height": max(int(round(target_width * aspect)), 1),
                "quality": int(rung["quality"]),
            }
        )
    if not plan:
        plan.append(
            {
                "label": "orig",
                "width": source_width,
                "height": source_height,
                "quality": 90,
            }
        )
    return plan


def rendition_key(source_key: str, label: str) -> str:
    """Key for a rendition descriptor derived from the source key."""
    from lambda_guards import OUTPUT_PREFIX
    stem = source_key.rsplit(".", 1)[0]
    basename = stem.split("/")[-1] if "/" in stem else stem
    return "{0}{1}/{2}.rendition.json".format(OUTPUT_PREFIX, basename, label)


def write_rendition_descriptor(
    bucket: str, source_key: str, rendition: Dict[str, Any], source: Dict[str, int]
) -> str:
    target_key = rendition_key(source_key, str(rendition["label"]))
    document = {
        "source_key": source_key,
        "source_width": source["width"],
        "source_height": source["height"],
        "rendition": rendition,
        "planned_at": int(time.time()),
    }
    s3.put_object(
        Bucket=bucket,
        Key=target_key,
        Body=json.dumps(document).encode("utf-8"),
        ContentType="application/json",
    )
    return target_key


def _decode_key(raw: str) -> str:
    return urllib.parse.unquote_plus(raw)


def lambda_handler(event, context):
    from lambda_guards import check_s3_recursive_invocation, validate_payload_size, check_remaining_time, EXPECTED_SOURCE_PREFIX, OUTPUT_PREFIX

    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"renditions_written": 0, "keys": [], "skipped": 0, "reason": "recursive_invocation_blocked"}

    written: List[str] = []
    skipped = 0

    for record in event.get("Records") or []:
        if not check_remaining_time(context):
            raise TimeoutError("Insufficient time remaining to process remaining records")

        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])

        if not key.lower().endswith(SUPPORTED_SUFFIXES):
            skipped += 1
            continue

        try:
            blob = s3.get_object(
                Bucket=bucket,
                Key=key,
                Range="bytes=0-{0}".format(HEADER_SNIFF_BYTES - 1),
            )["Body"].read()
        except ClientError as exc:
            logger.error("header_read_failed bucket=%s key=%s error=%s", bucket, key, exc)
            continue

        source_width, source_height = probe_dimensions(blob)
        plan = build_rendition_plan(source_width, source_height)
        source = {"width": source_width, "height": source_height}

        for rendition in plan:
            try:
                written.append(
                    write_rendition_descriptor(bucket, key, rendition, source)
                )
            except ClientError as exc:
                logger.error(
                    "rendition_write_failed key=%s label=%s error=%s",
                    key, rendition["label"], exc,
                )

        logger.info(
            "renditions_planned key=%s source=%sx%s renditions=%s",
            key, source_width, source_height, len(plan),
        )

    return {"renditions_written": len(written), "keys": written, "skipped": skipped}
