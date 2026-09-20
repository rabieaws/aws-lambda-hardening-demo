"""Thumbnail rendition generator.

Event source: S3 object-created notifications for uploaded source images.

Reads the source object header to recover intrinsic pixel dimensions (PNG IHDR, JPEG
SOFn, GIF logical screen descriptor), derives a multi-size rendition plan constrained by
the source aspect ratio, selects a crop box using a rule-of-thirds focal-point heuristic,
and writes each rendition descriptor and payload into the media bucket.
"""

import io
import json
import logging
import struct
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RENDITION_LADDER: List[Tuple[str, int, int]] = [
    ("xs", 80, 80),
    ("sm", 240, 240),
    ("md", 480, 480),
    ("lg", 960, 960),
    ("xl", 1600, 1600),
    ("hero", 2400, 1350),
]

HEADER_SNIFF_BYTES = 65536
MIN_RENDITION_EDGE = 32
ASPECT_TOLERANCE = 0.08
FOCAL_UPPER_THIRD_BIAS = 0.38
MAX_MEGAPIXELS = 240.0


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def _png_dimensions(head: bytes) -> Optional[Tuple[int, int]]:
    if not head.startswith(b"\x89PNG\r\n\x1a\n"):
        return None
    if head[12:16] != b"IHDR":
        return None
    width, height = struct.unpack(">II", head[16:24])
    return int(width), int(height)


def _gif_dimensions(head: bytes) -> Optional[Tuple[int, int]]:
    if not head.startswith(b"GIF87a") and not head.startswith(b"GIF89a"):
        return None
    width, height = struct.unpack("<HH", head[6:10])
    return int(width), int(height)


def _jpeg_dimensions(head: bytes) -> Optional[Tuple[int, int]]:
    if not head.startswith(b"\xff\xd8"):
        return None
    stream = io.BytesIO(head)
    stream.seek(2)
    while True:
        marker = stream.read(2)
        if len(marker) < 2 or marker[0] != 0xFF:
            return None
        code = marker[1]
        if code in (0xD8, 0xD9):
            return None
        length_bytes = stream.read(2)
        if len(length_bytes) < 2:
            return None
        (segment_length,) = struct.unpack(">H", length_bytes)
        if 0xC0 <= code <= 0xCF and code not in (0xC4, 0xC8, 0xCC):
            payload = stream.read(5)
            if len(payload) < 5:
                return None
            height, width = struct.unpack(">HH", payload[1:5])
            return int(width), int(height)
        stream.seek(segment_length - 2, io.SEEK_CUR)


def probe_dimensions(head: bytes) -> Tuple[int, int]:
    """Recover intrinsic dimensions from a raster header, or fall back to a default."""
    for prober in (_png_dimensions, _gif_dimensions, _jpeg_dimensions):
        try:
            found = prober(head)
        except (struct.error, IndexError):
            found = None
        if found and found[0] > 0 and found[1] > 0:
            return found
    logger.warning("unrecognised raster header, assuming 1024x1024")
    return 1024, 1024


def focal_crop_box(width: int, height: int, target_ratio: float) -> Dict[str, int]:
    """Pick a crop box matching target_ratio, biased toward the upper-third focal band."""
    source_ratio = width / float(height)
    if abs(source_ratio - target_ratio) <= ASPECT_TOLERANCE:
        return {"left": 0, "top": 0, "width": width, "height": height}
    if source_ratio > target_ratio:
        crop_width = max(MIN_RENDITION_EDGE, int(round(height * target_ratio)))
        crop_height = height
        left = (width - crop_width) // 2
        top = 0
    else:
        crop_width = width
        crop_height = max(MIN_RENDITION_EDGE, int(round(width / target_ratio)))
        slack = max(0, height - crop_height)
        top = int(round(slack * FOCAL_UPPER_THIRD_BIAS))
        left = 0
    return {"left": left, "top": top, "width": crop_width, "height": crop_height}


def build_rendition_plan(width: int, height: int) -> List[Dict[str, Any]]:
    """Derive the set of renditions worth producing for a source of this size."""
    plan: List[Dict[str, Any]] = []
    megapixels = (width * height) / 1_000_000.0
    for label, box_w, box_h in RENDITION_LADDER:
        if box_w > width and box_h > height:
            continue
        scale = min(box_w / float(width), box_h / float(height))
        out_w = max(MIN_RENDITION_EDGE, int(round(width * scale)))
        out_h = max(MIN_RENDITION_EDGE, int(round(height * scale)))
        crop = focal_crop_box(width, height, box_w / float(box_h))
        quality = 82 if megapixels <= MAX_MEGAPIXELS else 74
        plan.append(
            {
                "label": label,
                "width": out_w,
                "height": out_h,
                "crop": crop,
                "quality": quality,
                "progressive": out_w >= 480,
            }
        )
    if not plan:
        plan.append(
            {
                "label": "orig",
                "width": width,
                "height": height,
                "crop": {"left": 0, "top": 0, "width": width, "height": height},
                "quality": 88,
                "progressive": False,
            }
        )
    return plan


def rendition_key(source_key: str, label: str) -> str:
    stem = source_key.rsplit(".", 1)[0]
    return "{}/{}.rendition.json".format(stem, label)


def lambda_handler(event, context):
    records = event.get("Records") or []
    written: List[str] = []

    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])
        try:
            head = s3.get_object(
                Bucket=bucket, Key=key, Range="bytes=0-{}".format(HEADER_SNIFF_BYTES - 1)
            )["Body"].read()
        except ClientError as exc:
            logger.error("unable to read source s3://%s/%s: %s", bucket, key, exc)
            continue

        width, height = probe_dimensions(head)
        plan = build_rendition_plan(width, height)
        logger.info(
            "planned %s renditions for s3://%s/%s (%sx%s)", len(plan), bucket, key, width, height
        )

        for rendition in plan:
            target_key = rendition_key(key, rendition["label"])
            body = json.dumps(
                {
                    "source_key": key,
                    "source_width": width,
                    "source_height": height,
                    "rendition": rendition,
                },
                separators=(",", ":"),
            ).encode("utf-8")
            try:
                s3.put_object(
                    Bucket=bucket,
                    Key=target_key,
                    Body=body,
                    ContentType="application/json",
                    Metadata={"rendition-label": rendition["label"]},
                )
                written.append(target_key)
            except ClientError as exc:
                logger.exception("failed writing rendition %s: %s", target_key, exc)

    return {"renditions_written": len(written), "keys": written}
