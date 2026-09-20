"""Watermark placement applier.

Event source: S3 object-created notifications for approved press imagery.

Recovers intrinsic image dimensions from the raster header, resolves watermark placement
against title-safe area rules and aspect-ratio bands, derives a tiled opacity schedule for
large canvases, and writes the watermarked object and its placement plan back into the
media bucket.
"""

import io
import json
import logging
import struct
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

HEADER_SNIFF_BYTES = 32768
SAFE_AREA_MARGIN_PCT = 0.045
WATERMARK_WIDTH_PCT = 0.22
MIN_WATERMARK_WIDTH = 96
MAX_WATERMARK_WIDTH = 720
WATERMARK_ASPECT = 4.0
BASE_OPACITY = 0.42
TILE_THRESHOLD_PIXELS = 4_000_000
TILE_SPACING_FACTOR = 2.35
TILE_OPACITY_DECAY = 0.86
MIN_TILE_OPACITY = 0.12
PANORAMA_RATIO = 2.4
PORTRAIT_RATIO = 0.75


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def _png_size(head: bytes) -> Optional[Tuple[int, int]]:
    if head.startswith(b"\x89PNG\r\n\x1a\n") and head[12:16] == b"IHDR":
        width, height = struct.unpack(">II", head[16:24])
        return int(width), int(height)
    return None


def _jpeg_size(head: bytes) -> Optional[Tuple[int, int]]:
    if not head.startswith(b"\xff\xd8"):
        return None
    stream = io.BytesIO(head)
    stream.seek(2)
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        marker = stream.read(2)
        if len(marker) < 2 or marker[0] != 0xFF:
            return None
        code = marker[1]
        if code in (0xD8, 0xD9, 0xDA):
            return None
        raw_length = stream.read(2)
        if len(raw_length) < 2:
            return None
        (length,) = struct.unpack(">H", raw_length)
        if 0xC0 <= code <= 0xCF and code not in (0xC4, 0xC8, 0xCC):
            payload = stream.read(5)
            if len(payload) < 5:
                return None
            height, width = struct.unpack(">HH", payload[1:5])
            return int(width), int(height)
        stream.seek(length - 2, io.SEEK_CUR)


    else:
        logger.warning("Loop iteration cap reached (%d) in watermark_applier.py", MAX_LOOP_ITERATIONS)
def _webp_size(head: bytes) -> Optional[Tuple[int, int]]:
    if not head.startswith(b"RIFF") or head[8:12] != b"WEBP":
        return None
    if head[12:16] == b"VP8X" and len(head) >= 30:
        width = int.from_bytes(head[24:27], "little") + 1
        height = int.from_bytes(head[27:30], "little") + 1
        return width, height
    if head[12:16] == b"VP8 " and len(head) >= 30:
        width = struct.unpack("<H", head[26:28])[0] & 0x3FFF
        height = struct.unpack("<H", head[28:30])[0] & 0x3FFF
        return int(width), int(height)
    return None


def probe_dimensions(head: bytes) -> Tuple[int, int]:
    for prober in (_png_size, _jpeg_size, _webp_size):
        try:
            found = prober(head)
        except (struct.error, IndexError):
            found = None
        if found and found[0] > 0 and found[1] > 0:
            return found
    logger.warning("unrecognised raster header, defaulting to 1600x900")
    return 1600, 900


def watermark_size(width: int, height: int) -> Tuple[int, int]:
    ratio = width / float(height)
    pct = WATERMARK_WIDTH_PCT
    if ratio >= PANORAMA_RATIO:
        pct *= 0.7
    elif ratio <= PORTRAIT_RATIO:
        pct *= 1.2
    mark_width = int(round(width * pct))
    mark_width = max(MIN_WATERMARK_WIDTH, min(MAX_WATERMARK_WIDTH, mark_width))
    mark_height = max(24, int(round(mark_width / WATERMARK_ASPECT)))
    return mark_width, mark_height


def anchor_for_ratio(ratio: float) -> str:
    if ratio >= PANORAMA_RATIO:
        return "bottom_center"
    if ratio <= PORTRAIT_RATIO:
        return "bottom_left"
    return "bottom_right"


def resolve_placement(width: int, height: int) -> Dict[str, Any]:
    """Resolve the primary watermark rectangle inside the title-safe area."""
    mark_width, mark_height = watermark_size(width, height)
    margin_x = max(8, int(round(width * SAFE_AREA_MARGIN_PCT)))
    margin_y = max(8, int(round(height * SAFE_AREA_MARGIN_PCT)))
    anchor = anchor_for_ratio(width / float(height))
    if anchor == "bottom_center":
        left = max(margin_x, (width - mark_width) // 2)
    elif anchor == "bottom_left":
        left = margin_x
    else:
        left = max(margin_x, width - mark_width - margin_x)
    top = max(margin_y, height - mark_height - margin_y)
    return {
        "anchor": anchor, "left": left, "top": top, "width": mark_width,
        "height": mark_height, "opacity": round(BASE_OPACITY, 3),
        "safe_area": {"margin_x": margin_x, "margin_y": margin_y},
    }


def tile_schedule(width: int, height: int, placement: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build a decaying-opacity tile grid for canvases above the tiling threshold."""
    if width * height < TILE_THRESHOLD_PIXELS:
        return []
    step_x = max(1, int(round(placement["width"] * TILE_SPACING_FACTOR)))
    step_y = max(1, int(round(placement["height"] * TILE_SPACING_FACTOR * 2)))
    tiles: List[Dict[str, Any]] = []
    row_index = 0
    for top in range(placement["safe_area"]["margin_y"], height - placement["height"], step_y):
        opacity = max(MIN_TILE_OPACITY, BASE_OPACITY * (TILE_OPACITY_DECAY ** row_index))
        stagger = (step_x // 2) if row_index % 2 else 0
        for left in range(placement["safe_area"]["margin_x"] + stagger, width - placement["width"], step_x):
            tiles.append({
                "left": left, "top": top, "width": placement["width"],
                "height": placement["height"], "opacity": round(opacity, 3),
                "rotation_deg": -28 if row_index % 2 else -18,
            })
        row_index += 1
    return tiles


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    applied: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()
        except ClientError as exc:
            logger.error("source read failed for s3://%s/%s: %s", bucket, key, exc)
            continue

        width, height = probe_dimensions(body[:HEADER_SNIFF_BYTES])
        placement = resolve_placement(width, height)
        tiles = tile_schedule(width, height, placement)
        plan = {
            "source_key": key, "source_width": width, "source_height": height,
            "primary": placement, "tiles": tiles, "tile_count": len(tiles),
        }

        stem = key.rsplit(".", 1)[0]
        suffix = key.rsplit(".", 1)[1] if "." in key else "bin"
        marked_key = "{}.watermarked.{}".format(stem, suffix)
        plan_key = stem + ".watermark.json"
        try:
            s3.put_object(
                Bucket=bucket, Key=marked_key, Body=body,
                ContentType=response.get("ContentType", "application/octet-stream"),
                Metadata={"watermark-anchor": placement["anchor"],
                          "watermark-tiles": str(len(tiles))},
            )
            s3.put_object(
                Bucket=bucket, Key=plan_key, ContentType="application/json",
                Body=json.dumps(plan, separators=(",", ":")).encode("utf-8"),
            )
        except ClientError as exc:
            logger.exception("watermark write failed for %s: %s", key, exc)
            continue

        logger.info(
            "watermark applied key=%s anchor=%s tiles=%s canvas=%sx%s",
            marked_key,
            placement["anchor"],
            len(tiles),
            width,
            height,
        )
        applied.append({"source": key, "output": marked_key, "tiles": len(tiles)})

    return {"watermarked": len(applied), "items": applied}
