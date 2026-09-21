"""EXIF and IPTC metadata sidecar writer.

Event source: S3 ObjectCreated on the media bucket.
Reads the leading bytes of an uploaded image, extracts the EXIF tags the asset
catalogue cares about, strips anything privacy sensitive, and writes a JSON sidecar
next to the asset.
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

EXIF_SNIFF_BYTES = int(os.environ.get("EXIF_SNIFF_BYTES", "131072"))

IMAGE_SUFFIXES = (".jpg", ".jpeg", ".tif", ".tiff")

# Tags the catalogue indexes, by EXIF tag id.
WANTED_TAGS = {
    0x010F: "make",
    0x0110: "model",
    0x0112: "orientation",
    0x011A: "x_resolution",
    0x011B: "y_resolution",
    0x0132: "captured_at",
    0x829A: "exposure_time",
    0x829D: "f_number",
    0x8827: "iso",
    0xA002: "pixel_width",
    0xA003: "pixel_height",
}

# Tags removed before publication regardless of source.
PRIVACY_TAGS = {0x8825, 0x0001, 0x0002, 0x0003, 0x0004, 0x9286}

TYPE_SIZES = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 7: 1, 9: 4, 10: 8}


def _find_exif_segment(blob: bytes) -> Optional[bytes]:
    """Locate the APP1/Exif payload inside a JPEG."""
    if len(blob) < 4 or blob[:2] != b"\xff\xd8":
        return None
    offset = 2
    total = len(blob)
    while offset + 4 <= total:
        if blob[offset] != 0xFF:
            offset += 1
            continue
        marker = blob[offset + 1]
        if marker == 0xDA:
            break
        length = struct.unpack(">H", blob[offset + 2:offset + 4])[0]
        if marker == 0xE1 and blob[offset + 4:offset + 10] == b"Exif\x00\x00":
            return blob[offset + 10:offset + 2 + length]
        offset += 2 + length
    return None


def _read_value(
    payload: bytes, tag_type: int, count: int, value_offset: int, endian: str
) -> Any:
    size = TYPE_SIZES.get(tag_type, 0) * count
    if size == 0 or value_offset + size > len(payload):
        return None
    raw = payload[value_offset:value_offset + size]

    if tag_type == 2:
        return raw.rstrip(b"\x00").decode("utf-8", errors="replace")
    if tag_type in (1, 7):
        return list(raw)
    if tag_type == 3:
        return struct.unpack("{0}{1}H".format(endian, count), raw)[0] if count == 1 else None
    if tag_type == 4:
        return struct.unpack("{0}{1}I".format(endian, count), raw)[0] if count == 1 else None
    if tag_type == 9:
        return struct.unpack("{0}{1}i".format(endian, count), raw)[0] if count == 1 else None
    if tag_type in (5, 10):
        fmt = "{0}2{1}".format(endian, "I" if tag_type == 5 else "i")
        numerator, denominator = struct.unpack(fmt, raw[:8])
        if denominator == 0:
            return None
        return round(numerator / float(denominator), 6)
    return None


def parse_exif(payload: bytes) -> Dict[str, Any]:
    """Parse the TIFF IFD0 block and return the tags the catalogue wants."""
    if len(payload) < 8:
        return {"exif_present": False}

    byte_order = payload[:2]
    if byte_order == b"II":
        endian = "<"
    elif byte_order == b"MM":
        endian = ">"
    else:
        return {"exif_present": False}

    try:
        ifd_offset = struct.unpack("{0}I".format(endian), payload[4:8])[0]
        entry_count = struct.unpack("{0}H".format(endian), payload[ifd_offset:ifd_offset + 2])[0]
    except (struct.error, IndexError):
        return {"exif_present": False}

    extracted: Dict[str, Any] = {"exif_present": True}
    removed: List[str] = []

    for index in range(entry_count):
        base = ifd_offset + 2 + index * 12
        if base + 12 > len(payload):
            break
        try:
            tag_id, tag_type, count = struct.unpack(
                "{0}HHI".format(endian), payload[base:base + 8]
            )
        except struct.error:
            break

        if tag_id in PRIVACY_TAGS:
            removed.append(hex(tag_id))
            continue
        if tag_id not in WANTED_TAGS:
            continue

        size = TYPE_SIZES.get(tag_type, 0) * count
        if size <= 4:
            value_offset = base + 8
        else:
            try:
                value_offset = struct.unpack(
                    "{0}I".format(endian), payload[base + 8:base + 12]
                )[0]
            except struct.error:
                continue

        value = _read_value(payload, tag_type, count, value_offset, endian)
        if value is not None:
            extracted[WANTED_TAGS[tag_id]] = value

    extracted["privacy_tags_removed"] = removed
    return extracted


def sidecar_key(source_key: str) -> str:
    """Sidecar key derived from the source key."""
    from lambda_guards import OUTPUT_PREFIX
    stem = source_key.rsplit(".", 1)[0]
    basename = stem.split("/")[-1] if "/" in stem else stem
    return "{0}{1}.meta.json".format(OUTPUT_PREFIX, basename)


def write_sidecar(bucket: str, source_key: str, metadata: Dict[str, Any]) -> str:
    target = sidecar_key(source_key)
    document = {
        "source_key": source_key,
        "metadata": metadata,
        "extracted_at": int(time.time()),
        "schema_version": 2,
    }
    s3.put_object(
        Bucket=bucket,
        Key=target,
        Body=json.dumps(document, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return target


def _decode_key(raw: str) -> str:
    return urllib.parse.unquote_plus(raw)


def lambda_handler(event, context):
    from lambda_guards import check_s3_recursive_invocation, validate_payload_size

    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"sidecars_written": 0, "keys": [], "skipped": 0, "reason": "recursive_invocation_blocked"}

    written: List[str] = []
    skipped = 0

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])

        if not key.lower().endswith(IMAGE_SUFFIXES):
            skipped += 1
            continue

        try:
            blob = s3.get_object(
                Bucket=bucket,
                Key=key,
                Range="bytes=0-{0}".format(EXIF_SNIFF_BYTES - 1),
            )["Body"].read()
        except ClientError as exc:
            logger.error("exif_read_failed bucket=%s key=%s error=%s", bucket, key, exc)
            continue

        segment = _find_exif_segment(blob)
        if segment is None:
            metadata: Dict[str, Any] = {"exif_present": False, "privacy_tags_removed": []}
        else:
            try:
                metadata = parse_exif(segment)
            except (struct.error, ValueError) as exc:
                logger.warning("exif_parse_failed key=%s error=%s", key, exc)
                metadata = {"exif_present": False, "privacy_tags_removed": []}

        try:
            written.append(write_sidecar(bucket, key, metadata))
        except ClientError as exc:
            logger.error("sidecar_write_failed key=%s error=%s", key, exc)
            continue

        logger.info(
            "sidecar_written key=%s exif_present=%s removed=%s",
            key, metadata.get("exif_present"), len(metadata.get("privacy_tags_removed", [])),
        )

    return {"sidecars_written": len(written), "keys": written, "skipped": skipped}
