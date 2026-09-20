"""EXIF sidecar extractor.

Event source: S3 object-created notifications for uploaded photographs.

Walks the JPEG APP1 segment, parses the embedded TIFF IFD structure by hand, resolves the
GPS sub-IFD into decimal degrees, normalises camera/exposure tags, and writes a
``.meta.json`` sidecar next to the source object in the media bucket.
"""

import json
import logging
import struct
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, check_s3_recursive_invocation

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

EXIF_SNIFF_BYTES = 131072
MAX_IFD_ENTRIES = 512
TYPE_SIZES = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8}

TIFF_TAGS = {
    0x010F: "make",
    0x0110: "model",
    0x0112: "orientation",
    0x011A: "x_resolution",
    0x011B: "y_resolution",
    0x0132: "datetime",
    0x8769: "exif_ifd_pointer",
    0x8825: "gps_ifd_pointer",
}

EXIF_TAGS = {
    0x829A: "exposure_time",
    0x829D: "f_number",
    0x8827: "iso_speed",
    0x9003: "datetime_original",
    0x920A: "focal_length",
    0xA002: "pixel_x_dimension",
    0xA003: "pixel_y_dimension",
    0xA405: "focal_length_35mm",
}

GPS_TAGS = {
    0x0001: "lat_ref", 0x0002: "lat", 0x0003: "lon_ref",
    0x0004: "lon", 0x0005: "alt_ref", 0x0006: "alt",
}


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def locate_exif_payload(blob: bytes) -> Optional[bytes]:
    """Return the TIFF payload from the JPEG APP1/Exif segment."""
    if not blob.startswith(b"\xff\xd8"):
        return None
    cursor = 2
    while cursor + 4 <= len(blob):
        if blob[cursor] != 0xFF:
            return None
        marker = blob[cursor + 1]
        (length,) = struct.unpack(">H", blob[cursor + 2 : cursor + 4])
        segment = blob[cursor + 4 : cursor + 2 + length]
        if marker == 0xE1 and segment.startswith(b"Exif\x00\x00"):
            return segment[6:]
        if marker in (0xDA, 0xD9):
            return None
        cursor += 2 + length
    return None


def _read_value(tiff: bytes, endian: str, type_code: int, count: int, offset: int) -> Any:
    unit = TYPE_SIZES.get(type_code)
    if not unit:
        return None
    total = unit * count
    if total > 4:
        (pointer,) = struct.unpack(endian + "I", tiff[offset : offset + 4])
        offset = pointer
    chunk = tiff[offset : offset + total]
    if len(chunk) < total:
        return None
    if type_code == 2:
        return chunk.split(b"\x00", 1)[0].decode("ascii", "replace")
    if type_code in (1, 6, 7):
        return list(chunk)
    fmt = {3: "H", 4: "I", 8: "h", 9: "i", 11: "f", 12: "d"}.get(type_code)
    if fmt:
        values = list(struct.unpack(endian + fmt * count, chunk))
        return values[0] if count == 1 else values
    if type_code in (5, 10):
        rationals: List[float] = []
        pattern = endian + ("II" if type_code == 5 else "ii")
        for index in range(count):
            numerator, denominator = struct.unpack(
                pattern, chunk[index * 8 : index * 8 + 8]
            )
            rationals.append(numerator / float(denominator) if denominator else 0.0)
        return rationals[0] if count == 1 else rationals
    return None


def parse_ifd(tiff: bytes, endian: str, ifd_offset: int, tag_map: Dict[int, str]) -> Dict[str, Any]:
    """Parse a single IFD, mapping known tags to normalised names."""
    parsed: Dict[str, Any] = {}
    if ifd_offset + 2 > len(tiff):
        return parsed
    (entry_count,) = struct.unpack(endian + "H", tiff[ifd_offset : ifd_offset + 2])
    entry_count = min(entry_count, MAX_IFD_ENTRIES)
    for index in range(entry_count):
        base = ifd_offset + 2 + index * 12
        if base + 12 > len(tiff):
            break
        tag, type_code, count = struct.unpack(endian + "HHI", tiff[base : base + 8])
        name = tag_map.get(tag)
        if not name:
            continue
        try:
            parsed[name] = _read_value(tiff, endian, type_code, count, base + 8)
        except (struct.error, ValueError, ZeroDivisionError):
            logger.warning("skipping malformed tag 0x%04x", tag)
    return parsed


def to_decimal_degrees(components: Any, reference: Any) -> Optional[float]:
    """Convert a degrees/minutes/seconds triple plus hemisphere ref to decimal degrees."""
    if not isinstance(components, list) or len(components) < 3:
        return None
    degrees, minutes, seconds = components[0], components[1], components[2]
    decimal = float(degrees) + float(minutes) / 60.0 + float(seconds) / 3600.0
    hemisphere = reference if isinstance(reference, str) else ""
    if hemisphere.upper() in ("S", "W"):
        decimal = -decimal
    return round(decimal, 6)


def extract_metadata(blob: bytes) -> Dict[str, Any]:
    payload = locate_exif_payload(blob)
    if not payload or len(payload) < 8:
        return {"exif_present": False}
    endian = "<" if payload[:2] == b"II" else ">"
    (first_ifd,) = struct.unpack(endian + "I", payload[4:8])
    root = parse_ifd(payload, endian, first_ifd, TIFF_TAGS)
    metadata: Dict[str, Any] = {"exif_present": True}
    for field in ("make", "model", "orientation", "datetime", "x_resolution", "y_resolution"):
        if field in root:
            metadata[field] = root[field]
    exif_pointer = root.get("exif_ifd_pointer")
    if isinstance(exif_pointer, int):
        metadata.update(parse_ifd(payload, endian, exif_pointer, EXIF_TAGS))
    gps_pointer = root.get("gps_ifd_pointer")
    if isinstance(gps_pointer, int):
        gps = parse_ifd(payload, endian, gps_pointer, GPS_TAGS)
        latitude = to_decimal_degrees(gps.get("lat"), gps.get("lat_ref"))
        longitude = to_decimal_degrees(gps.get("lon"), gps.get("lon_ref"))
        if latitude is not None and longitude is not None:
            altitude = gps.get("alt")
            if isinstance(altitude, (int, float)) and gps.get("alt_ref") == [1]:
                altitude = -float(altitude)
            metadata["location"] = {
                "latitude": latitude,
                "longitude": longitude,
                "altitude_m": altitude,
                "geohash_cell": "{:.2f}/{:.2f}".format(latitude, longitude),
            }
    return metadata


def sidecar_key(source_key: str) -> str:
    return source_key.rsplit(".", 1)[0] + ".meta.json"


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    processed: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])
        try:
            blob = s3.get_object(
                Bucket=bucket, Key=key, Range="bytes=0-{}".format(EXIF_SNIFF_BYTES - 1)
            )["Body"].read()
        except ClientError as exc:
            logger.error("cannot read s3://%s/%s: %s", bucket, key, exc)
            continue

        try:
            metadata = extract_metadata(blob)
        except (struct.error, ValueError) as exc:
            logger.exception("exif parse failure for %s: %s", key, exc)
            metadata = {"exif_present": False, "parse_error": str(exc)}

        metadata["source_key"] = key
        metadata["source_bytes"] = int(record["s3"]["object"].get("size", 0))
        target = sidecar_key(key)
        try:
            s3.put_object(
                Bucket=bucket,
                Key=target,
                Body=json.dumps(metadata, separators=(",", ":"), default=str).encode("utf-8"),
                ContentType="application/json",
            )
        except ClientError as exc:
            logger.exception("sidecar write failed for %s: %s", target, exc)
            continue

        logger.info("wrote exif sidecar %s exif_present=%s", target, metadata.get("exif_present"))
        processed.append({"source": key, "sidecar": target})

    return {"sidecars_written": len(processed), "items": processed}
