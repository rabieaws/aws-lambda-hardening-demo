"""Master manifest builder.

Event source: S3 object-created notifications for completed rendition uploads.

Enumerates every rendition beneath the asset prefix using a ``list_objects_v2``
paginator, groups the objects into variant streams by codec/resolution/language, sorts the
variants by effective bandwidth, and emits a master manifest plus a JSON index back into
the media bucket.
"""

import json
import logging
import re
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import (
    validate_payload_size,
    check_remaining_time,
    check_s3_recursive_invocation,
    MAX_PAGINATION_PAGES,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RENDITION_RE = re.compile(
    r"(?P<height>\d{3,4})p"
    r"(?:_(?P<codec>h264|h265|av1|vp9))?"
    r"(?:_(?P<lang>[a-z]{2}(?:-[A-Z]{2})?))?"
)
AUDIO_EXTENSIONS = (".m4a", ".aac", ".mp3")
VIDEO_EXTENSIONS = (".m3u8", ".mp4", ".ts", ".cmfv")
SUBTITLE_EXTENSIONS = (".vtt", ".srt")

BANDWIDTH_BY_HEIGHT: Dict[int, int] = {
    144: 300_000, 240: 600_000, 360: 1_100_000, 480: 1_800_000,
    720: 3_200_000, 1080: 5_800_000, 1440: 9_500_000, 2160: 16_000_000,
}
DEFAULT_BANDWIDTH = 1_500_000
AUDIO_BANDWIDTH = 128_000
PAGE_SIZE = 1000
MANIFEST_VERSION = 6


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def asset_prefix(key: str) -> str:
    """The logical asset folder that owns a rendition key."""
    parts = key.split("/")
    if len(parts) <= 1:
        return ""
    for index in range(len(parts) - 1, 0, -1):
        if parts[index - 1] in ("hls", "dash", "renditions"):
            return "/".join(parts[:index])
    return "/".join(parts[:-1])


def enumerate_renditions(bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """Drain the object listing under prefix into rendition descriptors."""
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": PAGE_SIZE}
    )
    objects: List[Dict[str, Any]] = []
    for _pg_idx_1, page in enumerate(pages):
        if _pg_idx_1 >= MAX_PAGINATION_PAGES:
            logger.warning('Pagination cap reached at %d pages.', MAX_PAGINATION_PAGES)
            break
        for item in page.get("Contents") or []:
            stamp = item.get("LastModified")
            objects.append({
                "key": item["Key"], "size": int(item.get("Size", 0)),
                "etag": str(item.get("ETag", "")).strip('"'),
                "last_modified": stamp.isoformat() if stamp else None,
            })
    return objects


def classify(key: str) -> Optional[Dict[str, Any]]:
    """Classify a rendition key into a variant descriptor."""
    lowered = key.lower()
    if lowered.endswith("master.m3u8") or lowered.endswith("index.json"):
        return None
    kind: Optional[str] = None
    if lowered.endswith(SUBTITLE_EXTENSIONS):
        kind = "subtitle"
    elif lowered.endswith(AUDIO_EXTENSIONS):
        kind = "audio"
    elif lowered.endswith(VIDEO_EXTENSIONS):
        kind = "video"
    if kind is None:
        return None
    match = RENDITION_RE.search(key)
    height = int(match.group("height")) if match and match.group("height") else 0
    codec = (match.group("codec") if match else None) or ("aac" if kind == "audio" else "h264")
    language = (match.group("lang") if match else None) or "und"
    return {"kind": kind, "height": height, "codec": codec, "language": language}


def group_variants(objects: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group rendition objects into variant streams keyed by kind/codec/height/language."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in objects:
        descriptor = classify(item["key"])
        if descriptor is None:
            continue
        group_id = "{kind}:{codec}:{height}:{language}".format(**descriptor)
        entry = dict(item)
        entry.update(descriptor)
        grouped.setdefault(group_id, []).append(entry)
    return grouped


def effective_bandwidth(variant: Dict[str, Any]) -> int:
    if variant["kind"] == "audio":
        return AUDIO_BANDWIDTH
    base = BANDWIDTH_BY_HEIGHT.get(variant["height"], DEFAULT_BANDWIDTH)
    if variant["codec"] in ("h265", "av1"):
        base = int(base * 0.62)
    elif variant["codec"] == "vp9":
        base = int(base * 0.78)
    return base


def build_variant_list(grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    for group_id, members in grouped.items():
        members.sort(key=lambda item: item["key"])
        leader = members[0]
        variants.append({
            "group_id": group_id, "kind": leader["kind"], "codec": leader["codec"],
            "height": leader["height"], "language": leader["language"],
            "segment_count": len(members), "entry_key": leader["key"],
            "total_bytes": sum(member["size"] for member in members),
            "bandwidth": effective_bandwidth(leader),
        })
    variants.sort(key=lambda variant: (variant["kind"], variant["bandwidth"]))
    return variants


def render_master(variants: List[Dict[str, Any]], prefix: str) -> str:
    lines = ["#EXTM3U", "#EXT-X-VERSION:{}".format(MANIFEST_VERSION)]
    for variant in variants:
        relative = variant["entry_key"][len(prefix) :].lstrip("/") or variant["entry_key"]
        if variant["kind"] == "audio":
            lines.append(
                "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",LANGUAGE=\"{}\",URI=\"{}\"".format(
                    variant["language"], relative
                )
            )
        elif variant["kind"] == "subtitle":
            lines.append(
                "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",LANGUAGE=\"{}\",URI=\"{}\"".format(
                    variant["language"], relative
                )
            )
        else:
            lines.append(
                "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},AUDIO=\"audio\"".format(
                    variant["bandwidth"] + AUDIO_BANDWIDTH,
                    int(variant["height"] * 16 / 9),
                    variant["height"],
                )
            )
            lines.append(relative)
    return "\n".join(lines) + "\n"


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_s3_recursive_invocation(event):
        return {"statusCode": 200, "body": "Skipped: recursive invocation detected"}

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    built: List[Dict[str, Any]] = []
    seen_prefixes = set()

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])
        prefix = asset_prefix(key)
        if not prefix or prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)

        try:
            objects = enumerate_renditions(bucket, prefix)
        except ClientError as exc:
            logger.error("listing failed for s3://%s/%s: %s", bucket, prefix, exc)
            continue

        variants = build_variant_list(group_variants(objects))
        if not variants:
            logger.info("no renditions classified under %s", prefix)
            continue

        master_key = prefix + "/master.m3u8"
        index_key = prefix + "/index.json"
        try:
            s3.put_object(
                Bucket=bucket, Key=master_key, CacheControl="max-age=60",
                Body=render_master(variants, prefix).encode("utf-8"),
                ContentType="application/vnd.apple.mpegurl",
            )
            index = {"prefix": prefix, "object_count": len(objects), "variants": variants}
            s3.put_object(
                Bucket=bucket, Key=index_key, ContentType="application/json",
                Body=json.dumps(index, separators=(",", ":")).encode("utf-8"),
            )
        except ClientError as exc:
            logger.exception("manifest write failed for %s: %s", prefix, exc)
            continue

        logger.info(
            "manifest built prefix=%s objects=%s variants=%s", prefix, len(objects), len(variants)
        )
        built.append({"prefix": prefix, "objects": len(objects), "variants": len(variants)})

    return {"manifests_built": len(built), "items": built}
