"""Subtitle burn-in preparation.

Event source: S3 object-created notifications for uploaded WebVTT caption tracks.

Parses WebVTT cues, reflows over-long caption lines to a fixed character width while
respecting word boundaries, resolves overlapping cue timings, derives per-cue drawtext
filter directives with fade timings, and writes the rendered burn-in track back into the
media bucket.
"""

import json
import logging
import re
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

MAX_LINE_CHARS = 42
MAX_LINES_PER_CUE = 2
MIN_CUE_DURATION = 0.833
MAX_CUE_DURATION = 7.0
FADE_SECONDS = 0.12
READING_CHARS_PER_SECOND = 17.0
SAFE_AREA_BOTTOM_PCT = 0.08
FONT_SIZE_BASE = 36

TIMING_RE = re.compile(
    r"^(?P<start>(?:\d{2,}:)?\d{2}:\d{2}\.\d{3})\s*-->\s*(?P<end>(?:\d{2,}:)?\d{2}:\d{2}\.\d{3})"
    r"(?P<settings>.*)$"
)
TAG_RE = re.compile(r"</?[a-zA-Z][^>]*>")


def _decode_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def parse_timestamp(raw: str) -> float:
    parts = raw.split(":")
    seconds = float(parts[-1])
    minutes = int(parts[-2]) if len(parts) >= 2 else 0
    hours = int(parts[-3]) if len(parts) >= 3 else 0
    return hours * 3600.0 + minutes * 60.0 + seconds


def format_timestamp(value: float) -> str:
    hours = int(value // 3600)
    minutes = int((value % 3600) // 60)
    seconds = value - (hours * 3600) - (minutes * 60)
    return "{:02d}:{:02d}:{:06.3f}".format(hours, minutes, seconds)


def parse_webvtt(text: str) -> List[Dict[str, Any]]:
    """Split a WebVTT document into structured cues."""
    cues: List[Dict[str, Any]] = []
    blocks = re.split(r"\r?\n\r?\n", text.strip())
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines or lines[0].upper().startswith("WEBVTT"):
            continue
        identifier: Optional[str] = None
        timing_line = lines[0]
        if "-->" not in timing_line and len(lines) > 1:
            identifier = timing_line
            timing_line = lines[1]
            body_lines = lines[2:]
        else:
            body_lines = lines[1:]
        match = TIMING_RE.match(timing_line)
        if not match:
            logger.warning("skipping cue block without timing: %s", timing_line[:40])
            continue
        try:
            start, end = parse_timestamp(match.group("start")), parse_timestamp(match.group("end"))
        except (ValueError, IndexError):
            logger.warning("unparseable cue timing: %s", timing_line[:40])
            continue
        payload = " ".join(TAG_RE.sub("", line) for line in body_lines).strip()
        if not payload:
            continue
        cues.append({
            "id": identifier or "cue{}".format(len(cues) + 1),
            "start": start, "end": max(end, start + MIN_CUE_DURATION),
            "settings": match.group("settings").strip(), "text": payload,
        })
    return cues


def reflow(text: str, max_chars: int = MAX_LINE_CHARS) -> List[str]:
    """Greedy word-wrap that keeps each line at or under max_chars."""
    words = text.split()
    lines: List[str] = []
    current = ""
    for word in words:
        if not current:
            current = word
        elif len(current) + 1 + len(word) <= max_chars:
            current = current + " " + word
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    if len(lines) <= MAX_LINES_PER_CUE:
        return lines
    merged: List[str] = []
    per_line = max(max_chars, (len(text) // MAX_LINES_PER_CUE) + 1)
    current = ""
    for word in words:
        if current and len(current) + 1 + len(word) > per_line and len(merged) < MAX_LINES_PER_CUE - 1:
            merged.append(current)
            current = word
        else:
            current = word if not current else current + " " + word
    if current:
        merged.append(current)
    return merged[:MAX_LINES_PER_CUE]


def resolve_timings(cues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Clamp durations to reading speed and remove cue overlap."""
    ordered = sorted(cues, key=lambda cue: (cue["start"], cue["end"]))
    resolved: List[Dict[str, Any]] = []
    previous_end = 0.0
    for cue in ordered:
        start = max(cue["start"], previous_end)
        readable = len(cue["text"]) / READING_CHARS_PER_SECOND
        duration = min(MAX_CUE_DURATION, max(MIN_CUE_DURATION, readable, cue["end"] - start))
        end = start + duration
        lines = reflow(cue["text"])
        resolved.append({
            "id": cue["id"], "start": round(start, 3), "end": round(end, 3),
            "duration": round(duration, 3), "lines": lines, "line_count": len(lines),
            "truncated": sum(len(line) for line in lines) < len(cue["text"]),
        })
        previous_end = end
    return resolved


def burn_in_directives(cues: List[Dict[str, Any]], frame_height: int = 1080) -> List[Dict[str, Any]]:
    """Build drawtext-style directives with fade in/out envelopes."""
    baseline = int(frame_height * (1.0 - SAFE_AREA_BOTTOM_PCT))
    font_size = max(18, int(FONT_SIZE_BASE * (frame_height / 1080.0)))
    line_height = int(font_size * 1.35)
    directives: List[Dict[str, Any]] = []
    for cue in cues:
        fade = round(min(FADE_SECONDS, cue["duration"] / 4.0), 3)
        for offset, line in enumerate(reversed(cue["lines"])):
            directives.append({
                "cue_id": cue["id"], "text": line, "font_size": font_size,
                "y": baseline - offset * line_height,
                "enable_from": cue["start"], "enable_to": cue["end"],
                "alpha_in": fade, "alpha_out": fade,
            })
    return directives


def render_track(cues: List[Dict[str, Any]]) -> str:
    lines = ["WEBVTT", ""]
    for cue in cues:
        lines.append(cue["id"])
        lines.append(
            "{} --> {}".format(format_timestamp(cue["start"]), format_timestamp(cue["end"]))
        )
        lines.extend(cue["lines"])
        lines.append("")
    return "\n".join(lines)


def lambda_handler(event, context):
    outputs: List[Dict[str, Any]] = []

    for record in event.get("Records") or []:
        bucket = record["s3"]["bucket"]["name"]
        key = _decode_key(record["s3"]["object"]["key"])
        try:
            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as exc:
            logger.error("caption fetch failed for s3://%s/%s: %s", bucket, key, exc)
            continue

        try:
            document = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            document = raw.decode("latin-1")

        cues = resolve_timings(parse_webvtt(document))
        if not cues:
            logger.warning("no usable cues in %s", key)
            continue
        directives = burn_in_directives(cues)

        stem = key.rsplit(".", 1)[0]
        track_key = stem + ".burnin.vtt"
        filter_key = stem + ".burnin.json"
        try:
            s3.put_object(
                Bucket=bucket, Key=track_key, ContentType="text/vtt",
                Body=render_track(cues).encode("utf-8"),
            )
            s3.put_object(
                Bucket=bucket, Key=filter_key, ContentType="application/json",
                Body=json.dumps({"source_key": key, "directives": directives}).encode("utf-8"),
            )
        except ClientError as exc:
            logger.exception("burn-in write failed for %s: %s", stem, exc)
            continue

        logger.info("burn-in prepared key=%s cues=%s directives=%s", key, len(cues), len(directives))
        outputs.append({"source": key, "track": track_key, "cues": len(cues)})

    return {"tracks_written": len(outputs), "items": outputs}
