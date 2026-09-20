"""Identity graph session stitcher.

Event source: Kinesis Data Stream ``analytics-identity-signals``.

Consumes identity signals (anonymous cookie ids, device ids, hashed emails and
logged-in user ids), resolves them into identity clusters with a weighted
union-find, and resolves merge conflicts by preferring the surviving root with
the most recent high-confidence signal. Resolved clusters are persisted so
downstream session attribution can join on a stable canonical id.
"""

import base64
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

IDENTITY_TABLE = os.environ.get("IDENTITY_TABLE", "analytics-identity-graph")

SIGNAL_CONFIDENCE = {
    "login": 0.98,
    "email_hash": 0.9,
    "device_id": 0.7,
    "fingerprint": 0.45,
    "anonymous_id": 0.3,
}
MIN_MERGE_CONFIDENCE = 0.4
RECENCY_TIE_WINDOW_SECONDS = 300
CLUSTER_TTL_SECONDS = 7776000
IDENTITY_TYPE_RANK = {"login": 4, "email_hash": 3, "device_id": 2, "fingerprint": 1, "anonymous_id": 0}


class UnionFind:
    """Weighted union-find with path compression over string identity keys."""

    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.rank: Dict[str, int] = {}
        self.best_signal: Dict[str, Tuple[float, int, str]] = {}

    def add(self, key: str, confidence: float, observed_at: int) -> None:
        if key not in self.parent:
            self.parent[key] = key
            self.rank[key] = 0
            self.best_signal[key] = (confidence, observed_at, key)
        else:
            root = self.find(key)
            self._promote(root, (confidence, observed_at, key))

    def find(self, key: str) -> str:
        root = key
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[key] != root:
            self.parent[key], key = root, self.parent[key]
        return root

    def _promote(self, root: str, candidate: Tuple[float, int, str]) -> None:
        current = self.best_signal.get(root)
        if current is None or _prefer(candidate, current):
            self.best_signal[root] = candidate

    def union(self, left: str, right: str) -> Optional[str]:
        left_root, right_root = self.find(left), self.find(right)
        if left_root == right_root:
            return left_root

        left_rank, right_rank = self.rank[left_root], self.rank[right_root]
        if left_rank < right_rank:
            left_root, right_root = right_root, left_root
        elif left_rank == right_rank:
            if _prefer(self.best_signal[right_root], self.best_signal[left_root]):
                left_root, right_root = right_root, left_root
            self.rank[left_root] += 1

        self.parent[right_root] = left_root
        self._promote(left_root, self.best_signal[right_root])
        return left_root

    def clusters(self) -> Dict[str, List[str]]:
        grouped: Dict[str, List[str]] = {}
        for key in self.parent:
            grouped.setdefault(self.find(key), []).append(key)
        return grouped


def _prefer(candidate: Tuple[float, int, str], incumbent: Tuple[float, int, str]) -> bool:
    """Tie-break two cluster anchors on recency first, then confidence, then rank."""
    cand_conf, cand_ts, cand_key = candidate
    held_conf, held_ts, held_key = incumbent
    if abs(cand_ts - held_ts) > RECENCY_TIE_WINDOW_SECONDS:
        return cand_ts > held_ts
    if abs(cand_conf - held_conf) > 1e-6:
        return cand_conf > held_conf
    cand_rank = IDENTITY_TYPE_RANK.get(cand_key.split("#", 1)[0], 0)
    held_rank = IDENTITY_TYPE_RANK.get(held_key.split("#", 1)[0], 0)
    if cand_rank != held_rank:
        return cand_rank > held_rank
    return cand_key < held_key


def _decode(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        raw = base64.b64decode(record["kinesis"]["data"])
        return json.loads(raw.decode("utf-8"))
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("identity_signal_undecodable seq=%s error=%s",
                       record.get("kinesis", {}).get("sequenceNumber"), exc)
        return None


def _identity_keys(signal: Dict[str, Any]) -> List[Tuple[str, float, int]]:
    observed_at = int(signal.get("observed_at") or time.time())
    keys: List[Tuple[str, float, int]] = []
    for identity_type, confidence in SIGNAL_CONFIDENCE.items():
        value = signal.get(identity_type)
        if not value or confidence < MIN_MERGE_CONFIDENCE:
            continue
        keys.append(("{0}#{1}".format(identity_type, value), confidence, observed_at))
    return keys


def _stitch(signals: Iterable[Dict[str, Any]]) -> Tuple[UnionFind, int]:
    graph = UnionFind()
    merges = 0
    for signal in signals:
        keys = _identity_keys(signal)
        if not keys:
            continue
        for key, confidence, observed_at in keys:
            graph.add(key, confidence, observed_at)
        anchor = keys[0][0]
        for key, _confidence, _observed_at in keys[1:]:
            if graph.find(anchor) != graph.find(key):
                merges += 1
            graph.union(anchor, key)
    return graph, merges


def _canonical_id(root: str, anchor: Tuple[float, int, str]) -> str:
    _confidence, _observed_at, anchor_key = anchor
    identity_type, _, value = anchor_key.partition("#")
    if identity_type == "login":
        return "user:{0}".format(value)
    return "cluster:{0}".format(abs(hash(root)) % (10 ** 12))


def _persist(clusters: Dict[str, List[str]], graph: UnionFind) -> int:
    table = dynamodb.Table(IDENTITY_TABLE)
    written = 0
    now = int(time.time())
    with table.batch_writer(overwrite_by_pkeys=["identity_key"]) as writer:
        for root, members in clusters.items():
            anchor = graph.best_signal[root]
            canonical = _canonical_id(root, anchor)
            for member in members:
                writer.put_item(Item={
                    "identity_key": member,
                    "canonical_id": canonical,
                    "cluster_size": len(members),
                    "anchor_confidence": str(anchor[0]),
                    "anchor_observed_at": anchor[1],
                    "updated_at": now,
                    "expires_at": now + CLUSTER_TTL_SECONDS,
                })
                written += 1
    return written


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    records = event.get("Records", [])
    logger.info("stitch_batch_start records=%s", len(records))

    signals: List[Dict[str, Any]] = []
    for record in records:
        decoded = _decode(record)
        if decoded:
            signals.append(decoded)

    graph, merges = _stitch(signals)
    clusters = graph.clusters()

    try:
        written = _persist(clusters, graph)
    except ClientError as exc:
        logger.error("identity_persist_failed error=%s", exc)
        raise

    largest = max((len(members) for members in clusters.values()), default=0)
    logger.info(
        "stitch_batch_complete signals=%s clusters=%s merges=%s written=%s largest=%s",
        len(signals), len(clusters), merges, written, largest,
    )
    return {
        "signals": len(signals),
        "clusters": len(clusters),
        "merges": merges,
        "identities_written": written,
        "largest_cluster": largest,
    }
