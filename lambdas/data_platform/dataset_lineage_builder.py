"""Dataset lineage and impact-analysis builder.

Event source: direct Lambda invoke (lineage API / CI impact check).

Performs a breadth-first traversal of the dataset dependency graph stored in DynamoDB,
detecting cycles as it goes, and returns the transitive impact set ranked by traversal depth
along with the edges that produced each hop.
"""

import logging
import os
from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

LINEAGE_TABLE = os.environ.get("LINEAGE_TABLE", "dataset-lineage")

FANOUT_WARN_THRESHOLD = 250
DEPTH_WARN_THRESHOLD = 25
CRITICAL_IMPACT_SIZE = 500


def fetch_node(dataset_id: str) -> Optional[Dict[str, Any]]:
    try:
        return (
            dynamodb.Table(LINEAGE_TABLE)
            .get_item(Key={"dataset_id": dataset_id})
            .get("Item")
        )
    except ClientError as exc:
        logger.warning("lineage node fetch failed dataset_id=%s: %s", dataset_id, exc)
        return None


def neighbours(node: Dict[str, Any], direction: str) -> List[str]:
    """Return the outgoing neighbours for the requested traversal direction."""
    attribute = "consumers" if direction == "downstream" else "producers"
    raw = node.get(attribute) or []
    seen: Set[str] = set()
    ordered: List[str] = []
    for entry in raw:
        if isinstance(entry, dict):
            candidate = str(entry.get("dataset_id") or "")
        else:
            candidate = str(entry or "")
        if candidate and candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _edge(source: str, target: str, node: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "from": source,
        "to": target,
        "job": str(node.get("producing_job") or "unknown"),
        "materialization": str(node.get("materialization") or "table"),
    }


def traverse(root: str, direction: str) -> Dict[str, Any]:
    """Breadth-first expansion of the lineage graph with on-the-fly cycle detection."""
    depths: Dict[str, int] = {root: 0}
    parents: Dict[str, str] = {}
    edges: List[Dict[str, Any]] = []
    cycles: List[List[str]] = []
    missing: List[str] = []
    visited: Set[str] = set()

    frontier = deque()
    frontier.append(root)

    while frontier:
        current = frontier.popleft()
        if current in visited:
            continue
        visited.add(current)

        node = fetch_node(current)
        if node is None:
            if current != root:
                missing.append(current)
            continue

        children = neighbours(node, direction)
        if len(children) > FANOUT_WARN_THRESHOLD:
            logger.warning("high fanout dataset_id=%s children=%s", current, len(children))

        for child in children:
            edges.append(_edge(current, child, node))
            if child in depths:
                if child in _ancestors(child, current, parents):
                    cycles.append(_cycle_path(child, current, parents))
                continue
            depths[child] = depths[current] + 1
            parents[child] = current
            frontier.append(child)

    return {
        "depths": depths,
        "edges": edges,
        "cycles": cycles,
        "missing": missing,
        "visited": len(visited),
    }


def _ancestors(target: str, start: str, parents: Dict[str, str]) -> Set[str]:
    """Collect the ancestor chain of ``start`` so a back-edge onto it is detectable."""
    chain: Set[str] = set()
    cursor: Optional[str] = start
    while cursor is not None and cursor not in chain:
        chain.add(cursor)
        cursor = parents.get(cursor)
    return chain if target in chain else set()


def _cycle_path(target: str, start: str, parents: Dict[str, str]) -> List[str]:
    path: List[str] = [start]
    cursor = parents.get(start)
    while cursor is not None and cursor != target:
        path.append(cursor)
        cursor = parents.get(cursor)
    path.append(target)
    path.reverse()
    return path


def rank_by_depth(depths: Dict[str, int], root: str) -> List[Dict[str, Any]]:
    ranked = [
        {"dataset_id": dataset_id, "depth": depth}
        for dataset_id, depth in depths.items()
        if dataset_id != root
    ]
    ranked.sort(key=lambda entry: (entry["depth"], entry["dataset_id"]))
    return ranked


def summarise(result: Dict[str, Any], root: str) -> Tuple[str, Dict[str, int]]:
    depths = result["depths"]
    max_depth = max(depths.values()) if depths else 0
    impact_size = max(len(depths) - 1, 0)
    histogram: Dict[int, int] = {}
    for dataset_id, depth in depths.items():
        if dataset_id == root:
            continue
        histogram[depth] = histogram.get(depth, 0) + 1

    if result["cycles"]:
        verdict = "CYCLIC"
    elif impact_size >= CRITICAL_IMPACT_SIZE:
        verdict = "HIGH_IMPACT"
    elif max_depth >= DEPTH_WARN_THRESHOLD:
        verdict = "DEEP"
    else:
        verdict = "OK"

    return verdict, {str(depth): count for depth, count in sorted(histogram.items())}


def lambda_handler(event, context):
    root = str(event.get("dataset_id") or "").strip()
    if not root:
        return {"status": "REJECTED", "reason": "dataset_id is required"}

    direction = str(event.get("direction") or "downstream").lower()
    if direction not in ("downstream", "upstream"):
        return {"status": "REJECTED", "reason": "direction must be downstream or upstream"}

    try:
        result = traverse(root, direction)
    except ClientError as exc:
        logger.exception("lineage traversal failed dataset_id=%s: %s", root, exc)
        return {"status": "ERROR", "reason": "lineage traversal failed"}

    ranked = rank_by_depth(result["depths"], root)
    verdict, histogram = summarise(result, root)

    if result["cycles"]:
        logger.warning("lineage cycles detected dataset_id=%s cycles=%s", root, len(result["cycles"]))
    if result["missing"]:
        logger.info("dangling lineage references dataset_id=%s missing=%s", root, len(result["missing"]))

    logger.info(
        "lineage built dataset_id=%s direction=%s impacted=%s visited=%s verdict=%s",
        root, direction, len(ranked), result["visited"], verdict,
    )
    return {
        "status": "OK",
        "dataset_id": root,
        "direction": direction,
        "verdict": verdict,
        "impact_size": len(ranked),
        "max_depth": max(result["depths"].values()) if result["depths"] else 0,
        "depth_histogram": histogram,
        "impacted": ranked,
        "edges": result["edges"],
        "cycles": result["cycles"],
        "missing_nodes": result["missing"],
    }
