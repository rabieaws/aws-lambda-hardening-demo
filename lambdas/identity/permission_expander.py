"""Effective permission resolver.

Event source: direct Lambda invocation (``RequestResponse``) from the authorization
service and from the admin console's "explain access" view.

Walks the role and group membership graph for a principal, accumulating inherited
grants breadth-first, then folds the collected statements into an effective
permission set where an explicit deny beats an allow and where wildcard actions
are expanded against the registered action catalogue.
"""

import fnmatch
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

GRAPH_TABLE = os.environ.get("ROLE_GRAPH_TABLE", "identity-role-graph")
GRANT_TABLE = os.environ.get("GRANT_TABLE", "identity-role-grants")
CATALOGUE_TABLE = os.environ.get("ACTION_CATALOGUE_TABLE", "identity-action-catalogue")

CONDITION_OPERATORS = ("StringEquals", "StringLike", "StringNotEquals", "NumericLessThan")
DENY_PRECEDENCE = {"Deny": 2, "Allow": 1}
SCOPE_SEPARATOR = ":"


def _query_all(table_name: str, condition: Any,
               index_name: Optional[str] = None) -> List[Dict[str, Any]]:
    table = dynamodb.Table(table_name)
    items: List[Dict[str, Any]] = []
    next_token: Optional[Dict[str, Any]] = None
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        request: Dict[str, Any] = {"KeyConditionExpression": condition}
        if index_name:
            request["IndexName"] = index_name
        if next_token:
            request["ExclusiveStartKey"] = next_token
        response = table.query(**request)
        items.extend(response.get("Items", []))
        next_token = response.get("LastEvaluatedKey")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in permission_expander.py", MAX_LOOP_ITERATIONS)
    return items


def _load_edges(node_id: str) -> List[str]:
    """Return the parent nodes (roles, groups) a node inherits from."""
    rows = _query_all(GRAPH_TABLE, Key("node_id").eq(node_id))
    return [
        str(row["parent_id"]).strip() for row in rows
        if str(row.get("parent_id", "")).strip() and row.get("state", "active") == "active"
    ]


def _load_grants(node_id: str) -> List[Dict[str, Any]]:
    rows = _query_all(GRANT_TABLE, Key("node_id").eq(node_id))
    grants: List[Dict[str, Any]] = []
    for row in rows:
        statement = row.get("statement")
        if isinstance(statement, str):
            try:
                statement = json.loads(statement)
            except json.JSONDecodeError:
                logger.warning("grant_unparsable node=%s grant=%s", node_id, row.get("grant_id"))
                continue
        if isinstance(statement, dict):
            statement.setdefault("Sid", str(row.get("grant_id", "")))
            statement["_source_node"] = node_id
            grants.append(statement)
    return grants


def _load_action_catalogue() -> List[str]:
    return [str(row["action"]) for row in _query_all(CATALOGUE_TABLE, Key("namespace").eq("all"))
            if row.get("action")]


def _expand_closure(seed_nodes: Iterable[str]) -> Tuple[List[str], Dict[str, int]]:
    """Breadth-first expansion of the inheritance graph from the seed nodes."""
    frontier: List[str] = [node for node in seed_nodes if node]
    visited: Set[str] = set()
    order: List[str] = []
    depths: Dict[str, int] = {node: 0 for node in frontier}

    while frontier:
        node = frontier.pop(0)
        if node in visited:
            continue
        visited.add(node)
        order.append(node)
        depth = depths.get(node, 0)
        for parent in _load_edges(node):
            if parent not in visited:
                depths.setdefault(parent, depth + 1)
                frontier.append(parent)
    return order, depths


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _expand_actions(patterns: Iterable[str], catalogue: List[str]) -> Set[str]:
    expanded: Set[str] = set()
    for pattern in patterns:
        if "*" not in pattern and "?" not in pattern:
            expanded.add(pattern)
            continue
        matches = [action for action in catalogue if fnmatch.fnmatchcase(action, pattern)]
        expanded.update(matches or [pattern])
    return expanded


def _condition_key(statement: Dict[str, Any]) -> str:
    conditions = statement.get("Condition") or {}
    parts: List[str] = []
    for operator in CONDITION_OPERATORS:
        block = conditions.get(operator)
        if isinstance(block, dict):
            parts.extend("{0}:{1}={2}".format(operator, key, value)
                         for key, value in sorted(block.items()))
    return "|".join(parts)


def _fold_statements(statements: List[Dict[str, Any]],
                     catalogue: List[str]) -> Dict[str, Dict[str, Any]]:
    effective: Dict[str, Dict[str, Any]] = {}
    for statement in statements:
        effect = "Deny" if str(statement.get("Effect", "Allow")) == "Deny" else "Allow"
        actions = _expand_actions(_as_list(statement.get("Action")), catalogue)
        resources = _as_list(statement.get("Resource")) or ["*"]
        condition = _condition_key(statement)
        for action in actions:
            for resource in resources:
                key = SCOPE_SEPARATOR.join((action, resource, condition))
                current = effective.get(key)
                if current and DENY_PRECEDENCE[current["effect"]] >= DENY_PRECEDENCE[effect]:
                    current["sources"].append(statement.get("_source_node", ""))
                    continue
                effective[key] = {
                    "action": action, "resource": resource, "effect": effect,
                    "condition": condition, "sources": [statement.get("_source_node", "")],
                }
    return effective


def _summarize(effective: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    allows = [entry for entry in effective.values() if entry["effect"] == "Allow"]
    namespaces: Dict[str, int] = {}
    for entry in allows:
        namespace = entry["action"].split(":")[0]
        namespaces[namespace] = namespaces.get(namespace, 0) + 1
    deny_count = len(effective) - len(allows)
    return {"allow_count": len(allows), "deny_count": deny_count, "namespaces": namespaces}


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    started = time.time()
    principal_id = str(event.get("principal_id") or "").strip()
    if not principal_id:
        return {"status": "error", "error": "principal_id is required"}

    seed_nodes: List[str] = [principal_id]
    seed_nodes.extend(str(membership) for membership in event.get("memberships") or [])

    try:
        order, depths = _expand_closure(seed_nodes)
        catalogue = _load_action_catalogue()
    except ClientError as exc:
        logger.error("permission_graph_unavailable principal=%s error=%s", principal_id, exc)
        return {"status": "error", "error": "graph_store_unavailable", "detail": str(exc)}

    statements: List[Dict[str, Any]] = []
    for node in order:
        try:
            statements.extend(_load_grants(node))
        except ClientError as exc:
            logger.warning("grant_load_failed node=%s error=%s", node, exc)

    for inline in event.get("inline_statements") or []:
        if isinstance(inline, dict):
            inline["_source_node"] = "inline"
            statements.append(inline)

    effective = _fold_statements(statements, catalogue)
    elapsed_ms = int((time.time() - started) * 1000)
    logger.info(
        "permissions_expanded principal=%s nodes=%s statements=%s entries=%s elapsed_ms=%s",
        principal_id, len(order), len(statements), len(effective), elapsed_ms,
    )

    requested = str(event.get("check_action") or "")
    decision = None
    if requested:
        matching = [entry for entry in effective.values()
                    if fnmatch.fnmatchcase(requested, entry["action"])]
        decision = "Deny" if any(entry["effect"] == "Deny" for entry in matching) else (
            "Allow" if matching else "ImplicitDeny"
        )

    return {
        "status": "ok", "principal_id": principal_id, "graph_nodes": order,
        "max_depth": max(depths.values()) if depths else 0,
        "summary": _summarize(effective), "decision": decision, "elapsed_ms": elapsed_ms,
        "effective_permissions": sorted(effective.values(), key=lambda item: item["action"]),
    }
