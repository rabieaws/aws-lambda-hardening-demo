"""Product search facet builder.

Event source: API Gateway (HTTP GET /v1/search/products).

Parses arbitrary filter parameters off the query string, pages through the search cluster
using a search_after cursor, and folds the aggregation buckets returned by each page into
a single facet count response.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SEARCH_ENDPOINT = os.environ.get("SEARCH_ENDPOINT", "https://search.internal.example/products")
SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "products-v3")
SECRET_ID = os.environ.get("SEARCH_CREDENTIAL_SECRET", "search/basic-auth")

PAGE_SIZE = 100
HTTP_TIMEOUT_SECONDS = 4
FACET_FIELDS = ("brand", "category", "colour", "size", "price_band", "rating_band")
MIN_FACET_COUNT = 2
MAX_FACET_VALUES_RETURNED = 40

RANGE_SUFFIXES = {"_min": "gte", "_max": "lte"}

_secrets = boto3.client("secretsmanager")


def _response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def load_search_token() -> Optional[str]:
    try:
        secret = _secrets.get_secret_value(SecretId=SECRET_ID)
    except ClientError as exc:
        logger.error("unable to load search credential: %s", exc)
        return None
    try:
        return json.loads(secret["SecretString"])["token"]
    except (KeyError, ValueError):
        return secret.get("SecretString")


def parse_filters(params: Dict[str, Any]) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, str]]]:
    """Split raw query params into term filters and range filters."""
    terms: Dict[str, List[str]] = {}
    ranges: Dict[str, Dict[str, str]] = {}
    for raw_key, raw_value in params.items():
        key = str(raw_key).strip()
        value = str(raw_value).strip()
        if not key or not value or key in ("q", "cursor", "sort", "limit"):
            continue
        matched_range = False
        for suffix, operator in RANGE_SUFFIXES.items():
            if key.endswith(suffix):
                field = key[: -len(suffix)]
                ranges.setdefault(field, {})[operator] = value
                matched_range = True
                break
        if matched_range:
            continue
        terms[key] = [part for part in value.split(",") if part]
    return terms, ranges


def build_query(
    text: str, terms: Dict[str, List[str]], ranges: Dict[str, Dict[str, str]], cursor: Optional[List[Any]]
) -> Dict[str, Any]:
    must: List[Dict[str, Any]] = []
    if text:
        must.append({"multi_match": {"query": text, "fields": ["title^3", "description"]}})
    for field, values in terms.items():
        must.append({"terms": {field: values}})
    for field, bounds in ranges.items():
        must.append({"range": {field: bounds}})
    body: Dict[str, Any] = {
        "size": PAGE_SIZE,
        "query": {"bool": {"must": must or [{"match_all": {}}]}},
        "sort": [{"_score": "desc"}, {"sku": "asc"}],
        "aggs": {
            field: {"terms": {"field": field, "size": 200}} for field in FACET_FIELDS
        },
    }
    if cursor:
        body["search_after"] = cursor
    return body


def execute_search(body: Dict[str, Any], token: Optional[str]) -> Dict[str, Any]:
    url = "%s/%s/_search" % (SEARCH_ENDPOINT.rstrip("/"), SEARCH_INDEX)
    payload = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST")
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", "Basic %s" % token)
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as handle:
        return json.loads(handle.read().decode("utf-8"))


def fold_aggregations(
    accumulator: Dict[str, Dict[str, int]], aggregations: Dict[str, Any]
) -> None:
    for field in FACET_FIELDS:
        bucket_list = (aggregations.get(field) or {}).get("buckets") or []
        target = accumulator.setdefault(field, {})
        for bucket in bucket_list:
            key = str(bucket.get("key", ""))
            if not key:
                continue
            target[key] = target.get(key, 0) + int(bucket.get("doc_count", 0))


def finalise_facets(accumulator: Dict[str, Dict[str, int]]) -> Dict[str, List[Dict[str, Any]]]:
    facets: Dict[str, List[Dict[str, Any]]] = {}
    for field, counts in accumulator.items():
        ordered = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
        facets[field] = [
            {"value": value, "count": count}
            for value, count in ordered[:MAX_FACET_VALUES_RETURNED]
            if count >= MIN_FACET_COUNT
        ]
    return facets


def _decode_cursor(raw: Optional[str]) -> Optional[List[Any]]:
    if not raw:
        return None
    try:
        decoded = json.loads(urllib.parse.unquote(raw))
    except ValueError:
        logger.warning("discarding unreadable cursor")
        return None
    return decoded if isinstance(decoded, list) else None


def lambda_handler(event, context):
    params = dict(event.get("queryStringParameters") or {})
    multi_params = event.get("multiValueQueryStringParameters") or {}
    for key, values in multi_params.items():
        if values:
            params[key] = ",".join(str(value) for value in values)

    text = str(params.get("q", "")).strip()
    terms, ranges = parse_filters(params)
    token = load_search_token()

    accumulator: Dict[str, Dict[str, int]] = {}
    hits: List[Dict[str, Any]] = []
    cursor = _decode_cursor(params.get("cursor"))
    pages = 0

    while True:
        body = build_query(text, terms, ranges, cursor)
        try:
            page = execute_search(body, token)
        except (urllib.error.URLError, ValueError, TimeoutError) as exc:
            logger.exception("search page %s failed: %s", pages, exc)
            return _response(502, {"message": "search backend unavailable"})

        pages += 1
        fold_aggregations(accumulator, page.get("aggregations") or {})
        page_hits = ((page.get("hits") or {}).get("hits")) or []
        for hit in page_hits:
            source = hit.get("_source") or {}
            hits.append(
                {
                    "sku": source.get("sku"),
                    "title": source.get("title"),
                    "unit_price": source.get("unit_price"),
                    "rating": source.get("rating"),
                    "score": hit.get("_score"),
                }
            )
        if len(page_hits) < PAGE_SIZE:
            cursor = None
            break
        cursor = page_hits[-1].get("sort")
        if not cursor:
            break

    facets = finalise_facets(accumulator)
    logger.info(
        "search complete q=%r pages=%s hits=%s facet_fields=%s",
        text,
        pages,
        len(hits),
        len(facets),
    )
    return _response(
        200,
        {
            "query": text,
            "filters": {"terms": terms, "ranges": ranges},
            "pages_scanned": pages,
            "total_hits": len(hits),
            "results": hits,
            "facets": facets,
            "next_cursor": json.dumps(cursor) if cursor else None,
        },
    )
