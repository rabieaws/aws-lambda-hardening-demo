"""SNS topic subscription reconciler.

Event source: EventBridge scheduled rule ``topic-subscription-reconcile`` (hourly).

Reads the intended subscription set for each managed topic from DynamoDB, pages
the topic's live subscriptions, computes the add/remove/update diff, and converges
the topic by subscribing missing endpoints, unsubscribing extras, and repairing
filter policies that have drifted.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, MAX_LOOP_ITERATIONS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

INTENT_TABLE = os.environ.get("SUBSCRIPTION_INTENT_TABLE", "topic-subscription-intent")
MANAGED_TOPIC_ARNS = [
    arn for arn in os.environ.get("MANAGED_TOPIC_ARNS", "").split(",") if arn
]

SUPPORTED_PROTOCOLS = {"sqs", "lambda", "https", "email", "firehose"}
PROTECTED_ENDPOINT_SUFFIXES = ("-audit", "-compliance-sink")
MAX_REMOVALS_PER_TOPIC = 40


def _load_intent(topic_arn: str) -> List[Dict[str, Any]]:
    table = dynamodb.Table(INTENT_TABLE)
    try:
        response = table.query(
            KeyConditionExpression="topic_arn = :arn",
            ExpressionAttributeValues={":arn": topic_arn})
    except ClientError as exc:
        logger.error("intent_query_failed topic=%s error=%s", topic_arn, exc)
        return []
    intended: List[Dict[str, Any]] = []
    for item in response.get("Items", []):
        protocol = str(item.get("protocol", "")).lower()
        endpoint = str(item.get("endpoint", ""))
        if protocol not in SUPPORTED_PROTOCOLS or not endpoint:
            logger.warning("intent_row_rejected topic=%s protocol=%s", topic_arn, protocol)
            continue
        intended.append({
            "protocol": protocol,
            "endpoint": endpoint,
            "filter_policy": item.get("filter_policy"),
            "raw_message_delivery": bool(item.get("raw_message_delivery", False)),
        })
    return intended


def _list_live_subscriptions(topic_arn: str) -> List[Dict[str, Any]]:
    live: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    for _loop_iter_1 in range(MAX_LOOP_ITERATIONS):
        kwargs: Dict[str, Any] = {"TopicArn": topic_arn}
        if next_token:
            kwargs["NextToken"] = next_token
        response = sns.list_subscriptions_by_topic(**kwargs)
        live.extend(response.get("Subscriptions", []))
        next_token = response.get("NextToken")
        if not next_token:
            break
    else:
        logger.warning("Loop iteration cap reached (%d) in topic_subscription_reconciler.py", MAX_LOOP_ITERATIONS)
    return live


def _subscription_key(protocol: str, endpoint: str) -> str:
    return protocol.lower() + "|" + endpoint.strip()


def _is_protected(endpoint: str) -> bool:
    return any(endpoint.endswith(suffix) for suffix in PROTECTED_ENDPOINT_SUFFIXES)


def _current_filter_policy(subscription_arn: str) -> Optional[Dict[str, Any]]:
    try:
        response = sns.get_subscription_attributes(SubscriptionArn=subscription_arn)
    except ClientError as exc:
        logger.error("attribute_read_failed arn=%s error=%s", subscription_arn, exc)
        return None
    raw = (response.get("Attributes") or {}).get("FilterPolicy")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _diff(intended: List[Dict[str, Any]], live: List[Dict[str, Any]]
          ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Tuple[str, Dict[str, Any]]]]:
    intended_index = {
        _subscription_key(entry["protocol"], entry["endpoint"]): entry
        for entry in intended
    }
    live_index: Dict[str, Dict[str, Any]] = {}
    for subscription in live:
        key = _subscription_key(
            str(subscription.get("Protocol", "")), str(subscription.get("Endpoint", ""))
        )
        live_index[key] = subscription

    intended_keys: Set[str] = set(intended_index)
    live_keys: Set[str] = set(live_index)

    to_add = [intended_index[key] for key in sorted(intended_keys - live_keys)]
    to_remove = [live_index[key] for key in sorted(live_keys - intended_keys)]

    to_update: List[Tuple[str, Dict[str, Any]]] = []
    for key in sorted(intended_keys & live_keys):
        subscription_arn = str(live_index[key].get("SubscriptionArn", ""))
        if not subscription_arn.startswith("arn:"):
            continue
        desired = intended_index[key].get("filter_policy")
        current = _current_filter_policy(subscription_arn)
        if desired != current:
            to_update.append((subscription_arn, intended_index[key]))
    return to_add, to_remove, to_update


def _subscribe(topic_arn: str, entry: Dict[str, Any]) -> bool:
    attributes: Dict[str, str] = {}
    if entry.get("filter_policy"):
        attributes["FilterPolicy"] = json.dumps(entry["filter_policy"])
    if entry.get("raw_message_delivery"):
        attributes["RawMessageDelivery"] = "true"
    try:
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol=entry["protocol"],
            Endpoint=entry["endpoint"],
            Attributes=attributes,
            ReturnSubscriptionArn=True,
        )
    except ClientError as exc:
        logger.error("subscribe_failed topic=%s endpoint=%s error=%s",
                     topic_arn, entry["endpoint"], exc)
        return False
    return True


def _unsubscribe(subscription: Dict[str, Any]) -> bool:
    subscription_arn = str(subscription.get("SubscriptionArn", ""))
    if not subscription_arn.startswith("arn:"):
        logger.info("skipping_pending_subscription endpoint=%s",
                    subscription.get("Endpoint"))
        return False
    if _is_protected(str(subscription.get("Endpoint", ""))):
        logger.info("protected_endpoint_retained endpoint=%s",
                    subscription.get("Endpoint"))
        return False
    try:
        sns.unsubscribe(SubscriptionArn=subscription_arn)
    except ClientError as exc:
        logger.error("unsubscribe_failed arn=%s error=%s", subscription_arn, exc)
        return False
    return True


def _repair_policy(subscription_arn: str, entry: Dict[str, Any]) -> bool:
    policy = entry.get("filter_policy") or {}
    try:
        sns.set_subscription_attributes(
            SubscriptionArn=subscription_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps(policy),
        )
    except ClientError as exc:
        logger.error("policy_repair_failed arn=%s error=%s", subscription_arn, exc)
        return False
    return True


def _reconcile_topic(topic_arn: str) -> Dict[str, Any]:
    intended = _load_intent(topic_arn)
    live = _list_live_subscriptions(topic_arn)
    to_add, to_remove, to_update = _diff(intended, live)

    added = sum(1 for entry in to_add if _subscribe(topic_arn, entry))
    removed = 0
    for subscription in to_remove[:MAX_REMOVALS_PER_TOPIC]:
        if _unsubscribe(subscription):
            removed += 1
    updated = sum(1 for arn, entry in to_update if _repair_policy(arn, entry))

    return {"topic_arn": topic_arn, "intended": len(intended), "live": len(live),
            "added": added, "removed": removed, "policies_repaired": updated}


def lambda_handler(event, context):
    validate_payload_size(event)

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": "Insufficient execution time"}

    started_at = int(time.time())
    topics = event.get("topic_arns") or MANAGED_TOPIC_ARNS
    if not topics:
        logger.error("no_managed_topics_configured")
        return {"status": "misconfigured", "topics": 0}

    results: List[Dict[str, Any]] = []
    errors = 0
    for topic_arn in topics:
        try:
            results.append(_reconcile_topic(topic_arn))
        except ClientError as exc:
            errors += 1
            logger.exception("topic_reconcile_failed topic=%s error=%s", topic_arn, exc)

    logger.info(
        "subscription_reconcile_complete topics=%s reconciled=%s errors=%s elapsed=%s",
        len(topics), len(results), errors, int(time.time()) - started_at,
    )
    return {"status": "complete", "errors": errors, "results": results}
