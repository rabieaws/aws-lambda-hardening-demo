"""Double-entry ledger poster.

Event source: direct Lambda invoke (``RequestResponse``) from the payment
orchestrator state machine.

Expands a business event (capture, refund, fee, chargeback) into a balanced set of debit
and credit journal lines, splits the merchant payable leg across any requested sub-ledger
allocations, asserts that debits equal credits to the cent, and commits the journal plus
its balance deltas with ``TransactWriteItems`` under a conditional idempotency marker.
"""

import hashlib
import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.client("dynamodb")

JOURNAL_TABLE = os.environ.get("JOURNAL_TABLE", "ledger-journal")
BALANCE_TABLE = os.environ.get("BALANCE_TABLE", "ledger-balances")

MONEY_QUANTUM = Decimal("0.01")
TRANSACT_ITEM_LIMIT = 25

AR = "1100-merchant-receivable"
AP = "4000-merchant-payable"
FEE_REVENUE = "4200-processing-fee-revenue"
CHARGEBACK_LOSS = "5100-chargeback-loss"

POSTING_RULES: Dict[str, List[Tuple[str, str, str]]] = {
    "capture": [("debit", AR, "gross"), ("credit", AP, "net"), ("credit", FEE_REVENUE, "fee")],
    "refund": [("debit", AP, "net"), ("debit", FEE_REVENUE, "fee"), ("credit", AR, "gross")],
    "chargeback": [("debit", CHARGEBACK_LOSS, "gross"), ("credit", AR, "gross")],
    "fee": [("debit", AP, "gross"), ("credit", FEE_REVENUE, "gross")],
}


class UnbalancedJournal(Exception):
    """Raised when the constructed journal does not balance."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _component_amounts(event_body: Dict[str, Any]) -> Dict[str, Decimal]:
    gross = _money(event_body.get("gross_amount", "0"))
    fee = _money(event_body.get("fee_amount", "0"))
    net = (gross - fee).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    if gross <= 0 or fee < 0 or fee > gross:
        raise ValueError("gross=%s fee=%s outside accepted range" % (gross, fee))
    return {"gross": gross, "fee": fee, "net": net}


def _build_lines(event_type: str, amounts: Dict[str, Decimal], merchant: str) -> List[Dict[str, Any]]:
    rules = POSTING_RULES.get(event_type)
    if not rules:
        raise ValueError("unsupported_event_type:%s" % event_type)

    lines: List[Dict[str, Any]] = []
    for side, account, component in rules:
        amount = amounts[component]
        if amount == 0:
            continue
        lines.append({"side": side, "account": account, "sub_account": merchant, "amount": amount})
    return lines


def _split_payable(
    lines: List[Dict[str, Any]], allocations: List[Dict[str, Any]], merchant: str
) -> List[Dict[str, Any]]:
    """Replace the aggregate merchant-payable leg with one leg per sub-ledger split."""
    payable = [ln for ln in lines if ln["account"] == AP]
    if not allocations or not payable:
        return lines

    target = payable[0]
    side = target["side"]
    expanded = [ln for ln in lines if ln is not target]
    allocated = Decimal("0.00")
    for allocation in allocations:
        share = _money(allocation.get("amount", "0"))
        if share <= 0:
            continue
        sub_account = "%s#%s" % (merchant, allocation.get("sub_ledger", "default"))
        expanded.append({"side": side, "account": AP, "sub_account": sub_account, "amount": share})
        allocated += share
    if allocated != target["amount"]:
        raise UnbalancedJournal("allocations=%s payable=%s" % (allocated, target["amount"]))
    return expanded


def _assert_balanced(lines: List[Dict[str, Any]]) -> Tuple[Decimal, Decimal]:
    debits = sum((ln["amount"] for ln in lines if ln["side"] == "debit"), Decimal("0.00"))
    credits = sum((ln["amount"] for ln in lines if ln["side"] == "credit"), Decimal("0.00"))
    if debits != credits:
        raise UnbalancedJournal("debits=%s credits=%s" % (debits, credits))
    return debits, credits


def _journal_id(event_body: Dict[str, Any]) -> str:
    fingerprint = json.dumps({
        "type": event_body.get("event_type"), "reference": event_body.get("source_reference"),
        "merchant": event_body.get("merchant_id"), "gross": str(event_body.get("gross_amount")),
        "fee": str(event_body.get("fee_amount")),
    }, sort_keys=True)
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:32]


def _journal_put(journal_id: str, header: Dict[str, Any], lines: List[Dict[str, Any]]) -> Dict[str, Any]:
    serialized = [
        {"M": {
            "side": {"S": ln["side"]}, "account": {"S": ln["account"]},
            "sub_account": {"S": ln["sub_account"]}, "amount": {"N": str(ln["amount"])},
        }}
        for ln in lines
    ]
    item = {
        "journal_id": {"S": journal_id}, "event_type": {"S": header["event_type"]},
        "merchant_id": {"S": header["merchant_id"]}, "currency": {"S": header["currency"]},
        "source_reference": {"S": header["source_reference"]},
        "posted_at": {"N": str(header["posted_at"])},
        "total_debits": {"N": str(header["total_debits"])},
        "total_credits": {"N": str(header["total_credits"])},
        "lines": {"L": serialized},
    }
    condition = "attribute_not_exists(journal_id)"
    return {"Put": {"TableName": JOURNAL_TABLE, "Item": item, "ConditionExpression": condition}}


def _balance_update(line: Dict[str, Any], currency: str, journal_id: str) -> Dict[str, Any]:
    signed = line["amount"] if line["side"] == "debit" else -line["amount"]
    scope = "%s#%s" % (line["sub_account"], currency)
    return {
        "Update": {
            "TableName": BALANCE_TABLE,
            "Key": {"account": {"S": line["account"]}, "scope": {"S": scope}},
            "UpdateExpression": (
                "SET last_journal_id = :jid, updated_at = :now "
                "ADD balance :delta, posting_count :one"),
            "ExpressionAttributeValues": {
                ":delta": {"N": str(signed)}, ":one": {"N": "1"},
                ":jid": {"S": journal_id}, ":now": {"N": str(int(time.time()))},
            },
        }
    }


def _commit(journal_id: str, header: Dict[str, Any], lines: List[Dict[str, Any]]) -> str:
    items = [_journal_put(journal_id, header, lines)]
    for line in lines:
        items.append(_balance_update(line, header["currency"], journal_id))
    if len(items) > TRANSACT_ITEM_LIMIT:
        raise ValueError("journal_too_large:%s" % len(items))

    try:
        dynamodb.transact_write_items(TransactItems=items)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "TransactionCanceledException":
            reasons = exc.response.get("CancellationReasons", [])
            if any(r.get("Code") == "ConditionalCheckFailed" for r in reasons):
                logger.info("journal_already_posted journal_id=%s", journal_id)
                return "duplicate"
        raise
    return "posted"


def lambda_handler(event, context):
    event_body = event.get("detail", event) or {}
    event_type = str(event_body.get("event_type", "")).lower()
    merchant_id = str(event_body.get("merchant_id", ""))
    currency = str(event_body.get("currency", "USD")).upper()
    source_reference = str(event_body.get("source_reference", ""))

    if not merchant_id or not source_reference:
        return {"status": "rejected", "reason": "missing_merchant_or_reference"}

    try:
        amounts = _component_amounts(event_body)
        lines = _build_lines(event_type, amounts, merchant_id)
        lines = _split_payable(lines, event_body.get("allocations") or [], merchant_id)
        debits, credits = _assert_balanced(lines)
    except UnbalancedJournal as exc:
        logger.error("journal_unbalanced reference=%s detail=%s", source_reference, exc)
        return {"status": "rejected", "reason": "unbalanced", "detail": str(exc)}
    except (ValueError, ArithmeticError) as exc:
        logger.error("journal_invalid reference=%s detail=%s", source_reference, exc)
        return {"status": "rejected", "reason": "invalid_event", "detail": str(exc)}

    journal_id = _journal_id(event_body)
    header = {
        "event_type": event_type, "merchant_id": merchant_id, "currency": currency,
        "source_reference": source_reference, "posted_at": int(time.time()),
        "total_debits": debits, "total_credits": credits,
    }

    try:
        outcome = _commit(journal_id, header, lines)
    except ClientError as exc:
        logger.exception("journal_commit_failed reference=%s", source_reference)
        code = exc.response.get("Error", {}).get("Code", "unknown")
        return {"status": "error", "reason": code, "journal_id": journal_id}

    logger.info(
        "journal_%s journal_id=%s type=%s debits=%s lines=%s",
        outcome, journal_id, event_type, debits, len(lines),
    )
    return {
        "status": outcome, "journal_id": journal_id, "line_count": len(lines),
        "total_debits": str(debits), "total_credits": str(credits),
    }
