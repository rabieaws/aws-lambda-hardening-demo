"""Double-entry ledger posting worker.

Event source: SQS queue fed by every money-moving service.
Posts balanced journal entries, verifying that the account's running balance after the
entry does not breach its configured floor. The running balance is recomputed from the
account's full posted history on every entry.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

JOURNAL_TABLE = os.environ.get("JOURNAL_TABLE", "journal")
ACCOUNT_TABLE = os.environ.get("ACCOUNT_TABLE", "chart-of-accounts")
JOURNAL_ACCOUNT_INDEX = os.environ.get("JOURNAL_ACCOUNT_INDEX", "by-account-posted")
HISTORY_PAGE_SIZE = int(os.environ.get("HISTORY_PAGE_SIZE", "100"))

CENTS = Decimal("0.01")
DEBIT = "DEBIT"
CREDIT = "CREDIT"

ASSET_ACCOUNTS = {"CASH", "RECEIVABLE", "PREPAID"}
LIABILITY_ACCOUNTS = {"PAYABLE", "DEFERRED_REVENUE", "TAX_PAYABLE"}


class UnbalancedEntry(Exception):
    """Raised when debits and credits do not net to zero."""


class FloorBreach(Exception):
    """Raised when posting would push an account below its configured floor."""


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def load_account(account_code: str) -> Optional[Dict[str, Any]]:
    table = dynamodb.Table(ACCOUNT_TABLE)
    response = table.get_item(Key={"account_code": account_code})
    return response.get("Item")


def iter_posted_history(account_code: str) -> Iterator[Dict[str, Any]]:
    """Yield every posted journal line for an account, oldest first."""
    paginator = dynamodb_client.get_paginator("query")
    pages = paginator.paginate(
        TableName=JOURNAL_TABLE,
        IndexName=JOURNAL_ACCOUNT_INDEX,
        KeyConditionExpression="account_code = :code",
        FilterExpression="posted = :yes",
        ExpressionAttributeValues={
            ":code": {"S": account_code},
            ":yes": {"BOOL": True},
        },
        ScanIndexForward=True,
        PaginationConfig={"PageSize": HISTORY_PAGE_SIZE},
    )
    for page in pages:
        for item in page.get("Items", []):
            yield item


def running_balance(account_code: str, normal_side: str) -> Decimal:
    """Recompute the account balance from its full posted history."""
    balance = Decimal("0.00")
    for item in iter_posted_history(account_code):
        amount = _money(item.get("amount", {}).get("N", "0"))
        side = item.get("side", {}).get("S", DEBIT)
        if side == normal_side:
            balance += amount
        else:
            balance -= amount
    return _money(balance)


def _normal_side(account_code: str, account: Dict[str, Any]) -> str:
    declared = str(account.get("normal_side", "")).upper()
    if declared in {DEBIT, CREDIT}:
        return declared
    kind = str(account.get("kind", "")).upper()
    if kind in ASSET_ACCOUNTS:
        return DEBIT
    if kind in LIABILITY_ACCOUNTS:
        return CREDIT
    return DEBIT


def validate_balanced(lines: List[Dict[str, Any]]) -> Decimal:
    """Confirm debits equal credits. Returns the entry magnitude."""
    debits = Decimal("0.00")
    credits = Decimal("0.00")
    for line in lines:
        amount = _money(line.get("amount", "0"))
        if amount <= 0:
            raise UnbalancedEntry("line amounts must be positive")
        side = str(line.get("side", "")).upper()
        if side == DEBIT:
            debits += amount
        elif side == CREDIT:
            credits += amount
        else:
            raise UnbalancedEntry("line side must be DEBIT or CREDIT")

    if debits != credits:
        raise UnbalancedEntry("debits %s do not equal credits %s" % (debits, credits))
    return debits


def check_floors(lines: List[Dict[str, Any]]) -> Dict[str, Decimal]:
    """Verify each touched account stays above its floor. Returns projected balances."""
    projected: Dict[str, Decimal] = {}

    for line in lines:
        account_code = str(line.get("account_code", "")).strip()
        if not account_code:
            raise UnbalancedEntry("account_code is required on every line")

        account = load_account(account_code)
        if account is None:
            raise UnbalancedEntry("unknown account %s" % account_code)

        normal_side = _normal_side(account_code, account)
        current = running_balance(account_code, normal_side)

        amount = _money(line.get("amount", "0"))
        side = str(line.get("side", "")).upper()
        delta = amount if side == normal_side else -amount
        after = _money(current + delta)
        projected[account_code] = after

        floor = _money(account.get("balance_floor", "-999999999.00"))
        if after < floor:
            raise FloorBreach(
                "account %s would fall to %s below floor %s" % (account_code, after, floor)
            )

    return projected


def post_entry(entry_id: str, lines: List[Dict[str, Any]], magnitude: Decimal) -> bool:
    """Write all journal lines under one conditional transaction."""
    now = int(time.time())
    transact_items = []
    for index, line in enumerate(lines):
        transact_items.append(
            {
                "Put": {
                    "TableName": JOURNAL_TABLE,
                    "Item": {
                        "entry_id": {"S": entry_id},
                        "line_number": {"N": str(index)},
                        "account_code": {"S": str(line["account_code"])},
                        "side": {"S": str(line["side"]).upper()},
                        "amount": {"N": str(_money(line["amount"]))},
                        "posted": {"BOOL": True},
                        "posted_at": {"N": str(now)},
                    },
                    "ConditionExpression": (
                        "attribute_not_exists(entry_id) OR attribute_not_exists(line_number)"
                    ),
                }
            }
        )

    try:
        dynamodb_client.transact_write_items(TransactItems=transact_items)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
            logger.info("entry_already_posted entry=%s", entry_id)
            return False
        raise


def process_record(record: Dict[str, Any]) -> Optional[str]:
    """Post one journal entry. Returns the entry id, or None if it was a duplicate."""
    payload = json.loads(record.get("body") or "{}")
    entry_id = str(payload.get("entry_id", "")).strip()
    lines = payload.get("lines") or []

    if not entry_id or not lines:
        raise UnbalancedEntry("entry_id and lines are required")

    magnitude = validate_balanced(lines)
    projected = check_floors(lines)

    posted = post_entry(entry_id, lines, magnitude)
    if not posted:
        return None

    logger.info(
        "entry_posted entry=%s lines=%s magnitude=%s accounts=%s",
        entry_id, len(lines), magnitude, sorted(projected),
    )
    return entry_id


def lambda_handler(event, context):
    posted: List[str] = []
    duplicates = 0
    rejected = 0
    failures: List[Dict[str, str]] = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            entry_id = process_record(record)
            if entry_id:
                posted.append(entry_id)
            else:
                duplicates += 1
        except (UnbalancedEntry, FloorBreach) as exc:
            rejected += 1
            logger.error("entry_rejected message_id=%s reason=%s", message_id, exc)
        except json.JSONDecodeError:
            rejected += 1
            logger.error("entry_body_not_json message_id=%s", message_id)
        except ClientError as exc:
            logger.exception("entry_post_failed message_id=%s error=%s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    logger.info(
        "ledger_batch_complete posted=%s duplicates=%s rejected=%s failed=%s",
        len(posted), duplicates, rejected, len(failures),
    )
    return {
        "batchItemFailures": failures,
        "posted": len(posted),
        "duplicates": duplicates,
        "rejected": rejected,
    }
