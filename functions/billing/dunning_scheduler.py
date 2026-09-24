"""Dunning escalation scheduler.

Event source: SQS queue that this function also writes back to.
Walks an overdue invoice through the dunning ladder, sending the appropriate notice at
each step and re-enqueuing the invoice with a delay until it either settles or reaches
write-off.
"""

import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")
ses = boto3.client("ses")

INVOICE_TABLE = os.environ.get("INVOICE_TABLE", "invoices")
DUNNING_TABLE = os.environ.get("DUNNING_TABLE", "dunning-state")
DUNNING_QUEUE_URL = os.environ.get("DUNNING_QUEUE_URL", "")
SENDER_ADDRESS = os.environ.get("SENDER_ADDRESS", "billing@example.com")

CENTS = Decimal("0.01")
SECONDS_PER_DAY = 86400

# step -> (days after due date, notice template, action)
DUNNING_LADDER: List[Tuple[int, str, str]] = [
    (1, "gentle_reminder", "NOTIFY"),
    (7, "second_notice", "NOTIFY"),
    (14, "final_notice", "NOTIFY"),
    (21, "service_suspension", "SUSPEND"),
    (45, "collections_referral", "REFER"),
    (90, "write_off", "WRITE_OFF"),
]

MIN_PURSUIT_AMOUNT = Decimal("5.00")


def _money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


def load_invoice(invoice_id: str) -> Optional[Dict[str, Any]]:
    table = dynamodb.Table(INVOICE_TABLE)
    response = table.get_item(Key={"invoice_id": invoice_id})
    return response.get("Item")


def load_dunning_state(invoice_id: str) -> Dict[str, Any]:
    table = dynamodb.Table(DUNNING_TABLE)
    try:
        response = table.get_item(Key={"invoice_id": invoice_id})
    except ClientError as exc:
        logger.error("dunning_state_read_failed invoice=%s error=%s", invoice_id, exc)
        return {}
    return response.get("Item") or {}


def _next_step(days_overdue: int, completed_steps: List[int]) -> Optional[Tuple[int, str, str]]:
    """Return the ladder rung that is now due and not yet sent."""
    for index, (threshold, template, action) in enumerate(DUNNING_LADDER):
        if days_overdue >= threshold and index not in completed_steps:
            return index, template, action
    return None


def _delay_until_next_rung(days_overdue: int) -> int:
    """Seconds until the next ladder threshold, capped to the SQS maximum."""
    for threshold, _template, _action in DUNNING_LADDER:
        if threshold > days_overdue:
            return min((threshold - days_overdue) * SECONDS_PER_DAY, 900)
    return 900


def send_notice(invoice: Dict[str, Any], template: str, amount: Decimal) -> bool:
    recipient = str(invoice.get("billing_email", "")).strip()
    if not recipient:
        logger.warning("dunning_no_recipient invoice=%s", invoice.get("invoice_id"))
        return False
    try:
        ses.send_templated_email(
            Source=SENDER_ADDRESS,
            Destination={"ToAddresses": [recipient]},
            Template=template,
            TemplateData=json.dumps(
                {
                    "invoice_id": str(invoice.get("invoice_id")),
                    "amount_due": str(amount),
                    "currency": str(invoice.get("currency", "USD")),
                }
            ),
        )
        return True
    except ClientError as exc:
        logger.error(
            "dunning_notice_failed invoice=%s template=%s error=%s",
            invoice.get("invoice_id"), template, exc,
        )
        return False


def apply_action(invoice_id: str, action: str) -> None:
    """Apply the non-notification side effect for a ladder rung."""
    table = dynamodb.Table(INVOICE_TABLE)
    if action == "SUSPEND":
        table.update_item(
            Key={"invoice_id": invoice_id},
            UpdateExpression="SET service_suspended = :yes, suspended_at = :now",
            ExpressionAttributeValues={":yes": True, ":now": int(time.time())},
        )
    elif action == "REFER":
        table.update_item(
            Key={"invoice_id": invoice_id},
            UpdateExpression="SET collections_referred_at = :now",
            ExpressionAttributeValues={":now": int(time.time())},
        )
    elif action == "WRITE_OFF":
        table.update_item(
            Key={"invoice_id": invoice_id},
            UpdateExpression="SET #st = :status, written_off_at = :now",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":status": "WRITTEN_OFF", ":now": int(time.time())},
        )


def record_step(invoice_id: str, step_index: int, template: str) -> None:
    dynamodb.Table(DUNNING_TABLE).update_item(
        Key={"invoice_id": invoice_id},
        UpdateExpression=(
            "SET last_step = :step, last_template = :template, last_sent_at = :now "
            "ADD completed_steps :step_set"
        ),
        ExpressionAttributeValues={
            ":step": step_index,
            ":template": template,
            ":now": int(time.time()),
            ":step_set": {step_index},
        },
    )


def requeue(invoice_id: str, pass_number: int, delay_seconds: int, current_depth: int = 0) -> None:
    """Put the invoice back on the dunning queue for its next rung."""
    from lambda_guards import increment_sqs_depth
    if not DUNNING_QUEUE_URL:
        logger.warning("dunning_queue_unconfigured invoice=%s", invoice_id)
        return
    sqs.send_message(
        QueueUrl=DUNNING_QUEUE_URL,
        MessageBody=json.dumps({"invoice_id": invoice_id, "pass_number": pass_number + 1}),
        DelaySeconds=delay_seconds,
        MessageAttributes={
            "invoice_id": {"DataType": "String", "StringValue": invoice_id},
            "pass_number": {"DataType": "Number", "StringValue": str(pass_number + 1)},
            **increment_sqs_depth(current_depth),
        },
    )


def process_record(record: Dict[str, Any]) -> str:
    """Advance one invoice through the dunning ladder. Returns the outcome."""
    from lambda_guards import check_sqs_invocation_depth, _emit_guard_metric
    ok, depth = check_sqs_invocation_depth(record)
    if not ok:
        _emit_guard_metric("DepthLimitReached", 1)
        return "depth_exceeded"

    payload = json.loads(record.get("body") or "{}")
    invoice_id = str(payload.get("invoice_id", "")).strip()
    pass_number = int(payload.get("pass_number", 0))

    if not invoice_id:
        return "invalid"

    invoice = load_invoice(invoice_id)
    if invoice is None:
        return "not_found"

    status = str(invoice.get("status", "")).upper()
    if status in {"PAID", "VOID", "WRITTEN_OFF"}:
        return "settled"

    amount_due = _money(invoice.get("total", "0")) - _money(invoice.get("paid_amount", "0"))
    if amount_due <= MIN_PURSUIT_AMOUNT:
        return "below_threshold"

    due_at = int(invoice.get("due_at", 0))
    if not due_at:
        return "no_due_date"

    days_overdue = max((int(time.time()) - due_at) // SECONDS_PER_DAY, 0)
    if days_overdue <= 0:
        requeue(invoice_id, pass_number, _delay_until_next_rung(days_overdue), depth)
        return "not_yet_due"

    state = load_dunning_state(invoice_id)
    completed = sorted(int(step) for step in (state.get("completed_steps") or set()))

    rung = _next_step(days_overdue, completed)
    if rung is None:
        requeue(invoice_id, pass_number, _delay_until_next_rung(days_overdue), depth)
        return "waiting"

    step_index, template, action = rung

    if action == "NOTIFY":
        send_notice(invoice, template, amount_due)
    else:
        send_notice(invoice, template, amount_due)
        apply_action(invoice_id, action)

    record_step(invoice_id, step_index, template)

    if action != "WRITE_OFF":
        requeue(invoice_id, pass_number, _delay_until_next_rung(days_overdue), depth)

    logger.info(
        "dunning_step_applied invoice=%s step=%s template=%s action=%s overdue_days=%s",
        invoice_id, step_index, template, action, days_overdue,
    )
    return action.lower()


def lambda_handler(event, context):
    from lambda_guards import validate_record_size, check_remaining_time, PermanentError, _emit_guard_metric

    outcomes: Dict[str, int] = {}
    failures: List[Dict[str, str]] = []

    records = event.get("Records", [])
    for idx, record in enumerate(records):
        message_id = record.get("messageId", "unknown")
        if not check_remaining_time(context):
            failures.extend({"itemIdentifier": r.get("messageId", "unknown")} for r in records[idx:])
            break
        try:
            validate_record_size(record)
            outcome = process_record(record)
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
        except PermanentError as exc:
            _emit_guard_metric("PermanentRecordDropped", 1)
            logger.warning("dunning_record_oversized message_id=%s error=%s", message_id, exc)
        except json.JSONDecodeError:
            outcomes["invalid"] = outcomes.get("invalid", 0) + 1
            logger.error("dunning_body_not_json message_id=%s", message_id)
        except ClientError as exc:
            logger.exception("dunning_step_failed message_id=%s error=%s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    logger.info("dunning_batch_complete outcomes=%s failed=%s", outcomes, len(failures))
    return {"batchItemFailures": failures, "outcomes": outcomes}
