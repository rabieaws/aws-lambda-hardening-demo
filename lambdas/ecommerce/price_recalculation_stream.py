"""Bundle price recalculation stream processor.

Event source: DynamoDB Stream on the product-catalog table.

When a component SKU's price changes, every bundle or kit that contains that component is
repriced. Bundle prices keep their configured discount unless doing so would breach the
margin floor, in which case the price is lifted to the floor. Results are flushed with a
batch writer.
"""

import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "product-catalog")
BUNDLE_INDEX = os.environ.get("BUNDLE_COMPONENT_INDEX", "component-sku-index")

CENTS = Decimal("0.01")
MARGIN_FLOOR_RATIO = Decimal("0.18")
MAX_BUNDLE_DISCOUNT = Decimal("0.35")
MATERIAL_PRICE_DELTA = Decimal("0.01")


def _money(raw: Any) -> Decimal:
    return Decimal(str(raw)).quantize(CENTS, rounding=ROUND_HALF_UP)


def _deserialize_number(attribute: Optional[Dict[str, Any]]) -> Optional[Decimal]:
    if not attribute:
        return None
    if "N" in attribute:
        return _money(attribute["N"])
    if "S" in attribute:
        try:
            return _money(attribute["S"])
        except ArithmeticError:
            return None
    return None


def extract_price_change(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return sku/old/new price when the record represents a material price move."""
    if record.get("eventName") not in ("INSERT", "MODIFY"):
        return None
    stream = record.get("dynamodb") or {}
    new_image = stream.get("NewImage") or {}
    old_image = stream.get("OldImage") or {}
    sku_attr = new_image.get("sku") or {}
    sku = sku_attr.get("S")
    if not sku:
        return None
    new_price = _deserialize_number(new_image.get("unit_price"))
    if new_price is None:
        return None
    old_price = _deserialize_number(old_image.get("unit_price")) or Decimal("0")
    if abs(new_price - old_price) < MATERIAL_PRICE_DELTA:
        return None
    return {"sku": sku, "old_price": old_price, "new_price": new_price}


def fetch_dependent_bundles(component_sku: str) -> List[Dict[str, Any]]:
    table = dynamodb.Table(CATALOG_TABLE)
    try:
        response = table.query(
            IndexName=BUNDLE_INDEX,
            KeyConditionExpression=Key("component_sku").eq(component_sku),
        )
    except ClientError as exc:
        logger.error("bundle lookup failed component=%s: %s", component_sku, exc)
        return []
    return list(response.get("Items") or [])


def component_cost(bundle: Dict[str, Any], price_overrides: Dict[str, Decimal]) -> Decimal:
    total = Decimal("0")
    for component in bundle.get("components") or []:
        sku = str(component.get("sku", ""))
        try:
            quantity = Decimal(str(component.get("quantity", 1)))
        except ArithmeticError:
            quantity = Decimal("1")
        unit_price = price_overrides.get(sku)
        if unit_price is None:
            unit_price = _money(component.get("unit_price", "0"))
        total += unit_price * quantity
    return total.quantize(CENTS, rounding=ROUND_HALF_UP)


def enforce_margin_floor(price: Decimal, cost_of_goods: Decimal) -> Decimal:
    """Lift a price so the gross margin never falls below the configured floor."""
    if cost_of_goods <= Decimal("0"):
        return price
    minimum = (cost_of_goods / (Decimal("1") - MARGIN_FLOOR_RATIO)).quantize(
        CENTS, rounding=ROUND_HALF_UP
    )
    return max(price, minimum)


def reprice_bundle(
    bundle: Dict[str, Any], price_overrides: Dict[str, Decimal]
) -> Optional[Dict[str, Any]]:
    bundle_sku = str(bundle.get("sku", "")).strip()
    if not bundle_sku:
        return None
    gross = component_cost(bundle, price_overrides)
    try:
        configured_discount = Decimal(str(bundle.get("bundle_discount", "0")))
    except ArithmeticError:
        configured_discount = Decimal("0")
    discount = min(max(configured_discount, Decimal("0")), MAX_BUNDLE_DISCOUNT)
    discounted = (gross * (Decimal("1") - discount)).quantize(CENTS, rounding=ROUND_HALF_UP)
    cost_of_goods = _money(bundle.get("cost_of_goods", "0"))
    final_price = enforce_margin_floor(discounted, cost_of_goods)
    previous = _money(bundle.get("unit_price", "0"))
    if abs(final_price - previous) < MATERIAL_PRICE_DELTA:
        return None
    return {
        "sku": bundle_sku,
        "unit_price": final_price,
        "previous_price": previous,
        "component_total": gross,
        "applied_discount": discount,
        "margin_floored": final_price > discounted,
        "repriced_at": int(time.time()),
    }


def flush_updates(updates: Iterable[Dict[str, Any]]) -> int:
    table = dynamodb.Table(CATALOG_TABLE)
    written = 0
    try:
        with table.batch_writer(overwrite_by_pkeys=["sku"]) as batch:
            for update in updates:
                batch.put_item(
                    Item={
                        "sku": update["sku"],
                        "unit_price": update["unit_price"],
                        "component_total": update["component_total"],
                        "applied_discount": update["applied_discount"],
                        "margin_floored": update["margin_floored"],
                        "updated_at": update["repriced_at"],
                        "record_type": "BUNDLE",
                    }
                )
                written += 1
    except ClientError as exc:
        logger.exception("batch write failed after %s items: %s", written, exc)
        raise
    return written


def lambda_handler(event, context):
    records = event.get("Records") or []
    price_overrides: Dict[str, Decimal] = {}
    changed_components: List[str] = []

    for record in records:
        change = extract_price_change(record)
        if not change:
            continue
        price_overrides[change["sku"]] = change["new_price"]
        changed_components.append(change["sku"])
        logger.info(
            "component price change sku=%s old=%s new=%s",
            change["sku"],
            change["old_price"],
            change["new_price"],
        )

    if not changed_components:
        return {"processed": len(records), "repriced": 0}

    pending: Dict[str, Dict[str, Any]] = {}
    for component_sku in changed_components:
        for bundle in fetch_dependent_bundles(component_sku):
            update = reprice_bundle(bundle, price_overrides)
            if update:
                pending[update["sku"]] = update

    if not pending:
        logger.info("no bundle prices moved for %s components", len(changed_components))
        return {"processed": len(records), "repriced": 0}

    try:
        written = flush_updates(pending.values())
    except ClientError:
        return {"processed": len(records), "repriced": 0, "status": "FAILED"}

    floored = sum(1 for update in pending.values() if update["margin_floored"])
    logger.info(
        "repriced bundles written=%s margin_floored=%s components=%s",
        written,
        floored,
        len(changed_components),
    )
    return {
        "processed": len(records),
        "repriced": written,
        "margin_floored": floored,
        "components": changed_components,
    }
