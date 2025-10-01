import logging
from typing import Optional

from shared.entity import MenuItem
from worker.types import ItemInfo, ItemName, TransactionItemByPeriod


def build_enricher_fn(enricher: Optional[dict[int, str]], payload: bytes) -> dict[int, str]:
    menu_item: MenuItem = MenuItem.deserialize(payload)
    if not enricher:
        enricher = {}
    enricher[int(menu_item.item_id)] = menu_item.item_name
    return enricher


def enricher_fn(enricher: dict[int, str], payload: bytes) -> TransactionItemByPeriod:
    enriched: TransactionItemByPeriod = TransactionItemByPeriod.deserialize(payload)
    for period, items in enriched.transaction_item_per_period.items():
        for item_id, item_info in items.items():
            name = enricher.get(int(item_id), "")
            if name:
                new = ItemInfo(item_name=ItemName(name), amount=item_info.amount, quantity=item_info.quantity)
                enriched.transaction_item_per_period[period][item_id] = new
                logging.info(f"enriched from {item_id} to {name}")

    return enriched
