import logging
from typing import Optional

from worker.types import ItemId, TransactionItemByPeriod, MenuItem, ItemName


def build_enricher_fn(enricher: Optional[dict[ItemId, str]], payload: bytes) -> dict[ItemId, str]:
    menu_item: MenuItem = MenuItem.deserialize(payload)
    logging.info(f"Processing enriched message: {menu_item}")
    if not enricher:
        enricher = {}
    enricher[menu_item.item_id] = menu_item.item_name
    return enricher


def enricher_fn(payload: bytes, enricher: dict[ItemId, str]) -> TransactionItemByPeriod:
    enriched: TransactionItemByPeriod = TransactionItemByPeriod.deserialize(payload)
    logging.info("enriched: %s", enriched)
    for period in enriched.transaction_item_per_period.keys():
        for item_info in enriched.transaction_item_per_period[period].keys():
            new_key = item_info
            same_value = enriched.transaction_item_per_period[period].pop(item_info)
            new_key.item_name = ItemName(enricher.get(item_info.item_id))
            enriched.transaction_item_per_period[period][new_key] = same_value
    return enriched
