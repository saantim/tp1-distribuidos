import logging
from typing import Optional

from shared.entity import MenuItem
from worker.types import ItemId, TransactionItemByPeriod, ItemName


def build_enricher_fn(enricher: Optional[dict[ItemId, str]], payload: bytes) -> dict[ItemId, str]:
    menu_item: MenuItem = MenuItem.deserialize(payload)
    logging.info(f"Processing enriched message: {menu_item}")
    if not enricher:
        enricher = {}
    enricher[menu_item.item_id] = menu_item.item_name
    return enricher


def enricher_fn(enricher: dict[ItemId, str], payload: bytes) -> TransactionItemByPeriod:
    enriched: TransactionItemByPeriod = TransactionItemByPeriod.deserialize(payload)
    logging.info("enriched 1: %s", enriched)
    for period in enriched.transaction_item_per_period.keys():
        for item_id, item_info in enriched.transaction_item_per_period[period].items():
            logging.info(f"DEBUGGING --> item_id {item_id}, item_info: {item_info}")
            item_info.item_name = ItemName(enricher.get(item_id, ""))
    logging.info("enriched 2: %s", enriched)
    return enriched
