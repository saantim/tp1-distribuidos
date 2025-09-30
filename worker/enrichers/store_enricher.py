import logging
from typing import Optional

from shared.entity import MenuItem, Store, StoreId, StoreName
from worker.types import ItemId, TransactionItemByPeriod, ItemName, SemesterTPVByStore


def build_enricher_fn(enricher: Optional[dict[StoreId, StoreName]], payload: bytes) -> dict[StoreId, StoreName]:
    store: Store = Store.deserialize(payload)
    logging.info(f"Processing enriched message: {store}")
    if not enricher:
        enricher = {}
    enricher[store.store_id] = store.store_name
    return enricher


def enricher_fn(enricher: dict[StoreId, StoreName], payload: bytes) -> SemesterTPVByStore:
    enriched: SemesterTPVByStore = SemesterTPVByStore.deserialize(payload)
    logging.info("enriched 1: %s", enriched)

    for semester in enriched.semester_tpv_by_store.keys():
        for store_id, store_info in enriched.semester_tpv_by_store[semester].items():
            logging.info(f"DEBUGGING --> item_id {store_id}, item_info: {store_info}")
            store_info.store_name = StoreName(enricher.get(store_id, ""))
    logging.info("enriched 2: %s", enriched)
    return enriched
