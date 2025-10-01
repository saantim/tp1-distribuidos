import logging
from typing import Optional

from shared.entity import Store, StoreName
from worker.types import SemesterTPVByStore, StoreInfo


def build_enricher_fn(enricher: Optional[dict[int, StoreName]], payload: bytes) -> dict[int, StoreName]:
    store: Store = Store.deserialize(payload)
    if not enricher:
        enricher = {}
    enricher[int(store.store_id)] = store.store_name
    return enricher


def enricher_fn(enricher: dict[int, StoreName], payload: bytes) -> SemesterTPVByStore:
    enriched: SemesterTPVByStore = SemesterTPVByStore.deserialize(payload)
    for semester in enriched.semester_tpv_by_store.keys():
        for store_id, store_info in enriched.semester_tpv_by_store[semester].items():
            name = enricher.get(int(store_id), "")
            if name:
                new = StoreInfo(store_name=name, amount=store_info.amount)
                enriched.semester_tpv_by_store[semester][store_id] = new
                logging.info(f"enriched from {store_id} to {name}")
    return enriched
