import logging
from typing import Optional

from shared.entity import Store, StoreName
from worker.types import UserPurchasesByStore, UserPurchasesInfo


def build_enricher_fn(enricher: Optional[dict[int, StoreName]], payload: bytes) -> dict[int, StoreName]:
    store: Store = Store.deserialize(payload)
    if not enricher:
        enricher = {}
    enricher[int(store.store_id)] = store.store_name
    return enricher


def enricher_fn(enricher: dict[int, StoreName], payload: bytes) -> UserPurchasesByStore:
    enriched: UserPurchasesByStore = UserPurchasesByStore.deserialize(payload)

    for store_id, user_info in enriched.user_purchases_by_store.items():
        for user_id, user_purchase_info in user_info.items():
            store_name = enricher.get(int(store_id), "")
            if store_name:
                new = UserPurchasesInfo(
                    user=user_id, purchases=user_purchase_info.purchases, store_name=store_name, birthday=""
                )
                enriched.user_purchases_by_store[store_id][user_id] = new
                logging.info(f"enriched from {store_id} to {store_name}")
    return enriched
