from typing import Optional

from worker.types import UserPurchasesByStore


def build_enricher_fn(enricher: Optional[UserPurchasesByStore], payload: bytes) -> UserPurchasesByStore:
    user_purchases: UserPurchasesByStore = UserPurchasesByStore.deserialize(payload)
    if not enricher:
        return user_purchases
    for store_id, purchases in user_purchases.user_purchases_by_store.values():
        for _ in purchases:
            enricher.user_purchases_by_store[store_id] = enricher.user_purchases_by_store.get(store_id, {})
            # enricher.user_purchases_by_store[store_id][purchase.user].

        pass
    return enricher
