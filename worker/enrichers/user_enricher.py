from typing import Optional

from shared.entity import User
from worker.types import UserPurchasesByStore, UserPurchasesInfo


def build_enricher_fn(enricher: Optional[UserPurchasesByStore], payload: bytes) -> UserPurchasesByStore:
    incoming: UserPurchasesByStore = UserPurchasesByStore.deserialize(payload)

    if enricher is None:
        return incoming

    for store_id, users_map in incoming.user_purchases_by_store.items():
        target_users = enricher.user_purchases_by_store.setdefault(store_id, {})
        for user_id, info in users_map.items():
            if user_id in target_users:
                existing = target_users[user_id]
                target_users[user_id] = UserPurchasesInfo(
                    user=existing.user,
                    birthday=existing.birthday or info.birthday,
                    purchases=existing.purchases + info.purchases,
                    store_name=existing.store_name or info.store_name,
                )
            else:
                target_users[user_id] = info

    return enricher


def enricher_fn(to_enrich: UserPurchasesByStore, payload: bytes) -> UserPurchasesByStore:
    user: User = User.deserialize(payload)

    for store_id, users_map in to_enrich.user_purchases_by_store.items():
        for user_id, user_info in users_map.items():
            if user_id == user.user_id:
                new = UserPurchasesInfo(
                    user=user_id,
                    birthday=user.birthdate,
                    purchases=user_info.purchases,
                    store_name=user_info.store_name,
                )
                to_enrich.user_purchases_by_store[store_id][user_id] = new
    return to_enrich
