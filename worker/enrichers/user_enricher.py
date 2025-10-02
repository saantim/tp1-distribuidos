import logging
from typing import Optional

from shared.entity import User
from worker.packer import is_batch, unpack_batch
from worker.types import UserPurchasesByStore, UserPurchasesInfo


def build_enricher_fn(enricher: Optional[UserPurchasesByStore], payload: bytes) -> UserPurchasesByStore:
    """Builds lookup table from Top3 UserPurchasesByStore messages"""
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
    """Enriches the Top3 lookup with user birthdates from User messages"""

    if is_batch(payload):
        for user_entity in unpack_batch(payload, User):
            _enrich_with_user(to_enrich, user_entity)
    else:
        user = User.deserialize(payload)
        _enrich_with_user(to_enrich, user)

    return to_enrich


def _enrich_with_user(to_enrich: UserPurchasesByStore, incoming_user: User) -> None:
    """Helper to enrich with a single user's data"""
    for store_id, users_map in to_enrich.user_purchases_by_store.items():
        for user_id, user_info in users_map.items():
            if int(incoming_user.user_id) == int(user_id):
                to_enrich.user_purchases_by_store[store_id][user_id] = UserPurchasesInfo(
                    user=incoming_user.user_id,
                    birthday=str(incoming_user.birthdate),
                    purchases=user_info.purchases,
                    store_name=user_info.store_name,
                )
                logging.info(f"enriched {incoming_user.user_id} to {incoming_user.birthdate}")
