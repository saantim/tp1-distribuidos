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

    # Check if payload is a batch
    if is_batch(payload):
        # Process batch of users
        for user_entity in unpack_batch(payload, User):
            _enrich_with_user(to_enrich, user_entity)
    else:
        # Process single user
        user = User.deserialize(payload)
        _enrich_with_user(to_enrich, user)

    return to_enrich


def _enrich_with_user(to_enrich: UserPurchasesByStore, user: User) -> None:
    """Helper to enrich with a single user's data"""
    for _, users_map in to_enrich.user_purchases_by_store.items():
        if user.user_id in users_map:
            user_info = users_map[user.user_id]
            users_map[user.user_id] = UserPurchasesInfo(
                user=user.user_id,
                birthday=user.birthdate,
                purchases=user_info.purchases,
                store_name=user_info.store_name,
            )
