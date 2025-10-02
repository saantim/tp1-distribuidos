from typing import Optional

from shared.entity import UserId
from worker.types import UserPurchasesByStore, UserPurchasesInfo


def aggregator_fn(aggregated: Optional[UserPurchasesByStore], message: bytes) -> UserPurchasesByStore:
    message_received: UserPurchasesByStore = UserPurchasesByStore.deserialize(message)

    if aggregated is None:
        aggregated = UserPurchasesByStore(user_purchases_by_store={})

    return aggregate_user_purchases_by_store(aggregated, message_received)


def aggregate_user_purchases_by_store(
    aggregated: UserPurchasesByStore, some_instance: UserPurchasesByStore
) -> UserPurchasesByStore:
    for store_id, dict_of_user_purchases_info in some_instance.user_purchases_by_store.items():
        for user_id, user_purchases_info in dict_of_user_purchases_info.items():
            user_purchases_on_store: dict[UserId, UserPurchasesInfo] = aggregated.user_purchases_by_store.get(
                store_id, {}
            )
            user_purchases_on_store[user_id] = user_purchases_info
            if len(user_purchases_on_store) > 3:
                users_purchases_info = list(user_purchases_on_store.values())
                users_purchases_info.sort(key=lambda x: x.purchases, reverse=True)
                users_purchases_info.pop()
            aggregated.user_purchases_by_store[store_id] = user_purchases_on_store

    return aggregated
