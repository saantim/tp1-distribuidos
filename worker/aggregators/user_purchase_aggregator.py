from typing import Optional

from shared.entity import StoreName, Transaction
from worker.types import UserPurchasesByStore, UserPurchasesInfo


def aggregator_fn(aggregated: Optional[UserPurchasesByStore], message: bytes) -> UserPurchasesByStore:
    transaction = Transaction.deserialize(message)
    if aggregated is None:
        aggregated = UserPurchasesByStore(user_purchases_by_store={})

    if aggregated.user_purchases_by_store.get(transaction.store_id) is None:
        aggregated.user_purchases_by_store[transaction.store_id] = {}
    if aggregated.user_purchases_by_store[transaction.store_id].get(transaction.id) is None:
        aggregated.user_purchases_by_store[transaction.store_id][transaction.id] = UserPurchasesInfo(
            user=transaction.user_id, birthday="", purchases=0, store_name=StoreName("")
        )

    aggregated.user_purchases_by_store[transaction.store_id][transaction.id].purchases += 1

    return aggregated
