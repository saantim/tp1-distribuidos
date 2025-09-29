from typing import Optional

from shared.entity import Transaction
from worker.types import UserPurchasesByStore


def aggregator_fn(aggregated: Optional[UserPurchasesByStore], message: bytes):
    transaction = Transaction.deserialize(message)
    if aggregated is None:
        aggregated = UserPurchasesByStore(user_purchases_by_store={})

    curr = aggregated.user_purchases_by_store.get(transaction.store_id, [])
    aggregated.user_purchases_by_store[transaction.store_id] = curr + [transaction]

    return aggregated
