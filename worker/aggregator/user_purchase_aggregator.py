from typing import Optional

from worker.types import Transaction, UserPurchasesByStore


def aggregator_fn(aggregated: Optional[UserPurchasesByStore], message):
    transaction = Transaction.deserialize(message)
    if aggregated is None:
        aggregated = UserPurchasesByStore(user_purchases_by_store=dict())

    curr = aggregated.user_purchases_by_store.get(transaction.store_id, [])
    aggregated.user_purchases_by_store[transaction.store_id] = curr + [transaction]

    return aggregated
