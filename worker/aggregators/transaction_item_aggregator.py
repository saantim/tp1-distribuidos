from typing import Optional

from shared.entity import TransactionItem
from worker.types import TransactionItemByPeriod


def aggregator_fn(aggregated: Optional[TransactionItemByPeriod], message):
    transaction_item = TransactionItem.deserialize(message)
    period = transaction_item.created_at.strftime("%Y-%m")
    if aggregated is None:
        return TransactionItemByPeriod(transaction_item_per_period={period: transaction_item})
    aggregated.transaction_item_per_period[period] = (
        aggregated.transaction_item_per_period.get(period, 0) + transaction_item
    )
    return aggregated
