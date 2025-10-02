from typing import Optional

from shared.entity import ItemName, TransactionItem
from worker.types import ItemInfo, Period, TransactionItemByPeriod


def aggregator_fn(aggregated: Optional[TransactionItemByPeriod], message: bytes) -> TransactionItemByPeriod:
    tx_item: TransactionItem = TransactionItem.deserialize(message)
    period = Period(tx_item.created_at.strftime("%Y-%m"))
    item_id = tx_item.item_id
    if aggregated is None:
        aggregated = TransactionItemByPeriod(transaction_item_per_period={})
    if not aggregated.transaction_item_per_period.get(period):
        aggregated.transaction_item_per_period[period] = {}
    if not aggregated.transaction_item_per_period[period].get(item_id):
        aggregated.transaction_item_per_period[period][item_id] = ItemInfo(quantity=0, amount=0, item_name=ItemName(""))
    aggregated.transaction_item_per_period[period][item_id].quantity += tx_item.quantity
    aggregated.transaction_item_per_period[period][item_id].amount += tx_item.subtotal
    return aggregated
