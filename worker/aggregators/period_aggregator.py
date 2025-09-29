from typing import Optional

from worker.types import TransactionItem, TransactionItemByPeriod, ItemSold, ItemInfo, ItemName, Period


def aggregator_fn(aggregated: Optional[TransactionItemByPeriod], message: bytes) -> TransactionItemByPeriod:
    tx_item: TransactionItem = TransactionItem.deserialize(message)
    period = Period(tx_item.created_at.strftime("%Y-%m"))
    item_key = ItemInfo(item_id=tx_item.item_id, item_name=ItemName(""))
    if aggregated is None:
        aggregated = TransactionItemByPeriod(transaction_item_per_period={})
    if not aggregated.transaction_item_per_period.get(period):
        aggregated.transaction_item_per_period[period] = {}
    if not aggregated.transaction_item_per_period[period].get(item_key):
        aggregated.transaction_item_per_period[period][item_key] = ItemSold(quantity=0, amount=0)
    aggregated.transaction_item_per_period[period][item_key].quantity += tx_item.quantity
    aggregated.transaction_item_per_period[period][item_key].amount += tx_item.subtotal
    return aggregated
