from shared.entity import ItemId, ItemName
from worker.types import TransactionItemByPeriod, ItemInfo


def merger_fn(merged: TransactionItemByPeriod, payload: bytes) -> TransactionItemByPeriod:
    message: TransactionItemByPeriod = TransactionItemByPeriod.deserialize(payload)

    if merged is None:
        return message

    for period, dict_of_period_item_sold in message.transaction_item_per_period.values():
        for item_info, item_sold in dict_of_period_item_sold.values():
            merged_items: dict[ItemId, ItemInfo] = merged.transaction_item_per_period.get(period, {})

            item: ItemInfo = merged_items.get(item_info, ItemInfo(0, 0, ItemName("")))
            item.quantity += item_sold.get(item_info).quantity
            item.amount += item_sold.get(item_info).amount

            merged_items[item_info] = item
            merged.transaction_item_per_period[period] = merged_items

    return merged
