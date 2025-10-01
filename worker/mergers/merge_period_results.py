from shared.entity import ItemName
from worker.types import ItemInfo, TransactionItemByPeriod


def merger_fn(merged: TransactionItemByPeriod, payload: bytes) -> TransactionItemByPeriod:
    message: TransactionItemByPeriod = TransactionItemByPeriod.deserialize(payload)

    if merged is None:
        return message

    for period, dict_of_period_item_sold in message.transaction_item_per_period.items():
        if period not in merged.transaction_item_per_period:
            merged.transaction_item_per_period[period] = {}

        for item_id, item_info in dict_of_period_item_sold.items():
            if item_id not in merged.transaction_item_per_period[period]:
                merged.transaction_item_per_period[period][item_id] = ItemInfo(
                    quantity=0, amount=0.0, item_name=ItemName("")
                )

            merged.transaction_item_per_period[period][item_id].quantity += item_info.quantity
            merged.transaction_item_per_period[period][item_id].amount += item_info.amount

    return merged
