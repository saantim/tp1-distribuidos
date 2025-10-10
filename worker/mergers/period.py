from typing import Type

from shared.entity import ItemId, ItemName, Message
from worker.mergers.merger_base import MergerBase
from worker.types import ItemInfo, TransactionItemByPeriod


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def merger_fn(self, merged: TransactionItemByPeriod, message: TransactionItemByPeriod) -> TransactionItemByPeriod:
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
