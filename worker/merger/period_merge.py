from typing import Optional, Type

from shared.entity import ItemId, ItemName, Message
from worker.merger.merger_base import MergerBase
from worker.types import ItemInfo, TransactionItemByPeriod


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def merger_fn(
        self, merged: Optional[TransactionItemByPeriod], message: TransactionItemByPeriod
    ) -> TransactionItemByPeriod:
        if merged is None:
            return message

        for period, dict_of_period_item_sold in message.transaction_item_per_period.items():
            for item_id, item_info in dict_of_period_item_sold.items():
                merged_items: dict[ItemId, ItemInfo] = merged.transaction_item_per_period.get(period, {})

                item: ItemInfo = merged_items.get(item_id, ItemInfo(amount=0, quantity=0, item_name=ItemName("")))
                item.quantity += item_info.quantity
                item.amount += item_info.amount

                merged_items[item_id] = item
                merged.transaction_item_per_period[period] = merged_items

        return merged
