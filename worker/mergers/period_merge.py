from typing import cast, Type

from shared.entity import ItemId, ItemName, Message
from worker.mergers.merger_base import MergerBase
from worker.types import ItemInfo, TransactionItemByPeriod


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def merger_fn(self, message: TransactionItemByPeriod) -> None:
        if self._merged is None:
            self._merged = message
            return

        current = cast(TransactionItemByPeriod, self._merged)

        for period, dict_of_period_item_sold in message.transaction_item_per_period.items():
            for item_id, item_info in dict_of_period_item_sold.items():
                merged_items: dict[ItemId, ItemInfo] = current.transaction_item_per_period.get(period, {})

                item: ItemInfo = merged_items.get(item_id, ItemInfo(0, 0, ItemName("")))
                item.quantity += item_info.quantity
                item.amount += item_info.amount

                merged_items[item_id] = item
                current.transaction_item_per_period[period] = merged_items
