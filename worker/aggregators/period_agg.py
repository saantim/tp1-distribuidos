from typing import Type

from shared.entity import ItemName, Message, TransactionItem
from worker.aggregators.aggregator_base import AggregatorBase
from worker.types import ItemInfo, Period, TransactionItemByPeriod


class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItem

    def aggregator_fn(self, tx_item: TransactionItem) -> None:
        period = Period(tx_item.created_at.strftime("%Y-%m"))
        item_id = tx_item.item_id
        if self._aggregated is None:
            self._aggregated = TransactionItemByPeriod(transaction_item_per_period={})
        if not self._aggregated.transaction_item_per_period.get(period):
            self._aggregated.transaction_item_per_period[period] = {}
        if not self._aggregated.transaction_item_per_period[period].get(item_id):
            self._aggregated.transaction_item_per_period[period][item_id] = ItemInfo(
                quantity=0, amount=0, item_name=ItemName("")
            )
        self._aggregated.transaction_item_per_period[period][item_id].quantity += tx_item.quantity
        self._aggregated.transaction_item_per_period[period][item_id].amount += tx_item.subtotal
