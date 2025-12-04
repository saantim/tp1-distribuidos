from typing import Optional, Type

from pydantic import BaseModel

from shared.entity import ItemName, Message, TransactionItem
from worker.aggregator.aggregator_base import AggregatorBase
from worker.types import ItemInfo, Period, TransactionItemByPeriod


class SessionData(BaseModel):
    aggregated: Optional[TransactionItemByPeriod] = TransactionItemByPeriod(transaction_item_per_period={})
    message_count: int = 0

class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItem

    def aggregator_fn(
        self, aggregated: Optional[TransactionItemByPeriod], tx_item: TransactionItem
    ) -> TransactionItemByPeriod:
        period = Period(tx_item.created_at.strftime("%Y-%m"))
        item_id = tx_item.item_id

        if aggregated is None:
            aggregated = TransactionItemByPeriod(transaction_item_per_period={})
        if not aggregated.transaction_item_per_period.get(period):
            aggregated.transaction_item_per_period[period] = {}
        if not aggregated.transaction_item_per_period[period].get(item_id):
            aggregated.transaction_item_per_period[period][item_id] = ItemInfo(
                quantity=0, amount=0, item_name=ItemName("")
            )
        aggregated.transaction_item_per_period[period][item_id].quantity += tx_item.quantity
        aggregated.transaction_item_per_period[period][item_id].amount += tx_item.subtotal

        return aggregated

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData