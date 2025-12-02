from typing import Any, Optional, Type

from pydantic import BaseModel

from shared.entity import ItemName, Message, TransactionItem
from worker.aggregator.aggregator_base import AggregatorBase
from worker.aggregator.ops import AggregateItemOp
from worker.session.session import Session
from worker.session.storage import SessionStorage
from worker.storage import WALFileSessionStorage
from worker.storage.ops import BaseOp
from worker.types import ItemInfo, Period, TransactionItemByPeriod


class SessionData(BaseModel):
    aggregated: Optional[TransactionItemByPeriod] = TransactionItemByPeriod(transaction_item_per_period={})
    message_count: int = 0


def period_aggregator_reducer(state: TransactionItemByPeriod | None, op: BaseOp) -> TransactionItemByPeriod:
    if not isinstance(op, AggregateItemOp):
        return state or TransactionItemByPeriod(transaction_item_per_period={})

    if state is None:
        state = TransactionItemByPeriod(transaction_item_per_period={})

    period = Period(op.period)
    if period not in state.transaction_item_per_period:
        state.transaction_item_per_period[period] = {}

    if op.item_id not in state.transaction_item_per_period[period]:
        state.transaction_item_per_period[period][op.item_id] = ItemInfo(quantity=0, amount=0, item_name=ItemName(""))

    state.transaction_item_per_period[period][op.item_id].quantity += op.quantity_delta
    state.transaction_item_per_period[period][op.item_id].amount += op.amount_delta

    return state


def _session_reducer(state: Any, op: BaseOp) -> SessionData:
    if state is None or not isinstance(state, SessionData):
        if isinstance(state, dict):
            session_data = SessionData.model_validate(state)
        else:
            session_data = SessionData()
    else:
        session_data = state

    session_data.aggregated = period_aggregator_reducer(session_data.aggregated, op)
    return session_data


class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItem

    def aggregator_fn(
        self, aggregated: Optional[TransactionItemByPeriod], tx_item: TransactionItem, session: Session
    ) -> TransactionItemByPeriod:
        period = Period(tx_item.created_at.strftime("%Y-%m"))

        op = AggregateItemOp(
            period=period, item_id=tx_item.item_id, quantity_delta=tx_item.quantity, amount_delta=tx_item.subtotal
        )

        session.apply(op)
        return session.get_storage(SessionData).aggregated

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    def get_reducer(self):
        return _session_reducer

    def create_session_storage(self) -> SessionStorage:
        return WALFileSessionStorage(save_dir="./sessions/saves", reducer=_session_reducer, op_types=[AggregateItemOp])
