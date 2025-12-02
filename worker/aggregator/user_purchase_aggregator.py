from typing import Any, Type

from pydantic import BaseModel

from shared.entity import Message, StoreId, StoreName, Transaction, UserId
from worker.aggregator.aggregator_base import AggregatorBase
from worker.aggregator.ops import IncrementUserPurchaseOp
from worker.session.session import Session
from worker.session.storage import SessionStorage
from worker.storage import WALFileSessionStorage
from worker.storage.ops import BaseOp
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class SessionData(BaseModel):
    aggregated: dict[StoreId, dict[UserId, int]] = {}
    message_count: int = 0


def user_purchase_aggregator_reducer(
    state: dict[StoreId, dict[UserId, int]] | None, op: BaseOp
) -> dict[StoreId, dict[UserId, int]]:
    if not isinstance(op, IncrementUserPurchaseOp):
        return state or {}

    if state is None:
        state = {}

    if op.store_id not in state:
        state[op.store_id] = {}

    if op.user_id not in state[op.store_id]:
        state[op.store_id][op.user_id] = 0

    state[op.store_id][op.user_id] += op.increment

    return state


def _session_reducer(state: Any, op: BaseOp) -> SessionData:
    if state is None or not isinstance(state, SessionData):
        if isinstance(state, dict):
            session_data = SessionData.model_validate(state)
        else:
            session_data = SessionData()
    else:
        session_data = state

    session_data.aggregated = user_purchase_aggregator_reducer(session_data.aggregated, op)
    return session_data


class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def aggregator_fn(
        self, aggregated: dict[StoreId, dict[UserId, int]], transaction: Transaction, session: Session
    ) -> dict[StoreId, dict[UserId, int]]:
        if not transaction.user_id:
            return aggregated or {}

        op = IncrementUserPurchaseOp(store_id=transaction.store_id, user_id=transaction.user_id, increment=1)

        session.apply(op)
        return session.get_storage(SessionData).aggregated

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(SessionData)
        aggregated_counts = session_data.aggregated

        if aggregated_counts:
            aggregated_obj = self._truncate_top_3_and_build(aggregated_counts)
            self._send_message(messages=[aggregated_obj], session_id=session.session_id)

    @staticmethod
    def _truncate_top_3_and_build(aggregated_counts: dict[StoreId, dict[UserId, int]]) -> UserPurchasesByStore:
        result = UserPurchasesByStore(user_purchases_by_store={})

        for store_id, user_counts in aggregated_counts.items():
            sorted_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)
            top_3 = sorted_users[:3]

            store_result = {}
            for user_id, count in top_3:
                store_result[user_id] = UserPurchasesInfo(
                    user=user_id, birthday="", purchases=count, store_name=StoreName("")
                )
            result.user_purchases_by_store[store_id] = store_result

        return result

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    def get_reducer(self):
        return _session_reducer

    def create_session_storage(self) -> SessionStorage:
        return WALFileSessionStorage(
            save_dir="./sessions/saves", reducer=_session_reducer, op_types=[IncrementUserPurchaseOp]
        )
