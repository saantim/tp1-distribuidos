from typing import Type

from pydantic import BaseModel

from shared.entity import Message, StoreId, StoreName, Transaction, UserId
from worker.aggregator.aggregator_base import AggregatorBase
from worker.base import Session
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class SessionData(BaseModel):
    aggregated: dict[StoreId, dict[UserId, int]] = {}
    message_count: int = 0


class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def aggregator_fn(
        self, aggregated: dict[StoreId, dict[UserId, int]], transaction: Transaction
    ) -> dict[StoreId, dict[UserId, int]]:
        if aggregated is None:
            aggregated = {}

        if not transaction.user_id:
            return aggregated

        if transaction.store_id not in aggregated:
            aggregated[transaction.store_id] = {}

        if transaction.user_id not in aggregated[transaction.store_id]:
            aggregated[transaction.store_id][transaction.user_id] = 0

        aggregated[transaction.store_id][transaction.user_id] += 1
        return aggregated

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
