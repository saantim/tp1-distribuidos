from typing import Optional, Type

from shared.entity import Message, StoreId, StoreName, Transaction, UserId
from worker.aggregator.aggregator_base import AggregatorBase, SessionData
from worker.base import Session
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class UserPurchasesCount(Message):
    counts: dict[StoreId, dict[UserId, int]]


class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def get_session_data_type(self) -> Type[SessionData]:
        return SessionData[UserPurchasesCount]

    def aggregator_fn(self, aggregated: Optional[UserPurchasesCount], transaction: Transaction) -> UserPurchasesCount:
        if aggregated is None:
            aggregated = UserPurchasesCount(counts={})

        if not transaction.user_id:
            return aggregated

        if aggregated.counts.get(transaction.store_id) is None:
            aggregated.counts[transaction.store_id] = {}

        current_count = aggregated.counts[transaction.store_id].get(transaction.user_id, 0)
        aggregated.counts[transaction.store_id][transaction.user_id] = current_count + 1

        return aggregated

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(SessionData[UserPurchasesCount])
        aggregated_counts = session_data.aggregated

        if aggregated_counts is not None:
            result = self._truncate_and_convert(aggregated_counts)
            self._send_message(messages=[result], session_id=session.session_id)

    @staticmethod
    def _truncate_and_convert(aggregated_counts: UserPurchasesCount) -> UserPurchasesByStore:
        result_dict = {}

        for store_id, users_counts in aggregated_counts.counts.items():
            sorted_users = sorted(users_counts.items(), key=lambda item: item[1], reverse=True)

            top_3_users = sorted_users[:3]

            store_result = {}
            for user_id, count in top_3_users:
                store_result[user_id] = UserPurchasesInfo(
                    user=user_id, birthday="", purchases=count, store_name=StoreName("")
                )

            result_dict[store_id] = store_result

        return UserPurchasesByStore(user_purchases_by_store=result_dict)
