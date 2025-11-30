from typing import cast, Optional, Type

from pydantic import BaseModel

from shared.entity import Message, StoreName, Transaction
from worker.aggregator.aggregator_base import AggregatorBase
from worker.base import Session
from worker.types import UserPurchasesByStore, UserPurchasesInfo, UserPurchasesByStoreV2, Purchases


class SessionData(BaseModel):
    aggregated: Optional[UserPurchasesByStoreV2] = UserPurchasesByStoreV2(user_purchases_by_store={})
    message_count: int = 0

class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def aggregator_fn(
        self, aggregated: Optional[UserPurchasesByStore], transaction: Transaction
    ) -> UserPurchasesByStore:
        if aggregated is None:
            aggregated = UserPurchasesByStoreV2(user_purchases_by_store={})

        if not transaction.user_id:
            return aggregated

        if aggregated.user_purchases_by_store.get(transaction.store_id) is None:
            aggregated.user_purchases_by_store[transaction.store_id] = {}
        if aggregated.user_purchases_by_store[transaction.store_id].get(transaction.user_id) is None:
            aggregated.user_purchases_by_store[transaction.store_id][transaction.user_id] = Purchases(0)

        aggregated.user_purchases_by_store[transaction.store_id][transaction.user_id] += 1
        return aggregated

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(SessionData)
        aggregated = session_data.aggregated

        if aggregated is not None:
            aggregated = self._truncate_top_3(cast(UserPurchasesByStoreV2, cast(Message, aggregated))) # review
            self._send_message(messages=[aggregated], session_id=session.session_id)

    @staticmethod
    def _truncate_top_3(aggregated: UserPurchasesByStoreV2) -> UserPurchasesByStore:
        for store_id, dict_of_user_purchases in aggregated.user_purchases_by_store.items():
            users_purchases = list(dict_of_user_purchases.items())
            users_purchases.sort(key=lambda x: x[1], reverse=True)
            users_purchases = users_purchases[:3]
            aggregated.user_purchases_by_store[store_id] = dict(users_purchases)

        return UserPurchasesByStore.build_from_v2(aggregated)

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    # def create_session_storage(self) -> SessionStorage:
    #     return DeltaFileSessionStorage()