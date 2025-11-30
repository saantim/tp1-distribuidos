from typing import cast, Optional, Type

from pydantic import BaseModel

from shared.entity import Message, StoreName, Transaction
from worker.aggregator.aggregator_base import AggregatorBase
from worker.base import Session
from worker.session_storage import DeltaFileSessionStorage, SessionStorage
from worker.types import UserPurchasesByStore, UserPurchasesInfo

class SessionData(BaseModel):
    aggregated: Optional[UserPurchasesByStore] = UserPurchasesByStore(user_purchases_by_store={})
    message_count: int = 0

class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def aggregator_fn(
        self, aggregated: Optional[UserPurchasesByStore], transaction: Transaction
    ) -> UserPurchasesByStore:
        if aggregated is None:
            aggregated = UserPurchasesByStore(user_purchases_by_store={})

        if not transaction.user_id:
            return aggregated

        if aggregated.user_purchases_by_store.get(transaction.store_id) is None:
            aggregated.user_purchases_by_store[transaction.store_id] = {}
        if aggregated.user_purchases_by_store[transaction.store_id].get(transaction.user_id) is None:
            aggregated.user_purchases_by_store[transaction.store_id][transaction.user_id] = UserPurchasesInfo(
                user=transaction.user_id, birthday="", purchases=0, store_name=StoreName("")
            )

        aggregated.user_purchases_by_store[transaction.store_id][transaction.user_id].purchases += 1
        return aggregated

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(SessionData)
        aggregated = session_data.aggregated

        if aggregated is not None:
            aggregated = self._truncate_top_3(cast(UserPurchasesByStore, cast(Message, aggregated))) # review
            self._send_message(messages=[aggregated], session_id=session.session_id)

    @staticmethod
    def _truncate_top_3(aggregated: UserPurchasesByStore) -> UserPurchasesByStore:
        for store_id, dict_of_user_purchases_info in aggregated.user_purchases_by_store.items():
            users_purchases_info = list(dict_of_user_purchases_info.values())
            users_purchases_info.sort(key=lambda x: x.purchases, reverse=True)
            users_purchases_info = users_purchases_info[:3]
            aggregated.user_purchases_by_store[store_id] = {
                user_purchases_info.user: user_purchases_info for user_purchases_info in users_purchases_info
            }

        return aggregated

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    # def create_session_storage(self) -> SessionStorage:
    #     return DeltaFileSessionStorage()