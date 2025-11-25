import logging
import uuid
from typing import cast, Optional, Type

from shared.entity import Message, StoreName, Transaction
from worker.aggregators.aggregator_base import AggregatorBase, SessionData
from worker.base import Session
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Aggregator(AggregatorBase):

    PRUNE_INTERVAL = 50000
    TOP_K_BUFFER = 20

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._messages_since_prune = 0

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

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        """Override to add periodic pruning."""
        super()._on_entity_upstream(message, session)

        self._messages_since_prune += 1
        if self._messages_since_prune >= self.PRUNE_INTERVAL:
            session_data = session.get_storage(SessionData)
            if session_data.aggregated is not None:
                session_data.aggregated = self._prune_to_top_k(
                    cast(UserPurchasesByStore, session_data.aggregated), self.TOP_K_BUFFER
                )
                logging.info(
                    f"[{self._stage_name}] Pruned to top-{self.TOP_K_BUFFER} per store | "
                    f"messages: {session_data.message_count} | session: {session.session_id.hex[:8]}"
                )
            self._messages_since_prune = 0

    @staticmethod
    def _prune_to_top_k(aggregated: UserPurchasesByStore, k: int) -> UserPurchasesByStore:
        """Prune each store to keep only top-K users by purchase count."""
        for store_id, users_dict in aggregated.user_purchases_by_store.items():
            if len(users_dict) <= k:
                continue  # Already small enough

            users_list = list(users_dict.values())
            users_list.sort(key=lambda x: x.purchases, reverse=True)
            users_list = users_list[:k]

            aggregated.user_purchases_by_store[store_id] = {user_info.user: user_info for user_info in users_list}

        return aggregated

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(SessionData)
        aggregated = session_data.aggregated

        if aggregated is not None:
            aggregated = self._prune_to_top_k(aggregated, self.TOP_K_BUFFER)
            self._send_message(messages=[aggregated], session_id=session.session_id, message_id=uuid.uuid4())
