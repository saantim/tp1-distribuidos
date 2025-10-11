from typing import Type

from shared.entity import Message, UserId
from worker.aggregators.aggregator_base import AggregatorBase
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Aggregator(AggregatorBase):

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def aggregator_fn(self, message_received: UserPurchasesByStore) -> None:
        if self._aggregated is None:
            self._aggregated = UserPurchasesByStore(user_purchases_by_store={})

        self._aggregate_user_purchases_by_store(message_received)

    def _aggregate_user_purchases_by_store(self, some_instance: UserPurchasesByStore) -> None:
        for store_id, dict_of_user_purchases_info in some_instance.user_purchases_by_store.items():
            for user_id, user_purchases_info in dict_of_user_purchases_info.items():
                user_purchases_on_store: dict[UserId, UserPurchasesInfo] = self._aggregated.user_purchases_by_store.get(
                    store_id, {}
                )
                user_purchases_on_store[user_id] = user_purchases_info
                if len(user_purchases_on_store) > 3:
                    users_list = list(user_purchases_on_store.values())
                    users_list.sort(key=lambda x: x.purchases, reverse=True)
                    top_3 = users_list[:3]
                    user_purchases_on_store = {info.user: info for info in top_3}
                self._aggregated.user_purchases_by_store[store_id] = user_purchases_on_store
