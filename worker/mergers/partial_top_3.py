from typing import cast, Type

from shared.entity import Message, StoreName, UserId
from worker.mergers.merger_base import MergerBase
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def merger_fn(self, message: UserPurchasesByStore) -> None:
        if self._merged is None:
            self._merged = message
            return

        current = cast(UserPurchasesByStore, self._merged)

        for store_id, user_info in message.user_purchases_by_store.items():
            if store_id not in current.user_purchases_by_store:
                current.user_purchases_by_store[store_id] = {}

            for user_id, user_purchase_info in user_info.items():
                if user_id not in current.user_purchases_by_store[store_id]:
                    current.user_purchases_by_store[store_id][user_id] = UserPurchasesInfo(
                        user=UserId(user_id), birthday="", purchases=0, store_name=StoreName("")
                    )
                current.user_purchases_by_store[store_id][user_id].purchases += user_purchase_info.purchases
