from typing import Type

from shared.entity import Message, StoreName, UserId
from worker.mergers.merger_base import MergerBase
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def merger_fn(self, merged: UserPurchasesByStore, message: UserPurchasesByStore) -> UserPurchasesByStore:

        if merged is None:
            return message

        for store_id, user_info in message.user_purchases_by_store.items():
            if store_id not in merged.user_purchases_by_store:
                merged.user_purchases_by_store[store_id] = {}

            for user_id, user_purchase_info in user_info.items():
                if user_id not in merged.user_purchases_by_store[store_id]:
                    merged.user_purchases_by_store[store_id][user_id] = UserPurchasesInfo(
                        user=UserId(user_id), birthday="", purchases=0, store_name=StoreName("")
                    )
                merged.user_purchases_by_store[store_id][user_id].purchases += user_purchase_info.purchases

        return merged
