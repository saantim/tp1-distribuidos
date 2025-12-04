from typing import Optional, Type

from pydantic import BaseModel

from shared.entity import Message
from worker.merger.merger_base import MergerBase
from worker.types import UserPurchasesByStore

class SessionData(BaseModel):
    merged: Optional[UserPurchasesByStore] = UserPurchasesByStore(user_purchases_by_store={})
    message_count: int = 0

class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def merger_fn(self, merged: Optional[UserPurchasesByStore], message: UserPurchasesByStore) -> UserPurchasesByStore:
        if merged is None:
            return message

        for store_id, new_users in message.user_purchases_by_store.items():
            if store_id not in merged.user_purchases_by_store:
                sorted_users = sorted(new_users.items(), key=lambda x: x[1].purchases, reverse=True)[:3]
                merged.user_purchases_by_store[store_id] = dict(sorted_users)
            else:
                current_top_3 = merged.user_purchases_by_store[store_id]
                combined = {**current_top_3, **new_users}
                final_top3 = dict(sorted(combined.items(), key=lambda x: x[1].purchases, reverse=True)[:3])
                merged.user_purchases_by_store[store_id] = final_top3

        return merged

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData
