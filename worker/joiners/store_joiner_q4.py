from typing import Type

from shared.entity import Message, Store, StoreName
from worker.joiners.joiner_base import JoinerBase
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Joiner(JoinerBase):
    """
    Joiner for Q4: enriches UserPurchasesByStore with Store names.

    Reference data: Store (small - hundreds of stores)
    Primary data: UserPurchasesByStore (top-3 users per store after merge)
    """

    def _load_reference_fn(self, reference_data: dict, entity: Store) -> dict:
        """Load store into reference dictionary."""
        reference_data[int(entity.store_id)] = entity.store_name
        return reference_data

    def _join_entity_fn(self, reference_data: dict, entity: UserPurchasesByStore) -> UserPurchasesByStore:
        """Enrich user purchases with store names."""
        for store_id, user_info in entity.user_purchases_by_store.items():
            for user_id, user_purchase_info in user_info.items():
                store_name = reference_data.get(int(store_id), "")
                if store_name:
                    new = UserPurchasesInfo(
                        user=user_id,
                        purchases=user_purchase_info.purchases,
                        store_name=StoreName(store_name),
                        birthday=user_purchase_info.birthday,
                    )
                    entity.user_purchases_by_store[store_id][user_id] = new
        return entity

    def get_reference_type(self) -> Type[Message]:
        """Reference type is Store."""
        return Store

    def get_entity_type(self) -> Type[Message]:
        """Primary entity type is UserPurchasesByStore."""
        return UserPurchasesByStore
