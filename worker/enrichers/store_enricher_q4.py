import uuid
from typing import Type

from shared.entity import Message, Store, StoreName
from worker.enrichers.enricher_base import EnricherBase
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Enricher(EnricherBase):

    def _enrich_entity_fn(
        self, loaded_entities: dict, entity: UserPurchasesByStore, session_id: uuid.UUID = None
    ) -> UserPurchasesByStore:
        for store_id, user_info in entity.user_purchases_by_store.items():
            for user_id, user_purchase_info in user_info.items():
                store_name = loaded_entities.get(int(store_id), "")
                if store_name:
                    new = UserPurchasesInfo(
                        user=user_id,
                        purchases=user_purchase_info.purchases,
                        store_name=StoreName(store_name),
                        birthday=user_purchase_info.birthday,
                    )
                    entity.user_purchases_by_store[store_id][user_id] = new
        return entity

    def _load_entity_fn(self, loaded_entities: dict, entity: Store) -> dict:
        loaded_entities[int(entity.store_id)] = entity.store_name
        return loaded_entities

    def get_enricher_type(self) -> Type[Message]:
        return Store

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore
