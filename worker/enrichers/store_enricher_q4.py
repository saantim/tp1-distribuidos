from typing import Type

from shared.entity import Message, Store
from worker.enrichers.enricher_base import EnricherBase
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Enricher(EnricherBase):

    def _enrich_entity_fn(self, entity: UserPurchasesByStore) -> UserPurchasesByStore:
        for store_id, user_info in entity.user_purchases_by_store.items():
            for user_id, user_purchase_info in user_info.items():
                store_name = self._loaded_entities.get(int(store_id), "")
                if store_name:
                    new = UserPurchasesInfo(
                        user=user_id,
                        purchases=user_purchase_info.purchases,
                        store_name=store_name,
                        birthday=user_purchase_info.birthday,
                    )
                    entity.user_purchases_by_store[store_id][user_id] = new
                    self._enriched += 1
        return entity

    def _load_entity_fn(self, store: Store) -> None:
        if not self._loaded_entities:
            self._loaded_entities = {}
        self._loaded_entities[int(store.store_id)] = store.store_name

    def get_enricher_type(self) -> Type[Message]:
        return Store

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore
