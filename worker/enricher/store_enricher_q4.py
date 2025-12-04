from typing import Type

from pydantic import BaseModel

from shared.entity import Message, Store, StoreId, StoreName
from worker.enricher.enricher_base import EnricherBase, EnricherSessionData
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class StoreEnricherQ4SessionData(EnricherSessionData):
    loaded_entities: dict[StoreId, StoreName] = {}


class Enricher(EnricherBase):

    def _enrich_entity_fn(
        self, session_data: StoreEnricherQ4SessionData, entity: UserPurchasesByStore
    ) -> UserPurchasesByStore:
        for store_id, user_info in entity.user_purchases_by_store.items():
            for user_id, user_purchase_info in user_info.items():
                store_name = session_data.loaded_entities.get(store_id, "")
                if store_name:
                    new = UserPurchasesInfo(
                        user=user_id,
                        purchases=user_purchase_info.purchases,
                        store_name=StoreName(store_name),
                        birthday=user_purchase_info.birthday,
                    )
                    entity.user_purchases_by_store[store_id][user_id] = new

        return entity

    def _load_entity_fn(self, session_data: StoreEnricherQ4SessionData, entity: Store) -> None:
        session_data.loaded_entities[entity.store_id] = entity.store_name

    def get_enricher_type(self) -> Type[Message]:
        return Store

    def get_entity_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def get_session_data_type(self) -> Type[BaseModel]:
        return StoreEnricherQ4SessionData
