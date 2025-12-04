from typing import Type

from pydantic import BaseModel

from shared.entity import Message, Store, StoreId, StoreName
from worker.enricher.enricher_base import EnricherBase, EnricherSessionData
from worker.types import SemesterTPVByStore, StoreInfo


class StoreEnricherSessionData(EnricherSessionData):
    loaded_entities: dict[StoreId, StoreName] = {}


class Enricher(EnricherBase):

    def _load_entity_fn(self, session_data: StoreEnricherSessionData, entity: Store) -> None:
        session_data.loaded_entities[entity.store_id] = entity.store_name

    def _enrich_entity_fn(
        self, session_data: StoreEnricherSessionData, entity: SemesterTPVByStore
    ) -> SemesterTPVByStore:
        for semester in entity.semester_tpv_by_store.keys():
            for store_id, store_info in entity.semester_tpv_by_store[semester].items():
                name = session_data.loaded_entities.get(store_id, "")
                if name:
                    new = StoreInfo(store_name=StoreName(name), amount=store_info.amount)
                    entity.semester_tpv_by_store[semester][store_id] = new
        return entity

    def get_enricher_type(self) -> Type[Message]:
        return Store

    def get_entity_type(self) -> Type[Message]:
        return SemesterTPVByStore

    def get_session_data_type(self) -> Type[BaseModel]:
        return StoreEnricherSessionData
