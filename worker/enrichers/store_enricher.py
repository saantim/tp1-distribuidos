from typing import Type

from shared.entity import Message, Store, StoreName
from worker.enrichers.enricher_base import EnricherBase
from worker.types import SemesterTPVByStore, StoreInfo


class Enricher(EnricherBase):

    def _load_entity_fn(self, loaded_entities: dict, entity: Store) -> dict:
        loaded_entities[int(entity.store_id)] = entity.store_name
        return loaded_entities

    def _enrich_entity_fn(self, loaded_entities: dict, entity: SemesterTPVByStore) -> SemesterTPVByStore:
        for semester in entity.semester_tpv_by_store.keys():
            for store_id, store_info in entity.semester_tpv_by_store[semester].items():
                name = loaded_entities.get(int(store_id), "")
                if name:
                    new = StoreInfo(store_name=StoreName(name), amount=store_info.amount)
                    entity.semester_tpv_by_store[semester][store_id] = new
        return entity

    def get_enricher_type(self) -> Type[Message]:
        return Store

    def get_entity_type(self) -> Type[Message]:
        return SemesterTPVByStore
