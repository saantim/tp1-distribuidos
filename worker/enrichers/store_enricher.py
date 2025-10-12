from typing import Type

from shared.entity import Message, Store
from worker.enrichers.enricher_base import EnricherBase
from worker.types import SemesterTPVByStore, StoreInfo


class Enricher(EnricherBase):

    def _load_entity_fn(self, store: Store) -> None:
        if not self._loaded_entities:
            self._loaded_entities = {}
        self._loaded_entities[int(store.store_id)] = store.store_name

    def _enrich_entity_fn(self, entity: SemesterTPVByStore) -> Message:
        for semester in entity.semester_tpv_by_store.keys():
            for store_id, store_info in entity.semester_tpv_by_store[semester].items():
                name = self._loaded_entities.get(int(store_id), "")
                if name:
                    new = StoreInfo(store_name=name, amount=store_info.amount)
                    entity.semester_tpv_by_store[semester][store_id] = new
                    self._enriched += 1
        return entity

    def get_enricher_type(self) -> Type[Message]:
        return Store

    def get_entity_type(self) -> Type[Message]:
        return SemesterTPVByStore
