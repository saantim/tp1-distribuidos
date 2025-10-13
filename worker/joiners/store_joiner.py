from typing import Type

from shared.entity import Message, Store, StoreName
from worker.joiners.joiner_base import JoinerBase
from worker.types import SemesterTPVByStore, StoreInfo


class Joiner(JoinerBase):
    """
    Joiner for Q3: enriches SemesterTPVByStore with Store names.

    Reference data: Store (small - hundreds of stores)
    Primary data: SemesterTPVByStore (aggregated transactions by semester and store)
    """

    def _load_reference_fn(self, reference_data: dict, entity: Store) -> dict:
        """Load store into reference dictionary."""
        reference_data[int(entity.store_id)] = entity.store_name
        return reference_data

    def _join_entity_fn(self, reference_data: dict, entity: SemesterTPVByStore) -> SemesterTPVByStore:
        """Enrich semester TPV data with store names."""
        for semester in entity.semester_tpv_by_store.keys():
            for store_id, store_info in entity.semester_tpv_by_store[semester].items():
                name = reference_data.get(int(store_id), "")
                if name:
                    new = StoreInfo(store_name=StoreName(name), amount=store_info.amount)
                    entity.semester_tpv_by_store[semester][store_id] = new
        return entity

    def get_reference_type(self) -> Type[Message]:
        """Reference type is Store."""
        return Store

    def get_entity_type(self) -> Type[Message]:
        """Primary entity type is SemesterTPVByStore."""
        return SemesterTPVByStore
