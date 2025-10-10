from typing import Type

from shared.entity import Message, StoreName
from worker.mergers.merger_base import MergerBase
from worker.types import SemesterTPVByStore, StoreInfo


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return SemesterTPVByStore

    def merger_fn(self, merged: SemesterTPVByStore, message: SemesterTPVByStore) -> SemesterTPVByStore:
        if merged is None:
            return message

        for semester, dict_of_semester_store_tpv in message.semester_tpv_by_store.values():
            for store_id, amount in dict_of_semester_store_tpv.values():
                if not merged.semester_tpv_by_store.get(semester):
                    merged.semester_tpv_by_store[semester] = {}
                if not merged.semester_tpv_by_store[semester].get(store_id):
                    merged.semester_tpv_by_store[semester][store_id] = StoreInfo(store_name=StoreName(""), amount=0.0)
                merged.semester_tpv_by_store[semester][store_id].amount += amount
        return merged
