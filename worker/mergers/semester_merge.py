from typing import cast, Type

from shared.entity import Message, StoreName
from worker.mergers.merger_base import MergerBase
from worker.types import SemesterTPVByStore, StoreInfo


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return SemesterTPVByStore

    def merger_fn(self, message: SemesterTPVByStore) -> None:
        if self._merged is None:
            self._merged = message
            return

        current = cast(SemesterTPVByStore, self._merged)

        for semester, dict_of_semester_store_tpv in message.semester_tpv_by_store.items():

            if not current.semester_tpv_by_store.get(semester):
                current.semester_tpv_by_store[semester] = {}

            for store_id, store_info in dict_of_semester_store_tpv.items():
                if not current.semester_tpv_by_store[semester].get(store_id):
                    current.semester_tpv_by_store[semester][store_id] = StoreInfo(store_name=StoreName(""), amount=0.0)
                current.semester_tpv_by_store[semester][store_id].amount += store_info.amount
