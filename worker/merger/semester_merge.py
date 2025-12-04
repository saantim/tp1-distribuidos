from typing import Optional, Type

from pydantic import BaseModel

from shared.entity import Message, StoreName
from worker.merger.merger_base import MergerBase
from worker.types import SemesterTPVByStore, StoreInfo

class SessionData(BaseModel):
    merged: Optional[SemesterTPVByStore] = SemesterTPVByStore(semester_tpv_by_store={})
    message_count: int = 0

class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return SemesterTPVByStore

    def merger_fn(self, merged: Optional[SemesterTPVByStore], message: SemesterTPVByStore) -> SemesterTPVByStore:
        if merged is None:
            return message

        for semester, dict_of_semester_store_tpv in message.semester_tpv_by_store.items():

            if not merged.semester_tpv_by_store.get(semester):
                merged.semester_tpv_by_store[semester] = {}

            for store_id, store_info in dict_of_semester_store_tpv.items():
                if not merged.semester_tpv_by_store[semester].get(store_id):
                    merged.semester_tpv_by_store[semester][store_id] = StoreInfo(store_name=StoreName(""), amount=0.0)
                merged.semester_tpv_by_store[semester][store_id].amount += store_info.amount

        return merged

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData
