from datetime import datetime
from typing import Optional, Type

from pydantic import BaseModel

from shared.entity import Message, StoreName, Transaction
from worker.aggregator.aggregator_base import AggregatorBase
from worker.types import Semester, SemesterTPVByStore, StoreInfo

class SessionData(BaseModel):
    aggregated: Optional[SemesterTPVByStore] = SemesterTPVByStore(semester_tpv_by_store={})
    message_count: int = 0

class Aggregator(AggregatorBase):
    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def aggregator_fn(self, aggregated: Optional[SemesterTPVByStore], message: Transaction) -> SemesterTPVByStore:
        semester: Semester = self._get_semester(message.created_at)

        if aggregated is None:
            aggregated = SemesterTPVByStore(semester_tpv_by_store={})
        if not aggregated.semester_tpv_by_store.get(semester):
            aggregated.semester_tpv_by_store[semester] = {}
        if not aggregated.semester_tpv_by_store[semester].get(message.store_id):
            aggregated.semester_tpv_by_store[semester][message.store_id] = StoreInfo(
                store_name=StoreName(""), amount=0.0
            )
        aggregated.semester_tpv_by_store[semester][message.store_id].amount += message.final_amount

        return aggregated

    @staticmethod
    def _get_semester(dt: datetime) -> Semester:
        year = dt.year
        semester = 1 if dt.month <= 6 else 2
        return Semester(f"{year}-{semester}")

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData
