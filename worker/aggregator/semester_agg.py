from datetime import datetime
from typing import Any, Optional, Type

from pydantic import BaseModel

from shared.entity import Message, StoreName, Transaction
from worker.aggregator.aggregator_base import AggregatorBase
from worker.aggregator.ops import AggregateSemesterOp
from worker.base import Session
from worker.session.storage import SessionStorage
from worker.storage import WALFileSessionStorage
from worker.storage.ops import BaseOp
from worker.types import Semester, SemesterTPVByStore, StoreInfo


class SessionData(BaseModel):
    aggregated: Optional[SemesterTPVByStore] = SemesterTPVByStore(semester_tpv_by_store={})
    message_count: int = 0


def semester_aggregator_reducer(state: SemesterTPVByStore | None, op: BaseOp) -> SemesterTPVByStore:
    if not isinstance(op, AggregateSemesterOp):
        return state or SemesterTPVByStore(semester_tpv_by_store={})

    if state is None:
        state = SemesterTPVByStore(semester_tpv_by_store={})

    semester = Semester(op.semester)
    if semester not in state.semester_tpv_by_store:
        state.semester_tpv_by_store[semester] = {}

    if op.store_id not in state.semester_tpv_by_store[semester]:
        state.semester_tpv_by_store[semester][op.store_id] = StoreInfo(store_name=StoreName(""), amount=0.0)

    state.semester_tpv_by_store[semester][op.store_id].amount += op.amount_delta

    return state


def _session_reducer(state: Any, op: BaseOp) -> SessionData:
    if state is None or not isinstance(state, SessionData):
        if isinstance(state, dict):
            session_data = SessionData.model_validate(state)
        else:
            session_data = SessionData()
    else:
        session_data = state

    session_data.aggregated = semester_aggregator_reducer(session_data.aggregated, op)
    return session_data


class Aggregator(AggregatorBase):
    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def aggregator_fn(
        self, aggregated: Optional[SemesterTPVByStore], message: Transaction, session: Session
    ) -> SemesterTPVByStore:
        semester: Semester = self._get_semester(message.created_at)

        op = AggregateSemesterOp(semester=semester, store_id=message.store_id, amount_delta=message.final_amount)

        session.apply(op)
        return session.get_storage(SessionData).aggregated

    @staticmethod
    def _get_semester(dt: datetime) -> Semester:
        year = dt.year
        semester = 1 if dt.month <= 6 else 2
        return Semester(f"{year}-{semester}")

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    def get_reducer(self):
        return _session_reducer

    @staticmethod
    def create_session_storage() -> SessionStorage:
        return WALFileSessionStorage(
            save_dir="./sessions/saves", reducer=_session_reducer, op_types=[AggregateSemesterOp]
        )
