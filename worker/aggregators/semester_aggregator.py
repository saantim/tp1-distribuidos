from datetime import datetime
from typing import Optional

from shared.entity import StoreName, Transaction
from worker.types import Semester, SemesterTPVByStore, StoreInfo


def get_semester(dt: datetime) -> Semester:
    year = dt.year
    semester = 1 if dt.month <= 6 else 2
    return Semester(f"{year}-{semester}")


def aggregator_fn(aggregated: Optional[SemesterTPVByStore], message: bytes) -> SemesterTPVByStore:
    tx: Transaction = Transaction.deserialize(message)
    semester: Semester = get_semester(tx.created_at)

    if aggregated is None:
        aggregated = SemesterTPVByStore(semester_tpv_by_store={})
    if not aggregated.semester_tpv_by_store.get(semester):
        aggregated.semester_tpv_by_store[semester] = {}
    if not aggregated.semester_tpv_by_store[semester].get(tx.store_id):
        aggregated.semester_tpv_by_store[semester][tx.store_id] = StoreInfo(store_name=StoreName(""), amount=0.0)
    aggregated.semester_tpv_by_store[semester][tx.store_id].amount += tx.final_amount
    return aggregated
