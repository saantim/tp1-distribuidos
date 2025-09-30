from typing import Optional

from shared.entity import ItemId, ItemName, StoreName
from worker.types import TransactionItemByPeriod, ItemInfo, SemesterTPVByStore, StoreInfo


def merger_fn(merged: Optional[SemesterTPVByStore], payload: bytes) -> SemesterTPVByStore:
    message: SemesterTPVByStore = SemesterTPVByStore.deserialize(payload)

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
