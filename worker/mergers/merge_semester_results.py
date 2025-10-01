from typing import Optional

from shared.entity import StoreName
from worker.types import SemesterTPVByStore, StoreInfo


def merger_fn(merged: Optional[SemesterTPVByStore], payload: bytes) -> SemesterTPVByStore:
    message: SemesterTPVByStore = SemesterTPVByStore.deserialize(payload)

    if merged is None:
        return message

    for semester, dict_of_semester_store_tpv in message.semester_tpv_by_store.items():
        if semester not in merged.semester_tpv_by_store:
            merged.semester_tpv_by_store[semester] = {}

        for store_id, store_info in dict_of_semester_store_tpv.items():
            if store_id not in merged.semester_tpv_by_store[semester]:
                merged.semester_tpv_by_store[semester][store_id] = StoreInfo(store_name=StoreName(""), amount=0.0)

            merged.semester_tpv_by_store[semester][store_id].amount += store_info.amount

    return merged
