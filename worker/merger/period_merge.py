from typing import Optional, Type

from pydantic import BaseModel

from shared.entity import ItemId, ItemName, Message
from worker.merger.merger_base import MergerBase
from worker.merger.ops import PeriodMergeOp
from worker.session import BaseOp
from worker.session.session import Session
from worker.session.storage import SessionStorage
from worker.storage import WALFileSessionStorage
from worker.types import ItemInfo, TransactionItemByPeriod


class SessionData(BaseModel):
    merged: Optional[TransactionItemByPeriod] = TransactionItemByPeriod(transaction_item_per_period={})
    message_count: int = 0


class Merger(MergerBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def _do_merge(self, merged: TransactionItemByPeriod, message: TransactionItemByPeriod) -> TransactionItemByPeriod:
        for period, dict_of_period_item_sold in message.transaction_item_per_period.items():
            for item_id, item_info in dict_of_period_item_sold.items():
                merged_items: dict[ItemId, ItemInfo] = merged.transaction_item_per_period.get(period, {})

                item: ItemInfo = merged_items.get(item_id, ItemInfo(amount=0, quantity=0, item_name=ItemName("")))
                item.quantity += item_info.quantity
                item.amount += item_info.amount

                merged_items[item_id] = item
                merged.transaction_item_per_period[period] = merged_items

        return merged

    def merger_fn(
        self, merged: Optional[TransactionItemByPeriod], message: TransactionItemByPeriod, session: Session
    ) -> TransactionItemByPeriod:
        op = PeriodMergeOp(message=message)
        session.apply(op)
        return session.get_storage(SessionData).merged

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    def get_reducer(self):
        def reducer(storage: SessionData | None, op: BaseOp) -> SessionData:
            if storage is None:
                storage = SessionData()

            if isinstance(op, PeriodMergeOp):
                storage.merged = self._merge_logic(storage.merged, op.message)
                storage.message_count += 1

            return storage

        return reducer

    def create_session_storage(self) -> SessionStorage:
        return WALFileSessionStorage(save_dir="./sessions/saves", reducer=self.get_reducer(), op_types=[PeriodMergeOp])
