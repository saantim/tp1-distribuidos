from typing import Type

from pydantic import BaseModel

from shared.entity import Message, TransactionItem
from worker.filter.filter_base import FilterBase

class SessionData(BaseModel):
    buffer: list[TransactionItem] = []
    received: int = 0
    passed: int = 0

class Filter(FilterBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItem

    def filter_fn(self, message: TransactionItem) -> bool:
        return message.created_at.date().year in [2024, 2025]

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData