from typing import Type

from pydantic import BaseModel

from shared.entity import Message, Transaction
from worker.filter.filter_base import FilterBase

class SessionData(BaseModel):
    buffer: list[Transaction] = []
    received: int = 0
    passed: int = 0

class Filter(FilterBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def filter_fn(self, message: Transaction) -> bool:
        return 6 <= message.created_at.hour <= 23

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData