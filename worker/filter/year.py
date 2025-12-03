import typing

from pydantic import BaseModel, Field

from shared.entity import Message, Transaction
from worker.filter.filter_base import FilterBase


class SessionData(BaseModel):
    buffer: list[Transaction] = Field(default_factory=list, exclude=True)
    received: int = 0
    passed: int = 0


class Filter(FilterBase):

    def get_entity_type(self) -> typing.Type[Message]:
        return Transaction

    def filter_fn(self, message: Transaction) -> bool:
        return message.created_at.date().year in [2024, 2025]

    def get_session_data_type(self) -> typing.Type[BaseModel]:
        return SessionData
