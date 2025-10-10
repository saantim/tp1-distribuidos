from typing import Type

from shared.entity import Message, Transaction
from worker.filters.filter_main import FilterBase


class Filter(FilterBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def filter_fn(self, message: Transaction) -> bool:
        return message.created_at.date().year in [2024, 2025]
