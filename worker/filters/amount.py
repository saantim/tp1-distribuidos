from typing import Type

from shared.entity import Message, Transaction
from worker.filters.filter_base import FilterBase


class Filter(FilterBase):

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def filter_fn(self, message: Transaction) -> bool:
        return message.final_amount >= 75
