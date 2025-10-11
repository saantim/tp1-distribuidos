import hashlib
import os
from typing import Type

from shared.entity import Message, Transaction
from worker.router.router_base import RouterBase


SUFFIX_NUMBER: int = int(os.getenv("TO_SUFFIX_NUM", 5))


class Router(RouterBase):
    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def router_fn(self, tx: Transaction) -> str:
        key = f"{tx.user_id}{tx.store_id}".encode()
        digest = hashlib.sha256(key).hexdigest()

        hashed_num = int(digest, 16) % SUFFIX_NUMBER

        return f"tx_filtered_q4_{hashed_num}"
