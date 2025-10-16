import hashlib
from typing import Type

from shared.entity import Message, Transaction
from worker.router.router_base import RouterBase


class Router(RouterBase):
    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def router_fn(self, tx: Transaction) -> str:
        key = f"{tx.user_id}{tx.store_id}".encode()
        digest = hashlib.sha256(key).hexdigest()

        hashed_index = int(digest, 16) % len(self._routing_keys)
        return self._routing_keys[hashed_index]
