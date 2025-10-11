import logging
from typing import cast, Type

from shared.entity import Message, User
from worker.enrichers.enricher_base import EnricherBase
from worker.packer import pack_entity_batch
from worker.types import UserPurchasesByStore, UserPurchasesInfo


class Enricher(EnricherBase):

    def _on_entity_upstream(self, channel, method, properties, message: User) -> None:
        self._enrich_entity_fn(message)

    def _flush_buffer(self) -> None:
        """Flush buffer to all queues"""
        packed: bytes = pack_entity_batch([self._loaded_entities])
        for output in self._output:
            output.send(packed)

    def _enrich_entity_fn(self, incoming_user: User) -> None:
        """Enriches the Top3 lookup with user birthdates from User messages"""
        for store_id, users_map in self._loaded_entities.user_purchases_by_store.items():
            for user_id, user_info in users_map.items():
                if int(incoming_user.user_id) == int(user_id):
                    self._loaded_entities.user_purchases_by_store[store_id][user_id] = UserPurchasesInfo(
                        user=incoming_user.user_id,
                        birthday=str(incoming_user.birthdate),
                        purchases=user_info.purchases,
                        store_name=user_info.store_name,
                    )
                    logging.info(f"enriched {incoming_user.user_id} to {incoming_user.birthdate}")

    def _load_entity_fn(self, incoming: UserPurchasesByStore) -> None:
        """Builds lookup table from Top3 UserPurchasesByStore messages"""
        if self._loaded_entities is None:
            self._loaded_entities = incoming
            return

        for store_id, users_map in incoming.user_purchases_by_store.items():
            target_users = cast(UserPurchasesByStore, self._loaded_entities).user_purchases_by_store.setdefault(
                store_id, {}
            )
            for user_id, info in users_map.items():
                if user_id in target_users:
                    existing = target_users[user_id]
                    target_users[user_id] = UserPurchasesInfo(
                        user=existing.user,
                        birthday=existing.birthday or info.birthday,
                        purchases=existing.purchases + info.purchases,
                        store_name=existing.store_name or info.store_name,
                    )
                else:
                    target_users[user_id] = info

    def get_enricher_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def get_entity_type(self) -> Type[Message]:
        return User
