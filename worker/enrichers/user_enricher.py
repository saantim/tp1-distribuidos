import logging
from typing import cast, Type

from shared.entity import Message, User
from worker.enrichers.enricher_base import EnricherBase
from worker.packer import pack_entity_batch
from worker.types import UserPurchasesByStore


class Enricher(EnricherBase):

    def _on_entity_upstream(self, channel, method, properties, message: User) -> None:
        self._enrich_entity_fn(message)

    def _flush_buffer(self) -> None:
        """Flush buffer to all queues"""
        self._clear_non_enriched()
        packed: bytes = pack_entity_batch(self._loaded_entities)
        for output in self._output:
            output.send(packed)
        logging.info(f"action: finished_enrich | stage: {self._stage_name} | total: {self._enriched}")
        self._enriched = 0

    def _enrich_entity_fn(self, incoming_user: User) -> None:
        """Enriches the Top3 lookup with user birthdates from User messages"""
        if int(incoming_user.user_id) not in self._required_users:
            return

        for aggregation in self._loaded_entities:
            aggregation = cast(UserPurchasesByStore, aggregation)
            for _, users_dict in aggregation.user_purchases_by_store.items():
                if str(incoming_user.user_id) in users_dict:
                    users_dict[str(incoming_user.user_id)].birthday = str(incoming_user.birthdate)
                    self._enriched += 1

    def _load_entity_fn(self, incoming: UserPurchasesByStore) -> None:
        """Builds lookup table from Top3 UserPurchasesByStore messages"""
        if self._loaded_entities is None:
            self._required_users = set()
            self._loaded_entities = []

        self._loaded_entities.append(incoming)

        for _, incoming_users_map in incoming.user_purchases_by_store.items():
            for user_id, _ in incoming_users_map.items():
                self._required_users.add(int(user_id))

    def _clear_non_enriched(self):
        """Remove users that weren't enriched with birthdate"""
        for aggregation in self._loaded_entities:
            aggregation = cast(UserPurchasesByStore, aggregation)
            for store_id, users_dict in aggregation.user_purchases_by_store.items():
                enriched_users = {
                    user_id: info for user_id, info in users_dict.items() if info.birthday and info.birthday.strip()
                }
                aggregation.user_purchases_by_store[store_id] = enriched_users

    def get_enricher_type(self) -> Type[Message]:
        return UserPurchasesByStore

    def get_entity_type(self) -> Type[Message]:
        return User
