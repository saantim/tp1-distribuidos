import logging
import uuid
from typing import Type

from shared.entity import Message, User
from shared.protocol import SESSION_ID
from worker.enrichers.enricher_base import EnricherBase
from worker.packer import pack_entity_batch
from worker.types import UserPurchasesByStore


class Enricher(EnricherBase):

    DEFAULT_WAITING_TTL_MS = 10000

    def __init__(self, instances, index, stage_name, source, output, intra_exchange, enricher_input):
        super().__init__(instances, index, stage_name, source, output, intra_exchange, enricher_input)
        self._required_users_per_session: dict[uuid.UUID, set[int]] = {}

    def _load_entity_fn(self, loaded_entities: dict, entity: UserPurchasesByStore) -> dict:
        """
        Acumula múltiples UserPurchasesByStore y construye set de user_ids requeridos.
        Cada mensaje del aggregator trae un subset de users - los acumulamos todos.
        """
        if "user_purchases_list" not in loaded_entities:
            loaded_entities["user_purchases_list"] = []
            loaded_entities["required_users"] = set()

        loaded_entities["user_purchases_list"].append(entity)

        required_users = loaded_entities["required_users"]
        for _, users_dict in entity.user_purchases_by_store.items():
            for user_id in users_dict.keys():
                required_users.add(int(user_id))

        return loaded_entities

    def _enrich_entity_fn(self, loaded_entities: dict, entity: User) -> list[UserPurchasesByStore]:
        """
        Recibe User, verifica si está en required_users, y enriquece todos los UserPurchasesByStore.
        Retorna la lista completa (referencia, se modifica in-place).
        """
        required_users = loaded_entities.get("required_users", set())
        user_purchases_list = loaded_entities.get("user_purchases_list", [])

        if not user_purchases_list or int(entity.user_id) not in required_users:
            return user_purchases_list

        # Enriquecer todos los UserPurchasesByStore con el birthdate del User
        for user_purchases in user_purchases_list:
            for _, users_dict in user_purchases.user_purchases_by_store.items():
                if str(entity.user_id) in users_dict:
                    users_dict[str(entity.user_id)].birthday = str(entity.birthdate)

        return user_purchases_list

    def _on_entity_upstream(self, message: User, session_id: uuid.UUID) -> None:
        loaded = self._loaded_entities_per_session[session_id]
        self._enrich_entity_fn(loaded, message)

    def _flush_buffer(self, session_id: uuid.UUID) -> None:
        loaded = self._loaded_entities_per_session.get(session_id, {})
        user_purchases_list = loaded.get("user_purchases_list", [])

        if not user_purchases_list:
            return

        # Limpiar usuarios no enriquecidos (sin birthdate) en cada UserPurchasesByStore
        for user_purchases in user_purchases_list:
            for store_id, users_dict in user_purchases.user_purchases_by_store.items():
                enriched_users = {
                    user_id: info for user_id, info in users_dict.items() if info.birthday and info.birthday.strip()
                }
                user_purchases.user_purchases_by_store[store_id] = enriched_users

        packed = pack_entity_batch(user_purchases_list)
        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id.hex})

        logging.info(f"action: flushed_buffer | session_id: {session_id} | size: {len(packed)}")

    def get_enricher_type(self) -> Type[Message]:
        """Tipo de referencia que cargamos (el top-3)."""
        return UserPurchasesByStore

    def get_entity_type(self) -> Type[Message]:
        """Tipo de mensaje del main input (User)."""
        return User
