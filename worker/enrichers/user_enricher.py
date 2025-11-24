import logging
import uuid
from typing import Type

from shared.entity import Message, User
from worker.base import Session
from worker.enrichers.enricher_base import EnricherBase, EnricherSessionData
from worker.types import UserPurchasesByStore


class Enricher(EnricherBase):

    DEFAULT_WAITING_TTL_MS = 10000

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

    def _enrich_entity_fn(self, loaded_entities: dict, entity: User) -> User:
        """
        Recibe User, verifica si está en required_users, y enriquece todos los UserPurchasesByStore.
        Modifica in-place los UserPurchasesByStore acumulados.
        """
        required_users = loaded_entities.get("required_users", set())
        user_purchases_list = loaded_entities.get("user_purchases_list", [])

        if not user_purchases_list or int(entity.user_id) not in required_users:
            return entity

        # Enriquecer todos los UserPurchasesByStore con el birthdate del User
        for user_purchases in user_purchases_list:
            for _, users_dict in user_purchases.user_purchases_by_store.items():
                if str(entity.user_id) in users_dict:
                    users_dict[str(entity.user_id)].birthday = str(entity.birthdate)

        return entity

    def _on_entity_upstream(self, message: User, session: Session) -> None:
        loaded = session.get_storage(EnricherSessionData).loaded_entities
        self._enrich_entity_fn(loaded, message)

    def _flush_buffer(self, session: Session) -> None:
        session_id = session.session_id
        loaded = session.get_storage(EnricherSessionData).loaded_entities
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

        # Use base class helper instead of manual send
        self._send_message(messages=user_purchases_list, session_id=session_id, message_id=uuid.uuid4())

        logging.info(f"action: flushed_buffer | session_id: {session_id} | count: {len(user_purchases_list)}")

    def get_enricher_type(self) -> Type[Message]:
        """Tipo de referencia que cargamos (el top-3)."""
        return UserPurchasesByStore

    def get_entity_type(self) -> Type[Message]:
        """Tipo de mensaje del main input (User)."""
        return User
