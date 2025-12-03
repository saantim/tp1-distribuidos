import logging
from typing import Type

from pydantic import BaseModel

from shared.entity import Message, User, UserId
from worker.base import Session
from worker.enricher.enricher_base import EnricherBase, EnricherSessionData
from worker.types import UserPurchasesByStore


class UserEnricherSessionData(EnricherSessionData):
    user_purchases_list: list[UserPurchasesByStore] = []
    required_users: set[UserId] = set()


class Enricher(EnricherBase):

    def _load_entity_fn(self, session_data: UserEnricherSessionData, entity: UserPurchasesByStore) -> None:
        """
        Acumula múltiples UserPurchasesByStore y construye set de user_ids requeridos.
        Cada mensaje del aggregator trae un subset de users - los acumulamos todos.
        """
        session_data.user_purchases_list.append(entity)

        for _, users_dict in entity.user_purchases_by_store.items():
            for user_id in users_dict.keys():
                if user_id is None:
                    logging.warning(f"NONE USER ID: {entity}")
                session_data.required_users.add(user_id)

    def _enrich_entity_fn(self, session_data: UserEnricherSessionData, entity: User) -> User:
        """
        Recibe User, verifica si está en required_users, y enriquece todos los UserPurchasesByStore.
        Modifica in-place los UserPurchasesByStore acumulados.
        """
        if not session_data.user_purchases_list or entity.user_id not in session_data.required_users:
            return entity

        # Enriquecer todos los UserPurchasesByStore con el birthdate del User
        for user_purchases in session_data.user_purchases_list:
            for _, users_dict in user_purchases.user_purchases_by_store.items():
                if entity.user_id in users_dict:
                    users_dict[entity.user_id].birthday = str(entity.birthdate)

        return entity

    def _on_entity_upstream(self, message: User, session: Session) -> None:
        session_data = session.get_storage(self.get_session_data_type())
        self._enrich_entity_fn(session_data, message)

        session_data.enriched_count += 1
        if session_data.enriched_count % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] {session_data.enriched_count//1000}k enriched | "
                f"session: {session.session_id.hex[:8]}"
            )

    def _flush_buffer(self, session: Session) -> None:
        session_id = session.session_id
        session_data = session.get_storage(self.get_session_data_type())
        user_purchases_list = session_data.user_purchases_list

        if not user_purchases_list:
            return

        for user_purchases in user_purchases_list:
            for store_id, users_dict in user_purchases.user_purchases_by_store.items():
                enriched_users = {
                    user_id: info for user_id, info in users_dict.items() if info.birthday and info.birthday.strip()
                }
                user_purchases.user_purchases_by_store[store_id] = enriched_users

        self._send_message(messages=user_purchases_list, session_id=session_id)

        logging.info(f"action: flushed_buffer | session_id: {session_id} | count: {len(user_purchases_list)}")

    def get_enricher_type(self) -> Type[Message]:
        """Tipo de referencia que cargamos (el top-3)."""
        return UserPurchasesByStore

    def get_entity_type(self) -> Type[Message]:
        """Tipo de mensaje del main input (User)."""
        return User

    def get_session_data_type(self) -> Type[BaseModel]:
        return UserEnricherSessionData
