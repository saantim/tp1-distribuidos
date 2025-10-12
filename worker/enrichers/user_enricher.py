# import logging
# import uuid
# from typing import cast, Type, Set, List
#
# from shared.entity import Message, User
# from worker.enrichers.enricher_base import EnricherBase
# from worker.packer import pack_entity_batch
# from worker.types import UserPurchasesByStore
#
# DEFAULT_WAITING_TTL_MS = 10000
#
#
# class Enricher(EnricherBase):
#
#     def __init__(self, instances, index, stage_name, source, output, intra_exchange, enricher_input, waiting_queue):
#         super().__init__(instances, index, stage_name, source, output, intra_exchange, enricher_input, waiting_queue)
#         self._required_users_per_session: dict[uuid.UUID, Set[int]] = {}
#         self._loaded_entities_per_session: dict[uuid.UUID, List[UserPurchasesByStore]] = {}
#
#     def _on_entity_upstream(self, message: User, session_id: uuid.UUID) -> None:
#         loaded = self._loaded_entities_per_session[session_id]
#         self._loaded_entities_per_session[session_id] = self._enrich_entity_fn(loaded, message)
#
#     def _enrich_entity_fn(self, loaded_entities: list, incoming_user: User,
#                           session_id: uuid.UUID = None) -> list[UserPurchasesByStore]:
#         """Enriches the Top3 lookup with user birthdates from User messages"""
#         if int(incoming_user.user_id) not in self._required_users_per_session[session_id]:
#             return loaded_entities
#
#         for aggregation in loaded_entities:
#             aggregation = cast(UserPurchasesByStore, aggregation)
#             for _, users_dict in aggregation.user_purchases_by_store.items():
#                 if str(incoming_user.user_id) in users_dict:
#                     users_dict[str(incoming_user.user_id)].birthday = str(incoming_user.birthdate)
#
#         return loaded_entities
#
#     def _load_entity_fn(self, loaded_entities: list, incoming: UserPurchasesByStore) -> None:
#         """Builds lookup table from Top3 UserPurchasesByStore messages"""
#         self._loaded_entities_per_session.append(incoming)
#
#         for _, incoming_users_map in incoming.user_purchases_by_store.items():
#             for user_id, _ in incoming_users_map.items():
#                 self._required_users.add(int(user_id))
#
#     def _flush_buffer(self, session_id: uuid.UUID) -> None:
#         """Flush buffer to all queues"""
#         self._clear_non_enriched(session_id)
#         packed: bytes = pack_entity_batch(self._loaded_entities_per_session[session_id])
#         for output in self._output:
#             output.send(packed)
#         logging.info(f"action: finished_enrich | stage: {self._stage_name} | total: {self._enriched}")
#         self._enriched = 0
#
#     def _clear_non_enriched(self, session_id: uuid.UUID) -> None:
#         """Remove users that weren't enriched with birthdate"""
#         for aggregation in self._loaded_entities:
#             aggregation = cast(UserPurchasesByStore, aggregation)
#             for store_id, users_dict in aggregation.user_purchases_by_store.items():
#                 enriched_users = {
#                     user_id: info for user_id, info in users_dict.items() if info.birthday and info.birthday.strip()
#                 }
#                 aggregation.user_purchases_by_store[store_id] = enriched_users
#
#     def get_enricher_type(self) -> Type[Message]:
#         return UserPurchasesByStore
#
#     def get_entity_type(self) -> Type[Message]:
#         return User
