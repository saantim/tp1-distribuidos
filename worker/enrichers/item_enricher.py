import uuid
from typing import Type

from shared.entity import MenuItem, Message
from worker.enrichers.enricher_base import EnricherBase
from worker.types import ItemInfo, ItemName, TransactionItemByPeriod


class Enricher(EnricherBase):

    def _load_entity_fn(self, loaded_entities: dict, entity: MenuItem) -> dict:
        loaded_entities[int(entity.item_id)] = entity.item_name
        return loaded_entities

    def get_enricher_type(self) -> Type[Message]:
        return MenuItem

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def _enrich_entity_fn(
        self, loaded_entities: dict, entity: TransactionItemByPeriod, session_id: uuid.UUID = None
    ) -> TransactionItemByPeriod:
        for period, items in entity.transaction_item_per_period.items():
            for item_id, item_info in items.items():
                name = loaded_entities.get(int(item_id), "")
                if name:
                    new = ItemInfo(item_name=ItemName(name), amount=item_info.amount, quantity=item_info.quantity)
                    entity.transaction_item_per_period[period][item_id] = new

        return entity
