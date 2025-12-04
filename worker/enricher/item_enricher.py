from typing import Type

from pydantic import BaseModel

from shared.entity import ItemId, MenuItem, Message
from worker.enricher.enricher_base import EnricherBase, EnricherSessionData
from worker.types import ItemInfo, ItemName, TransactionItemByPeriod


class ItemEnricherSessionData(EnricherSessionData):
    loaded_entities: dict[ItemId, ItemName] = {}


class Enricher(EnricherBase):

    def _load_entity_fn(self, session_data: ItemEnricherSessionData, entity: MenuItem) -> None:
        session_data.loaded_entities[entity.item_id] = entity.item_name

    def get_enricher_type(self) -> Type[Message]:
        return MenuItem

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def get_session_data_type(self) -> Type[BaseModel]:
        return ItemEnricherSessionData

    def _enrich_entity_fn(
        self, session_data: ItemEnricherSessionData, entity: TransactionItemByPeriod
    ) -> TransactionItemByPeriod:
        for period, items in entity.transaction_item_per_period.items():
            for item_id, item_info in items.items():
                name = session_data.loaded_entities.get(item_id, "")
                if name:
                    new = ItemInfo(item_name=ItemName(name), amount=item_info.amount, quantity=item_info.quantity)
                    entity.transaction_item_per_period[period][item_id] = new

        return entity
