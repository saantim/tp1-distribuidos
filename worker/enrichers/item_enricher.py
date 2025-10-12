from typing import Type

from shared.entity import MenuItem, Message
from worker.enrichers.enricher_base import EnricherBase
from worker.types import ItemInfo, ItemName, TransactionItemByPeriod


class Enricher(EnricherBase):

    def _load_entity_fn(self, message: MenuItem) -> None:
        if not self._loaded_entities:
            self._loaded_entities = {}
        self._loaded_entities[int(message.item_id)] = message.item_name

    def get_enricher_type(self) -> Type[Message]:
        return MenuItem

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def _enrich_entity_fn(self, entity: TransactionItemByPeriod) -> TransactionItemByPeriod:
        for period, items in entity.transaction_item_per_period.items():
            for item_id, item_info in items.items():
                name = self._loaded_entities.get(int(item_id), "")
                if name:
                    new = ItemInfo(item_name=ItemName(name), amount=item_info.amount, quantity=item_info.quantity)
                    entity.transaction_item_per_period[period][item_id] = new
                    self._enriched += 1

        return entity
