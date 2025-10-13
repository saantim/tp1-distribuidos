from typing import Type

from shared.entity import MenuItem, Message
from worker.joiners.joiner_base import JoinerBase
from worker.types import ItemInfo, ItemName, TransactionItemByPeriod


class Joiner(JoinerBase):
    """
    Joiner for Q2: enriches TransactionItemByPeriod with MenuItem names.

    Reference data: MenuItem (small - hundreds of items)
    Primary data: TransactionItemByPeriod (aggregated transaction items by period)
    """

    def _load_reference_fn(self, reference_data: dict, entity: MenuItem) -> dict:
        """Load menu item into reference dictionary."""
        reference_data[int(entity.item_id)] = entity.item_name
        return reference_data

    def _join_entity_fn(self, reference_data: dict, entity: TransactionItemByPeriod) -> TransactionItemByPeriod:
        """Enrich transaction items with menu item names."""
        for period, items in entity.transaction_item_per_period.items():
            for item_id, item_info in items.items():
                name = reference_data.get(int(item_id), "")
                if name:
                    new = ItemInfo(item_name=ItemName(name), amount=item_info.amount, quantity=item_info.quantity)
                    entity.transaction_item_per_period[period][item_id] = new

        return entity

    def get_reference_type(self) -> Type[Message]:
        """Reference type is MenuItem."""
        return MenuItem

    def get_entity_type(self) -> Type[Message]:
        """Primary entity type is TransactionItemByPeriod."""
        return TransactionItemByPeriod
