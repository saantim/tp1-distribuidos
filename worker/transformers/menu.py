"""
Menu Item entity transformer module.

Expected CSV format:
item_id,item_name,category,price,is_seasonal,available_from,available_to

We only extract:
- item_id
- item_name
"""

from typing import Type

from shared.entity import MenuItem, Message
from worker.transformers.transformer_base import TransformerBase


class Transformer(TransformerBase):
    """
    Transformer for menu items.
    """

    def get_entity_type(self) -> Type[Message]:
        return MenuItem

    def parse_fn(self, csv_row: str) -> dict:
        """
        Parse CSV row string into dictionary.
        """
        parts = csv_row.split(",")

        if len(parts) < 2:
            raise ValueError(f"Expected at least 2 fields, got {len(parts)}")

        return {
            "item_id": int(parts[0].strip()),
            "item_name": parts[1].strip(),
        }

    def create_fn(self, row_dict: dict) -> MenuItem:
        """
        Create MenuItem entity from parsed row dictionary.

        Args:
            row_dict: Dictionary with item_id and item_name

        Returns:
            MenuItem entity
        """
        return MenuItem(
            item_id=row_dict["item_id"],
            item_name=row_dict["item_name"],
        )
