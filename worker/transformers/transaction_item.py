"""
Transaction Item entity transformer module.

Expected CSV format:
transaction_id,item_id,quantity,unit_price,subtotal,created_at

We only extract:
- item_id
- quantity
- created_at
"""

from datetime import datetime
from typing import Type

from shared.entity import Message, TransactionItem
from worker.transformers.transformer_base import TransformerBase


class Transformer(TransformerBase):
    """
    Transformer for transaction items.
    """

    def get_entity_type(self) -> Type[Message]:
        return TransactionItem

    def parse_csv_row(self, csv_row: str) -> dict:
        """
        Parse CSV row string into dictionary.

        Expected format:
        transaction_id,item_id,quantity,unit_price,subtotal,created_at

        We only care about:
        - item_id (column 1)
        - quantity (column 2)
        - created_at (column 5)
        """
        parts = csv_row.split(",")

        if len(parts) < 6:
            raise ValueError(f"Expected at least 6 fields, got {len(parts)}")

        created_at_str = parts[5].strip()
        created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S") if created_at_str else None

        return {
            "item_id": int(parts[1].strip()),
            "quantity": int(parts[2].strip()),
            "created_at": created_at,
        }

    def create_entity(self, row_dict: dict) -> TransactionItem:
        """
        Create TransactionItem entity from parsed row dictionary.

        Args:
            row_dict: Dictionary with item_id, quantity, created_at

        Returns:
            TransactionItem entity
        """
        return TransactionItem(
            item_id=row_dict["item_id"],
            quantity=row_dict["quantity"],
            created_at=row_dict["created_at"],
        )
