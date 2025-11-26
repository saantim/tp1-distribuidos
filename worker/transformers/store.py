"""
Store entity transformer module.

Expected CSV format:
store_id,store_name,street,postal_code,city,state,latitude,longitude

We only extract:
- store_id
- store_name
"""

from shared.entity import Store
from worker.transformers.transformer_base import TransformerBase


class Transformer(TransformerBase):
    """
    Transformer for stores.
    """

    def parse_fn(self, csv_row: str) -> dict:
        """
        Parse CSV row string into dictionary.
        """
        parts = csv_row.split(",")

        if len(parts) < 2:
            raise ValueError(f"Expected at least 2 fields, got {len(parts)}")

        return {
            "store_id": str(parts[0].strip()),
            "store_name": parts[1].strip(),
        }

    def create_fn(self, row_dict: dict) -> Store:
        """
        Create Store entity from parsed row dictionary.

        Args:
            row_dict: Dictionary with store_id and store_name

        Returns:
            Store entity
        """
        return Store(
            store_id=row_dict["store_id"],
            store_name=row_dict["store_name"],
        )
