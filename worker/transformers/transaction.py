"""
Transaction entity transformer module.

Expected CSV format:
transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at

We only extract:
- transaction_id
- store_id
- user_id
- final_amount
- created_at
"""

from datetime import datetime
from typing import Type

from shared.entity import Message, Transaction
from worker.transformers.transformer_base import TransformerBase


class Transformer(TransformerBase):
    """
    Transformer for transactions.
    """

    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def parse_fn(self, csv_row: str) -> dict:
        """
        Parse CSV row string into dictionary.

        Expected format:
        transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at

        We only care about:
        - store_id (column 1)
        - user_id (column 4)
        - final_amount (column 7)
        - created_at (column 8)
        """
        parts = csv_row.split(",")

        if len(parts) < 9:
            raise ValueError(f"Expected at least 9 fields, got {len(parts)}")

        created_at_str = parts[8].strip()
        created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S") if created_at_str else None

        user_id_str = parts[4].strip()
        user_id = int(float(user_id_str)) if user_id_str else None

        return {
            "transaction_id": parts[0],
            "store_id": int(parts[1].strip()),
            "user_id": user_id,
            "final_amount": float(parts[7].strip()),
            "created_at": created_at,
        }

    def create_fn(self, row_dict: dict) -> Transaction:
        """
        Create Transaction entity from parsed row dictionary.

        Args:
            row_dict: Dictionary with store_id, user_id, final_amount, created_at

        Returns:
            Transaction entity
        """
        return Transaction(
            id=row_dict["transaction_id"],
            store_id=row_dict["store_id"],
            user_id=row_dict["user_id"],
            final_amount=row_dict["final_amount"],
            created_at=row_dict["created_at"],
        )
