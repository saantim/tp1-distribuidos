"""
Transaction Item entity transformer module.
Provides CSV parsing and entity creation for TransactionItem entities.

Expected CSV format:
transaction_id,item_id,quantity,unit_price,subtotal,created_at

We only extract:
- item_id
- quantity
- created_at
"""

from datetime import datetime

from shared.entity import TransactionItem


def parse_csv_row(csv_row: str) -> dict:
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


def create_entity(row_dict: dict) -> TransactionItem:
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
