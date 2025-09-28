from worker.types import TransactionItem


def filter_fn(message_encoded: bytes) -> bool:
    transaction_item = TransactionItem.deserialize(message_encoded)
    return transaction_item.created_at.year in [2024, 2025]
