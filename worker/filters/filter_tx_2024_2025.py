from worker.types import Transaction


def filter_fn(message_encoded: bytes) -> bool:
    transaction = Transaction.deserialize(message_encoded)
    return transaction.created_at.year in [2024, 2025]
