from worker.types import Transaction


def filter_fn(message_encoded: bytes) -> bool:
    transaction: Transaction = Transaction.deserialize(message_encoded)
    return transaction.created_at.date().year in [2024, 2025]
