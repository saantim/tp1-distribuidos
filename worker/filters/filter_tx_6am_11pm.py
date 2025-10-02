from shared.entity import Transaction


def filter_fn(message: bytes) -> bool:
    transaction: Transaction = Transaction.deserialize(message)
    return 6 <= transaction.created_at.hour <= 23
