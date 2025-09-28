from worker.types import Transaction


def filter_fn(message: bytes) -> bool:
    transaction = Transaction.deserialize(message)
    return transaction.amount > 15
