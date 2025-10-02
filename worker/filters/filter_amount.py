import logging

from shared.entity import Transaction


def filter_fn(payload: bytes) -> bool:
    try:
        transaction: Transaction = Transaction.deserialize(payload)
        return transaction.final_amount >= 75  # todo checkear que sea final_amount
    except Exception as e:
        logging.exception(e)
        return False
