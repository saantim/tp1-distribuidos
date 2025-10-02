import hashlib
import os

from shared.entity import Transaction


SUFFIX_NUMBER: int = int(os.getenv("TO_SUFFIX_NUM", 1))


def router_fn(payload: bytes) -> str:
    tx: Transaction = Transaction.deserialize(payload)

    key = f"{tx.user_id}{tx.store_id}".encode()
    digest = hashlib.sha256(key).hexdigest()

    hashed_num = int(digest, 16) % SUFFIX_NUMBER

    return f"tx_filtered_{hashed_num + 1}"
