"""
Routing key functions for message distribution.

All routing functions must have the signature:
    fn(message: Message, downstream_stage: str, downstream_workers: int, message_id_int: int) -> str

Returns the routing key as a string.
"""

import hashlib

from shared.entity import Message, Transaction


def broadcast(message: Message, downstream_stage: str, downstream_workers: int, message_id_int: int) -> str:
    """
    Broadcast routing - sends to all workers via 'common' routing key.
    Ignores downstream_stage and downstream_workers.
    """
    return "common"


def default(message: Message, downstream_stage: str, downstream_workers: int, message_id_int: int) -> str:
    """
    Default modulo routing based on message_id.
    Distributes messages evenly across downstream workers.
    """
    worker_index = message_id_int % downstream_workers
    return f"{downstream_stage}_{worker_index}"


def tx_router(message: Message, downstream_stage: str, downstream_workers: int, message_id_int: int) -> str:
    """
    Transaction-based hash routing.
    Routes based on hash of user_id and store_id to ensure transactions
    from the same user-store pair go to the same worker.
    """
    if not isinstance(message, Transaction):
        raise TypeError(f"tx_router requires Transaction message, got {type(message)}")

    key = f"{message.user_id}{message.store_id}".encode()
    digest = hashlib.sha256(key).hexdigest()
    worker_index = int(digest, 16) % downstream_workers
    return f"{downstream_stage}_{worker_index}"


def by_stage_name(message: Message, downstream_stage: str, downstream_workers: int, message_id_int: int) -> str:
    """
    Stage name routing.
    Uses downstream_stage directly as the routing key.
    Useful for routing to specific stages/components by name without indexing.

    Example: For sink, exchange="results", downstream_stage="q1" -> routing_key="q1"
    Gateway subscribes to "results" exchange with routing keys ["q1", "q2", "q3", "q4"].
    """
    return downstream_stage
