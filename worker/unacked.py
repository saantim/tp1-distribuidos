import uuid
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class UnackedMessage:
    """Represents an unacknowledged RabbitMQ message in the batch commit queue."""

    delivery_tag: int
    session_id: uuid.UUID
    message_id: str


class UnackedMessageTracker:
    """
    Manages tracking of unacknowledged RabbitMQ messages for batched commits.

    Coordinates with the WAL (Write-Ahead Log) pattern to ensure:
    1. Messages are tracked before processing
    2. Session state is persisted before acking to RabbitMQ
    3. Batch acknowledgments use the correct delivery tags
    """

    def __init__(self):
        self._messages: list[UnackedMessage] = []

    def track(self, delivery_tag: int, session_id: uuid.UUID, message_id: str) -> None:
        """Track an unacked message for batched commit."""
        self._messages.append(UnackedMessage(delivery_tag, session_id, message_id))

    def remove(self, delivery_tag: int) -> None:
        """Remove a specific delivery tag from unacked list (on error)."""
        self._messages = [msg for msg in self._messages if msg.delivery_tag != delivery_tag]

    def get_max_delivery_tag(self) -> int:
        """Get the highest delivery tag from unacked messages."""
        return max(msg.delivery_tag for msg in self._messages)

    def group_by_session(self) -> dict[uuid.UUID, list[str]]:
        """Group unacked messages by session for batch save."""
        messages_by_session = defaultdict(list)
        for msg in self._messages:
            messages_by_session[msg.session_id].append(msg.message_id)
        return messages_by_session

    def size(self) -> int:
        """Get the number of unacked messages."""
        return len(self._messages)

    def is_empty(self) -> bool:
        """Check if there are any unacked messages."""
        return len(self._messages) == 0

    def clear(self) -> None:
        """Clear all tracked messages (after successful batch commit)."""
        self._messages.clear()
