import uuid
from typing import Type, TypeVar

from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


class Session(BaseModel):
    """
    Tracks state for a single session, including EOFs from workers and processed message IDs.
    """

    session_id: uuid.UUID
    eof_collected: set[str] = set()
    msgs_received: set[str] = set()
    storage: dict = {}

    def get_storage(self, data_type: Type[T]) -> T:
        """
        Deserialize the internal storage payload into a typed Pydantic model.
        """
        return data_type.model_validate(self.storage)

    def set_storage(self, storage: BaseModel) -> None:
        """
        Update the storage payload with a new Pydantic model.
        """
        self.storage = storage.model_dump(mode="json")

    def add_eof(self, worker_id: str) -> None:
        """
        Record that a worker has sent an EOF for this session.
        """
        self.eof_collected.add(worker_id)

    def get_eof_collected(self) -> set[str]:
        """
        Get the set of worker IDs that have sent EOFs.
        """
        return self.eof_collected

    def add_msg_received(self, msg_id: str) -> None:
        """
        Mark a message ID as processed to prevent duplicates.
        """
        self.msgs_received.add(msg_id)

    def is_duplicated_msg(self, msg_id: str) -> bool:
        """
        Check if a message ID has already been processed.
        """
        return msg_id in self.msgs_received
