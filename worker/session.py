import uuid
from typing import Type, TypeVar

from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


class Session(BaseModel):
    """
    Represent the lifecycle and bookkeeping data of a worker session.

    A session is identified by a UUID and keeps track of:
    - Which workers have already sent an EOF marker.
    - Which message IDs have been processed, allowing duplicate detection.
    - An arbitrary typed storage payload, serialized as a JSON-compatible dict.

    The `storage` field is intended to hold a single logical payload at a time,
    which can be read and written using `set_storage` / `get_storage`.
    """

    session_id: uuid.UUID
    eof_collected: set[str] = set()
    msgs_received: set[str] = set()
    storage: dict = {}

    def get_storage(self, data_type: Type[T]) -> T:
        """
        Deserialize the internal storage payload into a typed Pydantic model.

        The method assumes that ``self.storage`` contains a JSON-compatible
        dictionary previously produced by :meth:`set_storage`. It uses the
        provided Pydantic model class (a subclass of ``BaseModel``) to
        validate and instantiate the typed payload.

        Args:
            data_type: Pydantic model class (subclass of ``BaseModel``) that
                will be used to validate and construct the storage object.

        Returns:
            An instance of the given ``data_type`` built from the current
            ``storage`` dictionary.

        Raises:
            pydantic.ValidationError: If the stored data is not compatible with
                the given ``data_type`` schema.
        """
        return data_type.model_validate(self.storage)

    def set_storage(self, storage: BaseModel) -> None:
        """
        Replace the current storage payload with the given Pydantic model.

        The provided model is serialized using ``model_dump(mode="json")`` so
        that the internal representation is JSON-compatible and safe to persist
        or send over the wire.

        Args:
            storage: Pydantic model instance representing the new storage
                payload for this session.
        """
        self.storage = storage.model_dump(mode="json")

    def add_eof(self, worker_id: str) -> None:
        """
        Mark that the given worker has sent its EOF for this session.

        The worker ID is added to the ``eof_collected`` set, allowing the
        caller to know which workers already signaled the end of their stream.

        Args:
            worker_id: Identifier of the worker that has produced EOF.
        """
        self.eof_collected.add(worker_id)

    def get_eof_collected(self) -> set[str]:
        """
        Return the set of workers that have already reported EOF.

        Returns:
            A set of worker IDs that have previously been registered via
            :meth:`add_eof`.
        """
        return self.eof_collected

    def add_msg_received(self, msg_id: str) -> None:
        """
        Register that a message with the given ID has been processed.

        The message ID is added to the ``msgs_received`` set so that subsequent
        calls to :meth:`is_duplicated_msg` can be used to detect duplicates.

        Args:
            msg_id: Unique identifier of the message that has just been
                processed.
        """
        self.msgs_received.add(msg_id)

    def is_duplicated_msg(self, msg_id: str) -> bool:
        """
        Check whether a message ID has already been processed in this session.

        Args:
            msg_id: Identifier of the message to check.

        Returns:
            True if the given message ID is already present in
            ``msgs_received`` (i.e. it has been processed before),
            False otherwise.
        """
        return msg_id in self.msgs_received
