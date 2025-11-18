"""
Utilities for packing and unpacking entity batches.
"""

import logging
from dataclasses import dataclass
from typing import Iterator, List, Optional, Type

from shared.entity import Message, RawMessage
from shared.protocol import Batch, Header, Packet, PacketType
from shared.utils import ByteReader, ByteWriter


@dataclass
class EntityBatch:
    """
    Generic batch container for serialized entities.
    Holds a list of serialized entity bytes for efficient transport.

    Binary format:
    - entity_count: uint32
    - For each entity:
      - entity_length: uint32
      - entity_bytes: variable length
    """

    entities: List[bytes]

    def serialize(self) -> bytes:
        """Serialize batch using ByteWriter."""
        writer = ByteWriter()
        writer.write_uint32(len(self.entities))

        for entity_bytes in self.entities:
            writer.write_uint32(len(entity_bytes))
            writer.write_bytes(entity_bytes)

        return writer.get_bytes()

    @classmethod
    def deserialize(cls, payload: bytes) -> "EntityBatch":
        """Deserialize batch using ByteReader."""
        reader = ByteReader(payload)

        entity_count = reader.read_uint32()
        entities = []

        for _ in range(entity_count):
            entity_length = reader.read_uint32()
            entity_bytes = reader.read_bytes(entity_length)
            entities.append(entity_bytes)

        return cls(entities=entities)

    def get_entities(self, entity_class: Type[Message]) -> List[Message]:
        """
        Deserialize all entities in the batch.

        Args:
            entity_class: The entity class to deserialize into

        Returns:
            List of deserialized entity objects
        """
        return [entity_class.deserialize(e) for e in self.entities]


def _deserialize_batch_packet(body: bytes) -> Optional[Batch]:
    """
    Internal helper to deserialize a Batch packet.

    Returns:
        Batch packet or None if invalid/wrong type
    """
    if len(body) < Header.SIZE:
        return None

    try:
        header_bytes = body[: Header.SIZE]
        payload_bytes = body[Header.SIZE :]

        header = Header.deserialize(header_bytes)
        packet = Packet.deserialize(header, payload_bytes)

        if isinstance(packet, Batch):
            return packet
    except Exception as e:
        logging.debug(f"Failed to deserialize as Batch packet: {e}")

    return None


def get_batch_metadata(body: bytes) -> dict:
    """
    Extract batch metadata without unpacking entities.

    Returns:
        {"eof": bool, "entity_type": EntityType, "row_count": int}
    """
    batch = _deserialize_batch_packet(body)

    if batch:
        return {"eof": batch.eof, "entity_type": batch.entity_type, "row_count": len(batch.csv_rows)}

    return {"eof": False, "entity_type": None, "row_count": 0}


def unpack_raw_batch(body: bytes) -> Iterator[str]:
    """
    Unpacks a raw CSV batch into individual CSV row strings.
    Use get_batch_metadata() to check EOF flag.

    Args:
        body: Serialized Batch packet bytes

    Yields:
        CSV row strings
    """
    batch = _deserialize_batch_packet(body)

    if not batch:
        logging.warning("Failed to unpack raw batch")
        return

    for csv_row in batch.csv_rows:
        yield csv_row


def unpack_entity_batch(body: bytes, entity_class: Type[Message]) -> Iterator[Message]:
    """
    Unpacks an EntityBatch into individual deserialized entities.

    Args:
        body: Serialized EntityBatch bytes
        entity_class: The entity class to deserialize into

    Yields:
        Deserialized Message entities
    """

    if entity_class == RawMessage:
        yield RawMessage(raw_data=body)
        return

    try:
        entity_batch = EntityBatch.deserialize(body)

        for entity_bytes in entity_batch.entities:
            yield entity_class.deserialize(entity_bytes)

    except Exception as e:
        logging.error(f"Failed to unpack entity batch: {e}")
        return


def pack_entity_batch(entities: List[Message]) -> bytes:
    """
    Packs entities into an EntityBatch.

    Args:
        entities: List of Message entities

    Returns:
        Serialized EntityBatch bytes
    """
    entity_bytes_list = [entity.serialize() for entity in entities]
    entity_batch = EntityBatch(entities=entity_bytes_list)
    return entity_batch.serialize()


def is_raw_batch(body: bytes) -> bool:
    """
    Check if body contains a raw Batch packet (CSV strings).

    Returns:
        True if body is a Batch packet, False otherwise
    """
    if len(body) < Header.SIZE:
        return False

    try:
        header = Header.deserialize(body[: Header.SIZE])
        return header.message_type == PacketType.BATCH
    except Exception as e:
        _ = e
        return False


def is_entity_batch(body: bytes) -> bool:
    """
    Check if body contains an EntityBatch.

    Returns:
        True if body is a valid EntityBatch, False otherwise
    """
    try:
        EntityBatch.deserialize(body)
        return True
    except Exception as e:
        _ = e
        return False
