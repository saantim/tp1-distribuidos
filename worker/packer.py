# worker/packer.py
import logging
from typing import cast, Iterator, Type

from shared.entity import Message
from shared.protocol import BatchPacket, Header, Packet


def unpack_batch(body: bytes, entity_class: Type[Message]) -> Iterator[Message]:
    """
    Unpacks a batch into individual entities.

    Yields:
        Deserialized Message entities
    """
    if len(body) < Header.SIZE:
        logging.warning(f"Message too short: {len(body)} bytes")
        return

    header_bytes = body[: Header.SIZE]
    payload_bytes = body[Header.SIZE :]

    header = Header.deserialize(header_bytes)
    packet = Packet.deserialize(header, payload_bytes)

    if not isinstance(packet, BatchPacket):
        logging.error(f"Expected BatchPacket, got {type(packet)}")
        return

    batch_packet = cast(BatchPacket, packet)

    for row in batch_packet.csv_rows:
        yield entity_class.from_dict(row)


def pack_batch(entities: list[dict], packet_class: Type[BatchPacket]) -> bytes:
    """
    Packs entities back into a batch.

    Args:
        entities: List of entity dictionaries (from entity.to_dict())
        packet_class: The specific BatchPacket class (TransactionsBatch, etc.)

    Returns:
        Serialized batch packet
    """
    batch = packet_class(csv_rows=entities, eof=False)
    return batch.serialize()


def is_batch(body: bytes) -> bool:
    """Quick check if body contains a batch packet"""
    try:
        if len(body) < Header.SIZE:
            return False
        header = Header.deserialize(body[: Header.SIZE])
        # Use PacketType enum values, not class objects
        from shared.protocol import PacketType

        return header.message_type in [
            PacketType.STORE_BATCH,
            PacketType.USERS_BATCH,
            PacketType.TRANSACTIONS_BATCH,
            PacketType.TRANSACTION_ITEMS_BATCH,
            PacketType.MENU_ITEMS_BATCH,
        ]
    except Exception as e:
        _ = e
        return False
