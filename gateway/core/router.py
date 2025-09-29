"""
packet router that routes incoming packets to appropriate middleware queues
and creates entities from batch data.
"""

import logging
from typing import Dict, Type

from shared.entity import EOF, Message
from shared.middleware.interface import (
    MessageMiddleware,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from shared.protocol import BatchPacket, PacketType


#   TODO: Tener una pool de workers para separar el trabajo de serializado y ruteo (CPU) del
#       I/O bloqueante de recibir las batches. Con una simple queue para publicar las batches
#       y workers stateless con acceso a este router bastaría.
#       PRIORIDAD: tienes doble loop y es raro usar hasattr()


class PacketRouter:
    """routes packets to appropriate middleware queues and creates entities from batch data."""

    def __init__(
        self, publishers: Dict[PacketType, MessageMiddleware], entity_mappings: Dict[PacketType, Type[Message]]
    ):
        """
        create packet router with publishers mapping and entity type mappings.
        """

        self.publishers = publishers
        self.entity_mappings = entity_mappings

    def route_packet(self, packet: BatchPacket):
        """route packet to appropriate middleware queue and create entities."""
        packet_type = packet.get_message_type()
        publisher = self.publishers.get(PacketType(packet_type))
        entity_class = self.entity_mappings.get(PacketType(packet_type))

        if not publisher:
            raise ValueError(f"no publisher configured for packet type: {packet_type}")

        if not entity_class:
            raise ValueError(f"no entity mapping configured for packet type: {packet_type}")

        try:

            for row in packet.csv_rows:
                entity = entity_class.from_dict(row)
                publisher.send(entity.serialize())

            if packet.eof:
                print(f"rows: {packet.csv_rows}, is_eof: {packet.eof}")
                eof_entity = EOF()
                publisher.send(eof_entity.serialize())

            logging.debug(f"action: route_packet | result: success | type: {packet_type}")

        except MessageMiddlewareDisconnectedError as e:
            logging.error(f"action: route_packet | result: middleware_disconnected | type: {packet_type} | error: {e}")
            raise Exception(f"middleware disconnected for packet type {packet_type}")

        except MessageMiddlewareMessageError as e:
            logging.error(f"action: route_packet | result: middleware_error | type: {packet_type} | error: {e}")
            raise Exception(f"middleware error for packet type {packet_type}")

        except Exception as e:
            logging.error(f"action: route_packet | result: fail | type: {packet_type} | error: {e}")
            raise Exception(f"failed to route packet type {packet_type}: {e}")
