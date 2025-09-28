"""
packet router that routes incoming packets to appropriate middleware queues.
"""

import logging
from typing import Dict

from shared.middleware.interface import (
    MessageMiddleware,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from shared.protocol import Packet, PacketType


class PacketRouter:
    """routes packets to appropriate middleware queues based on packet type."""

    def __init__(self, publishers: Dict[PacketType, MessageMiddleware]):
        """create packet router with publishers mapping."""
        self.publishers = publishers

    def route_packet(self, packet: Packet):
        """route packet to appropriate middleware queue."""
        # todo: shutdown middleware connection after receive EOF
        packet_type = packet.get_message_type()
        publisher = self.publishers.get(PacketType(packet_type))

        if not publisher:
            raise ValueError(f"no publisher configured for packet type: {packet_type}")

        try:
            packet_data = packet.serialize()
            publisher.send(packet_data)

            logging.debug(f"action: route_packet | result: success | type: {packet_type} | size: {len(packet_data)}")

        except MessageMiddlewareDisconnectedError as e:
            logging.error(f"action: route_packet | result: middleware_disconnected | type: {packet_type} | error: {e}")
            raise Exception(f"middleware disconnected for packet type {packet_type}")

        except MessageMiddlewareMessageError as e:
            logging.error(f"action: route_packet | result: middleware_error | type: {packet_type} | error: {e}")
            raise Exception(f"middleware error for packet type {packet_type}")

        except Exception as e:
            logging.error(f"action: route_packet | result: fail | type: {packet_type} | error: {e}")
            raise Exception(f"failed to route packet type {packet_type}: {e}")
