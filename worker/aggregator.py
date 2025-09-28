from dataclasses import dataclass
from typing import Any, Callable

from middleware.interface import MessageMiddleware
from shared.protocol import Header, Packet, PacketType


@dataclass
class PacketItem(Packet):
    item: str

    @classmethod
    def deserialize(cls, data: dict):
        return cls(item=data["item"])


class Aggregator:

    def __init__(
        self, from_queue: MessageMiddleware, to_queue: MessageMiddleware, aggregator_fn: Callable[[Any, Packet], Any]
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._aggregator_fn = aggregator_fn
        self._aggregated: Any = None

    def _on_message(self, message: bytes) -> None:
        header = Header.deserialize(message)
        packet = Packet.deserialize(header, message)
        if packet.get_message_type() == PacketType.EOF:
            self._to_queue.send(self._aggregated)
            self.stop()
            return
        self._aggregated = self._aggregator_fn(self._aggregated, packet)

    def start(self) -> None:
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send("EOF")
        self._to_queue.close()
