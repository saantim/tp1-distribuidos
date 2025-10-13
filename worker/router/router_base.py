import logging
import uuid
from abc import abstractmethod

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import pack_entity_batch


class RouterBase(WorkerBase):

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
        routing_keys: list[str],
        batch_size: int = 500,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self._buffer_size = batch_size
        self._buffer_per_session: dict[uuid.UUID, dict[str, list[Message]]] = {}
        self._routed_per_session: dict[uuid.UUID, int] = {}
        self._routing_keys = routing_keys

    def _start_of_session(self, session_id: uuid.UUID):
        """Hook cuando una nueva sesión comienza."""
        logging.info(f"action: session_start | stage: {self._stage_name} | session: {session_id}")
        self._buffer_per_session[session_id] = {}
        self._routed_per_session[session_id] = 0

    def _end_of_session(self, session_id: uuid.UUID):
        """Flush final y limpieza cuando una sesión termina."""
        buffer_per_key = self._buffer_per_session.get(session_id, {})
        for routing_key in buffer_per_key.keys():
            self._flush_buffer(session_id, routing_key)

        self._buffer_per_session.pop(session_id, None)
        self._routed_per_session.pop(session_id, None)
        logging.info(f"action: session_end | stage: {self._stage_name} | session: {session_id}")

    @abstractmethod
    def router_fn(self, message: Message) -> str:
        """Determina la routing key para un mensaje dado."""
        pass

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        """Procesa una entidad upstream y la rutea según router_fn."""
        self._routed_per_session[session_id] += 1
        routing_key = self.router_fn(message)

        # Inicializar buffer para esta routing key si no existe
        buffer_per_key = self._buffer_per_session[session_id]
        if routing_key not in buffer_per_key:
            buffer_per_key[routing_key] = []

        selected_buffer = buffer_per_key[routing_key]
        selected_buffer.append(message)

        if len(selected_buffer) >= self._buffer_size:
            self._flush_buffer(session_id, routing_key)

        routed = self._routed_per_session[session_id]
        if routed % 100000 == 0:
            logging.info(f"action: checkpoint | stage: {self._stage_name} | session: {session_id} | routed: {routed}")

    def _flush_buffer(self, session_id: uuid.UUID, routing_key: str) -> None:
        """Flush buffer para una routing key específica."""
        buffer_per_key = self._buffer_per_session[session_id]
        buffer = buffer_per_key.get(routing_key, [])

        if not buffer:
            return

        packed: bytes = pack_entity_batch(buffer)
        for output in self._output:
            output.send(packed, routing_key, headers={SESSION_ID: session_id.hex})

        buffer_per_key[routing_key] = []
