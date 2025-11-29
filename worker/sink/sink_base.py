import logging
from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar

from pydantic.generics import GenericModel

from shared.entity import Message, RawMessage
from shared.middleware.interface import MessageMiddlewareExchange
from worker.base import Session, WorkerBase


TypedMSG = TypeVar("TypedMSG", bound=Message)


class SessionData(GenericModel, Generic[TypedMSG]):
    result: list[TypedMSG] = []
    message_count: int = 0


class SinkBase(WorkerBase, ABC):
    """
    Collects results from pipeline, formats them using a query-specific function,
    and sends formatted results to the results exchange with by_stage_name routing.
    """

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddlewareExchange,
        outputs: list,
    ):
        super().__init__(instances, index, stage_name, source, outputs)

    @abstractmethod
    def format_fn(self, results_collected: list[TypedMSG]) -> RawMessage: ...

    def output_size_calculation(self, msg: list[RawMessage]) -> int:
        """Default implementation: returns total byte size of output messages."""
        try:
            if not msg:
                return 0
            return sum(len(m.raw_data) for m in msg)
        except Exception:
            return 0

    def get_session_data_type(self) -> Type[SessionData]:
        return SessionData

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        session_data: SessionData = session.get_storage(SessionData)
        formatted_results: list[RawMessage] = [self.format_fn(session_data.result)]
        if formatted_results:
            self._send_message(formatted_results, session_id=session.session_id)
            logging.info(
                f"action: sent_final_results | size: {self.output_size_calculation(formatted_results)} |"
                f" session: {session.session_id.hex[:8]}"
            )
            session_data.result.clear()

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage(SessionData)
        session_data.result.append(message)
        session_data.message_count += 1
