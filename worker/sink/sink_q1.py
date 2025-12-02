"""
Query 1 sink: Filtered transactions (2024-2025, 6PM-11PM, amount >= 75)
Streams results one-at-a-time to avoid memory issues with large result sets.
"""

import datetime
import json
import logging
from typing import Any, Type

from pydantic import BaseModel

from shared.entity import CreatedAt, FinalAmount, Message, RawMessage, StoreId, Transaction, TransactionId, UserId
from worker.session.session import Session
from worker.session.storage import SessionStorage
from worker.sink.ops import AppendTransactionOp
from worker.sink.sink_base import SinkBase
from worker.storage import WALFileSessionStorage
from worker.storage.ops import BaseOp


class SessionData(BaseModel):
    result: list[Transaction] = []
    message_count: int = 0


def sink_q1_reducer(state: list[Transaction] | None, op: BaseOp) -> list[Transaction]:
    if not isinstance(op, AppendTransactionOp):
        return state or []

    if state is None:
        state = []

    state.append(
        Transaction(
            id=TransactionId(op.transaction_id),
            store_id=StoreId(0),
            user_id=UserId(0),
            created_at=CreatedAt(datetime.datetime.now()),
            final_amount=FinalAmount(op.final_amount),
        )
    )

    return state


def _session_reducer(state: Any, op: BaseOp) -> SessionData:
    if state is None or not isinstance(state, SessionData):
        if isinstance(state, dict):
            session_data = SessionData.model_validate(state)
        else:
            session_data = SessionData()
    else:
        session_data = state

    session_data.result = sink_q1_reducer(session_data.result, op)
    return session_data


class Sink(SinkBase):
    def get_entity_type(self) -> Type[Message]:
        return Transaction

    def format_fn(self, results_collected: list[Transaction]) -> RawMessage:
        """
        Format Query 1 results for streaming.
        Receives a list of transactions.
        """

        if not results_collected:
            logging.warning("No results collected for Q1, sending empty results.")
            return RawMessage(raw_data=b"")

        output = []
        try:
            for tx in results_collected:
                result = {
                    "transaction_id": tx.id,
                    "final_amount": float(tx.final_amount),
                }
                output.append(result)
            return RawMessage(raw_data=json.dumps(output).encode("utf-8"))
        except Exception as e:
            logging.error(f"Error formatting Q1 result: {e}")
            return RawMessage(raw_data=b"")

    def output_size_calculation(self, msg: list[RawMessage]) -> int:
        """Calculate the number of transactions in the output."""
        try:
            if not msg or not msg[0].raw_data:
                return 0
            data = json.loads(msg[0].raw_data.decode("utf-8"))
            return len(data) if isinstance(data, list) else 0
        except Exception:
            return 0

    def _on_entity_upstream(self, message: Transaction, session: Session) -> None:
        op = AppendTransactionOp(transaction_id=message.id, final_amount=message.final_amount)

        session.apply(op)

    def get_session_data_type(self) -> Type[BaseModel]:
        return SessionData

    def get_reducer(self):
        return _session_reducer

    def create_session_storage(self) -> SessionStorage:
        return WALFileSessionStorage(
            save_dir="./sessions/saves", reducer=_session_reducer, op_types=[AppendTransactionOp]
        )
