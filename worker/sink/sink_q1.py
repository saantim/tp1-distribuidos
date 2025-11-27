"""
Query 1 sink: Filtered transactions (2024-2025, 6PM-11PM, amount >= 75)
Streams results one-at-a-time to avoid memory issues with large result sets.
"""

import json
import logging
from typing import Type

from shared.entity import Message, RawMessage, Transaction
from worker.sink.sink_base import SinkBase


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
