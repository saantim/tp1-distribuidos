"""
Query 2 sink: Top products per period (most sold and highest revenue)
Collects all results and formats as a batch.
"""

import json
import logging
from typing import Type

from shared.entity import Message, RawMessage
from worker.sink.sink_base import SinkBase
from worker.types import TransactionItemByPeriod


class Sink(SinkBase):

    def get_entity_type(self) -> Type[Message]:
        return TransactionItemByPeriod

    def format_fn(self, results_collected: list[TransactionItemByPeriod]) -> RawMessage:
        """
        Format Query 2 results for batch output.
        Receives all enriched period aggregations, formats as JSON.

        Args:
            results_collected: List of deserialized TransactionItemByPeriod objects

        Returns:
            JSON-encoded array of period results
        """
        if not results_collected:
            return RawMessage(raw_data=b"")

        try:
            formatted_periods = []

            for period_data in results_collected:

                for period_key, items_dict in period_data.transaction_item_per_period.items():
                    most_sold_item = None
                    most_sold_qty = 0

                    highest_revenue_item = None
                    highest_revenue_amount = 0.0

                    for item_id, item_info in items_dict.items():
                        if item_info.quantity > most_sold_qty:
                            most_sold_qty = item_info.quantity
                            most_sold_item = {
                                "item_id": item_id,
                                "item_name": item_info.item_name,
                                "quantity": item_info.quantity,
                            }

                        if item_info.amount > highest_revenue_amount:
                            highest_revenue_amount = item_info.amount
                            highest_revenue_item = {
                                "item_id": item_id,
                                "item_name": item_info.item_name,
                                "revenue": float(item_info.amount),
                            }

                    formatted_periods.append(
                        {
                            "period": str(period_key),
                            "most_sold_product": most_sold_item,
                            "highest_revenue_product": highest_revenue_item,
                        }
                    )

            formatted_periods.sort(key=lambda x: x["period"])

            output = {"query": "Q2", "description": "Top products per period (2024-2025)", "results": formatted_periods}

            return RawMessage(raw_data=json.dumps(output, indent=2).encode("utf-8"))

        except Exception as e:
            logging.error(f"Error formatting Q2 results: {e}", exc_info=True)
            return RawMessage(raw_data=b"")
