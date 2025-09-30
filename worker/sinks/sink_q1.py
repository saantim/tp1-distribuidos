"""
Query 1 sink: Filtered transactions (2024-2025, 6PM-11PM, amount >= 75)
Streams results one-at-a-time to avoid memory issues with large result sets.
"""

import json
import logging

from shared.entity import Transaction


def format_fn(results: list[bytes]) -> bytes:
    """
    Format Query 1 results for streaming.
    Receives a single transaction at a time (list with one element).
    """

    if not results:
        return b""

    try:
        transaction = Transaction.deserialize(results[0])

        result = {
            "transaction_id": transaction.transaction_id,
            "final_amount": float(transaction.final_amount),
            "created_at": (
                transaction.created_at.isoformat()
                if hasattr(transaction.created_at, "isoformat")
                else str(transaction.created_at)
            ),
        }

        return json.dumps(result).encode("utf-8")

    except Exception as e:
        logging.error(f"Error formatting Q1 result: {e}")
        return b""
