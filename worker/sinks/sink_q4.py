"""
Query 4 sink: Birthday date of the 3 customers who have made the most purchases for each branch
Collects all results and formats as a table-like JSON structure.
"""

import json
import logging

from worker.types import UserPurchasesByStore


def format_fn(results: list[bytes]) -> bytes:
    """
    Format Query 4 results for batch output.
    Receives all enriched Top3 data
    """
    if not results:
        return b""

    try:
        formatted_rows = []
        for result_bytes in results:
            top_3_data: UserPurchasesByStore = UserPurchasesByStore.deserialize(result_bytes)
            for _, user_info in top_3_data.user_purchases_by_store.items():
                for user_purchases_info in user_info.values():
                    formatted_rows.append(
                        {
                            "store_name": user_purchases_info.store_name,
                            "birthdate": user_purchases_info.birthday,
                            "purchases_qty": user_purchases_info.purchases,
                        }
                    )

        formatted_rows.sort(key=lambda x: (x["store_name"], x["purchases_qty"], x["birthdate"]))

        output = {
            "query": "Q4",
            "description": "Birthday date of the 3 customers who have made the most purchases for each branch",
            "results": formatted_rows,
        }

        return json.dumps(output, indent=2).encode("utf-8")

    except Exception as e:
        logging.error(f"Error formatting Q3 results: {e}", exc_info=True)
        return b""
