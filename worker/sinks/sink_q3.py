"""
Query 3 sink: TPV (Total Payment Value) per semester and store
Collects all results and formats as a table-like JSON structure.
"""

import json
import logging

from worker.types import SemesterTPVByStore


def format_fn(results: list[bytes]) -> bytes:
    """
    Format Query 3 results for batch output.
    Receives all enriched semester TPV data, formats as JSON array.

    Args:
        results: List of serialized SemesterTPVByStore objects

    Returns:
        JSON-encoded array of semester/store/TPV rows
    """
    if not results:
        return b""

    try:
        formatted_rows = []

        for result_bytes in results:
            semester_data: SemesterTPVByStore = SemesterTPVByStore.deserialize(result_bytes)

            for semester_key, stores_dict in semester_data.semester_tpv_by_store.items():
                year = semester_key.split("-")[0]
                half = semester_key.split("-")[1]
                semester_label = f"{year}-H{half}"

                for store_id, store_info in stores_dict.items():
                    formatted_rows.append(
                        {
                            "store_id": store_id,
                            "semester": semester_label,
                            "store_name": store_info.store_name,
                            "tpv": float(store_info.amount),
                        }
                    )

        formatted_rows.sort(key=lambda x: (x["semester"], x["store_name"]))

        output = {
            "query": "Q3",
            "description": "TPV per semester per store (6AM-11PM transactions)",
            "results": formatted_rows,
        }

        return json.dumps(output, indent=2).encode("utf-8")

    except Exception as e:
        logging.error(f"Error formatting Q3 results: {e}", exc_info=True)
        return b""
