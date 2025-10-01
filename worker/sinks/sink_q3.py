"""
Query 3 sink: TPV Per Semester Merged
Collects all results and formats as a batch.
"""

import logging

from worker.types import SemesterTPVByStore


def format_fn(results: list[bytes]) -> bytes:
    """
    Format Query 3 results for batch output.
    Receives all enriched period aggregations, formats as JSON.

    Args:
        results: List of serialized SemesterTPVByStore objects

    Returns:
        JSON-encoded array of period results
    """
    if not results:
        return b""

    try:

        for result_bytes in results:
            semester_data: SemesterTPVByStore = SemesterTPVByStore.deserialize(result_bytes)
            logging.info(f"result: {semester_data}")

        return b""

    except Exception as e:
        logging.error(f"Error formatting Q2 results: {e}", exc_info=True)
        return b""
