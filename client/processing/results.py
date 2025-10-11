"""
results collection and display from gateway.
handles streaming result packets and EOF detection.
"""

import json
import logging
import time
from pathlib import Path

from shared.entity import EOF
from shared.protocol import PacketType


class ResultsSaver:
    """Saves query results to disk for validation."""

    def __init__(self, results_dir: str = ".results"):
        pipeline_dir = Path(results_dir) / "pipeline"
        self.results_dir = pipeline_dir
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.results_by_query = {}

    def save_result(self, query_id: str, data: bytes):
        """Save individual result data for a query."""
        if query_id not in self.results_by_query:
            self.results_by_query[query_id] = []

        try:
            result = json.loads(data.decode("utf-8"))
            self.results_by_query[query_id].append(result)
        except Exception as e:
            logging.error(f"Failed to parse result for {query_id}: {e}")

    def flush_to_disk(self):
        """Write all accumulated results to disk."""
        for query_id, results in self.results_by_query.items():
            output_file = self.results_dir / f"{query_id.lower()}.json"

            merged = self._merge_query_results(query_id, results)

            with open(output_file, "w") as f:
                json.dump(merged, f, indent=2)

            logging.info(f"Saved {query_id} results to {output_file}")

    @staticmethod
    def _merge_query_results(query_id: str, results: list) -> dict:
        """Merge multiple result chunks into single structure."""
        if not results:
            return {}

        # Q1 returns list of transactions
        if query_id == "Q1":
            return results[0] if len(results) == 1 else results

        # Q2, Q3, Q4 return structured dict
        return results[0] if len(results) == 1 else {"merged": results}


class ResultsCollector:
    """collects and displays query results from gateway."""

    def __init__(self, network, shutdown_signal, expected_queries: set, results_dir: str = ".results"):
        self.network = network
        self.shutdown_signal = shutdown_signal
        self.expected_queries = expected_queries
        self.results_by_query = {}
        self.queries_complete = set()
        self.saver = ResultsSaver(results_dir)

    def collect(self):
        """
        collect results from all expected queries.
        blocks until all queries complete or error occurs.
        """
        logging.info("action: waiting_for_results | status: started")
        start_time = time.time()

        try:
            while len(self.queries_complete) < len(self.expected_queries):
                if self.shutdown_signal.should_shutdown():
                    break

                packet = self.network.recv_packet()

                if packet is None:
                    logging.warning("action: collect_results | result: connection_closed")
                    break

                if packet.get_message_type() == PacketType.RESULT:
                    self._handle_result_packet(packet, start_time)

                elif packet.get_message_type() == PacketType.ERROR:
                    logging.error(f"action: collect_results | error: {packet.message}")
                    break
                else:
                    logging.warning(f"unexpected packet type: {packet.get_message_type()}")

            self.saver.flush_to_disk()
            self.display_results()

        except Exception as e:
            logging.error(f"action: collect_results | error: {e}")

    def _handle_result_packet(self, packet, start_time):
        """process individual result packet."""
        query_id = packet.query_id
        data = packet.data

        if self._is_eof(data):
            self.queries_complete.add(query_id)
            elapsed = time.time() - start_time
            logging.info(
                f"query {query_id} complete "
                f"({len(self.queries_complete)}/{len(self.expected_queries)}) "
                f"received {elapsed:.2f}s after waiting."
            )
            return

        # Save to disk
        self.saver.save_result(query_id, data)

        # Keep in memory for display
        if query_id not in self.results_by_query:
            self.results_by_query[query_id] = []
        self.results_by_query[query_id].append(data)

    @staticmethod
    def _is_eof(data: bytes) -> bool:
        """
        check if data represents EOF.

        TODO: refactor this when redesigning EOF handling.
        current implementation uses exception-based detection.
        """
        try:
            EOF.deserialize(data)
            return True
        except Exception:
            return False

    def display_results(self):
        """display all collected results."""
        for query_id in sorted(self.results_by_query.keys()):
            logging.info(f"\n========== {query_id} Results ==========")
            results = self.results_by_query[query_id]

            for result_bytes in results:
                try:
                    result = json.loads(result_bytes.decode("utf-8"))
                    logging.info(json.dumps(result, indent=2))
                    if query_id == "Q1":
                        logging.info(f"Q1 Total Transactions: {len(list(result))}")
                except Exception as e:
                    logging.error(f"Failed to parse result for {query_id}: {e}")
