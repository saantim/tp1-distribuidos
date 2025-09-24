"""
csv batch processor for creating sized batches from csv files.
designed for threaded processing where each folder runs in its own thread.
"""

import csv
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from shared.protocol import Packet
from shared.shutdown import ShutdownSignal


class BatchConfig:
    """configuration for batch processing."""

    def __init__(self, max_bytes: int, max_rows: int = None):
        self.max_bytes = max_bytes
        self.max_rows = max_rows


class BatchProcessor:
    """
    processes all csv files in a folder, yielding packets for a queue.
    designed to run in a thread per folder.
    """

    def __init__(
        self,
        folder_path: str,
        packet_creator: Callable[[List[Dict[str, Any]], bool], Packet],
        config: BatchConfig,
        shutdown_signal: Optional[ShutdownSignal] = None,
    ):
        """
        create batch processor for a folder.

        args:
            folder_path: path to folder containing csv files
            packet_creator: function to create packets from row batches
            config: batch size configuration
            shutdown_signal: optional shutdown signal handler
        """
        self.folder_path = Path(folder_path)
        self.packet_creator = packet_creator
        self.config = config
        self.shutdown_signal = shutdown_signal

    def process(self):
        """
        generator that yields all packets from all csv files in folder.
        processes files in order. designed for queue.put() in threads.
        """
        csv_files = sorted(self.folder_path.glob("*.csv"))

        if not csv_files:
            return

        for csv_file in csv_files:
            if self.shutdown_signal and self.shutdown_signal.should_shutdown():
                break

            yield from self._process_file(csv_file)

    def _process_file(self, csv_path: Path):
        """process single csv file, yielding batches."""
        with open(csv_path, "r", encoding="utf-8") as file:
            reader: csv.DictReader = csv.DictReader(file, delimiter=",", skipinitialspace=True)

            current_batch = []

            for row in reader:
                if self.shutdown_signal and self.shutdown_signal.should_shutdown():
                    break

                clean_row = {key: value.strip() for key, value in row.items()}

                if self._can_add_to_batch(current_batch, clean_row):
                    current_batch.append(clean_row)
                else:
                    if current_batch:
                        yield self.packet_creator(current_batch, False)
                    current_batch = [clean_row]

                if self.config.max_rows and len(current_batch) >= self.config.max_rows:
                    yield self.packet_creator(current_batch, False)
                    current_batch = []

            if current_batch:
                yield self.packet_creator(current_batch, True)

    def _can_add_to_batch(self, current_batch: List[Dict[str, Any]], new_row: Dict[str, Any]) -> bool:
        """check if adding new row would exceed size limit."""
        if not current_batch:
            return True

        test_batch = current_batch + [new_row]
        test_packet = self.packet_creator(test_batch, False)
        estimated_size = len(test_packet.serialize())

        return estimated_size <= self.config.max_bytes
