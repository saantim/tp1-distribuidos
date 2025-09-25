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
        packet_size: int,
        config: BatchConfig,
        shutdown_signal: Optional[ShutdownSignal] = None,
    ):
        """
        create batch processor for a folder.

        args:
            folder_path: path to folder containing csv files
            packet_creator: function to create packets from row batches
            packet_size: size of packets to create
            config: batch size configuration
            shutdown_signal: optional shutdown signal handler
        """
        self.folder_path = Path(folder_path)
        self.packet_creator = packet_creator
        self.packet_size = packet_size
        self.config = config
        self.shutdown_signal = shutdown_signal

    def process(self):
        """
        generator that yields all packets from all csv files in folder.
        processes files in order. designed for queue.put() in threads.
        sends final empty EOF packet when folder processing is complete.
        """
        csv_files = sorted(self.folder_path.glob("*.csv"))

        if not csv_files:
            yield self.packet_creator([], True)
            return

        for csv_file in csv_files:
            yield from self._process_file(csv_file)

        if not self.shutdown_signal.should_shutdown():
            yield self.packet_creator([], True)

    def _process_file(self, csv_path: Path):
        """process single csv file, yielding batches."""
        with open(csv_path, "r", encoding="utf-8") as file:
            reader: csv.DictReader = csv.DictReader(file, delimiter=",", skipinitialspace=True)

            current_batch = []

            for row in reader:
                if self.shutdown_signal and self.shutdown_signal.should_shutdown():
                    break

                if self._can_add_to_batch(current_batch):
                    current_batch.append(row)
                else:
                    if current_batch:
                        yield self.packet_creator(current_batch, False)
                    current_batch = [row]

                if self.config.max_rows and len(current_batch) >= self.config.max_rows:
                    yield self.packet_creator(current_batch, False)
                    current_batch = []

            if current_batch:
                yield self.packet_creator(current_batch, False)

    def _can_add_to_batch(self, current_batch: List[Dict[str, Any]]) -> bool:
        """check if adding new row would exceed size limit."""
        if not current_batch:
            return True

        new_row_amount = len(current_batch) + 1
        estimated_size = new_row_amount * self.packet_size

        return estimated_size <= self.config.max_bytes
