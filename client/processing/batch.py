"""
csv batch processor for creating sized batches from csv files.
designed for threaded processing where each folder runs in its own thread.
"""

from pathlib import Path
from typing import Optional

from shared.protocol import Batch, EntityType
from shared.shutdown import ShutdownSignal


class BatchConfig:
    """configuration for batch processing."""

    def __init__(self, max_rows: int = 1000):
        """
        args:
            max_rows: maximum number of rows per batch (default: 1000)
        """
        self.max_rows = max_rows


class BatchProcessor:
    """
    processes all csv files in a folder, yielding Batch packets for a queue.
    designed to run in a thread per folder.
    """

    def __init__(
        self,
        folder_path: str,
        entity_type: EntityType,
        config: BatchConfig,
        shutdown_signal: Optional[ShutdownSignal] = None,
    ):
        """
        create batch processor for a folder.

        args:
            folder_path: path to folder containing csv files
            entity_type: type of entities in this folder
            config: batch size configuration
            shutdown_signal: optional shutdown signal handler
        """
        self.folder_path = Path(folder_path)
        self.entity_type = entity_type
        self.config = config
        self.shutdown_signal = shutdown_signal

    def process(self):
        """
        generator that yields all Batch packets from all csv files in folder.
        processes files in order. designed for queue.put() in threads.
        sends final empty EOF packet when folder processing is complete.
        """
        csv_files = sorted(self.folder_path.glob("*.csv"))

        if not csv_files:
            yield Batch(self.entity_type, [], True)
            return

        for csv_file in csv_files:
            yield from self._process_file(csv_file)

        if not self.shutdown_signal or not self.shutdown_signal.should_shutdown():
            yield Batch(self.entity_type, [], True)

    def _process_file(self, csv_path: Path):
        """process single csv file, yielding batches of raw csv strings."""
        with open(csv_path, "r", encoding="utf-8") as file:

            next(file, None)

            current_batch = []

            for line in file:
                if self.shutdown_signal.should_shutdown():
                    break

                stripped = line.strip()
                if not stripped:
                    continue

                current_batch.append(stripped)

                if len(current_batch) >= self.config.max_rows:
                    yield Batch(self.entity_type, current_batch, False)
                    current_batch = []

            if current_batch:
                yield Batch(self.entity_type, current_batch, False)
