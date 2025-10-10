"""
main coffee shop analysis engine.
orchestrates batch processing, network communication, and result collection.
"""

import logging
import queue
import threading
import time

from shared.protocol import EntityType
from shared.shutdown import ShutdownSignal

from .batch import BatchConfig, BatchProcessor
from .results import ResultsCollector
from .sender import NetworkSender
from .session import Session


class AnalyzerConfig:
    """configuration for the analyzer."""

    def __init__(self, gateway_host: str, gateway_port: int):
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port


class FolderConfig:
    """configuration for a folder of CSVs to be processed."""

    def __init__(self, path: str, entity_type: EntityType, batch_size: int):
        self.path = path
        self.entity_type = entity_type
        self.batch_size = batch_size


class Analyzer:
    """
    orchestrates csv processing and gateway communication.
    manages parallel folder processing and result collection.
    """

    def __init__(self, config: AnalyzerConfig, folders: list[FolderConfig], shutdown_signal: ShutdownSignal):
        self.config = config
        self.folders = folders
        self.shutdown_signal = shutdown_signal
        self.send_queue = queue.Queue(maxsize=100)
        self.processing_threads = []

    def run(self):
        """run complete analysis workflow."""
        session = Session(self.config.gateway_host, self.config.gateway_port, self.shutdown_signal)

        try:

            session.connect()
            session.start()

            sender = NetworkSender(session.network, self.send_queue, self.shutdown_signal)
            sender.start()

            start_time = time.time()
            self._process_all_folders()
            send_duration = time.time() - start_time
            logging.info(f"action: file_send | status: complete | duration: {send_duration:.2f}s")

            sender.stop()
            session.end()

            results = ResultsCollector(session.network, self.shutdown_signal, expected_queries={"Q1"})
            results.collect()

            total_duration = time.time() - start_time
            logging.info(f"action: analysis_complete | total_duration: {total_duration:.2f}s")

        except Exception as e:
            logging.error(f"analysis error: {e}")
            self.shutdown_signal.trigger_shutdown()
        finally:
            session.close()

    def _process_all_folders(self):
        """start folder processing threads and wait for completion."""

        for folder_config in self.folders:
            thread = threading.Thread(target=self._process_folder, args=(folder_config,))
            self.processing_threads.append(thread)
            thread.start()

        for thread in self.processing_threads:
            thread.join()

    def _process_folder(self, folder_config: FolderConfig):
        """process single folder in dedicated thread."""
        start_time = time.time()

        try:
            batch_config = BatchConfig(folder_config.batch_size)
            processor = BatchProcessor(
                folder_config.path,
                folder_config.entity_type,
                batch_config,
                self.shutdown_signal,
            )

            for packet in processor.process():
                if self.shutdown_signal.should_shutdown():
                    break
                self.send_queue.put(packet)

            if not self.shutdown_signal.should_shutdown():
                duration = time.time() - start_time
                logging.info(f"action: folder_complete | folder: {folder_config.path} | duration: {duration:.2f}s")

        except Exception as e:
            logging.error(f"error processing folder {folder_config.path}: {e}")
            self.shutdown_signal.trigger_shutdown()
