"""
main coffee shop analysis engine that coordinates batch sending and network communication.
manages threads for csv processing and handles tcp communication with gateway.
"""

import queue
import socket
import threading
from typing import Any, Callable, Dict, List

from batch import BatchConfig, BatchProcessor

from shared.network import Network
from shared.protocol import FileSendEnd, FileSendStart, Packet
from shared.shutdown import ShutdownSignal


class AnalyzerConfig:
    """configuration for the analyzer."""

    def __init__(self, gateway_ip: str, gateway_port: int, batch_config: BatchConfig):
        self.gateway_host = gateway_ip
        self.gateway_port = gateway_port
        self.batch_config = batch_config


class FolderConfig:
    """configuration for a folder of CSVs to be processed."""

    def __init__(self, path: str, packet_creator: Callable[[List[Dict[str, Any]], bool], Packet]):
        self.path = path
        self.packet_creator = packet_creator


class Analyzer:
    """
    main analysis engine that processes csv folders and sends data to server.
    coordinates multiple threads for parallel csv processing.
    """

    def __init__(self, config: AnalyzerConfig, folders: List[FolderConfig], shutdown_signal: ShutdownSignal):
        """
        create analyzer instance.

        args:
            config: analyzer configuration
            folders: list of folders to process
            shutdown_signal: shutdown signal handler
        """
        self.config = config
        self.folders = folders
        self.shutdown_signal = shutdown_signal

        self.packet_queue = queue.Queue()
        self.threads = []
        self.network = None

    def run(self):
        """
        run the complete analysis process.
        connects to gateway, starts processing threads, and handles communication.
        """
        try:
            self._connect_to_gateway()
            self._start_processing_threads()
            self._send_session_start()
            self._process_packet_queue()
            self._send_session_end()

        except Exception as e:
            print(f"analysis failed: {e}")
            self.shutdown_signal.trigger_shutdown()
        finally:
            self._cleanup()

    def _connect_to_gateway(self):
        """establish tcp connection to server."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.config.gateway_host, self.config.gateway_port))
        self.network = Network(sock, self.shutdown_signal)

    def _start_processing_threads(self):
        """start worker threads for each folder."""
        for folder_config in self.folders:
            thread = threading.Thread(target=self._process_folder, args=(folder_config,))
            self.threads.append(thread)
            thread.start()

    def _process_folder(self, folder_config: FolderConfig):
        """
        worker thread function that processes a single folder.
        sends packets to the shared queue.
        """
        try:
            processor = BatchProcessor(
                folder_config.path, folder_config.packet_creator, self.config.batch_config, self.shutdown_signal
            )

            for packet in processor.process():
                if self.shutdown_signal.should_shutdown():
                    break
                self.packet_queue.put(packet)

        except Exception as e:
            print(f"error processing folder {folder_config.path}: {e}")
        finally:
            self.packet_queue.put(None)

    def _send_session_start(self):
        """send session start packet to server."""
        start_packet = FileSendStart()
        self.network.send_packet(start_packet)

    def _process_packet_queue(self):
        """
        main loop that processes packets from the queue and sends to server.
        continues until all threads finish.
        """
        finished_threads = 0
        total_threads = len(self.threads)

        while finished_threads < total_threads:
            if self.shutdown_signal.should_shutdown():
                break

            try:
                packet = self.packet_queue.get(timeout=1.0)

                if packet is None:
                    finished_threads += 1
                    continue

                self.network.send_packet(packet)

            except queue.Empty:
                continue

    def _send_session_end(self):
        """send session end packet to server."""
        end_packet = FileSendEnd()
        self.network.send_packet(end_packet)

    def _wait_for_threads(self):
        """wait for all processing threads to complete."""
        for thread in self.threads:
            thread.join()

    def _cleanup(self):
        """cleanup resources and wait for threads."""
        self._wait_for_threads()
        if self.network:
            self.network.close()
