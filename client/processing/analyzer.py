"""
main coffee shop analysis engine that coordinates batch sending and network communication.
manages threads for csv processing and handles tcp communication with gateway.
"""

import logging
import queue
import socket
import threading
from typing import Any, Callable, Dict, List

from shared.network import Network
from shared.protocol import FileSendEnd, FileSendStart, Packet, PacketType
from shared.shutdown import ShutdownSignal

from .batch import BatchConfig, BatchProcessor


class AnalyzerConfig:
    """configuration for the analyzer."""

    def __init__(self, gateway_host: str, gateway_port: int, batch_config: BatchConfig):
        self.gateway_host = gateway_host
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
            logging.info("action: analysis_complete | result: success")
            # todo: aca debemos empezar a esperar por el "resultado final" ya sea
            #   a través de la conexión TCP o mediante el middleware esperar la cola "results."

        except Exception as e:
            logging.error(f"coffee shop analysis error: {e}")
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
            logging.error(f"error processing folder {folder_config.path}: {e}")
        finally:
            self.packet_queue.put(None)

    def _send_session_start(self):
        """send session start packet to server and wait for ACK."""
        start_packet = FileSendStart()
        self.network.send_packet(start_packet)
        self._wait_for_ack("session start")

    def _process_packet_queue(self):
        """
        main loop that processes packets from the queue and sends to server.
        continues until all threads finish.
        """
        finished_threads = 0
        total_threads = len(self.threads)
        packet_count = 0

        while finished_threads < total_threads:
            if self.shutdown_signal.should_shutdown():
                break

            try:
                packet = self.packet_queue.get(timeout=2.0)

                if packet is None:
                    finished_threads += 1
                    continue

                self.network.send_packet(packet)
                packet_count += 1
                self._wait_for_ack(f"data packet #{packet_count}")

            except queue.Empty:
                continue

    def _send_session_end(self):
        """send session end packet to server and wait for ACK."""
        end_packet = FileSendEnd()
        self.network.send_packet(end_packet)
        self._wait_for_ack("session end")

    def _wait_for_ack(self, stage: str):
        """wait for ACK packet and handle errors."""
        ack_packet = self.network.recv_packet()
        if not ack_packet:
            raise Exception(f"connection lost while waiting for ACK for {stage}")
        elif ack_packet.get_message_type() == PacketType.ERROR:
            raise Exception(f"server error for {stage}: {ack_packet.message}")
        elif ack_packet.get_message_type() != PacketType.ACK:
            raise Exception(f"did not receive ACK for {stage}")

    def _wait_for_threads(self):
        """wait for all processing threads to complete."""
        for thread in self.threads:
            thread.join()

    def _cleanup(self):
        """cleanup resources and wait for threads."""
        self._wait_for_threads()
        if self.network:
            self.network.close()
