"""
main coffee shop analysis engine that coordinates batch sending and network communication.
manages threads for csv processing and handles tcp communication with gateway.
now includes result listening capability.
"""

import json
import logging
import socket
import threading
import time
from typing import Any, Callable, Dict, List

from shared.network import Network
from shared.protocol import FileSendEnd, FileSendStart, Packet, PacketType, ResultPacket
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

    def __init__(self, path: str, packet_creator: Callable[[List[Dict[str, Any]], bool], Packet], packet_size: int):
        self.path = path
        self.packet_creator = packet_creator
        self.packet_size_estimate = packet_size


class Analyzer:
    """
    main analysis engine that processes csv folders and sends data to server.
    coordinates multiple threads for parallel csv processing and result listening.
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

        self.network_lock = threading.Lock()
        self.threads = []
        self.network = None

    def run(self):
        """
        run the complete analysis process.
        connects to gateway, starts processing threads, handles communication, and waits for results.
        """
        try:
            self._connect_to_gateway()
            self._send_session_start()

            start_time = time.time()
            self._start_processing_threads()
            self._wait_for_threads()
            end_time = time.time()

            total_time = end_time - start_time
            logging.info(f"action: file_send | status: complete | total_time: {total_time:.2f}s")

            self._send_session_end()

            self._wait_for_results()
            logging.info("action: analysis_complete | result: success")

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

    def _wait_for_results(self):
        """wait for result packet on the same TCP connection after session end."""
        try:
            logging.info("action: waiting_for_results | status: started")

            packet = self.network.recv_packet()
            if packet is None:
                logging.warning("action: wait_for_results | result: connection_closed")
                return

            if packet.get_message_type() == PacketType.RESULT:
                self._handle_result_packet(packet)
            elif packet.get_message_type() == PacketType.ERROR:
                logging.error(f"action: receive_results | result: server_error | error: {packet.message}")
            else:
                logging.warning(
                    f"action: wait_for_results | result: unexpected_packet |" f" type: {packet.get_message_type()}"
                )

        except Exception as e:
            logging.error(f"action: wait_for_results | result: fail | error: {e}")

    def _handle_result_packet(self, packet: ResultPacket):
        """handle received result packet."""
        try:
            results = packet.data
            logging.info("action: receive_results | result: success")
            logging.info(f"Pipeline results: {json.dumps(results, indent=2)}")
            self._send_ack_for_results()

        except Exception as e:
            logging.error(f"action: handle_result_packet | result: fail | error: {e}")

    def _send_ack_for_results(self):
        """send ACK packet for received results."""
        try:
            from shared.protocol import AckPacket

            ack_packet = AckPacket()
            self.network.send_packet(ack_packet)
            logging.info("action: ack_results | result: success")
        except Exception as e:
            logging.error(f"action: ack_results | result: fail | error: {e}")

    def _start_processing_threads(self):
        """start worker threads for each folder."""
        for folder_config in self.folders:
            thread = threading.Thread(target=self._process_folder, args=(folder_config,))
            self.threads.append(thread)
            thread.start()

    def _process_folder(self, folder_config: FolderConfig):
        """
        worker thread function that processes a single folder.
        sends packets directly to network with lock synchronization.
        """
        folder_start_time = time.time()

        try:
            processor = BatchProcessor(
                folder_config.path,
                folder_config.packet_creator,
                folder_config.packet_size_estimate,
                self.config.batch_config,
                self.shutdown_signal,
            )

            for packet in processor.process():
                if self.shutdown_signal.should_shutdown():
                    break

                with self.network_lock:
                    self.network.send_packet(packet)

            if not self.shutdown_signal.should_shutdown():
                folder_end_time = time.time()
                folder_duration = folder_end_time - folder_start_time
                logging.info(
                    f"action: folder_send | status: complete |"
                    f" folder: {folder_config.path} | duration: {folder_duration:.2f}s"
                )

        except Exception as e:
            logging.error(f"error processing folder {folder_config.path}: {e}")
            self.shutdown_signal.trigger_shutdown()

    def _send_session_start(self):
        """send session start packet to server and wait for ACK."""
        start_packet = FileSendStart()
        self.network.send_packet(start_packet)
        self._wait_for_ack("session start")

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
