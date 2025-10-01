"""
main coffee shop analysis engine that coordinates batch sending and network communication.
manages threads for csv processing and handles tcp communication with gateway.
"""

import json
import logging
import queue
import socket
import threading
import time
from typing import Any, Callable, Dict, List

from shared.entity import EOF
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

    def __init__(self, path: str, packet_creator: Callable[[List[Dict[str, Any]], bool], Packet], packet_size: int):
        self.path = path
        self.packet_creator = packet_creator
        self.packet_size_estimate = packet_size


class Analyzer:
    """
    main analysis engine that processes csv folders and sends data to server.
    coordinates multiple threads for parallel csv processing and result listening.
    uses dedicated sender thread to decouple processing from network I/O.
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

        self.send_queue = queue.Queue(maxsize=100)
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

            sender_thread = threading.Thread(target=self._network_sender_loop, name="network-sender")
            sender_thread.start()

            start_time = time.time()
            self._start_processing_threads()
            self._wait_for_threads()
            end_time = time.time()

            total_time = end_time - start_time
            logging.info(f"action: file_send | status: complete | total_time: {total_time:.2f}s")

            self.send_queue.put(None)
            sender_thread.join()
            logging.info("action: sender_thread | status: complete")

            self._send_session_end()

            start_time = time.time()
            self._wait_for_results()
            end_time = time.time()

            total_time = end_time - start_time
            logging.info(f"action: results_wait | status: complete | total_time: {total_time:.2f}s")
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

    def _network_sender_loop(self):
        """
        dedicated thread for network sending.
        """
        packets_sent = 0

        try:
            while not self.shutdown_signal.should_shutdown():
                try:
                    packet = self.send_queue.get(timeout=1.0)

                    if packet is None:  # shutdown signal
                        logging.info(f"action: sender_shutdown | packets_sent: {packets_sent}")
                        break

                    self.network.send_packet(packet)
                    packets_sent += 1

                    if packets_sent % 100 == 0:
                        queue_size = self.send_queue.qsize()
                        logging.debug(f"sender progress: {packets_sent} packets | queue_size: {queue_size}")

                    self.send_queue.task_done()

                except queue.Empty:
                    continue

        except Exception as e:
            logging.error(f"sender thread error: {e}")
            self.shutdown_signal.trigger_shutdown()

    def _wait_for_results(self):
        """
        Wait for result packets on the same TCP connection after session end.
        Receives multiple ResultPackets (streamed), one EOF per query signals completion.
        """
        try:
            logging.info("action: waiting_for_results | status: started")

            results_by_query = {}
            queries_complete = set()
            expected_queries = {"Q1", "Q2"}

            while len(queries_complete) < len(expected_queries):
                if self.shutdown_signal.should_shutdown():
                    break

                packet = self.network.recv_packet()

                if packet is None:
                    logging.warning("action: wait_for_results | result: connection_closed")
                    break

                if packet.get_message_type() == PacketType.RESULT:
                    query_id = packet.query_id
                    data = packet.data

                    try:
                        EOF.deserialize(data)
                        queries_complete.add(query_id)
                        logging.info(f"query {query_id} complete ({len(queries_complete)}/{len(expected_queries)})")
                        continue
                    except Exception:
                        pass

                    if query_id not in results_by_query:
                        results_by_query[query_id] = []
                    results_by_query[query_id].append(data)

                elif packet.get_message_type() == PacketType.ERROR:
                    logging.error(f"action: receive_results | result: server_error | error: {packet.message}")
                    break
                else:
                    logging.warning(
                        f"action: wait_for_results | result: unexpected_packet |" f" type: {packet.get_message_type()}"
                    )

            self._display_results(results_by_query)

        except Exception as e:
            logging.error(f"action: wait_for_results | result: fail | error: {e}")

    @staticmethod
    def _display_results(results_by_query: dict):
        """Display collected results from all queries."""
        for query_id in sorted(results_by_query.keys()):
            logging.info(f"\n========== {query_id} Results ==========")
            results = results_by_query[query_id]

            if query_id == "Q1":
                logging.info(f"Total Q1 filtered transactions: {len(results)}")
            else:
                for result_bytes in results:
                    try:
                        result = json.loads(result_bytes.decode("utf-8"))
                        logging.info(json.dumps(result, indent=2))
                    except Exception as e:
                        logging.error(f"Failed to parse result: {e}")

    def _start_processing_threads(self):
        """start worker threads for each folder."""
        for folder_config in self.folders:
            thread = threading.Thread(target=self._process_folder, args=(folder_config,))
            self.threads.append(thread)
            thread.start()

    def _process_folder(self, folder_config: FolderConfig):
        """
        worker thread function that processes a single folder.
        queues packets to send queue instead of sending directly.
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

                self.send_queue.put(packet)

            if not self.shutdown_signal.should_shutdown():
                folder_end_time = time.time()
                folder_duration = folder_end_time - folder_start_time
                logging.info(
                    f"action: folder_process | status: complete |"
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
