"""
dedicated network sender thread.
decouples csv processing from network I/O.
"""

import logging
import queue
import threading

from shared.shutdown import ShutdownSignal


class NetworkSender:
    """handles queued packet sending in dedicated thread."""

    def __init__(self, network, send_queue: queue.Queue, shutdown_signal: ShutdownSignal):
        self.network = network
        self.send_queue = send_queue
        self.shutdown_signal = shutdown_signal
        self.thread = None
        self.packets_sent = 0

    def start(self):
        """start sender thread."""
        self.thread = threading.Thread(target=self._run, name="network-sender")
        self.thread.start()

    def stop(self):
        """signal sender to stop and wait for completion."""
        self.send_queue.put(None)  # sentinel value
        if self.thread:
            self.thread.join()
        logging.info(f"action: sender_stopped | packets_sent: {self.packets_sent}")

    def _run(self):
        """sender thread main loop."""
        try:
            while not self.shutdown_signal.should_shutdown():
                try:
                    packet = self.send_queue.get(timeout=1.0)

                    if packet is None:  # shutdown signal
                        break

                    self.network.send_packet(packet)
                    self.packets_sent += 1

                    if self.packets_sent % 100 == 0:
                        logging.debug(
                            f"sender progress: {self.packets_sent} packets | queue: {self.send_queue.qsize()}"
                        )

                    self.send_queue.task_done()

                except queue.Empty:
                    continue

        except Exception as e:
            logging.error(f"sender thread error: {e}")
            self.shutdown_signal.trigger_shutdown()
