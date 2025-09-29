"""
packet router that separates CPU processing from I/O using queues.
Parser workers handle entity creation/serialization.
"""

import logging
import queue
import threading
import time
from typing import Dict, NamedTuple, Type

from shared.entity import EOF, Message
from shared.middleware.interface import (
    MessageMiddleware,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from shared.middleware.mock import MockPublisher
from shared.protocol import BatchPacket, PacketType


class RouteJob(NamedTuple):
    packet: BatchPacket
    entity_class: Type[Message]
    packet_type: int


class PacketRouter:
    """routes packets using direct per-worker publishers."""

    def __init__(
        self,
        publishers_config: Dict[PacketType, Dict],
        entity_mappings: Dict[PacketType, Type[Message]],
        worker_count: int = 5,
        queue_size: int = 100,
    ):
        self.publishers_config = publishers_config
        self.entity_mappings = entity_mappings

        self.parse_queue = queue.Queue(maxsize=queue_size)
        self.parse_workers = []
        self.shutdown_event = threading.Event()

        for i in range(worker_count):
            worker = threading.Thread(target=self._worker_loop, name=f"route-worker-{i}")
            worker.start()
            self.parse_workers.append(worker)

        logging.info(f"router initialized: {worker_count} workers, queue size {queue_size}")

    def route_packet(self, packet: BatchPacket):
        """route packet to worker pool for processing."""
        packet_type = packet.get_message_type()
        entity_class = self.entity_mappings.get(PacketType(packet_type))

        if not entity_class:
            raise ValueError(f"no entity mapping configured for packet type: {packet_type}")

        route_task = RouteJob(packet, entity_class, packet_type)
        self.parse_queue.put(route_task)

    def _worker_loop(self):
        publishers = self._create_publishers()
        batch_count = 0
        worker_name = threading.current_thread().name

        while not self.shutdown_event.is_set():
            try:
                task = self.parse_queue.get(timeout=1.0)
                if task is None:
                    break

                parse_start = time.time()
                messages = self._process_batch(task)
                parse_time = time.time() - parse_start

                send_start = time.time()
                publisher = publishers.get(task.packet_type)
                self._send_messages(publisher, messages, task.packet_type)
                send_time = time.time() - send_start

                batch_count += 1

                if batch_count % 100 == 0 or send_time > 1.0 or parse_time > 0.5:
                    total = parse_time + send_time
                    logging.info(
                        f"{worker_name} stats: batch={batch_count} parse={parse_time:.3f}s "
                        f"send={send_time:.3f}s total={total:.3f}s msgs={len(messages)}"
                    )

                self.parse_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"{worker_name} error: {e}")
                self.parse_queue.task_done()

    def _create_publishers(self) -> Dict[int, MessageMiddleware]:
        """create dedicated publishers for this worker."""
        publishers = {}

        for packet_type, config in self.publishers_config.items():
            try:
                publisher = MockPublisher(config["host"] + config["queue_name"])
                publishers[packet_type] = publisher

            except Exception as e:
                logging.error(f"failed to create publisher for type {packet_type}: {e}")
                raise

        return publishers

    @staticmethod
    def _send_messages(publisher: MessageMiddleware, messages: list, packet_type: int):
        """send messages into publisher"""
        try:
            for message in messages:
                publisher.send(message)

        except MessageMiddlewareDisconnectedError as e:
            logging.error(f"middleware disconnected for type {packet_type}: {e}")
            raise

        except MessageMiddlewareMessageError as e:
            logging.error(f"middleware error for type {packet_type}: {e}")
            raise

        except Exception as e:
            logging.error(f"send failed for type {packet_type}: {e}")
            raise

    @staticmethod
    def _process_batch(task: RouteJob) -> list:
        """process batch task and return serialized messages."""
        try:
            messages = []

            for row in task.packet.csv_rows:
                entity = task.entity_class.from_dict(row)
                messages.append(entity.serialize())

            if task.packet.eof:
                eof_entity = EOF()
                messages.append(eof_entity.serialize())

            return messages

        except Exception as e:
            logging.error(f"batch processing failed for type {task.packet_type}: {e}")
            raise

    def shutdown(self):
        """gracefully shutdown the router and all workers."""
        logging.info("shutting down router...")

        self.shutdown_event.set()

        for _ in self.parse_workers:
            self.parse_queue.put(None)

        for worker in self.parse_workers:
            worker.join()

        logging.info("router shutdown complete")
