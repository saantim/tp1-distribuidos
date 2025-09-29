"""
packet router that separates CPU processing from I/O using queues.
Parser workers handle entity creation/serialization, single sender handles all middleware I/O.
"""

import logging
import queue
import threading
from typing import Dict, NamedTuple, Type

from shared.entity import EOF, Message
from shared.middleware.interface import (
    MessageMiddleware,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from shared.protocol import BatchPacket, PacketType


class ParseJob(NamedTuple):
    packet: BatchPacket
    entity_class: Type[Message]
    publisher: MessageMiddleware
    packet_type: int


class SendJob(NamedTuple):
    publisher: MessageMiddleware
    messages: list
    packet_type: int


class PacketRouter:
    """routes packets using separate CPU workers and single I/O sender."""

    def __init__(
        self,
        publishers: Dict[PacketType, MessageMiddleware],
        entity_mappings: Dict[PacketType, Type[Message]],
        worker_count: int = 5,
        queue_size: int = 100,
    ):
        self.publishers = publishers
        self.entity_mappings = entity_mappings

        self.parse_queue = queue.Queue(maxsize=queue_size)
        self.send_queue = queue.Queue(maxsize=queue_size)

        self.parse_workers = []
        self.shutdown_event = threading.Event()

        for i in range(worker_count):
            worker = threading.Thread(target=self._parser_loop, name=f"cpu-worker-{i}")
            worker.start()
            self.parse_workers.append(worker)

        self.sender = threading.Thread(target=self._sender_loop, name="sender-thread")
        self.sender.start()

        logging.info(f"router initialized: {worker_count} workers, queue size {queue_size}")

    def route_packet(self, packet: BatchPacket):
        """route packet to parse worker pool for processing."""
        packet_type = packet.get_message_type()
        publisher = self.publishers.get(PacketType(packet_type))
        entity_class = self.entity_mappings.get(PacketType(packet_type))

        if not publisher:
            raise ValueError(f"no publisher configured for packet type: {packet_type}")

        if not entity_class:
            raise ValueError(f"no entity mapping configured for packet type: {packet_type}")

        parsing_task = ParseJob(packet, entity_class, publisher, packet_type)
        self.parse_queue.put(parsing_task)

    def _parser_loop(self):
        """parser worker loop - handles entity processing only."""
        while not self.shutdown_event.is_set():
            try:
                task = self.parse_queue.get()
                if task is None:
                    break

                messages = self._process_batch(task)

                send_task = SendJob(task.publisher, messages, task.packet_type)
                self.send_queue.put(send_task)

                self.parse_queue.task_done()

            except Exception as e:
                logging.error(f"parser worker error: {e}")
                self.parse_queue.task_done()

    def _sender_loop(self):
        """sender loop - I/O operations"""
        while not self.shutdown_event.is_set():
            try:
                send_task = self.send_queue.get()
                if send_task is None:
                    break

                self._process_send_task(send_task)
                self.send_queue.task_done()

            except Exception as e:
                logging.error(f"sender error: {e}")
                self.send_queue.task_done()

    @staticmethod
    def _process_batch(task: ParseJob) -> list:
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

    @staticmethod
    def _process_send_task(task: SendJob):
        """send all messages for a task."""
        try:
            for message in task.messages:
                task.publisher.send(message)

        except MessageMiddlewareDisconnectedError as e:
            logging.error(f"middleware disconnected for type {task.packet_type}: {e}")
            raise

        except MessageMiddlewareMessageError as e:
            logging.error(f"middleware error for type {task.packet_type}: {e}")
            raise

        except Exception as e:
            logging.error(f"send failed for type {task.packet_type}: {e}")
            raise

    def shutdown(self):
        """gracefully shutdown the router and all workers."""
        logging.info("shutting down router...")

        self.shutdown_event.set()

        for _ in self.parse_workers:
            self.parse_queue.put(None)

        self.send_queue.put(None)

        for worker in self.parse_workers:
            worker.join()

        self.sender.join()

        logging.info("router shutdown complete")
