import logging
import threading
from typing import Optional

import pika
from pika.exceptions import AMQPConnectionError

from .interface import (
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


PREFETCH_COUNT = 1000


class MessageMiddlewareQueueMQ(MessageMiddlewareQueue):
    """RabbitMQ queue middleware based on Pika's BlockingConnection."""

    def __init__(self, host: str, queue_name: str, arguments: dict = None) -> None:
        super().__init__(host, queue_name)
        self._host: str = host
        self._queue_name: str = queue_name
        self._local = threading.local()
        self._should_stop = False
        self._arguments = arguments

    def _ensure_connection(self):
        """Ensure connection exists for current thread (thread-local storage)."""
        if not hasattr(self._local, "connection") or self._local.connection is None or self._local.connection.is_closed:
            self._local.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self._host,
                    port=5672,
                    credentials=pika.PlainCredentials(username="admin", password="admin"),
                    heartbeat=6000,
                )
            )
            self._local.channel = self._local.connection.channel()
            self._local.channel.queue_declare(queue=self._queue_name, durable=False, arguments=self._arguments)
            self._local.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    def __str__(self):
        return f"[{self.__class__.__name__}|{self._queue_name}]"

    def start_consuming(self, on_message_callback) -> None:
        """
        Start the consume loop on the configured queue.

        Uses process_data_events() instead of start_consuming() for better control.
        This allows graceful shutdown by checking self._should_stop.
        """
        try:
            self._ensure_connection()
            self._should_stop = False

            self._local.channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=on_message_callback,
                auto_ack=False,
            )

            while not self._should_stop:
                self._local.connection.process_data_events(time_limit=2.0)

        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self) -> None:
        """
        Stop the consume loop.

        Sets the stop flag which will be checked in the next iteration
        of the process_data_events loop (within 1 second).
        """
        self._should_stop = True

    def send(self, message: str | bytes, routing_key: Optional[str] = None, headers: Optional[dict] = None) -> None:
        """Publish a message to the queue."""
        try:
            self._ensure_connection()
            properties = pika.BasicProperties(delivery_mode=1, headers=headers)
            self._local.channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=properties,
            )
        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def close(self) -> None:
        """Close the underlying connection and channel."""
        if hasattr(self._local, "connection") and self._local.connection:
            try:
                if not self._local.connection.is_closed:
                    self._local.connection.close()
            except Exception:
                pass

    def delete(self) -> None:
        """Delete the underlying queue from the broker."""
        try:
            self._ensure_connection()
            self._local.channel.queue_delete(self._queue_name)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareDeleteError(e)


class MessageMiddlewareExchangeRMQ(MessageMiddlewareExchange):
    """RabbitMQ direct-exchange middleware built on Pika's BlockingConnection."""

    def __init__(self, host: str, exchange_name: str, route_keys: list[str] = None, queue_name: str = None) -> None:
        super().__init__(host, exchange_name, route_keys)
        self._host: str = host
        self._exchange_name: str = exchange_name
        self._route_keys: list[str] = route_keys if route_keys is not None else []
        self._queue_name: str = queue_name
        self._local = threading.local()
        self._should_stop = False  # Shared across threads

    def _ensure_connection(self):
        """Ensure connection exists for current thread (thread-local storage)."""
        if not hasattr(self._local, "connection") or self._local.connection is None or self._local.connection.is_closed:
            self._local.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self._host,
                    port=5672,
                    credentials=pika.PlainCredentials(username="admin", password="admin"),
                    heartbeat=6000,
                )
            )
            self._local.channel = self._local.connection.channel()
            self._local.channel.exchange_declare(exchange=self._exchange_name, exchange_type="direct", durable=False)
            self._local.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    def __str__(self):
        return f"[{self.__class__.__name__}|{self._exchange_name}]"

    def start_consuming(self, on_message_callback) -> None:
        """
        Start consuming messages from all bound routing keys.

        Uses process_data_events() instead of start_consuming() for better control.
        """
        try:
            self._ensure_connection()
            self._should_stop = False  # Reset flag at start

            if self._queue_name:
                self._local.channel.queue_declare(queue=self._queue_name)
                queue_name = self._queue_name
            else:
                result = self._local.channel.queue_declare(queue="")
                queue_name = result.method.queue

            for route_key in self._route_keys:
                self._local.channel.queue_bind(exchange=self._exchange_name, queue=queue_name, routing_key=route_key)

            self._local.channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback, auto_ack=False)

            while not self._should_stop:
                self._local.connection.process_data_events(time_limit=2.0)

        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self) -> None:
        """
        Stop the consume loop.

        Sets the stop flag which will be checked in the next iteration
        of the process_data_events loop (within 1 second).
        """
        self._should_stop = True

    def send(self, message: str | bytes, routing_key: Optional[str] = None, headers: Optional[dict] = None) -> None:
        """Publish the given message to each configured routing key on the exchange."""
        try:
            self._ensure_connection()
            properties = pika.BasicProperties(delivery_mode=1, headers=headers)
            if routing_key:
                self._local.channel.basic_publish(
                    exchange=self._exchange_name, routing_key=routing_key, body=message, properties=properties
                )
            else:
                for route_key in self._route_keys:
                    self._local.channel.basic_publish(
                        exchange=self._exchange_name, routing_key=route_key, body=message, properties=properties
                    )
        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def close(self) -> None:
        """Stop consumption (if active) and close the underlying connection."""
        if hasattr(self._local, "connection") and self._local.connection:
            try:
                if not self._local.connection.is_closed:
                    self._local.connection.close()
            except Exception:
                pass

    def delete(self) -> None:
        """Delete the underlying exchange from the broker."""
        try:
            self._ensure_connection()
            self._local.channel.exchange_delete(self._exchange_name)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareDeleteError(e)
