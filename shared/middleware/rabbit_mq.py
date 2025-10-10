import logging
import threading

import pika
from pika.exceptions import AMQPConnectionError

from .interface import (
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class MessageMiddlewareQueueMQ(MessageMiddlewareQueue):
    """RabbitMQ queue middleware based on Pika's BlockingConnection."""

    def __init__(self, host: str, queue_name: str) -> None:
        super().__init__(host, queue_name)
        self._host: str = host
        self._queue_name: str = queue_name
        self._local = threading.local()
        self._should_stop = False  # Shared across threads, accessed from worker thread

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
            self._local.channel.queue_declare(queue=self._queue_name, durable=False)

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

    def send(self, message: str | bytes) -> None:
        """Publish a message to the queue."""
        try:
            self._ensure_connection()
            self._local.channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
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

    def __init__(self, host: str, exchange_name: str, route_keys: list[str]) -> None:
        super().__init__(host, exchange_name, route_keys)
        self._host: str = host
        self._exchange_name: str = exchange_name
        self._route_keys: list[str] = route_keys
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

            for route_key in self._route_keys:
                result = self._local.channel.queue_declare(queue="", exclusive=True)
                queue_name = result.method.queue
                self._local.channel.queue_bind(exchange=self._exchange_name, queue=queue_name, routing_key=route_key)
                self._local.channel.basic_consume(
                    queue=queue_name, on_message_callback=on_message_callback, auto_ack=False
                )

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

    def send(self, message: str | bytes) -> None:
        """Publish the given message to each configured routing key on the exchange."""
        try:
            self._ensure_connection()
            for route_key in self._route_keys:
                self._local.channel.basic_publish(exchange=self._exchange_name, routing_key=route_key, body=message)
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
