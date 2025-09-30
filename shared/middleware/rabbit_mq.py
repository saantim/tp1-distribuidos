import logging
from typing import Optional

import pika
from pika.exceptions import AMQPConnectionError

from .interface import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class MessageMiddlewareQueueMQ(MessageMiddlewareQueue):
    """RabbitMQ queue middleware based on Pika's BlockingConnection.

    This class provides a minimal wrapper to:
    - open a connection and channel,
    - declare a (persist) queue,
    - publish messages, and
    - start/stop a blocking consumer loop.

    Notes:
        - The consumer loop started by `start_consuming()` is **blocking** and
          runs on the current thread until `stop_consuming()` or `close()` is called.

    Args:
        host: RabbitMQ host to connect to.
        queue_name: Name of the queue to declare and use as routing key.

    Raises:
        pika.exceptions.AMQPConnectionError: If the initial connection cannot be established.
    """

    def __init__(self, host: str, queue_name: str) -> None:
        super().__init__(host, queue_name)
        self._host: str = host
        self._queue_name: str = queue_name
        self._consumer_tag: Optional[str] = None
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self._host, port=5672, credentials=pika.PlainCredentials(username="admin", password="admin")
            )
        )
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._queue_name, durable=False)

    def start_consuming(self, on_message_callback) -> None:
        """
        Start the blocking consume loop on the configured queue.

        Registers `on_message_callback` as the message handler and enters
        Pika's blocking consuming loop. This method does not return until
        `stop_consuming()` (from the same thread) is called or the channel
        is otherwise closed.

        Callback signature:
            on_message_callback(channel, method, properties, body) -> None

            - channel: ``pika.adapters.blocking_connection.BlockingChannel``
            - method:  ``pika.spec.Basic.Deliver`` (contains delivery_tag, routing_key, etc.)
            - properties: ``pika.BasicProperties``
            - body:    ``bytes`` payload

        Args:
            on_message_callback: Function called for every delivered message.

        Raises:
            MessageMiddlewareDisconnectedError: If the broker connection is lost or refused.
            MessageMiddlewareMessageError: For any other unexpected runtime error while starting consumption.
        """

        try:
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=on_message_callback,
                auto_ack=True,
            )
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self) -> None:
        """
        Stop the blocking consume loop if it is currently running.

        If a consumer is active, this requests Pika to stop the consuming loop.
        If no consumer is active, this method is a no-op.

        Raises:
            MessageMiddlewareDisconnectedError: If the broker connection is lost while stopping.
        """
        if self._consumer_tag is None:
            return

        try:
            self._channel.stop_consuming(self._consumer_tag)
        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)

    def send(self, message: str | bytes) -> None:
        """
        Publish a message to the queue using the default exchange.

        The queue name is used as the routing key (direct-to-queue publish via ``exchange=""``).
        Messages are marked as persistent (``delivery_mode=2``) so they are stored by the broker
        when the queue is durable.

        Args:
            message: Message payload.

        Raises:
            MessageMiddlewareDisconnectedError: If the broker connection is lost while publishing.
            MessageMiddlewareMessageError: For any other unexpected runtime error while publishing.
        """
        try:
            self._channel.basic_publish(
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
        """
        Close the underlying connection and channel gracefully.

        Attempts to stop consumption if it was active and then closes the connection.

        Raises:
            MessageMiddlewareCloseError: If an unexpected error occurs while closing.
        """
        try:
            self.stop_consuming()
        except Exception:
            pass

        try:
            self._connection.close()
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareCloseError(e)

    def delete(self) -> None:
        """
        Delete the underlying queue from the broker.

        Raises:
            MessageMiddlewareDeleteError: If an unexpected error occurs while deleting the queue.
        """
        try:
            self._channel.queue_delete(self._queue_name)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareDeleteError(e)


class MessageMiddlewareExchangeRMQ(MessageMiddlewareExchange):
    """RabbitMQ direct-exchange middleware built on Pika's BlockingConnection.

    This class:
      - opens a connection/channel to RabbitMQ,
      - declares a **durable** direct exchange,
      - registers one **ephemeral, exclusive queue per routing key** and binds each queue
        to the exchange, and
      - starts/stops a **blocking** consumption loop receiving messages from all bindings.

    Design notes:
        - `start_consuming()` is **blocking** and runs on the current thread until
          `stop_consuming()` or `close()` is called, or the channel is closed.
        - For each routing key in `route_keys`, an **exclusive server-named queue** is created.
          These queues are automatically deleted when the connection closes.
        - Publishing via `send()` will send the same message to **all** configured routing keys.

    Args:
        host: RabbitMQ hostname or IP (e.g., "localhost" or a container hostname).
        exchange_name: Name of the direct exchange to declare/use.
        route_keys: Routing keys to bind and consume from.

    Raises:
        pika.exceptions.AMQPConnectionError: If the initial connection cannot be established.
    """

    def __init__(self, host: str, exchange_name: str, route_keys: list[str]) -> None:
        super().__init__(host, exchange_name, route_keys)
        self._host: str = host
        self._exchange_name: str = exchange_name
        self._route_keys: list[str] = route_keys
        self._consumer_tags: list[str] = []
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self._host, port=5672, credentials=pika.PlainCredentials(username="admin", password="admin")
            )
        )
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type="direct", durable=False)

    def start_consuming(self, on_message_callback) -> None:
        """
        Start consuming messages from all bound routing keys using a blocking loop.

        For each routing key in ``self._route_keys``, this method:
          1) declares an exclusive, server-named queue,
          2) binds it to ``self._exchange_name`` with that routing key, and
          3) registers ``on_message_callback`` as the consumer callback.

        After registering all consumers, it enters Pika’s blocking consuming loop
        via ``channel.start_consuming()``, which does not return until the loop is
        stopped or the channel/connection is closed.

        Callback signature:
            on_message_callback(channel, method, properties, body) -> None
              - channel:  pika.adapters.blocking_connection.BlockingChannel
              - method:   pika.spec.Basic.Deliver (e.g., delivery_tag, routing_key)
              - properties: pika.BasicProperties
              - body:     bytes

        Args:
            on_message_callback: Function invoked for each delivered message.

        Raises:
            MessageMiddlewareMessageError: If any unexpected error occurs while declaring/binding
                queues, registering consumers, or entering the blocking consume loop (including
                broker disconnections).
        """

        try:
            for route_key in self._route_keys:
                result = self._channel.queue_declare(queue="", exclusive=True)
                queue_name = result.method.queue
                self._channel.queue_bind(exchange=self._exchange_name, queue=queue_name, routing_key=route_key)
                self._consumer_tags.append(
                    self._channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
                )
            self._channel.start_consuming()
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self) -> None:
        """
        Cancel registered consumers and stop the blocking consume loop.

        This method iterates over all stored consumer tags and cancels each consumer,
        If there is no active consumer, this method is a no-op.

        Raises:
            MessageMiddlewareDisconnectedError: If the broker connection is lost while stopping.
        """
        try:
            while len(self._consumer_tags) > 0:
                consumer_tag = self._consumer_tags.pop(0)
                self._channel.stop_consuming(consumer_tag)
        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)

    def send(self, message: str | bytes) -> None:
        """
        Publish the given message to **each** configured routing key on the exchange.

        The message is sent once per routing key in `self._route_keys`. The default
        exchange publish semantics of the direct exchange apply (exact-match routing).

        Args:
            message: Message payload (use `bytes` for binary data; `str` will be sent as-is).

        Raises:
            MessageMiddlewareDisconnectedError: If the broker connection is lost while publishing.
            MessageMiddlewareMessageError: For any other unexpected runtime error while publishing.
        """
        try:
            for route_key in self._route_keys:
                self._channel.basic_publish(exchange=self._exchange_name, routing_key=route_key, body=message)
        except AMQPConnectionError as e:
            logging.exception(e)
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareMessageError(e)

    def close(self) -> None:
        """
        Stop consumption (if active) and close the underlying connection.

        This attempts a graceful shutdown:
          1) calls `stop_consuming()` (ignoring errors), and
          2) closes the connection.

        Raises:
            MessageMiddlewareCloseError: If an unexpected error occurs while closing.
        """
        try:
            self.stop_consuming()
        except Exception:
            pass

        try:
            self._connection.close()
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareCloseError(e)

    def delete(self) -> None:
        """
        Delete the underlying exchange from the broker.

        Note:
            Deleting an exchange that still has bindings or active use may fail,
            depending on broker state and policies.

        Raises:
            MessageMiddlewareDeleteError: If an unexpected error occurs while deleting the exchange.
        """
        try:
            self._channel.exchange_delete(self._exchange_name)
        except Exception as e:
            logging.exception(e)
            raise MessageMiddlewareDeleteError(e)
