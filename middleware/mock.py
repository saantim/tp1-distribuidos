import logging

from .interface import MessageMiddleware


class MockPublisher(MessageMiddleware):
    """Mock implementation of MessageMiddleware for testing purposes."""

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.consuming = False

    def start_consuming(self, on_message_callback):
        logging.debug(f"action: mock_start_consuming | queue: {self.queue_name}")
        self.consuming = True

    def stop_consuming(self):
        logging.debug(f"action: mock_stop_consuming | queue: {self.queue_name}")
        self.consuming = False

    def send(self, message):
        logging.debug(f"action: mock_publish | queue: {self.queue_name} | size: {len(message)}")

    def close(self):
        logging.debug(f"action: mock_close | queue: {self.queue_name}")
        self.consuming = False

    def delete(self):
        logging.debug(f"action: mock_delete | queue: {self.queue_name}")
        self.consuming = False
