import logging


class MockPublisher:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name

    def send(self, data: bytes):
        logging.info(f"action: mock_publish | queue: {self.queue_name} | size: {len(data)}")
