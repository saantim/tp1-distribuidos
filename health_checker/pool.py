"""
Simple fixed-size thread pool for processing tasks.
"""

import queue
import threading
from typing import Callable

from shared.shutdown import ShutdownSignal


class ThreadPool:
    """Fixed-size thread pool that processes tasks from a queue."""

    def __init__(self, size: int, shutdown_signal: ShutdownSignal):
        self.size = size
        self.shutdown_signal = shutdown_signal
        self.task_queue = queue.Queue()
        self.workers = []

    def start(self):
        """Start all worker threads."""
        for _ in range(self.size):
            thread = threading.Thread(target=self._worker, daemon=True)
            thread.start()
            self.workers.append(thread)

    def submit(self, task: Callable[[], None]):
        """Submit a task to be executed by a worker."""
        self.task_queue.put(task)

    def stop(self, timeout: float = 5.0):
        """Stop all worker threads and wait for them to finish."""
        for worker in self.workers:
            worker.join(timeout=timeout)
        self.workers.clear()

    def _worker(self):
        """Worker loop that processes tasks from the queue."""
        while not self.shutdown_signal.should_shutdown():
            try:
                task = self.task_queue.get(timeout=1.0)
                task()
                self.task_queue.task_done()
            except queue.Empty:
                continue
