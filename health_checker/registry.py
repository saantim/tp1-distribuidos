"""Thread-safe registry for tracking heartbeats with optional persistence."""

import json
import logging
import os
import tempfile
import threading
import time


class Registry:
    """Thread-safe registry for tracking heartbeats with optional persistence."""

    def __init__(self, persist_path: str | None = None):
        self._entries: dict[str, float] = {}
        self._lock = threading.Lock()
        self._persist_path = persist_path

    def update(self, key: str, timestamp: float) -> None:
        """Register or update an entry's heartbeat."""
        with self._lock:
            self._entries[key] = timestamp

    def get_dead(self, timeout: float) -> list[str]:
        """Return list of keys that haven't sent a heartbeat within timeout."""
        now = time.time()
        with self._lock:
            return [key for key, ts in self._entries.items() if now - ts > timeout]

    def get_alive(self, timeout: float) -> list[str]:
        """Return list of keys that have sent a heartbeat within timeout."""
        now = time.time()
        with self._lock:
            return [key for key, ts in self._entries.items() if now - ts <= timeout]

    def get_all(self) -> dict[str, float]:
        """Return copy of all entries."""
        with self._lock:
            return dict(self._entries)

    def remove(self, key: str) -> None:
        """Remove an entry from the registry."""
        with self._lock:
            self._entries.pop(key, None)

    def persist(self) -> None:
        """Persist the registry to disk using atomic write."""
        if not self._persist_path:
            return

        with self._lock:
            data = dict(self._entries)

        try:
            dir_path = os.path.dirname(self._persist_path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            fd, tmp_path = tempfile.mkstemp(dir=dir_path)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(data, f)
                os.replace(tmp_path, self._persist_path)
            except Exception:
                os.unlink(tmp_path)
                raise
        except Exception as e:
            logging.error(f"action: registry_persist | result: fail | error: {e}")

    def load(self) -> None:
        """Load the registry from disk."""
        if not self._persist_path or not os.path.exists(self._persist_path):
            return

        try:
            with open(self._persist_path, "r") as f:
                data = json.load(f)

            with self._lock:
                self._entries.update(data)

            logging.info(f"action: registry_load | result: success | entries: {len(data)}")
        except Exception as e:
            logging.error(f"action: registry_load | result: fail | error: {e}")
