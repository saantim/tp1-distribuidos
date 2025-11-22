"""
graceful shutdown signal handler for distributed system components.
handles SIGTERM and SIGINT (Ctrl+C) to allow clean shutdown of workers, clients, and servers.
"""

import signal
import threading


class ShutdownSignal:
    """
    captures SIGTERM and SIGINT signals and provides a clean interface
    for checking if shutdown has been requested.

    uses a threading.Event internally, allowing both polling via should_shutdown()
    and blocking via wait() or wait_for_shutdown().
    """

    def __init__(self, custom_callback=None):
        self._shutdown_event = threading.Event()
        if custom_callback:
            self._setup_custom_signal_handlers(custom_callback)
        else:
            self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """register signal handlers for SIGTERM and SIGINT."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    @staticmethod
    def _setup_custom_signal_handlers(callback):
        """register custom signal handlers for SIGTERM and SIGINT."""
        signal.signal(signal.SIGTERM, callback)
        signal.signal(signal.SIGINT, callback)

    def _signal_handler(self, signum: int, frame):
        """internal signal handler that sets the shutdown flag."""
        self._shutdown_event.set()

    def should_shutdown(self) -> bool:
        """
        check if shutdown has been requested.
        """
        return self._shutdown_event.is_set()

    def trigger_shutdown(self):
        """
        manually trigger shutdown.
        useful for testing or internal shutdown conditions.
        """
        self._shutdown_event.set()

    def wait(self, timeout: float | None = None) -> bool:
        """
        block until shutdown is requested or timeout expires.

        args:
            timeout: maximum seconds to wait. None means wait forever.

        returns:
            True if shutdown was requested, False if timeout expired.
        """
        return self._shutdown_event.wait(timeout)

    @property
    def event(self) -> threading.Event:
        """
        get the underlying threading.Event for advanced use cases.
        useful when you need to pass the event to other waiting mechanisms.
        """
        return self._shutdown_event
