"""
graceful shutdown signal handler for distributed system components.
handles SIGTERM and SIGINT (Ctrl+C) to allow clean shutdown of workers, clients, and servers.
"""

import signal


class ShutdownSignal:
    """
    captures SIGTERM and SIGINT signals and provides a clean interface
    for checking if shutdown has been requested.
    """

    def __init__(self, custom_callback=None):
        self._shutdown_requested = False
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
        self._shutdown_requested = True

    def should_shutdown(self) -> bool:
        """
        check if shutdown has been requested.
        """
        return self._shutdown_requested

    def trigger_shutdown(self):
        """
        manually trigger shutdown.
        useful for testing or internal shutdown conditions.
        """
        self._shutdown_requested = True
