"""
Output configuration for workers.
Contains middleware and routing metadata.
"""

import importlib
from dataclasses import dataclass
from typing import Callable, Optional

from shared.entity import Message
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ


@dataclass
class WorkerOutput:
    """
    Configuration for a single worker output.
    Contains middleware and routing information.
    """

    exchange: MessageMiddlewareExchangeRMQ
    name: str
    downstream_stage: Optional[str]
    downstream_workers: Optional[int]
    routing_function: Callable

    @staticmethod
    def from_config(output_config: dict, exchange: MessageMiddlewareExchangeRMQ) -> "WorkerOutput":
        """
        Create WorkerOutput from config dict and middleware.

        Args:
            output_config: Dict with 'name', 'downstream_stage', 'downstream_workers', 'routing_fn'
            exchange: The MessageMiddlewareExchangeRMQ for this output

        Returns:
            WorkerOutput instance
        """
        routing_module = importlib.import_module("shared.routing")
        routing_fn_name = output_config.get("routing_fn")

        if routing_fn_name:
            if not hasattr(routing_module, routing_fn_name):
                raise AttributeError(f"Routing function '{routing_fn_name}' not found in shared.routing")
            routing_function = getattr(routing_module, routing_fn_name)
        else:
            # Default routing function
            routing_function = routing_module.default

        return WorkerOutput(
            exchange=exchange,
            name=output_config["name"],
            downstream_stage=output_config.get("downstream_stage"),
            downstream_workers=output_config.get("downstream_workers"),
            routing_function=routing_function,
        )

    def get_routing_key(self, message: Message, message_id_int: int) -> str:
        """
        Get the routing key for a message.

        Args:
            message: The message to route
            message_id_int: Message ID as integer

        Returns:
            Routing key as string
        """
        return self.routing_function(message, self.downstream_stage, self.downstream_workers, message_id_int)

    def __repr__(self) -> str:
        """Clean representation for logging."""
        return f"WorkerOutput(name={self.name}, downstream={self.downstream_stage}, workers={self.downstream_workers})"
