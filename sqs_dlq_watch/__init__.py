"""aws-sqs-dlq-watch — audit SQS dead-letter queues."""

from __future__ import annotations

__version__ = "0.2.0"

from .analyze import Thresholds, analyze
from .model import Finding, Queue, Topology, build_topology

__all__ = [
    "__version__",
    "Thresholds",
    "analyze",
    "Finding",
    "Queue",
    "Topology",
    "build_topology",
]
