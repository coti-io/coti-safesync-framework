from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class QueueMessage:
    """Represents a message from a Redis Stream.

    Attributes:
        stream: The Redis stream key
        group: The consumer group name
        id: The Redis stream entry ID (e.g., "1234567890-0")
        payload: The deserialized JSON payload as a dictionary
    """

    stream: str
    group: str
    id: str
    payload: Dict[str, Any]

