from __future__ import annotations

from ..metrics.registry import (
    QUEUE_MESSAGES_ACK_TOTAL,
    QUEUE_MESSAGES_CLAIMED_TOTAL,
    QUEUE_MESSAGES_READ_TOTAL,
    QUEUE_READ_LATENCY_SECONDS,
)


def observe_queue_read(stream: str, message_count: int, latency_s: float) -> None:
    """
    Record queue read operation metrics.

    Args:
        stream: Stream name
        message_count: Number of messages read
        latency_s: Read operation latency in seconds
    """
    try:
        QUEUE_MESSAGES_READ_TOTAL.labels(stream=stream).inc(message_count)
        if message_count > 0:
            QUEUE_READ_LATENCY_SECONDS.labels(stream=stream).observe(latency_s)
    except Exception:
        # Metrics must never raise exceptions
        pass


def observe_queue_ack(stream: str) -> None:
    """
    Record queue message acknowledgment metric.

    Args:
        stream: Stream name
    """
    try:
        QUEUE_MESSAGES_ACK_TOTAL.labels(stream=stream).inc()
    except Exception:
        # Metrics must never raise exceptions
        pass


def observe_queue_claimed(stream: str, message_count: int) -> None:
    """
    Record queue message claiming metric.

    Args:
        stream: Stream name
        message_count: Number of messages claimed
    """
    try:
        QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=stream).inc(message_count)
    except Exception:
        # Metrics must never raise exceptions
        pass

