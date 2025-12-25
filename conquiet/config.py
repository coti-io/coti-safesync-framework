from dataclasses import dataclass


@dataclass
class QueueConfig:
    """Configuration for Redis Streams queue operations.
    This configuration applies to a single QueueConsumer instance
    (typically one worker process/thread).
    
    Attributes:
        stream_key: Redis stream key/name where messages are stored and read from.
        consumer_group: Consumer group name for coordinating message consumption.
            - Consumers within the same group share work (load-balancing).
            - Different consumer groups on the same stream operate independently and
              each receive all messages (fan-out).
        consumer_name: Unique name identifying this consumer within the consumer group.
            Used by Redis to track pending messages per-consumer.
        claim_idle_ms: Minimum idle time in milliseconds before a pending message is
            considered stale and eligible for claiming via XCLAIM.
            (Redis defines idle time as "time since last delivery".)
            Default: 60,000 ms (60 seconds).
        block_ms: Maximum time in milliseconds to block in XREADGROUP when no messages
            are available. Must be > 0 (Redis interprets 0 as infinite blocking).
            Default: 5,000 ms (5 seconds).
        max_read_count: Advisory maximum number of messages to read per XREADGROUP call.
            Higher values improve throughput but return larger batches.
            Default: 1.
    """
    stream_key: str
    consumer_group: str
    consumer_name: str
    claim_idle_ms: int = 60_000
    block_ms: int = 5_000
    max_read_count: int = 1

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.block_ms <= 0:
            raise ValueError(
                "block_ms must be > 0; Redis interprets 0 as infinite blocking"
            )

