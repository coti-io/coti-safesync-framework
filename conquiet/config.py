from dataclasses import dataclass


@dataclass
class QueueConfig:
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

