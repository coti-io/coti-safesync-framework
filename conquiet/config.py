from dataclasses import dataclass


@dataclass
class DbConfig:
    table_name: str
    id_column: str = "id"
    # Extra flags can be added later.


@dataclass
class QueueConfig:
    stream_key: str
    consumer_group: str
    consumer_name: str
    claim_idle_ms: int = 60_000
    block_ms: int = 5_000
    max_read_count: int = 1

