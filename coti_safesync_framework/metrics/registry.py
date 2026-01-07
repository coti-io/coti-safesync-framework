from prometheus_client import Counter, Histogram

# DB metrics
DB_WRITE_TOTAL = Counter(
    "coti_safesync_db_write_total",
    "Total DB write operations",
    ["table", "op_type", "status"],
)

DB_WRITE_LATENCY_SECONDS = Histogram(
    "coti_safesync_db_write_latency_seconds",
    "Latency of DB write operations",
    ["table", "op_type"],
)

DB_LOCK_ACQUIRE_LATENCY_SECONDS = Histogram(
    "coti_safesync_db_lock_acquire_latency_seconds",
    "Latency of DB lock acquisition",
    ["strategy"],
)

# Queue metrics
QUEUE_MESSAGES_READ_TOTAL = Counter(
    "coti_safesync_queue_messages_read_total",
    "Total queue messages read",
    ["stream"],
)

QUEUE_MESSAGES_ACK_TOTAL = Counter(
    "coti_safesync_queue_messages_ack_total",
    "Total queue messages acknowledged",
    ["stream"],
)

QUEUE_MESSAGES_CLAIMED_TOTAL = Counter(
    "coti_safesync_queue_messages_claimed_total",
    "Total stale queue messages claimed",
    ["stream"],
)

QUEUE_READ_LATENCY_SECONDS = Histogram(
    "coti_safesync_queue_read_latency_seconds",
    "Latency of queue reads",
    ["stream"],
)

