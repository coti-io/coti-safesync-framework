# COTI SafeSync Framework

**Safe concurrent MySQL writes and Redis Streams queue operations**

COTI SafeSync Framework is a Python library for building robust, concurrent backend systems. It provides explicit concurrency control primitives for MySQL and safe message consumption from Redis Streams, designed for multi-process and multi-host environments.

## Features

- 🔒 **Explicit concurrency control** - Pessimistic locking, optimistic concurrency control (OCC), and atomic SQL operations
- 🗄️ **Transactional MySQL operations** - Safe, explicit transaction management with `DbSession`
- 📨 **Redis Streams queue consumption** - At-least-once delivery with explicit acknowledgment
- 🚀 **Multi-process safe** - Designed for distributed systems with multiple workers
- 📊 **Prometheus metrics** - Built-in observability for operations and locks
- 🎯 **Framework-agnostic** - Works with FastAPI, CLI workers, schedulers, etc.

## Installation

```bash
pip install coti-safesync-framework
```

## Requirements

- Python >= 3.11
- MySQL 8.0+ with InnoDB storage engine
- Redis 5.0+ (for queue operations)

## Quick Start

### Database Operations

```python
from sqlalchemy import create_engine
from coti_safesync_framework.db.session import DbSession

# Create engine (typically done once at application startup)
engine = create_engine("mysql+pymysql://user:password@host/database")

# Use DbSession for transactional operations
with DbSession(engine) as session:
    # Execute SQL
    session.execute(
        "UPDATE accounts SET balance = balance + :amount WHERE id = :id",
        {"id": 123, "amount": 100}
    )
    # Transaction commits automatically on success
```

### Queue Consumption

```python
from redis import Redis
from coti_safesync_framework.config import QueueConfig
from coti_safesync_framework.queue.consumer import QueueConsumer
from coti_safesync_framework.db.session import DbSession
from coti_safesync_framework.queue.models import QueueMessage

# Setup
redis_client = Redis(host="localhost", port=6379)
config = QueueConfig(
    stream_key="orders",
    consumer_group="workers",
    consumer_name="worker_1"
)
consumer = QueueConsumer(redis_client, config)

# Process messages
def handle_message(msg: QueueMessage, session: DbSession) -> None:
    order_id = msg.payload["order_id"]
    session.execute(
        "UPDATE orders SET status = 'processed' WHERE id = :id",
        {"id": order_id}
    )

consumer.run(handler=handle_message, engine=engine)
```

## Database Examples

### 1. Atomic SQL Updates

For simple operations that can be expressed as a single SQL statement:

```python
from coti_safesync_framework.db.session import DbSession

def increment_counter(engine, counter_id: int) -> None:
    """Increment a counter atomically - no locks needed."""
    with DbSession(engine) as session:
        session.execute(
            "UPDATE counters SET value = value + 1 WHERE id = :id",
            {"id": counter_id}
        )
        # MySQL guarantees atomicity for single statements
```

### 2. Pessimistic Row Locking

When you need strict serialization for read-modify-write operations:

```python
from coti_safesync_framework.db.session import DbSession
from coti_safesync_framework.db.locking.row_lock import RowLock

def process_order(engine, order_id: int) -> None:
    """Process an order - only one worker can process a specific order."""
    with DbSession(engine) as session:
        # Acquire exclusive lock on the order row
        order = RowLock(session, "orders", {"id": order_id}).acquire()
        
        if order is None:
            return  # Order doesn't exist
        
        if order["status"] == "processed":
            return  # Already processed
        
        # Safe to modify - we hold the lock
        session.execute(
            "UPDATE orders SET status = :status WHERE id = :id",
            {"id": order_id, "status": "processed"}
        )
        # Lock released when transaction commits
```

### 3. Optimistic Concurrency Control (OCC)

For high-throughput scenarios where conflicts are rare:

```python
from coti_safesync_framework.db.session import DbSession
from coti_safesync_framework.db.helpers import occ_update
import time
import random

def update_account_balance(engine, account_id: int, amount_change: int) -> None:
    """Update account balance using OCC with retry."""
    MAX_RETRIES = 10
    
    for attempt in range(MAX_RETRIES):
        with DbSession(engine) as session:
            # Read current balance and version
            account = session.fetch_one(
                "SELECT balance, version FROM accounts WHERE id = :id",
                {"id": account_id}
            )
            
            if account is None:
                raise ValueError(f"Account {account_id} not found")
            
            # Attempt OCC update
            rowcount = occ_update(
                session=session,
                table="accounts",
                id_column="id",
                id_value=account_id,
                version_column="version",
                version_value=account["version"],
                updates={"balance": account["balance"] + amount_change}
            )
            
            if rowcount == 1:
                return  # Success!
        
        # Version mismatch - retry with new transaction
        time.sleep(random.uniform(0.001, 0.01))
    
    raise RuntimeError(f"Failed to update account after {MAX_RETRIES} retries")
```

**Important**: Each OCC attempt must use a **new transaction**. Never retry inside a single `DbSession`.

### 4. Advisory Locks

For application-level synchronization across multiple tables:

```python
from coti_safesync_framework.db.session import DbSession
from coti_safesync_framework.db.locking.advisory_lock import AdvisoryLock
from coti_safesync_framework.errors import LockTimeoutError

def process_user_data(engine, user_id: int) -> None:
    """Process all data for a user - only one worker at a time."""
    lock_key = f"user_processing:{user_id}"
    
    try:
        with DbSession(engine) as session:
            with AdvisoryLock(session, lock_key, timeout=10):
                # Lock acquired - we're the only worker processing this user
                
                # Read user's orders
                orders = session.fetch_all(
                    "SELECT id, total FROM orders WHERE user_id = :user_id",
                    {"user_id": user_id}
                )
                
                # Update user's summary
                total_spent = sum(order["total"] for order in orders)
                session.execute(
                    "UPDATE users SET total_spent = :total WHERE id = :id",
                    {"id": user_id, "total": total_spent}
                )
                # Lock released when connection closes (after commit)
                
    except LockTimeoutError:
        # Another worker is processing this user
        print(f"Could not acquire lock for user {user_id}")
```

### 5. Idempotent INSERTs

For safe duplicate inserts using database constraints:

```python
from coti_safesync_framework.db.session import DbSession
from coti_safesync_framework.db.helpers import insert_idempotent

def create_user_profile(engine, user_id: int, initial_data: dict) -> None:
    """Create user profile - safe to call multiple times."""
    with DbSession(engine) as session:
        inserted = insert_idempotent(
            session,
            """
            INSERT INTO user_profiles (user_id, display_name, created_at)
            VALUES (:user_id, :display_name, NOW())
            """,
            {
                "user_id": user_id,
                "display_name": initial_data.get("display_name", "User")
            }
        )
        
        if inserted:
            print("Profile created")
        else:
            print("Profile already exists")
```

## Queue Examples

### 1. Basic Message Consumption

```python
from redis import Redis
from coti_safesync_framework.config import QueueConfig
from coti_safesync_framework.queue.consumer import QueueConsumer

redis_client = Redis(host="localhost", port=6379)
config = QueueConfig(
    stream_key="orders",
    consumer_group="workers",
    consumer_name="worker_1",
    block_ms=5_000,  # Block 5 seconds when no messages
)

consumer = QueueConsumer(redis_client, config)

# Iterator-based consumption
for msg in consumer.iter_messages():
    try:
        process_message(msg.payload)
        consumer.ack(msg)  # Acknowledge after successful processing
    except Exception as e:
        # Don't ack on failure - message remains pending
        print(f"Failed to process: {e}")
```

### 2. Template-Method Pattern (Recommended)

The `run()` method handles the complete flow: fetch → process → commit → ack:

```python
from sqlalchemy import create_engine
from coti_safesync_framework.queue.models import QueueMessage
from coti_safesync_framework.db.session import DbSession

engine = create_engine("mysql+pymysql://user:password@host/database")

def handle_message(msg: QueueMessage, session: DbSession) -> None:
    """Process message within a database transaction."""
    order_id = msg.payload["order_id"]
    
    # Read current state
    order = session.fetch_one(
        "SELECT id, status FROM orders WHERE id = :id",
        {"id": order_id}
    )
    
    if not order:
        raise ValueError(f"Order {order_id} not found")
    
    # Update order
    session.execute(
        "UPDATE orders SET status = :status WHERE id = :id",
        {"id": order_id, "status": "processed"}
    )
    # Transaction commits automatically on exit
    # Message is ACKed after commit

# Run the consumer
consumer.run(handler=handle_message, engine=engine)
```

**Error handling**: If `handle_message` raises an exception:
- Transaction rolls back automatically
- Message is **NOT** acknowledged
- Message remains pending for retry

### 3. Stale Message Recovery

Messages may become stale if a worker crashes before acknowledging them. These messages remain pending in Redis and are not automatically redelivered. Use `run_claim_stale()` in a separate worker process to recover stale messages:

```python
def recovery_worker():
    """Run in a separate process to recover stale messages."""
    consumer = QueueConsumer(redis_client, config)
    
    consumer.run_claim_stale(
        handler=handle_message,  # Same handler as main consumer
        engine=engine,
        min_idle_ms=60_000,  # Claim messages idle > 60 seconds
        claim_interval_ms=5_000,  # Check every 5 seconds
        max_claim_count=10  # Claim up to 10 messages per check
    )
```

**How it works**: `run_claim_stale()` periodically checks for stale messages (every `claim_interval_ms`), claims them, and processes them using the same handler pattern as `run()`. It loops until `stop()` is called.

**Important**: Run the recovery worker in a separate process alongside your main consumer. The recovery worker should use the same `handler` function for consistency.

### 4. Manual Message Fetching

For more control over the consumption loop:

```python
while not consumer._stopping.is_set():
    msg = consumer.next(block_ms=5_000)
    if msg is None:
        continue  # No message available
    
    try:
        with DbSession(engine) as session:
            process_message(msg.payload, session)
        consumer.ack(msg)
    except Exception:
        # Transaction rolled back, message not acked
        raise
```

### 5. Graceful Shutdown

```python
import signal

consumer = QueueConsumer(redis_client, config)

def shutdown_handler(signum, frame):
    consumer.stop()

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# Consumer will stop after current message completes
consumer.run(handler=handle_message, engine=engine)
```

## Concurrency Strategies

COTI SafeSync Framework provides multiple strategies for safe concurrent operations:

| Strategy | Use When | Performance | Contention |
|----------|----------|-------------|------------|
| **Atomic SQL** | Single-statement operations | Highest | Low |
| **Idempotent INSERT** | One-time initialization | High | Low |
| **OCC** | Low contention, can retry | High | Low |
| **Row Lock** | Need strict serialization | Medium | High |
| **Advisory Lock** | Cross-table synchronization | Medium | Medium |

See [LOCKING_STRATEGIES.md](docs/LOCKING_STRATEGIES.md) for detailed guidance.

## Design Principles

1. **Explicit over implicit** - Locks and transactions are always explicit
2. **Primitives, not workflows** - Building blocks you compose
3. **Control stays with you** - You compose logic inside locks/transactions
4. **No magic retries** - Retry logic is your decision
5. **Framework-agnostic** - Works with any Python framework

## Important Notes

### Database Transactions

- ⚠️ **Never retry inside a single `DbSession`** - Each retry must use a new transaction
- ⚠️ **Keep transactions short** - Long-held locks increase contention
- ⚠️ **Index WHERE clauses** - Non-indexed predicates can cause performance issues

### Queue Semantics

- ⚠️ **At-least-once delivery** - Messages may be redelivered if ACK fails
- ⚠️ **Handlers must be idempotent** - Or use DB constraints/locks/OCC
- ⚠️ **Stale message recovery** - Run a separate recovery worker for stale messages

### OCC Usage

- ⚠️ **Each attempt uses a new transaction** - Never retry inside `DbSession`
- ⚠️ **Must retry on `rowcount == 0`** - Indicates version mismatch
- ⚠️ **Must re-read before retrying** - Don't reuse stale data

See [docs/occ.md](docs/occ.md) for the complete OCC usage guide.

## Metrics

COTI SafeSync Framework exposes Prometheus metrics:

- `coti_safesync_db_write_total` - DB write operation counts
- `coti_safesync_db_write_latency_seconds` - DB write latencies
- `coti_safesync_db_lock_acquire_latency_seconds` - Lock acquisition timing
- `coti_safesync_queue_messages_read_total` - Queue message reads
- `coti_safesync_queue_messages_ack_total` - Message acknowledgments
- `coti_safesync_queue_messages_claimed_total` - Stale messages claimed

## Documentation

- [Complete API Reference](docs/bootstrap.md) - Authoritative design document
- [Locking Strategies Guide](docs/LOCKING_STRATEGIES.md) - When to use each strategy
- [OCC Usage Guide](docs/occ.md) - Optimistic concurrency control patterns
- [Queue Consumer Guide](docs/queue_consumer_bootstrap.md) - Redis Streams patterns

## Requirements

- **Database**: MySQL 8.0+ with InnoDB storage engine
- **Queue**: Redis 5.0+ with Streams support
- **Python**: >= 3.11

## License

MIT

## Author

COTI - dev@coti.io
