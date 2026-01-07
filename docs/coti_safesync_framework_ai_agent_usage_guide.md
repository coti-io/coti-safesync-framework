# COTI SafeSync Framework Usage Guide for AI Agents

This document provides a practical reference for AI agents on how to use the `coti_safesync_framework` library's DB and Redis Streams components.

---

## Table of Contents

1. [Database (DB) Component](#database-db-component)
   - [DbSession: Transaction Management](#dbsession-transaction-management)
   - [Idempotent INSERTs](#idempotent-inserts)
   - [Optimistic Concurrency Control (OCC) Updates](#optimistic-concurrency-control-occ-updates)
   - [Basic SQL Execution](#basic-sql-execution)
   - [Common Patterns](#common-db-patterns)

2. [Redis Streams Component](#redis-streams-component)
   - [QueueConsumer: High-Level Message Consumption](#queueconsumer-high-level-message-consumption)
   - [RedisStreamsQueue: Low-Level Queue Operations](#redisstreamsqueue-low-level-queue-operations)
   - [Message Processing Patterns](#message-processing-patterns)
   - [Stale Message Recovery](#stale-message-recovery)

---

## Database (DB) Component

### DbSession: Transaction Management

`DbSession` is the primary interface for database operations. It represents a **single database transaction** that begins on `__enter__` and commits/rolls back on `__exit__`.

**⚠️ CRITICAL RULE**: Do NOT perform retry loops (e.g., OCC retries) inside a single `DbSession`. Each retry must use a new `DbSession` / transaction.

#### Basic Usage

```python
from sqlalchemy import create_engine
from coti_safesync_framework.db.session import DbSession

# Create engine (typically done once at application startup)
engine = create_engine("mysql+pymysql://user:password@host/database")

# Use DbSession as a context manager
with DbSession(engine) as session:
    # All operations use the same connection/transaction
    row = session.fetch_one(
        "SELECT id, amount FROM orders WHERE id = :id",
        {"id": 123}
    )
    
    if row:
        session.execute(
            "UPDATE orders SET amount = :amount WHERE id = :id",
            {"id": 123, "amount": row["amount"] + 10}
        )
    # Transaction commits automatically on exit (or rolls back on exception)
```

#### DbSession API

```python
class DbSession:
    def execute(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> int:
        """Execute a non-SELECT statement. Returns affected row count."""
    
    def fetch_one(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Execute a SELECT expected to return 0 or 1 row. Raises if more than one row."""
    
    def fetch_all(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Execute a SELECT returning multiple rows."""
    
    def execute_scalar(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> Any:
        """Execute a statement expected to return a single scalar value."""
```

### Idempotent INSERTs

For idempotent INSERT operations (safe under concurrent execution), use the `insert_idempotent` helper. It suppresses duplicate-key errors and returns `True` if inserted, `False` if already existed.

```python
from coti_safesync_framework.db.helpers import insert_idempotent

with DbSession(engine) as session:
    inserted = insert_idempotent(
        session=session,
        sql="INSERT INTO orders (id, amount, status) VALUES (:id, :amount, :status)",
        params={"id": 123, "amount": 100, "status": "pending"}
    )
    
    if inserted:
        print("Row was inserted")
    else:
        print("Row already existed (duplicate key suppressed)")
```

**When to use**: When INSERT operations are logically idempotent and you want to handle concurrent duplicate attempts gracefully.

**⚠️ Security**: Table and column names in SQL must be trusted identifiers (hardcoded or validated), not from user input.

### Optimistic Concurrency Control (OCC) Updates

For conditional updates based on version checking, use the `occ_update` helper. It automatically increments the version column and only updates if the version matches.

**⚠️ CRITICAL USAGE PATTERN**: Each OCC attempt must use its own `DbSession`. Retry when `rowcount == 0`.

```python
from coti_safesync_framework.db.helpers import occ_update

MAX_RETRIES = 10

for attempt in range(MAX_RETRIES):
    with DbSession(engine) as session:
        # Read current state
        row = session.fetch_one(
            "SELECT amount, version FROM orders WHERE id = :id",
            {"id": 42}
        )
        
        if not row:
            break  # Row doesn't exist
        
        # Attempt OCC update
        rowcount = occ_update(
            session=session,
            table="orders",
            id_column="id",
            id_value=42,
            version_column="version",
            version_value=row["version"],  # Expected version
            updates={"amount": row["amount"] + 10},  # New values
        )
        
        if rowcount == 1:
            # Success! Transaction commits on exit
            break
        # rowcount == 0 means version mismatch; retry with NEW transaction
    
    # If we get here, retry with a new DbSession
```

**Return values**:
- `1`: Update succeeded (version matched)
- `0`: Update failed (version mismatch, missing row, or no-op)

**⚠️ Security**: Table and column names must be trusted identifiers, not from user input.

### Basic SQL Execution

#### INSERT

```python
with DbSession(engine) as session:
    rowcount = session.execute(
        "INSERT INTO orders (id, amount, status) VALUES (:id, :amount, :status)",
        {"id": 123, "amount": 100, "status": "pending"}
    )
    # rowcount will be 1 if successful
```

#### UPDATE

```python
with DbSession(engine) as session:
    rowcount = session.execute(
        "UPDATE orders SET status = :status WHERE id = :id",
        {"id": 123, "status": "completed"}
    )
    # rowcount indicates how many rows were updated
```

#### SELECT (single row)

```python
with DbSession(engine) as session:
    row = session.fetch_one(
        "SELECT id, amount, status FROM orders WHERE id = :id",
        {"id": 123}
    )
    if row:
        print(f"Order {row['id']} has amount {row['amount']}")
    else:
        print("Order not found")
```

#### SELECT (multiple rows)

```python
with DbSession(engine) as session:
    rows = session.fetch_all(
        "SELECT id, amount FROM orders WHERE status = :status",
        {"status": "pending"}
    )
    for row in rows:
        print(f"Order {row['id']}: {row['amount']}")
```

#### Scalar queries (e.g., COUNT, MAX)

```python
with DbSession(engine) as session:
    count = session.execute_scalar(
        "SELECT COUNT(*) FROM orders WHERE status = :status",
        {"status": "pending"}
    )
    print(f"Pending orders: {count}")
```

### Common DB Patterns

#### Pattern 1: Read-Modify-Write with OCC

```python
MAX_RETRIES = 10

for _ in range(MAX_RETRIES):
    with DbSession(engine) as session:
        row = session.fetch_one(
            "SELECT balance, version FROM accounts WHERE id = :id",
            {"id": "account_123"}
        )
        
        if not row:
            raise ValueError("Account not found")
        
        new_balance = row["balance"] - 50
        
        rowcount = occ_update(
            session=session,
            table="accounts",
            id_column="id",
            id_value="account_123",
            version_column="version",
            version_value=row["version"],
            updates={"balance": new_balance},
        )
        
        if rowcount == 1:
            break  # Success
    # Retry with new transaction if version mismatch
```

#### Pattern 2: Idempotent INSERT with Duplicate Handling

```python
with DbSession(engine) as session:
    inserted = insert_idempotent(
        session=session,
        sql="INSERT INTO events (event_id, data) VALUES (:event_id, :data)",
        params={"event_id": "evt_123", "data": "some data"}
    )
    
    if not inserted:
        # Already exists, fetch it instead
        existing = session.fetch_one(
            "SELECT * FROM events WHERE event_id = :event_id",
            {"event_id": "evt_123"}
        )
        # Use existing event
```

#### Pattern 3: Transaction with Multiple Operations

```python
with DbSession(engine) as session:
    # Read
    order = session.fetch_one(
        "SELECT id, amount FROM orders WHERE id = :id",
        {"id": 123}
    )
    
    if not order:
        raise ValueError("Order not found")
    
    # Update order
    session.execute(
        "UPDATE orders SET status = :status WHERE id = :id",
        {"id": 123, "status": "processing"}
    )
    
    # Insert audit log
    session.execute(
        "INSERT INTO audit_log (order_id, action) VALUES (:order_id, :action)",
        {"order_id": 123, "action": "status_changed"}
    )
    
    # All operations commit together, or all roll back on exception
```

---

## Redis Streams Component

### QueueConsumer: High-Level Message Consumption

`QueueConsumer` is the recommended high-level API for consuming messages from Redis Streams. It provides:
- Message retrieval with blocking
- Graceful shutdown support
- Explicit acknowledgment
- Stale message claiming

**⚠️ IMPORTANT**: `QueueConsumer` provides **at-least-once delivery**, not exactly-once. Messages may be redelivered if ACK fails after commit. Handlers must be idempotent or use DB constraints/locks/OCC.

#### Basic Setup

```python
from redis import Redis
from coti_safesync_framework.config import QueueConfig
from coti_safesync_framework.queue.consumer import QueueConsumer

# Create Redis client
redis_client = Redis(host="localhost", port=6379, db=0)

# Configure queue
config = QueueConfig(
    stream_key="my_stream",           # Redis stream name
    consumer_group="my_group",       # Consumer group name
    consumer_name="worker_1",        # Unique consumer name
    claim_idle_ms=60_000,            # 60 seconds for stale messages
    block_ms=5_000,                  # Block 5 seconds when no messages
    max_read_count=1,                # Read 1 message at a time
)

# Create consumer
consumer = QueueConsumer(redis_client, config)
```

#### Pattern 1: Iterator-Based Consumption

```python
for msg in consumer.iter_messages():
    try:
        # Process message
        process_message(msg.payload)
        
        # Explicitly acknowledge after successful processing
        consumer.ack(msg)
    except Exception as e:
        # Don't ack on failure; message remains pending and can be reclaimed
        print(f"Failed to process message: {e}")
        # Optionally: log, alert, etc.
```

#### Pattern 2: Template-Method Runner (Recommended)

The `run()` method provides a template that:
1. Fetches a message
2. Opens a `DbSession` (one transaction)
3. Calls your handler
4. Commits the transaction
5. Acknowledges the message

```python
from sqlalchemy import create_engine
from coti_safesync_framework.queue.models import QueueMessage
from coti_safesync_framework.db.session import DbSession

engine = create_engine("mysql+pymysql://user:password@host/database")

def handle_message(msg: QueueMessage, session: DbSession) -> None:
    """Process a message within a database transaction."""
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
    
    # Insert audit log
    session.execute(
        "INSERT INTO audit_log (order_id, action) VALUES (:order_id, :action)",
        {"order_id": order_id, "action": "processed"}
    )
    # Transaction commits automatically on exit

# Run the consumer
consumer.run(handler=handle_message, engine=engine)
```

**Error handling**: If `handle_message` raises an exception:
- The `DbSession` transaction rolls back automatically
- The message is **NOT** acknowledged
- The exception is re-raised (not swallowed)
- The message remains pending and can be reclaimed later

#### Pattern 3: Manual Message Fetching

```python
while True:
    msg = consumer.next(block_ms=5_000)  # Block up to 5 seconds
    if msg is None:
        continue  # No message available
    
    try:
        process_message(msg.payload)
        consumer.ack(msg)
    except Exception:
        # Don't ack; message will remain pending
        pass
```

#### Graceful Shutdown

```python
import signal
from coti_safesync_framework.signals import install_termination_handlers

# Install signal handlers for graceful shutdown
install_termination_handlers(consumer.stop)

# Run consumer
consumer.run(handler=handle_message, engine=engine)
```

Or manually:

```python
# Signal shutdown
consumer.stop()

# Consumer will stop fetching new messages after current block timeout
# In-flight messages remain pending (not auto-acked)
```

### RedisStreamsQueue: Low-Level Queue Operations

`RedisStreamsQueue` provides direct access to Redis Streams operations. Use this when you need more control than `QueueConsumer` provides.

#### Enqueueing Messages

```python
from coti_safesync_framework.config import QueueConfig
from coti_safesync_framework.queue.redis_streams import RedisStreamsQueue

redis_client = Redis(host="localhost", port=6379, db=0)

config = QueueConfig(
    stream_key="my_stream",
    consumer_group="my_group",
    consumer_name="producer",
)

queue = RedisStreamsQueue(redis_client, config)

# Enqueue a message
entry_id = queue.enqueue({
    "order_id": 123,
    "action": "process",
    "timestamp": "2024-01-01T00:00:00Z"
})
print(f"Message enqueued with ID: {entry_id}")
```

#### Reading Messages

```python
# Read messages (blocking)
messages = queue.read(block_ms=5_000, count=1)

for msg in messages:
    print(f"Message ID: {msg.id}")
    print(f"Payload: {msg.payload}")
    # Process message...
    queue.ack(msg)  # Acknowledge after processing
```

#### Acknowledging Messages

```python
from coti_safesync_framework.queue.models import QueueMessage

# After processing a message
queue.ack(msg)  # msg is a QueueMessage object
```

#### Claiming Stale Messages

```python
# Claim messages that have been pending for at least 60 seconds
stale_messages = queue.claim_stale(min_idle_ms=60_000, count=10)

for msg in stale_messages:
    try:
        process_message(msg.payload)
        queue.ack(msg)
    except Exception:
        # Don't ack; will be reclaimed again later
        pass
```

### Message Processing Patterns

#### Pattern 1: Idempotent Processing with DB Constraints

```python
def handle_message(msg: QueueMessage, session: DbSession) -> None:
    """Handler that uses DB constraints for idempotency."""
    event_id = msg.payload["event_id"]
    
    # Use idempotent insert to handle duplicates
    from coti_safesync_framework.db.helpers import insert_idempotent
    
    inserted = insert_idempotent(
        session=session,
        sql="INSERT INTO processed_events (event_id, data) VALUES (:event_id, :data)",
        params={"event_id": event_id, "data": msg.payload["data"]}
    )
    
    if not inserted:
        # Already processed, skip
        return
    
    # Process the event
    process_event(msg.payload)
```

#### Pattern 2: Processing with OCC Updates

```python
def handle_message(msg: QueueMessage, session: DbSession) -> None:
    """Handler that uses OCC for safe concurrent updates."""
    order_id = msg.payload["order_id"]
    
    MAX_RETRIES = 10
    for attempt in range(MAX_RETRIES):
        with DbSession(engine) as session:
            row = session.fetch_one(
                "SELECT balance, version FROM accounts WHERE id = :id",
                {"id": order_id}
            )
            
            if not row:
                raise ValueError("Account not found")
            
            new_balance = row["balance"] - msg.payload["amount"]
            
            rowcount = occ_update(
                session=session,
                table="accounts",
                id_column="id",
                id_value=order_id,
                version_column="version",
                version_value=row["version"],
                updates={"balance": new_balance},
            )
            
            if rowcount == 1:
                break  # Success
        # Retry with new transaction if needed
```

#### Pattern 3: Processing with Error Handling

```python
def handle_message(msg: QueueMessage, session: DbSession) -> None:
    """Handler with explicit error handling."""
    try:
        order_id = msg.payload["order_id"]
        
        # Validate payload
        if not order_id:
            raise ValueError("Missing order_id in payload")
        
        # Process
        session.execute(
            "UPDATE orders SET status = :status WHERE id = :id",
            {"id": order_id, "status": "processed"}
        )
        
    except ValueError as e:
        # Validation error - don't retry
        log_error(f"Invalid message: {e}")
        raise  # Re-raise to prevent ack
    except Exception as e:
        # Unexpected error - may retry
        log_error(f"Processing failed: {e}")
        raise  # Re-raise to prevent ack
```

### Stale Message Recovery

Stale messages are messages that were delivered but not acknowledged. They remain in the pending list and can be reclaimed.

#### Automatic Recovery Pattern

```python
import time

consumer = QueueConsumer(redis_client, config)

# Periodic stale message recovery
last_recovery = time.time()
RECOVERY_INTERVAL = 300  # 5 minutes

for msg in consumer.iter_messages():
    try:
        process_message(msg.payload)
        consumer.ack(msg)
    except Exception:
        pass
    
    # Periodically claim stale messages
    if time.time() - last_recovery > RECOVERY_INTERVAL:
        stale = consumer.claim_stale(min_idle_ms=60_000)
        for stale_msg in stale:
            try:
                process_message(stale_msg.payload)
                consumer.ack(stale_msg)
            except Exception:
                pass
        last_recovery = time.time()
```

#### Dedicated Recovery Worker (Manual Pattern)

```python
# In a separate worker/thread
def recovery_worker():
    while not consumer._stopping.is_set():
        stale = consumer.claim_stale(min_idle_ms=60_000, count=10)
        for msg in stale:
            try:
                process_message(msg.payload)
                consumer.ack(msg)
            except Exception:
                pass
        time.sleep(60)  # Check every minute
```

#### Template-Method Recovery Worker (Recommended)

The `run_claim_stale()` method provides a template-method pattern for stale message recovery, similar to `run()` but for recovery:

```python
from sqlalchemy import create_engine
from coti_safesync_framework.queue.models import QueueMessage
from coti_safesync_framework.db.session import DbSession

engine = create_engine("mysql+pymysql://user:password@host/database")

def handle_message(msg: QueueMessage, session: DbSession) -> None:
    """Same handler used in main consumer - ensures consistency."""
    order_id = msg.payload["order_id"]
    
    # Process message within transaction
    session.execute(
        "UPDATE orders SET status = :status WHERE id = :id",
        {"id": order_id, "status": "processed"}
    )
    # Transaction commits automatically, message ACKed after commit

# In a separate process/worker
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

**How it works**: `run_claim_stale()` loops until `stop()` is called, periodically checking for stale messages (every `claim_interval_ms`). For each claimed message, it:
1. Opens a `DbSession` (one transaction)
2. Calls your handler
3. Commits the transaction
4. Acknowledges the message

**Error handling**: If the handler raises an exception:
- The transaction rolls back automatically
- The message is **NOT** acknowledged
- The exception is re-raised (not swallowed)
- The message remains pending and may be reclaimed again later

**Parameters**:
- `handler`: Same handler function used in `run()` for consistency
- `engine`: SQLAlchemy Engine for database sessions
- `min_idle_ms`: Minimum idle time for a message to be considered stale. If `None`, uses `config.claim_idle_ms`
- `claim_interval_ms`: How often to check for stale messages (default: 5,000 ms)
- `max_claim_count`: Maximum messages to claim per iteration (default: 10)

---

## Error Handling

### DB Errors

- `DbWriteError`: General DB write failures
- `LockAcquisitionError`: Failed to acquire a lock
- `IntegrityError`: Duplicate key or constraint violation (from SQLAlchemy)

### Queue Errors

- `QueueError`: General queue-related issues (Redis failures, invalid format, etc.)

### Best Practices

1. **Always use context managers** for `DbSession` to ensure proper transaction handling
2. **Don't retry inside a single `DbSession`** - use a new session for each retry
3. **Acknowledge messages only after successful processing**
4. **Make handlers idempotent** or use DB constraints/OCC to handle duplicates
5. **Don't swallow exceptions** - let them propagate so messages remain pending for retry

---

## Summary

### DB Component Quick Reference

- **Transaction**: Use `DbSession` as a context manager
- **Idempotent INSERT**: Use `insert_idempotent()` helper
- **OCC Update**: Use `occ_update()` helper with retry loop (new session per retry)
- **SQL Execution**: Use `execute()`, `fetch_one()`, `fetch_all()`, `execute_scalar()`

### Redis Streams Component Quick Reference

- **High-level consumption**: Use `QueueConsumer.run()` with handler function
- **Low-level operations**: Use `RedisStreamsQueue` for direct control
- **Message acknowledgment**: Always call `ack()` explicitly after successful processing
- **Stale messages**: Use `claim_stale()` for recovery (not automatic)
- **Graceful shutdown**: Use `stop()` or signal handlers

---

## Additional Resources

- `docs/bootstrap_legacy.md`: Complete architecture specification
- `docs/queue_consumer_bootstrap.md`: QueueConsumer design principles
- `docs/occ.md`: Optimistic Concurrency Control detailed guide
- `docs/LOCKING_STRATEGIES.md`: Locking strategies documentation

