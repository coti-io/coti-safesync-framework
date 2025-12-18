````markdown
# conquiet – Bootstrap Specification

This document describes the architecture and low-level design of the **`conquiet`** Python package so that code can be generated methodically and precisely.

`conquiet` (concurrent + quiet) is an internal library used across backend services at COTI to provide:

- **Safe concurrent MySQL writes** (INSERT and UPDATE) from multiple hosts/processes.
- **Safe concurrent queue operations** using Redis Streams (multi-host consumer groups).
- **Configurable locking strategies** for DB writes (row, table, advisory, advisory+row, none).
- **Graceful shutdown** (SIGTERM-aware).
- **Prometheus metrics** for DB and queue operations.

The **DB subsystem** and the **Queue subsystem** are **completely independent**. There is **no coupling** between them in this package.

---

## 1. Technology & Core Principles

### 1.1 Tech stack

- **Language**: Python 3.11+ (assumed)
- **DB**: MySQL (only)
- **DB access**: `sqlalchemy.Engine` for connection pooling and transaction handling
- **DB queries**: **raw MySQL SQL** using `sqlalchemy.text`
- **Queue**: Redis Streams (via `redis` Python client)
- **Metrics**: `prometheus_client`
- **Signals**: `SIGTERM` / `SIGINT` for graceful shutdown

### 1.2 Core principles

- DB and queue functionality are **separate** and can be used independently.
- All DB writes must be **safe under concurrency** across processes/hosts.
- All queue operations must be **safe across hosts** using Redis consumer groups.
- The library never hides business logic. For queues:
  - The user **receives messages**, processes them however they want, and then **explicitly acknowledges** them.
- Locking strategies are **explicitly selected** by users.
- All DB and queue operations should expose **Prometheus metrics**.
- Graceful shutdown must:
  - Stop fetching new work on SIGTERM.
  - Allow in-flight operations to finish best-effort.
  - Cleanly close DB and Redis resources.

---

## 2. Package Layout

Create the following package structure:

```text
conquiet/
  __init__.py
  config.py
  errors.py
  signals.py

  db/
    __init__.py
    models.py
    locking/
      __init__.py
      base.py
      strategy.py
      advisory_mysql.py
      row_mysql.py
      table_mysql.py
      composite.py
    writer.py
    metrics.py

  queue/
    __init__.py
    models.py
    redis_streams.py
    consumer.py
    metrics.py

  metrics/
    __init__.py
    registry.py
````

Top-level `__init__.py` should expose primary entry points:

* `DbWriter`
* `LockStrategy`
* `RedisStreamsQueue`
* `QueueConsumer`

Example export:

```python
from .db.writer import DbWriter
from .db.locking.strategy import LockStrategy
from .queue.redis_streams import RedisStreamsQueue
from .queue.consumer import QueueConsumer

__all__ = ["DbWriter", "LockStrategy", "RedisStreamsQueue", "QueueConsumer"]
```

---

## 3. Shared Configuration & Errors

### 3.1 `conquiet/config.py`

Provide basic configuration dataclasses used by both subsystems.

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class DbConfig:
    table_name: str
    op_type=DbOperationType.INSERT
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
```

### 3.2 `conquiet/errors.py`

Define library-specific exceptions:

```python
class ConquietError(Exception):
    """Base exception for conquiet errors."""

class DbWriteError(ConquietError):
    """Any failure during DB write."""

class LockAcquisitionError(ConquietError):
    """Failed to acquire a lock within the expected constraints."""

class QueueError(ConquietError):
    """General queue-related issues."""
```

---

## 4. Signal Handling

### 4.1 `conquiet/signals.py`

Provide simple helpers to install SIGTERM/SIGINT handlers that call a provided callback.

```python
import signal
import logging
from typing import Callable

def install_termination_handlers(stop_callback: Callable[[], None]) -> None:
    """
    Install SIGTERM and SIGINT handlers that call stop_callback exactly once.
    """

    def handler(signum, frame):
        logging.info("Received signal %s, initiating graceful shutdown", signum)
        stop_callback()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
```

This is primarily intended for queue consumers that run loops. DB writes are direct calls and do not require a worker loop.

---

## 5. DB Subsystem

The DB subsystem provides **direct** insert/update operations to MySQL, safe across multiple hosts/processes, with configurable locking.

There is **no queue integration** in this subsystem.

INSERT and UPDATE operations have different concurrency semantics.

INSERT operations are idempotent and non-locking. They rely on MySQL uniqueness constraints to prevent duplicates. Duplicate-key errors are expected under concurrency, logged, and suppressed.

UPDATE operations are correctness-critical and may involve read–modify–write cycles. UPDATE operations may be executed with configurable locking strategies to prevent lost updates under concurrent access.

### 5.1 Models: `conquiet/db/models.py`

Define data types representing DB operations:

```python
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional

class DbOperationType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"

@dataclass
class DbOperation:
    """
    A single DB mutation operation.
    """
    table: str
    op_type: DbOperationType
    id_value: Any  # primary key or unique identifier
    payload: Mapping[str, Any]  # column -> value
    # metadata is optional contextual info, not used by core logic
    metadata: Optional[Mapping[str, Any]] = None
```

### 5.2 Lock Strategies: `conquiet/db/locking/strategy.py`

Define the lock strategy enumeration:

```python
from enum import Enum

class LockStrategy(str, Enum):
    ADVISORY = "advisory"
    ROW = "row"
    ADVISORY_AND_ROW = "advisory_and_row"
    TABLE = "table"
    NONE = "none"
```

The only allowed combination is `ADVISORY_AND_ROW`. No other combos are supported.

INSERT

No locking is applied.

INSERT relies on MySQL unique constraints.

Duplicate key exceptions are:

caught

logged

not rethrown

INSERT is considered successful if:

the row exists after execution (regardless of who inserted it).

UPDATE

UPDATE operations may be executed with a LockStrategy.

UPDATE correctness assumes potential read–modify–write behavior.

Without locking, lost updates are possible and expected.

With locking, UPDATE correctness is guaranteed.

Lock Strategy Applicability:
Locking strategies apply **only to UPDATE operations**.

INSERT operations never acquire locks, regardless of the configured LockStrategy.

This is an intentional design decision to maximize throughput for idempotent inserts while preserving correctness for concurrent updates.


### 5.3 Lock Backend Base: `conquiet/db/locking/base.py`

Locks should operate **on the same DB connection/transaction** that the writer uses. Therefore, lock backends receive a `Connection` object instead of using the engine directly.

```python
from abc import ABC, abstractmethod
from sqlalchemy.engine import Connection
from ..models import DbOperation

class LockBackend(ABC):
    """
    Abstract base for MySQL lock backends.
    All locking is performed using the provided Connection.
    """

    @abstractmethod
    def acquire(self, conn: Connection, op: DbOperation) -> None:
        """Acquire the lock for this operation on the given connection."""
        ...

    @abstractmethod
    def release(self, conn: Connection, op: DbOperation) -> None:
        """Release the lock for this operation on the given connection."""
        ...
```

### 5.4 Advisory Lock Backend: `conquiet/db/locking/advisory_mysql.py`

Uses MySQL `GET_LOCK` and `RELEASE_LOCK` with a lock name derived from the table and id.

```python
from sqlalchemy import text
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation
from ...errors import LockAcquisitionError

class MySQLAdvisoryLockBackend(LockBackend):
    def __init__(self, lock_timeout: int = 10, prefix: str = "conquiet_lock"):
        self.lock_timeout = lock_timeout
        self.prefix = prefix

    def _lock_name(self, op: DbOperation) -> str:
        return f"{self.prefix}:{op.table}:{op.id_value}"

    def acquire(self, conn: Connection, op: DbOperation) -> None:
        lock_name = self._lock_name(op)
        stmt = text("SELECT GET_LOCK(:lock_name, :timeout)")
        res = conn.execute(stmt, {"lock_name": lock_name, "timeout": self.lock_timeout}).scalar()

        if res != 1:
            raise LockAcquisitionError(f"Failed to acquire advisory lock '{lock_name}'")

    def release(self, conn: Connection, op: DbOperation) -> None:
        lock_name = self._lock_name(op)
        stmt = text("SELECT RELEASE_LOCK(:lock_name)")
        conn.execute(stmt, {"lock_name": lock_name})
```

### 5.5 Row Lock Backend: `conquiet/db/locking/row_mysql.py`

Uses `SELECT ... FOR UPDATE` to lock the row within the active transaction. It assumes that either the row exists or a gap lock will protect inserts.

```python
from sqlalchemy import text
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation

class MySQLRowLockBackend(LockBackend):
    def __init__(self, id_column: str = "id"):
        self.id_column = id_column

    def acquire(self, conn: Connection, op: DbOperation) -> None:
        stmt = text(
            f"SELECT {self.id_column} FROM {op.table} "
            f"WHERE {self.id_column} = :id FOR UPDATE"
        )
        conn.execute(stmt, {"id": op.id_value})

    def release(self, conn: Connection, op: DbOperation) -> None:
        # Row locks are released when the surrounding transaction commits/rolls back.
        # Nothing special to do here.
        return None
```

### 5.6 Table Lock Backend: `conquiet/db/locking/table_mysql.py`

Uses `LOCK TABLES ... WRITE` / `UNLOCK TABLES`. These must be on the same connection as the write.

```python
from sqlalchemy import text
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation

class MySQLTableLockBackend(LockBackend):
    def acquire(self, conn: Connection, op: DbOperation) -> None:
        stmt = text(f"LOCK TABLES {op.table} WRITE")
        conn.execute(stmt)

    def release(self, conn: Connection, op: DbOperation) -> None:
        conn.execute(text("UNLOCK TABLES"))
```

### 5.7 Composite Lock Backend: `conquiet/db/locking/composite.py`

Only used for `ADVISORY_AND_ROW`.

```python
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation

class CompositeLockBackend(LockBackend):
    """
    Applies multiple lock backends in sequence.
    Only used for the ADVISORY_AND_ROW strategy.
    """

    def __init__(self, backends: list[LockBackend]):
        self.backends = backends

    def acquire(self, conn: Connection, op: DbOperation) -> None:
        for backend in self.backends:
            backend.acquire(conn, op)

    def release(self, conn: Connection, op: DbOperation) -> None:
        for backend in reversed(self.backends):
            backend.release(conn, op)
```

### 5.8 Lock Backend Factory: `conquiet/db/locking/__init__.py`

```python
from sqlalchemy.engine import Engine
from .strategy import LockStrategy
from .base import LockBackend
from .advisory_mysql import MySQLAdvisoryLockBackend
from .row_mysql import MySQLRowLockBackend
from .table_mysql import MySQLTableLockBackend
from .composite import CompositeLockBackend

def make_lock_backend(strategy: LockStrategy, db_config) -> LockBackend | None:
    if strategy == LockStrategy.NONE:
        return None

    if strategy == LockStrategy.ADVISORY:
        return MySQLAdvisoryLockBackend()

    if strategy == LockStrategy.ROW:
        return MySQLRowLockBackend(id_column=db_config.id_column)

    if strategy == LockStrategy.ADVISORY_AND_ROW:
        return CompositeLockBackend([
            MySQLAdvisoryLockBackend(),
            MySQLRowLockBackend(id_column=db_config.id_column),
        ])

    if strategy == LockStrategy.TABLE:
        return MySQLTableLockBackend()

    raise ValueError(f"Unknown lock strategy: {strategy}")
```

### 5.9 DB Metrics: `conquiet/db/metrics.py`

Use `prometheus_client` to define DB-related metrics. Implementation details can be simple counters/histograms.

Metric examples:

* `conquiet_db_write_total{table, op_type, status}` (Counter)
* `conquiet_db_write_latency_seconds{table, op_type}` (Histogram)
* `conquiet_db_lock_acquire_latency_seconds{strategy}` (Histogram)

Provide helper functions:

```python
def observe_db_write(table: str, op_type: str, status: str, latency_s: float) -> None:
    ...

def observe_lock_acquisition(strategy: str, latency_s: float, success: bool) -> None:
    ...
```

### 5.10 DbWriter: `conquiet/db/writer.py`

This is the main entry point for DB writes.

```python
import time
from sqlalchemy.engine import Engine
from sqlalchemy import text
from .models import DbOperation, DbOperationType
from .locking import make_lock_backend
from .locking.strategy import LockStrategy
from .metrics import observe_db_write, observe_lock_acquisition
from ..config import DbConfig
from ..errors import DbWriteError

class DbWriter:
    def __init__(
        self,
        engine: Engine,
        db_config: DbConfig,
        lock_strategy: LockStrategy = LockStrategy.NONE,
    ) -> None:
        self.engine = engine
        self.db_config = db_config
        self.lock_strategy = lock_strategy
        self.lock_backend = make_lock_backend(lock_strategy, db_config)

    def execute(self, op: DbOperation) -> None:
        """
        Perform the DB operation with appropriate locking.
        Raises DbWriteError on failure.
        """
        start_time = time.monotonic()
        status = "success"

        try:
            with self.engine.begin() as conn:
                # Lock (if any)
                if self.lock_backend:
                    lock_start = time.monotonic()
                    self.lock_backend.acquire(conn, op)
                    lock_latency = time.monotonic() - lock_start
                    observe_lock_acquisition(self.lock_strategy.value, lock_latency, True)

                # Perform operation
                if op.op_type == DbOperationType.INSERT:
                    self._insert(conn, op)
                elif op.op_type == DbOperationType.UPDATE:
                    self._update(conn, op)
                else:
                    raise DbWriteError(f"Unsupported operation type: {op.op_type}")

                # Release lock (if any)
                if self.lock_backend:
                    self.lock_backend.release(conn, op)

        except Exception as exc:
            status = "error"
            raise DbWriteError(str(exc)) from exc
        finally:
            latency = time.monotonic() - start_time
            observe_db_write(op.table, op.op_type.value, status, latency)

    # NOTE: the only mode allowed will be INSERT not INSERT IGNORE or UPSERT.
    def _insert(self, conn, op: DbOperation) -> None:
        cols = list(op.payload.keys())
        col_names = ", ".join(cols)
        placeholders = ", ".join(f":{c}" for c in cols)

        base_sql = f"INSERT INTO {op.table} ({col_names}) VALUES ({placeholders})"

        if self.db_config.insert_mode == "insert_ignore":
            sql = base_sql.replace("INSERT", "INSERT IGNORE", 1)
        elif self.db_config.insert_mode == "upsert":
            # simple "ON DUPLICATE KEY UPDATE" using payload values
            updates = ", ".join(f"{c}=VALUES({c})" for c in cols)
            sql = f"{base_sql} ON DUPLICATE KEY UPDATE {updates}"
        else:
            sql = base_sql

        stmt = text(sql)
        conn.execute(stmt, op.payload)

    def _update(self, conn, op: DbOperation) -> None:
        # UPDATE based on primary key
        cols = [c for c in op.payload.keys() if c != self.db_config.id_column]
        if not cols:
            return  # nothing to update

        set_clause = ", ".join(f"{c} = :{c}" for c in cols)
        sql = (
            f"UPDATE {op.table} SET {set_clause} "
            f"WHERE {self.db_config.id_column} = :id_value"
        )

        params = dict(op.payload)
        params["id_value"] = op.id_value

        stmt = text(sql)
        conn.execute(stmt, params)
```

DbWriter.execute(op) behaves as follows:

- If op_type == INSERT:
  - Execute INSERT inside a transaction
  - Catch and log duplicate-key errors
  - Do not acquire or release any locks
  - Do not rethrow duplicate-key exceptions

- If op_type == UPDATE:
  - Start a transaction
  - Acquire lock according to LockStrategy
  - Execute UPDATE
  - Release lock (if applicable)
  - Commit transaction


Usage example (not part of the file, but for context):

```python
from sqlalchemy import create_engine
from conquiet.db.writer import DbWriter
from conquiet.db.models import DbOperation, DbOperationType
from conquiet.db.locking.strategy import LockStrategy
from conquiet.config import DbConfig

engine = create_engine("mysql+pymysql://user:pw@host/db")
db_config = DbConfig(table_name="orders", id_column="id", insert_mode="insert")

writer = DbWriter(engine, db_config, lock_strategy=LockStrategy.ADVISORY_AND_ROW)

op = DbOperation(
    table="orders",
    op_type=DbOperationType.INSERT,
    id_value=123,
    payload={"id": 123, "amount": 10, "status": "new"},
)

writer.execute(op)
```

---

## 6. Queue Subsystem (Redis Streams)

The Queue subsystem provides safe, multi-host consumption of Redis Streams with consumer groups.

There is **no DB coupling** here. The package **does not** automatically write queue messages to the DB.

### 6.1 Queue Models: `conquiet/queue/models.py`

```python
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class QueueMessage:
    stream: str
    group: str
    id: str
    payload: Dict[str, Any]
```

`payload` is a dict of string keys to primitive values. Serialization to/from Redis fields (`{field: value}`) will be done as JSON or simple string mapping depending on design choice (implement with JSON for safety).

### 6.2 Redis Streams Queue: `conquiet/queue/redis_streams.py`

This class wraps a `redis.Redis` client to operate on a single stream and consumer group.

```python
import json
from typing import Dict, Any, List, Optional
from redis import Redis
from .models import QueueMessage
from ..config import QueueConfig
from ..errors import QueueError

class RedisStreamsQueue:
    """
    A wrapper for a single Redis Stream + consumer group.
    Provides enqueue, read, ack, and claim-stale operations.
    """

    def __init__(self, redis: Redis, config: QueueConfig) -> None:
        self.redis = redis
        self.config = config
        self._ensure_group_exists()

    def _ensure_group_exists(self) -> None:
        try:
            # MKSTREAM ensures the stream is created if it doesn't exist.
            self.redis.xgroup_create(
                name=self.config.stream_key,
                groupname=self.config.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except Exception as exc:
            # XGROUP CREATE fails if group exists; ignore that specific case.
            msg = str(exc)
            if "BUSYGROUP" in msg:
                return
            raise

    def enqueue(self, payload: Dict[str, Any]) -> str:
        """
        Add a new entry to the stream. Returns the entry ID.
        """
        data = {"data": json.dumps(payload)}
        return self.redis.xadd(self.config.stream_key, data)

    def _parse_entry(self, entry_id: str, fields: Dict[bytes, bytes]) -> QueueMessage:
        # Expect single field "data" with JSON payload
        raw = fields.get(b"data")
        if raw is None:
            payload = {}
        else:
            payload = json.loads(raw.decode("utf-8"))
        return QueueMessage(
            stream=self.config.stream_key,
            group=self.config.consumer_group,
            id=entry_id,
            payload=payload,
        )

    def read(
        self,
        block_ms: Optional[int] = None,
        count: int = 1,
    ) -> List[QueueMessage]:
        """
        Read messages for this consumer from the stream using XREADGROUP.
        Returns a list of QueueMessage (possibly empty).
        """
        block = block_ms if block_ms is not None else 0
        resp = self.redis.xreadgroup(
            groupname=self.config.consumer_group,
            consumername=self.config.consumer_name,
            streams={self.config.stream_key: ">"},
            count=count,
            block=block,
        )
        messages: List[QueueMessage] = []

        for stream_key, entries in resp:
            for entry_id, fields in entries:
                messages.append(self._parse_entry(entry_id.decode("utf-8"), fields))

        return messages

    def ack(self, msg: QueueMessage) -> None:
        """
        Acknowledge successful processing of a message.
        """
        self.redis.xack(
            self.config.stream_key,
            self.config.consumer_group,
            msg.id,
        )

    def claim_stale(
        self,
        min_idle_ms: int,
        count: int = 10,
    ) -> List[QueueMessage]:
        """
        Claim stale pending messages that have been idle longer than min_idle_ms.
        This uses XPENDING + XCLAIM.
        """
        # XPENDING: stream, group, start, end, count
        pending = self.redis.xpending_range(
            self.config.stream_key,
            self.config.consumer_group,
            min="-",
            max="+",
            count=count,
        )
        stale_ids = [
            p["message_id"]
            for p in pending
            if p["time_since_delivered"] >= min_idle_ms
        ]
        if not stale_ids:
            return []

        claimed = self.redis.xclaim(
            self.config.stream_key,
            self.config.consumer_group,
            self.config.consumer_name,
            min_idle_ms,
            message_ids=stale_ids,
        )
        messages: List[QueueMessage] = []
        for entry_id, fields in claimed:
            messages.append(self._parse_entry(entry_id.decode("utf-8"), fields))
        return messages
```

Failure Modes & Expected Behavior

Duplicate INSERT attempts:

Expected

Logged

Suppressed

UPDATE without locking:

May result in lost updates

Considered incorrect usage unless explicitly desired

UPDATE with locking:

Guarantees correctness

May reduce throughput depending on strategy

### 6.3 Queue Metrics: `conquiet/queue/metrics.py`

Define Prometheus metrics for queue operations, such as:

* `conquiet_queue_messages_read_total{stream}`
* `conquiet_queue_messages_ack_total{stream}`
* `conquiet_queue_messages_claimed_total{stream}`
* `conquiet_queue_read_latency_seconds{stream}`

Provide small helper functions:

```python
def observe_queue_read(stream: str, count: int, latency_s: float) -> None:
    ...

def observe_queue_ack(stream: str) -> None:
    ...

def observe_queue_claim(stream: str, claimed_count: int) -> None:
    ...
```

### 6.4 Queue Consumer (Worker-like abstraction): `conquiet/queue/consumer.py`

The **QueueConsumer** is an optional, worker-like abstraction that:

* Reads messages from Redis Streams.
* Claims stale messages.
* Yields messages to the user.
* Does **not** process or ack messages by itself.
* Supports graceful shutdown via an internal stop flag.

The user is responsible for:

* Doing something with the message.
* Calling `ack()` when done (or intentionally not acking for retry via staleness).

```python
import time
from typing import Iterator, Optional, Callable
from redis import Redis
from .redis_streams import RedisStreamsQueue
from .models import QueueMessage
from ..config import QueueConfig
from ..signals import install_termination_handlers
from .metrics import observe_queue_read, observe_queue_ack, observe_queue_claim

class QueueConsumer:
    """
    High-level consumer for Redis Streams that:
      - reads messages using XREADGROUP
      - optionally claims stale messages
      - hands messages to the user
      - leaves ack decision to the user
    """

    def __init__(
        self,
        redis: Redis,
        config: QueueConfig,
        enable_signal_handlers: bool = True,
    ) -> None:
        self.queue = RedisStreamsQueue(redis, config)
        self.config = config
        self._stopping = False
        self._deadline: Optional[float] = None

        if enable_signal_handlers:
            install_termination_handlers(self.stop)

    def stop(self) -> None:
        """
        Initiate graceful shutdown.
        New reads will be stopped as soon as possible.
        """
        if not self._stopping:
            self._stopping = True
            # Optional: we could allow a timeout but leave that up to the caller.

    def next(self, block_ms: Optional[int] = None) -> Optional[QueueMessage]:
        """
        Fetch the next message for this consumer.
        Returns None if no message is available within block_ms.
        """
        if self._stopping:
            return None

        start = time.monotonic()
        msgs = self.queue.read(
            block_ms=block_ms or self.config.block_ms,
            count=self.config.max_read_count,
        )
        latency = time.monotonic() - start
        if msgs:
            observe_queue_read(self.config.stream_key, len(msgs), latency)
            return msgs[0]
        return None

    def ack(self, msg: QueueMessage) -> None:
        """
        Acknowledge that a message has been processed.
        """
        self.queue.ack(msg)
        observe_queue_ack(self.config.stream_key)

    def claim_stale(self, min_idle_ms: Optional[int] = None, count: int = 10) -> list[QueueMessage]:
        """
        Claim stale messages and return them as QueueMessage objects.
        """
        idle = min_idle_ms if min_idle_ms is not None else self.config.claim_idle_ms
        start = time.monotonic()
        msgs = self.queue.claim_stale(min_idle_ms=idle, count=count)
        latency = time.monotonic() - start
        if msgs:
            observe_queue_claim(self.config.stream_key, len(msgs))
            # Optionally also track latency; reuse observe_queue_read or a new metric.
        return msgs

    def iter_messages(self) -> Iterator[QueueMessage]:
        """
        Convenience generator that yields messages until stop() is called.
        The user is still responsible for calling ack() for each message.
        """
        while not self._stopping:
            msg = self.next()
            if msg is None:
                continue
            yield msg
```

Usage example (context):

```python
consumer = QueueConsumer(redis_client, queue_config)

for msg in consumer.iter_messages():
    try:
        # business logic
        process(msg.payload)
        consumer.ack(msg)
    except Exception:
        # don't ack; message will become stale and later be claimed again
        pass
```

---

## 7. Metrics Registry: `conquiet/metrics/registry.py`

Central place to initialize and expose Prometheus metrics. This can be a thin wrapper around `prometheus_client`.

```python
from prometheus_client import Counter, Histogram

# Example DB metrics
DB_WRITE_TOTAL = Counter(
    "conquiet_db_write_total",
    "Total DB write operations",
    ["table", "op_type", "status"],
)

DB_WRITE_LATENCY_SECONDS = Histogram(
    "conquiet_db_write_latency_seconds",
    "Latency of DB write operations",
    ["table", "op_type"],
)

DB_LOCK_ACQUIRE_LATENCY_SECONDS = Histogram(
    "conquiet_db_lock_acquire_latency_seconds",
    "Latency of DB lock acquisition",
    ["strategy"],
)

# Example Queue metrics
QUEUE_MESSAGES_READ_TOTAL = Counter(
    "conquiet_queue_messages_read_total",
    "Total queue messages read",
    ["stream"],
)

QUEUE_MESSAGES_ACK_TOTAL = Counter(
    "conquiet_queue_messages_ack_total",
    "Total queue messages acknowledged",
    ["stream"],
)

QUEUE_MESSAGES_CLAIMED_TOTAL = Counter(
    "conquiet_queue_messages_claimed_total",
    "Total stale queue messages claimed",
    ["stream"],
)

QUEUE_READ_LATENCY_SECONDS = Histogram(
    "conquiet_queue_read_latency_seconds",
    "Latency of queue reads",
    ["stream"],
)
```

The `db/metrics.py` and `queue/metrics.py` modules should import and use these metric objects.

---

## 8. Testing & CI/CD

### 8.1 Unit tests

* Use `pytest` as the test runner.
* Provide unit tests for:

  * Lock backends (advisory, row, table, advisory+row).
  * DbWriter (insert/update paths).
  * RedisStreamsQueue (enqueue, read, ack, claim_stale).
  * QueueConsumer (next, ack, iter_messages, stop).

### 8.2 Integration tests

* Use `docker-compose` to spin up:

  * MySQL
  * Redis
* Provide integration tests that:

  * Run multiple `DbWriter` instances in parallel and verify no duplicates or lost updates using different lock strategies.
  * Run multiple `QueueConsumer` instances and verify:

    * Only one consumes each message at a time.
    * Stale messages get reclaimed.

### 8.3 Coverage

* Use `coverage` or `pytest-cov` to collect coverage.
* Enforce a minimum coverage threshold (e.g., 80%).

### 8.4 CI/CD workflow

* Create a GitHub Actions (or GitLab CI) pipeline that:

  * Installs dependencies.
  * Spins up MySQL and Redis (services) (for testing).
  * Runs `pytest`.
  * Fails on coverage or test failures.

---

## 9. Summary of Key Design Decisions

* The package is named **`conquiet`**.
* DB and Queue subsystems are **independent**; **no coupling**.
* **DB subsystem**:

  * Uses `sqlalchemy.Engine` for pooling.
  * Uses **raw MySQL SQL** for all queries.
  * Supports **INSERT** and **UPDATE** operations.
  * Provides **lock strategies**: `ADVISORY`, `ROW`, `ADVISORY_AND_ROW`, `TABLE`, `NONE`.
  * Lock backends **operate on the same connection** as the writes.
* **Queue subsystem**:

  * Uses Redis Streams via `redis.Redis`.
  * Uses consumer groups for multi-host safe consumption.
  * Provides `RedisStreamsQueue` and `QueueConsumer`.
  * `QueueConsumer` handles `read -> (user processing) -> ack`, where **ack is explicit**.
  * Supports claiming stale messages for retry via `claim_stale`.
* **Graceful shutdown**:

  * For QueueConsumer: `stop()` + SIGTERM handler via `install_termination_handlers`.
  * For DB: direct calls; no worker loop; DB connections are managed via Engine context managers.
* **Metrics**:

  * Prometheus metrics for DB writes and queue operations.
* **Testing & CI**:

  * `pytest` + integration tests with real MySQL + Redis.
  * Coverage enforced.

This `bootstrap.md` describes the complete architecture and low-level design of `conquiet`. Code generation should follow this structure, names, data models, and patterns exactly.

```
```
