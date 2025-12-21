# 🧭 `bootstrap.md` — Conquiet (v2)

## Purpose

This document defines the **authoritative low-level design** of **conquiet**, a Python package for **safe concurrent database and queue operations** in multi-process, multi-host backend systems.

This document supersedes all previous bootstrap or design documents.
It reflects the final, refined architecture after initial implementation and validation.

The intent of this document is to:

* Guide implementation and refactoring
* Serve as a contract for users of the package
* Be used by Cursor to generate and refactor code methodologically and precisely

---

## 1. High-Level Overview

**conquiet** is an internal Python infrastructure library used across backend services to:

1. Safely perform **concurrent MySQL writes** (insert and update)
2. Safely consume and acknowledge **Redis Streams queues**
3. Support **multi-host and multi-process execution**
4. Provide explicit concurrency control primitives
5. Integrate cleanly with application-managed lifecycle and shutdown

### Non-goals

* conquiet does **not** own application lifecycle
* conquiet does **not** manage schema creation
* conquiet does **not** hide SQL or business logic
* conquiet does **not** provide ORM abstractions

---

## 2. Design Principles

1. **Explicit over implicit**

   * Locks, transactions, and concurrency strategies must be explicit
2. **Primitives, not workflows**

   * conquiet exposes building blocks, not full pipelines
3. **Control stays with the user**

   * Users compose logic inside locks and transactions
4. **Concurrency semantics must be visible**

   * Atomic SQL, pessimistic locking, and OCC are distinct and explicit
5. **No magic retries**

   * Retrying, backoff, and conflict resolution are application decisions
6. **Framework-agnostic**

   * Works with FastAPI, APScheduler, CLI workers, etc.

---

## 3. DB Subsystem Overview

The DB subsystem provides **transactional sessions** and **concurrency-control primitives** for MySQL.

### Supported patterns

| Pattern                              | Locking | Description                               |
| ------------------------------------ | ------- | ----------------------------------------- |
| Idempotent INSERT                    | ❌       | Duplicate-safe inserts via DB constraints |
| Atomic UPDATE                        | ❌       | Single-statement atomic updates           |
| Optimistic Concurrency Control (OCC) | ❌       | Conditional updates based on version      |
| Pessimistic Locking                  | ✅       | Advisory or row-level locking             |

---

## 4. Transactions — `DbSession`

### Purpose

`DbSession` represents **one transactional MySQL session**, backed by a SQLAlchemy Engine connection.

It is the **only owner of transaction lifecycle**.

### Responsibilities

* Acquire and release a DB connection
* Begin and commit/rollback a transaction
* Execute raw SQL safely
* Ensure all operations use the same connection
* Provide a narrow, explicit execution API

### API

```python
class DbSession:
    def execute(
        self,
        sql: str | TextClause,
        params: dict | None = None,
    ) -> int:
        """
        Execute a non-SELECT statement.
        Returns affected row count.
        """

    def fetch_one(
        self,
        sql: str | TextClause,
        params: dict | None = None,
    ) -> dict | None:
        """
        Execute a SELECT expected to return 0 or 1 row.
        Raises if more than one row is returned.
        """

    def fetch_all(
        self,
        sql: str | TextClause,
        params: dict | None = None,
    ) -> list[dict]:
        """
        Execute a SELECT returning multiple rows.
        """
```

### Usage

```python
with db.session() as session:
    ...
```

All locks and writes must occur **inside** a `DbSession`.

---

## 5. INSERT Semantics (Idempotent Writes)

### Behavior

* INSERT operations **never acquire locks**
* Correctness relies on:

  * MySQL uniqueness constraints
  * Catching duplicate-key exceptions
* Duplicate-key exceptions:

  * are logged
  * increment metrics
  * are **not rethrown**

### Semantics

> An INSERT is considered successful if the row exists after execution, regardless of which process inserted it.

This design maximizes throughput and supports at-least-once delivery semantics.

---

## 6. Atomic SQL Updates

### Description

Some updates can be expressed as **single SQL statements** that are atomic by definition.

Example:

```sql
UPDATE counters
SET value = value + 1
WHERE id = 1;
```

### API

Atomic updates use `DbSession.execute` directly.

```python
with db.session() as session:
    session.execute(
        "UPDATE counters SET value = value + 1 WHERE id = :id",
        {"id": 1},
    )
```

### Notes

* No locks are used
* No prior read is required
* MySQL guarantees atomicity

This is the **preferred approach** when applicable.

---

## 7. Optimistic Concurrency Control (OCC)

### Description

OCC is implemented via **conditional UPDATE statements**.

Example:

```sql
UPDATE orders
SET amount = :amount,
    version = version + 1
WHERE id = :id AND version = :version;
```

### Semantics

* `rowcount == 1` → update succeeded
* `rowcount == 0` → condition not met

A zero-row update may indicate:

* concurrent modification
* missing row
* version mismatch

**The library does not assume which one.**

### API

OCC uses `DbSession.execute`.

```python
rows = session.execute(...)

if rows == 0:
    # caller decides:
    # retry, re-read, abort, or ignore
```

### Design decision

conquiet **does not raise by default** for OCC failures.
Caller intent determines semantics.

---

## 8. Pessimistic Locking

Locks are **capabilities**, not side effects of writes.

### 8.1 Advisory Lock

#### Purpose

* General-purpose mutex
* Protect arbitrary logic

#### API

```python
class AdvisoryLock:
    def __init__(self, session: DbSession, key: str, timeout: int = 10)

    def __enter__(self) -> AdvisoryLock
    def __exit__(self, exc_type, exc, tb)
```

#### Usage

```python
with db.session() as session:
    with AdvisoryLock(session, "order:42"):
        # arbitrary logic
        row = session.fetch_one(...)
        session.execute(...)
```

* Lock is connection-scoped
* Lock is released on `__exit__`
* Does not own transaction

---

### 8.2 Row Lock (`SELECT … FOR UPDATE`)

#### Purpose

* Pessimistic read–modify–write protection

#### API

```python
class RowLock:
    def __init__(self, session: DbSession, table: str, where: dict)

    def acquire(self) -> dict | None
```

#### Usage

```python
with db.session() as session:
    row = RowLock(session, "orders", {"id": 42}).acquire()
    if row is None:
        return

    # logic using row
    session.execute(...)
```

#### Notes

* Row locks are released on transaction commit/rollback
* `RowLock` is **not** a context manager
* Returning the row is intentional
* Row locks acquired via SELECT … FOR UPDATE are held for the duration of the surrounding transaction and are released only when the transaction commits or rolls back. Executing additional SQL statements does not release the lock.

---

### 8.3 Lock Composition

conquiet does **not** provide combined locks.

If users want both advisory and row locks, they compose explicitly:

```python
with db.session() as session:
    with AdvisoryLock(session, "orders:42"):
        row = RowLock(session, "orders", {"id": 42}).acquire()
        session.execute(...)
```

This avoids hidden ordering and deadlocks.

---

## 9. Queue Subsystem (Redis Streams)

### Overview

The queue subsystem provides **safe distributed consumption** of Redis Streams.

DB and queue subsystems are **fully decoupled**.

### Features

* Redis Streams + consumer groups
* Explicit message acknowledgment
* At-least-once delivery
* Stale message claiming via XPENDING + XCLAIM
* Multi-host safe

### Consumer Model

1. Read message
2. Claim message
3. User processes message
4. User explicitly ACKs message

conquiet does **not** auto-process messages.

---

## 10. Shutdown & Lifecycle

### Ownership

* conquiet does **not** own SIGTERM or SIGINT by default
* Application owns lifecycle
* conquiet exposes explicit stop methods

### Queue Consumer

```python
consumer.stop()
```

Behavior:

* Stop fetching new messages
* Allow in-flight message to complete
* Release resources

### Integration Examples

#### FastAPI

```python
@app.on_event("shutdown")
async def shutdown():
    consumer.stop()
```

#### CLI / Worker

User may install their own SIGTERM handler and call `stop()`.

---

## 11. Metrics & Observability

conquiet exposes Prometheus metrics for:

* DB operations (counts, latency)
* Duplicate inserts
* Lock acquisition timing
* Queue reads, ACKs, claims
* Stale message recovery

conquiet does **not** expose an HTTP server.

---

## 12. CI/CD & Testing

* Unit tests for DB primitives
* Integration tests with MySQL and Redis
* Stress tests for:

  * update contention
  * locking correctness
  * OCC behavior
* Coverage enforced in CI

---

## 13. Summary

conquiet is a **concurrency-control toolkit**, not a workflow engine.

It provides:

* Explicit transactions
* Explicit locking primitives
* Explicit concurrency semantics
* Safe defaults
* Maximum composability

All concurrency behavior is **visible, intentional, and testable**.

## 3. Database Assumptions & Guarantees

conquiet is designed specifically for **MySQL with the InnoDB storage engine**.

### Required assumptions

- **Storage engine**: InnoDB
- **Transactions**: Enabled
- **Row-level locking**: Available
- **Advisory locks**: Supported via GET_LOCK / RELEASE_LOCK

### Recommended isolation level

- **READ COMMITTED** (preferred)
- **REPEATABLE READ** is supported but has important implications (see below)

### Row lock behavior

- `SELECT ... FOR UPDATE` acquires row-level locks on matched rows
- Locks are held until transaction commit or rollback
- Locks apply only to rows matched by indexed predicates

### Gap locks and phantom behavior

Under **REPEATABLE READ**, MySQL may acquire **gap locks**:
- Inserts into locked index ranges may block
- Unexpected contention may occur
- This is MySQL behavior, not a conquiet bug

Users should:
- Prefer **READ COMMITTED** unless gap-lock behavior is explicitly desired
- Ensure row-lock predicates are indexed
- Keep transactions short when using row locks

### Advisory lock behavior

- Advisory locks are **connection-scoped**, not transaction-scoped
- They are independent of isolation level
- They must be explicitly released

### DbSession.execute return semantics

`DbSession.execute(...)` returns the database-reported **affected row count**.

Important notes:

- For UPDATE statements:
  - A row is counted as “affected” only if its values actually change
  - Setting a column to its existing value may return `0`
- For conditional updates (e.g. OCC):
  - `0` may indicate:
    - version mismatch
    - missing row
    - no-op update
- For INSERT and INSERT IGNORE:
  - Behavior follows MySQL driver semantics

conquiet does **not normalize or reinterpret rowcount**.
The raw database behavior is exposed intentionally.

Callers must interpret `rowcount` in the context of their SQL.
OCC helpers rely on conditional UPDATE semantics, not on a guarantee that rowcount == 1 implies success in all cases.


### AdvisoryLock acquisition semantics

`AdvisoryLock` uses MySQL `GET_LOCK(key, timeout)`.

Acquisition behavior is explicit and deterministic:

- Lock acquisition **blocks up to `timeout` seconds**
- If the lock is acquired:
  - `__enter__` returns successfully
  - Lock ownership is guaranteed for the duration of the context
- If the lock cannot be acquired within `timeout`:
  - A `LockTimeoutError` is raised
  - The protected block is not executed

AdvisoryLock never returns a boolean.
Failure to acquire a lock is considered a control-flow event that must be handled explicitly by the caller.

### Important: RowLock scope and transaction duration

Row locks acquired via `SELECT ... FOR UPDATE` are held for the **entire duration of the surrounding transaction**.

This means:
- The lock is not released when `RowLock.acquire()` returns
- The lock is not released when application logic completes
- The lock is released only when the `DbSession` commits or rolls back

Users must:
- Keep transactions short
- Avoid long-running logic while holding row locks
- Avoid I/O or network calls inside row-locked transactions

### Optimistic Concurrency Control (OCC)

Conquiet provides a low-level OCC helper (`occ_update`), but correctness depends
on strict usage rules regarding transaction boundaries and retries.

**All users MUST read:** `docs/occ.md`