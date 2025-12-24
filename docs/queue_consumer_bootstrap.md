# Queue Consumer Bootstrap Specification

## 1. Overview

This document defines the **high-level QueueConsumer API** for consuming messages from a Redis Streams–based queue.

The QueueConsumer is a **control-flow abstraction**, not a delivery guarantee mechanism.

It coordinates:

* Message retrieval
* Shutdown behavior
* Delivery to user code

It deliberately avoids retries, buffering, fairness, and automatic recovery.

---

## 1.1 
⚠️ IMPORTANT

The following run() function is NOT the required API.

It is a non-normative reference implementation whose sole purpose is to
demonstrate the required control-flow semantics.

Implementations MAY:

rename this function

split it into helpers

inline it into application code

Implementations MUST NOT:

change the ordering

add retries

acknowledge before commit

swallow user exceptions

UNDER NO CIRCUMSTANCES SHOULD THIS LOOP RETRY A MESSAGE.
```

    def run(
        self,
        *,
        handler: Callable[[QueueMessage, DbTx], None],
        db_factory: DbFactory,
    ) -> None:
        """
        Template-method runner:
        1) fetch
        2) handler(msg, tx)
        3) commit
        4) ack

        - Does not retry
        - Does not swallow user exceptions (re-raises)
        - Guarantees rollback on exceptions where possible
        - Redis connection failures propagate as QueueError
        """
        while not self._stopping:
            msg = self.next(block_ms=self.config.block_ms)
            if msg is None:
                continue

            tx = db_factory.begin()
            try:
                handler(msg, tx)
                tx.commit()
            except Exception:
                # Best-effort rollback; do not swallow original exception.
                try:
                    tx.rollback()
                finally:
                    raise
            else:
                # IMPORTANT: if ack fails (e.g., Redis down), it raises QueueError and propagates.
                # Commit already happened -> safe duplicates; user must ensure idempotent effects.
                self.ack(msg)
```


## 2. Design Principles

The QueueConsumer is designed according to the following principles:

* **At-least-once delivery**
* **Explicit acknowledgment**
* **Best-effort recovery**
* **Throughput-first**
* **No hidden retries**
* **No background work**
* **No internal state machines**

Failures are surfaced, not hidden.

---

## 3. Responsibilities

The QueueConsumer **MUST**:

* Provide a consumer loop abstraction
* Deliver messages to user code
* Support graceful shutdown
* Avoid CPU spinning when idle
* Propagate Redis connectivity failures
* Emit metrics via lower-level components

---

## 4. Explicit Non-Responsibilities

The QueueConsumer **MUST NOT**:

* Automatically acknowledge messages
* Retry failed message processing
* Catch or swallow user exceptions
* Implement buffering beyond one message
* Enforce delivery timeouts
* Guarantee fairness or ordering
* Perform background reclaiming
* Manage connection health state
* Emit health or liveness signals

---

## 5. Shutdown Semantics

### 5.1 Stop Signal

The QueueConsumer **MUST** support an explicit `stop()` mechanism.

Upon `stop()` (or SIGTERM):

* No new reads are initiated
* In-flight messages remain pending
* No automatic acknowledgment occurs
* No reclaiming occurs

Shutdown is **best-effort**.

Messages delivered but not acknowledged **remain pending** and may be reclaimed by another consumer later.

---

## 6. Message Retrieval

### 6.1 Single Retrieval

```python
def next(block_ms: Optional[int]) -> Optional[QueueMessage]
```

Rules:

* Returns **at most one message**
* Returns `None` if no message is available
* Must block using Redis Streams (`XREADGROUP`)
* Must not spin without blocking
* Must not drop messages
* Must propagate Redis errors as `QueueError`

If `block_ms` is not provided, the consumer **MUST** use its configured default.

---

## 7. Iteration API

```python
def iter_messages() -> Iterator[QueueMessage]
```

Behavior:

* Blocks using configured `block_ms`
* Yields messages until stopped
* Does not busy-loop when idle
* Stops yielding after `stop()` is observed
* Propagates all exceptions

---

## 8. Acknowledgment

```python
def ack(msg: QueueMessage) -> None
```

Rules:

* Must be called **explicitly** by user code
* Must not be automatic
* Failure to acknowledge implies retry via staleness
* Redis failures must propagate as `QueueError`

The QueueConsumer **MUST NOT** acknowledge messages on behalf of user code.

---

## 9. Error Propagation

### 9.1 Redis Connectivity Failures

If the connection to Redis fails during:

* Message read
* Acknowledgment
* Claiming (if invoked)

The QueueConsumer **MUST**:

* Surface the failure as a `QueueError`
* Stop further processing
* Allow the hosting service to react (restart, alert, degrade)

The QueueConsumer **MUST NOT**:

* Retry internally
* Suppress the error
* Track connection state

---

## 10. Recovery & Reclaiming

### 10.1 Stale Message Recovery

Stale messages are messages that were delivered but not acknowledged.

Redis Streams **does not** automatically retry such messages.

Recovery requires explicit use of:

* `XPENDING`
* `XCLAIM`

---

### 10.2 Recovery Policy

The QueueConsumer **MUST**:

* Prioritize `XREADGROUP` (new messages) over recovery
* Treat reclaiming as a **recovery mechanism**, not part of the hot path

The QueueConsumer **MUST NOT**:

* Aggressively reclaim messages on every loop
* Reclaim messages while new messages are continuously available
* Guarantee fairness or timeliness of recovery

---

### 10.3 Priority Model

The default behavior intentionally gives:

```
New messages > stale messages
```

This means:

* Under continuous load, stale messages may be delayed
* Delivery is guaranteed, timeliness is not
* Applications with strict latency requirements must implement periodic or dedicated recovery logic

---

## 11. Template-Method Processing (Optional)

An optional higher-level runner MAY be provided:

```python
def run(handler, db_factory)
```

This runner MAY:

* Fetch one message
* Invoke user handler
* Commit side effects
* Acknowledge the message

Rules:

* Exceptions from user code MUST be re-raised
* Rollback MAY be performed as cleanup
* No retries or swallowing of errors
* Redis failures MUST propagate

This pattern **guides correct usage** but does not enforce atomicity across systems.

---

## 12. Observability

### 12.1 Metrics

QueueConsumer relies on lower-level components for metrics emission.

It **MUST NOT**:

* Emit backlog gauges
* Emit connection health metrics
* Poll Redis for observability

---

### 12.2 Backlog Inspection

Backlog and staleness inspection MAY be exposed as **explicit query methods** (e.g., `backlog_stats()`), but:

* Must be best-effort
* Must be bounded
* Must have no side effects
* Must not run automatically

---

## 13. Summary of Guarantees

The QueueConsumer guarantees:

* At-least-once delivery
* Explicit acknowledgment
* Best-effort recovery
* Honest error propagation

The QueueConsumer does **not** guarantee:

* Exactly-once delivery
* Fairness
* Ordering
* Timeliness
* Automatic retries
* Automatic recovery

---

## 14. Final Design Invariant

> **The QueueConsumer exposes Redis Streams semantics honestly.
> It makes the right thing easy, the wrong thing explicit, and failure visible.**

