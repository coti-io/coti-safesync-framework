# `bootstrap_redis_streams.md`

## Conquiet – Redis Streams Queue Subsystem (Low-Level Specification)

This document defines the **complete and authoritative low-level specification** for the **Redis Streams queue subsystem** of the `conquiet` package.

This subsystem is **fully independent** from the DB subsystem and may be implemented, tested, and reasoned about in isolation.

The code generated from this specification **must not rely on undocumented Redis behavior or implicit assumptions**.

---

## 1. Scope & Non-Goals

### 1.1 In Scope

* Redis Streams–based queueing using **consumer groups**
* Multi-host safe consumption
* Explicit acknowledgment model
* Best-effort retry via stale message claiming
* Graceful shutdown behavior
* Prometheus metrics

### 1.2 Explicit Non-Goals

The following are **intentionally out of scope** and must not be implemented implicitly:

* Dead-letter queues
* Message prioritization
* Exactly-once delivery
* Global ordering guarantees
* Automatic consumer cleanup (`XGROUP DELCONSUMER`)
* Stream trimming (`XTRIM`)
* Backpressure or rate limiting

---

## 2. Delivery Semantics & Guarantees

### 2.1 Delivery Model

* The queue provides **at-least-once delivery**.
* Messages **may be delivered more than once**.
* Consumers **must be idempotent**.

### 2.2 Ordering

* Ordering is guaranteed **only within a single stream entry ID sequence**.
* There is **no ordering guarantee across consumers**.
* Message reclaiming via `XCLAIM` **breaks ordering**.
* Users **must not rely on message order** for correctness.

### 2.3 Message Duplication & Re-delivery

Due to the at-least-once delivery model, a single stream entry may be delivered more than once over its lifetime.
Re-delivery may occur when:
A consumer crashes or terminates before acknowledging a message
A message is reclaimed via XCLAIM after exceeding the idle threshold
Network failures cause ambiguous delivery outcomes
A message will not be delivered concurrently to multiple consumers, but may be processed sequentially by different consumers over time.
Consumers must not assume that message processing is unique, ordered, or exactly-once.
All message handling logic must be idempotent.

---

## 3. Message Format

### 3.1 Stream Entry Format (Strict)

Each Redis Stream entry **MUST**:

* Contain **exactly one field**
* Field name: `"data"`
* Field value: **UTF-8 encoded JSON string**

```text
XADD mystream * data "<json>"
```

No other fields are permitted.

### 3.2 Payload Semantics

* JSON must decode into `Dict[str, Any]`
* Payload values must be JSON-serializable primitives
* Binary data is not supported

---

## 4. Configuration

### 4.1 QueueConfig

```python
@dataclass
class QueueConfig:
    stream_key: str
    consumer_group: str
    consumer_name: str
    block_ms: int = 5_000
    max_read_count: int = 1
    claim_idle_ms: int = 60_000
```

### 4.2 Invariants

* `max_read_count` **MUST be 1** when using `QueueConsumer`
* Consumer names **must be unique per active process**

---

## 5. RedisStreamsQueue (Low-Level API)

### 5.1 Responsibilities

* Direct interaction with Redis Streams
* No looping, retries, or signal handling
* No message buffering
* No business logic

---

### 5.2 Consumer Group Initialization

```python
XGROUP CREATE <stream> <group> 0-0 MKSTREAM
```

Rules:

* Must be attempted at startup
* `BUSYGROUP` error must be ignored
* Any other error must propagate

Group recreation is **not automatic**.

---

### 5.3 Enqueue

```python
def enqueue(payload: Dict[str, Any]) -> str
```

Behavior:

* Serialize payload to JSON
* Add to stream via `XADD`
* Return entry ID
* No retries

---

### 5.4 Read (XREADGROUP)

```python
def read(block_ms: int, count: int = 1) -> list[QueueMessage]
```

Rules:

* Must use `XREADGROUP GROUP <group> <consumer> STREAMS <stream> >`
* `block_ms` must be a positive integer (> 0)
* `block_ms=0` is rejected (Redis interprets 0 as infinite blocking)
* `count` is advisory
* Returned messages become **pending**
* Returned list may be empty

---

### 5.5 Acknowledge

```python
def ack(msg: QueueMessage) -> None
```

Behavior:

* Call `XACK`
* Acking a non-pending or already-acked ID is allowed
* Errors propagate

---

### 5.6 Claim Stale Messages

```python
def claim_stale(min_idle_ms: int, count: int = 10) -> list[QueueMessage]
```

Semantics:

* Best-effort reclaim only
* Uses:

  1. `XPENDING` (bounded, non-exhaustive)
  2. `XCLAIM` with `min_idle_ms`
* Ordering is not preserved
* Starvation is possible
* Redis is authoritative on idle time

This method **does not guarantee**:

* Fairness
* Complete reclamation
* Deterministic behavior

### 5.7 Reclaiming Responsibility

This specification defines how stale messages may be reclaimed, but does not prescribe who performs reclamation or how often.

Any consumer may invoke claim_stale() at its discretion.
Reclaiming is:

Opportunistic

Best-effort

Non-fair

Not guaranteed to discover or reclaim all stale messages

The system does not guarantee:

Fair distribution of reclaimed messages

Eventual reclamation of all pending messages

Absence of message starvation

Reclaiming policy (frequency, ownership, limits) is an application-level concern and intentionally left undefined by this specification.

---

## 6. QueueConsumer (High-Level API)

### 6.1 Responsibilities

* Consumer loop abstraction
* Graceful shutdown
* Metric emission
* Message delivery to user

### 6.2 Explicit Non-Responsibilities

* No auto-ack
* No retry logic
* No exception handling of user code
* No buffering beyond one message

---

### 6.3 Shutdown Semantics

* On `stop()` or SIGTERM:

  * No new reads are initiated
  * In-flight messages remain pending
  * No automatic ack or reclaim occurs

Shutdown is **best-effort**.
In-flight (delivered but unacknowledged) messages remain pending and may be re-delivered to another consumer at a later time.

---

### 6.4 Message Retrieval

```python
def next(block_ms: Optional[int]) -> Optional[QueueMessage]
```

Rules:

* Returns **at most one message**
* If no message is available, returns `None`
* Must not drop messages
* Must not spin without blocking

---

### 6.5 Iteration

```python
def iter_messages() -> Iterator[QueueMessage]
```

Behavior:

* Blocks using `block_ms`
* Yields messages until stopped
* Does not swallow CPU when idle

---

### 6.6 Acknowledgment

```python
def ack(msg: QueueMessage) -> None
```

Rules:

* Must be called explicitly by user
* Failure to ack implies retry via staleness
* No timeout enforcement

---

## 7. Metrics

Required metrics:

* `conquiet_queue_messages_read_total{stream}`
* `conquiet_queue_messages_ack_total{stream}`
* `conquiet_queue_messages_claimed_total{stream}`
* `conquiet_queue_read_latency_seconds{stream}`

Metrics must:

* Be emitted only on successful Redis calls
* Never raise exceptions

---

## 8. Error Handling

### 8.1 Error Policy

* Redis errors propagate as `QueueError`
* No retries are performed internally
* Connection issues are caller responsibility

### 8.2 Retriability

* The library does not distinguish retriable vs fatal errors
* Users must implement retry policies externally

---

## 9. Summary of Guarantees

| Property | Guarantee      |
| -------- | -------------- |
| Delivery | At-least-once  |
| Ordering | Not guaranteed |
| Ack      | Explicit       |
| Retry    | Via staleness  |
| Shutdown | Best-effort    |
| Fairness | Not guaranteed |

---