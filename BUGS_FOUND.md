# Bugs Found in Conquiet

## Bug 1: Connection Leak in `DbSession.__enter__`

**Location**: `conquiet/db/session.py:39-46`

**Issue**: If `self._conn.begin()` raises an exception, `self._conn` is already set but the connection won't be properly cleaned up. The `__exit__` method will be called (Python context manager protocol), but if the exception occurs during `begin()`, the connection state is inconsistent.

**Current Code**:
```python
def __enter__(self) -> "DbSession":
    if self._conn is not None:
        raise RuntimeError("DbSession is already active; nested sessions are not allowed")
    self._conn = self.engine.connect()  # Connection created
    self._tx = self._conn.begin()       # If this raises, _conn is set but _tx is None
    self._execute_operations = []
    return self
```

**Problem**: If `begin()` raises, `_conn` is set but `_tx` is None. When `__exit__` is called, it checks `if self._tx is not None` before rollback, so no rollback happens. The connection is closed in the finally block, but the transaction state is inconsistent.

**Fix**: Wrap in try-except to clean up connection if begin() fails:
```python
def __enter__(self) -> "DbSession":
    if self._conn is not None:
        raise RuntimeError("DbSession is already active; nested sessions are not allowed")
    try:
        self._conn = self.engine.connect()
        self._tx = self._conn.begin()
        self._execute_operations = []
        return self
    except Exception:
        # Clean up connection if begin() fails
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        raise
```

---

## Bug 2: Incorrect Metrics Status When Commit/Rollback Fails

**Location**: `conquiet/db/session.py:48-86`

**Issue**: The `status` variable is determined based on `exc_type` (exception from the `with` block), but if `commit()` or `rollback()` fails, `exc_type` will be None (because the handler succeeded), so metrics will incorrectly show "success" even though the transaction failed.

**Current Code**:
```python
def __exit__(self, exc_type, exc, tb) -> None:
    # Determine transaction status for metrics
    status = "error" if exc_type else "success"  # Set based on handler exception
    end_time = time.monotonic()
    
    try:
        if self._tx is not None:
            if exc_type:
                self._tx.rollback()  # If this raises, status is still "error" (correct)
            else:
                self._tx.commit()     # If this raises, status is "success" (WRONG!)
    finally:
        # ... cleanup ...
        # Metrics emitted here use 'status' variable
```

**Problem**: If `commit()` raises an exception, `status` was already set to "success" based on `exc_type` being None. The metrics will incorrectly record a successful operation even though the commit failed.

**Fix**: Track commit/rollback failures separately:
```python
def __exit__(self, exc_type, exc, tb) -> None:
    end_time = time.monotonic()
    commit_failed = False
    
    try:
        if self._tx is not None:
            if exc_type:
                self._tx.rollback()
            else:
                try:
                    self._tx.commit()
                except Exception:
                    commit_failed = True
                    raise
    finally:
        # Determine final status
        status = "error" if (exc_type or commit_failed) else "success"
        # ... rest of cleanup and metrics ...
```

---

## Bug 3: Missing `count` Parameter in `QueueConsumer.claim_stale()`

**Location**: `conquiet/queue/consumer.py:208-235`

**Issue**: `QueueConsumer.claim_stale()` doesn't accept a `count` parameter, but `RedisStreamsQueue.claim_stale()` does. This means users of the high-level API can't control how many messages to claim per call, while `run_claim_stale()` can (via `max_claim_count`). This is an API inconsistency.

**Current Code**:
```python
def claim_stale(self, min_idle_ms: Optional[int] = None) -> List[QueueMessage]:
    # ...
    return self._queue.claim_stale(min_idle_ms=min_idle_ms)  # Uses default count=10
```

**Problem**: The high-level API doesn't expose the `count` parameter that the low-level API supports. This limits flexibility for users who want to claim different numbers of messages.

**Fix**: Add `count` parameter to match the low-level API:
```python
def claim_stale(
    self, 
    min_idle_ms: Optional[int] = None,
    count: int = 10
) -> List[QueueMessage]:
    # ...
    return self._queue.claim_stale(min_idle_ms=min_idle_ms, count=count)
```

---

## Bug 4: Potential Issue with `run_claim_stale()` Exception Handling

**Location**: `conquiet/queue/consumer.py:307-316`

**Issue**: If `self.ack(msg)` raises an exception after a successful commit, the exception is propagated and the loop continues. However, if this happens for multiple messages in the same batch, only the first failure will be seen. Subsequent messages in the batch that were successfully committed won't be ACKed, but the exception will prevent processing the rest.

**Current Code**:
```python
for msg in claimed_messages:
    if self._stopping.is_set():
        break
    
    try:
        with DbSession(engine) as session:
            handler(msg, session)
    except Exception:
        # DbSession rolled back automatically
        raise
    else:
        # Commit already happened
        self.ack(msg)  # If this raises, exception propagates, loop stops
```

**Problem**: This is actually correct behavior (fail fast), but it means that if ACK fails for one message, other messages in the batch that were successfully committed won't be ACKed. This is documented as "at-least-once" semantics, so it's not necessarily a bug, but it's worth noting that partial batch failures can occur.

**Note**: This might be intentional design (fail fast), but it could be improved to ACK all successfully committed messages even if one ACK fails.

---

## Bug 5: Race Condition in `run_claim_stale()` Stop Check

**Location**: `conquiet/queue/consumer.py:320-324`

**Issue**: The stop check uses `self._stopping.wait(timeout=claim_interval_seconds)`, which returns `False` if the timeout is reached (no stop signal). However, if the stop signal is set during the wait, it returns `True`. The logic then breaks, which is correct. But there's a subtle issue: if `stop()` is called right after the wait starts, the wait will return immediately, but if it's called right before the wait, there's a small window where the stop signal might not be checked until the next iteration.

**Current Code**:
```python
# Wait for the next interval, but check stop signal periodically
if not self._stopping.wait(timeout=claim_interval_seconds):
    # Timeout reached, continue to next iteration
    continue
# Stop signal was set, exit loop
break
```

**Note**: This is actually correct - `wait()` will return immediately if the event is already set. The logic is sound. This is not a bug, just noting the behavior.

---

## Summary

**Critical Bugs**:
1. ✅ Bug 1: Connection leak if `begin()` raises
2. ✅ Bug 2: Incorrect metrics when commit/rollback fails

**Minor Issues**:
3. ⚠️ Bug 3: Missing `count` parameter in `claim_stale()` (API inconsistency)
4. ⚠️ Bug 4: Partial batch ACK failures (might be intentional)

**Not Bugs**:
5. ❌ Bug 5: Actually correct behavior
