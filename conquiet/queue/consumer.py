from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from typing import Optional

from redis import Redis

from ..config import QueueConfig
from ..db.tx import DbFactory, DbTx
from ..errors import QueueError
from .models import QueueMessage
from .redis_streams import RedisStreamsQueue


class QueueConsumer:
    """
    High-level QueueConsumer API for consuming messages from a Redis Streams-based queue.
    
    The QueueConsumer is a control-flow abstraction, not a delivery guarantee mechanism.
    It coordinates:
    - Message retrieval
    - Shutdown behavior
    - Delivery to user code
    
    It deliberately avoids retries, buffering, fairness, and automatic recovery.
    
    Design Principles:
    - At-least-once delivery
    - Explicit acknowledgment
    - Best-effort recovery
    - Throughput-first
    - No hidden retries
    - No background work
    - No internal state machines
    
    Failures are surfaced, not hidden.
    
    Usage:
        consumer = QueueConsumer(redis_client, config)
        
        # Option 1: Manual control
        while True:
            msg = consumer.next()
            if msg is None:
                continue
            # Process msg...
            consumer.ack(msg)
        
        # Option 2: Iterator
        for msg in consumer.iter_messages():
            # Process msg...
            consumer.ack(msg)
        
        # Option 3: Template method
        def handler(msg: QueueMessage, tx: DbTx) -> None:
            tx.execute("INSERT INTO ...", {...})
        
        consumer.run(handler=handler, db_factory=DbFactory(engine))
    """

    def __init__(self, redis: Redis, config: QueueConfig) -> None:
        """
        Initialize the QueueConsumer.
        
        Args:
            redis: Redis client instance
            config: Queue configuration
            
        Raises:
            QueueError: If consumer group creation fails (except BUSYGROUP)
        """
        self.config = config
        self._queue = RedisStreamsQueue(redis, config)
        self._stopping = threading.Event()

    def next(self, block_ms: Optional[int] = None) -> Optional[QueueMessage]:
        """
        Fetch at most one message from the queue.
        
        Returns at most one message, blocking if necessary. Returns None
        if no message is available after the blocking period.
        
        Rules:
        - Returns at most one message
        - Returns None if no message is available
        - Must block using Redis Streams (XREADGROUP)
        - Must not spin without blocking
        - Must not drop messages
        - Must propagate Redis errors as QueueError
        
        Args:
            block_ms: Maximum time to block in milliseconds. If None, uses
                     config.block_ms. Must be a positive integer (> 0).
        
        Returns:
            QueueMessage if available, None otherwise
            
        Raises:
            QueueError: If Redis operation fails or block_ms is invalid
        """
        # Check if stopping before blocking
        if self._stopping.is_set():
            return None
        
        # Use configured block_ms if not provided
        actual_block_ms = block_ms if block_ms is not None else self.config.block_ms
        
        # Validate block_ms
        if not isinstance(actual_block_ms, int) or actual_block_ms <= 0:
            raise QueueError("block_ms must be a positive integer (> 0)")
        
        # Read messages (at most one per spec)
        messages = self._queue.read(block_ms=actual_block_ms, count=1)
        
        if not messages:
            return None
        
        # Return first message (should be only one due to count=1)
        return messages[0]

    def iter_messages(self) -> Iterator[QueueMessage]:
        """
        Iterator that yields messages until stopped.
        
        Behavior:
        - Blocks using configured block_ms
        - Yields messages until stopped
        - Does not busy-loop when idle
        - Stops yielding after stop() is observed
        - Propagates all exceptions
        
        Yields:
            QueueMessage instances
            
        Raises:
            QueueError: If Redis operation fails
        """
        while not self._stopping.is_set():
            msg = self.next(block_ms=self.config.block_ms)
            if msg is None:
                # Continue loop to check _stopping again
                continue
            yield msg

    def ack(self, msg: QueueMessage) -> None:
        """
        Explicitly acknowledge a message.
        
        Rules:
        - Must be called explicitly by user code
        - Must not be automatic
        - Failure to acknowledge implies retry via staleness
        - Redis failures must propagate as QueueError
        
        The QueueConsumer MUST NOT acknowledge messages on behalf of user code.
        
        Args:
            msg: The QueueMessage to acknowledge
            
        Raises:
            QueueError: If Redis operation fails
        """
        self._queue.ack(msg)

    def stop(self) -> None:
        """
        Signal graceful shutdown.
        
        Upon stop():
        - No new reads are initiated
        - In-flight messages remain pending
        - No automatic acknowledgment occurs
        - No reclaiming occurs
        
        Shutdown is best-effort. Messages delivered but not acknowledged
        remain pending and may be reclaimed by another consumer later.
        """
        self._stopping.set()

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
        
        This is a non-normative reference implementation. Implementations MAY:
        - rename this function
        - split it into helpers
        - inline it into application code
        
        Implementations MUST NOT:
        - change the ordering
        - add retries
        - acknowledge before commit
        - swallow user exceptions
        
        UNDER NO CIRCUMSTANCES SHOULD THIS LOOP RETRY A MESSAGE.
        
        Args:
            handler: User-provided handler function that processes messages
            db_factory: Factory for creating database transactions
            
        Raises:
            QueueError: If Redis operation fails
            Any exception raised by handler is propagated
        """
        while not self._stopping.is_set():
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

    def recover_stale(self, min_idle_ms: Optional[int] = None, count: int = 1) -> list[QueueMessage]:
        """
        Claim stale messages that have been pending longer than min_idle_ms.
        
        This is a best-effort recovery mechanism, not part of the hot path.
        The QueueConsumer prioritizes new messages over stale messages.
        
        Recovery requires explicit use of XPENDING and XCLAIM. This method
        provides a convenient way to invoke recovery, but it is optional
        and should be called periodically by user code, not automatically.
        
        By default, recovers at most one message to avoid buffering beyond
        one message. Call repeatedly if you want to drain more stale messages.
        
        Args:
            min_idle_ms: Minimum idle time in milliseconds for a message to be claimed.
                        If None, uses config.claim_idle_ms.
            count: Maximum number of messages to claim (default 1)
        
        Returns:
            List of claimed QueueMessage objects (may be empty)
            
        Raises:
            QueueError: If Redis operation fails
        """
        actual_min_idle_ms = min_idle_ms if min_idle_ms is not None else self.config.claim_idle_ms
        return self._queue.claim_stale(min_idle_ms=actual_min_idle_ms, count=count)

