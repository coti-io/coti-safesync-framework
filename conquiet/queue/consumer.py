from __future__ import annotations

import threading
from typing import Callable, Iterator, List, Optional

from redis import Redis
from sqlalchemy.engine import Engine

from ..config import QueueConfig
from ..db.session import DbSession
from .models import QueueMessage
from .redis_streams import RedisStreamsQueue


class QueueConsumer:
    """
    High-level API for consuming messages from a Redis Streams-based queue.

    QueueConsumer is a control-flow abstraction that coordinates:
    - Message retrieval
    - Shutdown behavior
    - Delivery to user code

    It deliberately avoids retries, buffering, fairness, and automatic recovery.
    Failures are surfaced, not hidden.

    ⚠️ QueueConsumer provides at-least-once delivery, not exactly-once.
    Messages may be redelivered if ACK fails after commit, requiring idempotent handlers.

    Usage:
        consumer = QueueConsumer(redis_client, config)
        for msg in consumer.iter_messages():
            # Process message
            consumer.ack(msg)
    """

    def __init__(self, redis: Redis, config: QueueConfig) -> None:
        """
        Initialize the QueueConsumer.

        Args:
            redis: Redis client instance
            config: Queue configuration

        Raises:
            QueueError: If consumer group creation fails (from RedisStreamsQueue)
        """
        self.config = config
        self._queue = RedisStreamsQueue(redis, config)
        self._stopping = threading.Event()

    def next(self, block_ms: Optional[int] = None) -> Optional[QueueMessage]:
        """
        Fetch at most one message from the queue.

        Blocks using Redis Streams XREADGROUP if no message is available.
        Returns None if no message is available after the blocking period.

        Stop is best-effort: if a blocking read is in progress, `stop()` takes
        effect after the block timeout.

        Args:
            block_ms: Maximum time to block in milliseconds. If None, uses
                     the configured default from config.block_ms.

        Returns:
            A QueueMessage if available, None if no message after blocking.

        Raises:
            QueueError: If Redis operation fails
        """
        # Use configured default if block_ms is not provided
        if block_ms is None:
            block_ms = self.config.block_ms

        # Best-effort check: don't start a new read if we're stopping
        if self._stopping.is_set():
            return None

        # Read at most one message
        messages = self._queue.read(block_ms=block_ms, count=1)

        # Return first message if available, None otherwise
        if messages:
            return messages[0]
        return None

    def iter_messages(self) -> Iterator[QueueMessage]:
        """
        Iterator that yields messages until stopped.

        Blocks using the configured block_ms when no messages are available.
        Stops yielding after stop() is called. Does not busy-loop when idle.
        Propagates all exceptions.

        Yields:
            QueueMessage objects from the queue

        Raises:
            QueueError: If Redis operation fails
        """
        while not self._stopping.is_set():
            msg = self.next(block_ms=self.config.block_ms)
            if msg is None:
                # No message available, continue loop (will block on next iteration)
                continue
            yield msg

    def ack(self, msg: QueueMessage) -> None:
        """
        Explicitly acknowledge a message.

        Acknowledging a message removes it from the pending list in Redis.
        This must be called explicitly by user code after successfully processing
        a message. Failure to acknowledge implies the message will remain pending
        and may be reclaimed by another consumer later.

        Ack may be called even after stop() was requested to allow draining
        in-flight work.

        Args:
            msg: The QueueMessage to acknowledge

        Raises:
            QueueError: If Redis operation fails
        """
        self._queue.ack(msg)

    def stop(self) -> None:
        """
        Signal graceful shutdown.

        After stop() is called:
        - No new reads are initiated (best-effort)
        - In-flight messages remain pending
        - No automatic acknowledgment occurs
        - No reclaiming occurs

        stop() does not interrupt an active blocking read; it takes effect after
        block_ms timeout.

        SIGTERM handling is expected to be wired externally (signal handler calls
        stop()).

        Shutdown is best-effort. Messages delivered but not acknowledged
        remain pending and may be reclaimed by another consumer later.
        """
        self._stopping.set()

    def run(
        self,
        *,
        handler: Callable[[QueueMessage, DbSession], None],
        engine: Engine,
    ) -> None:
        """
        Template-method runner.

        Executes the following control flow for each message:
        1) fetch message
        2) open DbSession (one transaction)
        3) handler(msg, session)
        4) commit (on DbSession exit)
        5) ack message

        No retries. No swallowing exceptions.

        DB commit happens on DbSession.__exit__ (success case). ACK occurs after
        commit. If ACK fails, the message remains pending and will be redelivered.
        Handlers MUST be idempotent or use DB constraints/locks/OCC to handle
        duplicate processing safely.

        ⚠️ Stale Message Recovery:
        Messages may become stale if the consumer crashes, restarts, or is killed
        before ACKing. Stale messages remain pending in Redis and are NOT automatically
        redelivered. You MUST run a separate worker process using `run_claim_stale()`
        to recover stale messages. Without a recovery worker, stale messages will remain
        pending indefinitely.

        Args:
            handler: User-provided function that processes a message within a
                    database transaction. Receives the message and DbSession.
            engine: SQLAlchemy Engine for creating database sessions.

        Raises:
            QueueError: If Redis operation fails (including ACK failures after commit)
            Any exception raised by the handler (never caught or swallowed)

        See Also:
            run_claim_stale: Recovery worker for processing stale messages
        """
        while not self._stopping.is_set():
            msg = self.next(block_ms=self.config.block_ms)
            if msg is None:
                continue

            try:
                with DbSession(engine) as session:
                    handler(msg, session)
            except Exception:
                # DbSession rolled back automatically
                # Message NOT acked → remains pending
                raise
            else:
                # Commit already happened
                self.ack(msg)

    def claim_stale(
        self, min_idle_ms: Optional[int] = None, count: int = 10
    ) -> List[QueueMessage]:
        """
        Claim stale messages that have been pending longer than the threshold.

        This is an explicit recovery API that:
        - Is not called automatically
        - Is not part of the hot path
        - Aligns with the "New messages > stale messages" priority model

        Must be invoked explicitly by user code when recovery is desired.
        Under continuous load, stale messages may be delayed.

        Args:
            min_idle_ms: Minimum idle time in milliseconds for a message to be
                        claimed. If None, uses the configured default from
                        config.claim_idle_ms.
            count: Maximum number of messages to claim per call. Default: 10.

        Returns:
            List of claimed QueueMessage objects (may be empty)

        Raises:
            QueueError: If Redis operation fails
        """
        # Use configured default if min_idle_ms is not provided
        if min_idle_ms is None:
            min_idle_ms = self.config.claim_idle_ms

        return self._queue.claim_stale(min_idle_ms=min_idle_ms, count=count)

    def run_claim_stale(
        self,
        *,
        handler: Callable[[QueueMessage, DbSession], None],
        engine: Engine,
        min_idle_ms: Optional[int] = None,
        claim_interval_ms: int = 5_000,
        max_claim_count: int = 10,
    ) -> None:
        """
        Template-method runner for stale message recovery.

        Periodically claims and processes stale messages that have been pending
        longer than the threshold. This is a recovery mechanism that should run
        in a separate worker process alongside the main `run()` consumer.

        Executes the following control flow for each claimed message:
        1) claim stale messages (periodically, every claim_interval_ms)
        2) for each claimed message:
           a) open DbSession (one transaction)
           b) handler(msg, session)
           c) commit (on DbSession exit)
           d) ack message

        No retries. No swallowing exceptions.

        DB commit happens on DbSession.__exit__ (success case). ACK occurs after
        commit. If ACK fails, the message remains pending and may be reclaimed again.
        Handlers MUST be idempotent or use DB constraints/locks/OCC to handle
        duplicate processing safely.

        This method loops until `stop()` is called, checking for stale messages
        every `claim_interval_ms` milliseconds. If no stale messages are found,
        it waits for the next interval before checking again.

        Args:
            handler: User-provided function that processes a message within a
                    database transaction. Receives the message and DbSession.
                    Should be the same handler used in `run()` for consistency.
            engine: SQLAlchemy Engine for creating database sessions.
            min_idle_ms: Minimum idle time in milliseconds for a message to be
                        claimed. If None, uses the configured default from
                        config.claim_idle_ms.
            claim_interval_ms: How often to check for stale messages in milliseconds.
                             Default: 5,000 ms (5 seconds).
            max_claim_count: Maximum number of messages to claim per iteration.
                           Default: 10.

        Raises:
            QueueError: If Redis operation fails (including claim or ACK failures)
            Any exception raised by the handler (never caught or swallowed)
        """
        # Use configured default if min_idle_ms is not provided
        if min_idle_ms is None:
            min_idle_ms = self.config.claim_idle_ms

        claim_interval_seconds = claim_interval_ms / 1000.0

        while not self._stopping.is_set():
            # Claim stale messages
            claimed_messages = self._queue.claim_stale(
                min_idle_ms=min_idle_ms, count=max_claim_count
            )

            # Process each claimed message
            for msg in claimed_messages:
                # Check if we should stop before processing each message
                if self._stopping.is_set():
                    break

                try:
                    with DbSession(engine) as session:
                        handler(msg, session)
                except Exception:
                    # DbSession rolled back automatically
                    # Message NOT acked → remains pending (may be reclaimed again)
                    raise
                else:
                    # Commit already happened
                    self.ack(msg)

            # Wait for the next interval, but check stop signal periodically
            # Use wait() with timeout for better responsiveness to stop()
            if not self._stopping.wait(timeout=claim_interval_seconds):
                # Timeout reached, continue to next iteration
                continue
            # Stop signal was set, exit loop
            break

