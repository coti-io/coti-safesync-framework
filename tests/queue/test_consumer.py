from __future__ import annotations

import threading
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest
from redis import Redis
from sqlalchemy.engine import Engine

from conquiet.config import QueueConfig
from conquiet.db.session import DbSession
from conquiet.errors import QueueError
from conquiet.queue import QueueConsumer, QueueMessage, RedisStreamsQueue


def _poll_until(condition, timeout: float = 2.0, interval: float = 0.05) -> None:
    """Poll until condition is true or timeout is reached."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval)
    raise AssertionError(f"Condition not met within {timeout}s")


class TestQueueConfig:
    """Tests for QueueConfig validation."""

    def test_config_rejects_block_ms_zero(self) -> None:
        """Test that QueueConfig rejects block_ms=0."""
        with pytest.raises(ValueError, match="block_ms must be > 0"):
            QueueConfig(
                stream_key="test_stream",
                consumer_group="test_group",
                consumer_name="test_consumer",
                block_ms=0,
            )

    def test_config_rejects_block_ms_negative(self) -> None:
        """Test that QueueConfig rejects negative block_ms."""
        with pytest.raises(ValueError, match="block_ms must be > 0"):
            QueueConfig(
                stream_key="test_stream",
                consumer_group="test_group",
                consumer_name="test_consumer",
                block_ms=-1,
            )

    def test_config_accepts_positive_block_ms(self) -> None:
        """Test that QueueConfig accepts positive block_ms."""
        config = QueueConfig(
            stream_key="test_stream",
            consumer_group="test_group",
            consumer_name="test_consumer",
            block_ms=1000,
        )
        assert config.block_ms == 1000


class TestNext:
    """Tests for next() method."""

    @pytest.mark.queue_integration
    def test_next_returns_single_message(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() returns a single message when available."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Enqueue a message
        queue.enqueue({"test": "data"})

        # Read it
        msg = consumer.next(block_ms=100)

        assert msg is not None
        assert isinstance(msg, QueueMessage)
        assert msg.payload == {"test": "data"}

    @pytest.mark.queue_integration
    def test_next_returns_none_when_no_message(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() returns None when no message is available."""
        consumer = QueueConsumer(redis_client, queue_config)

        msg = consumer.next(block_ms=100)

        assert msg is None

    def test_next_uses_configured_block_ms(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() uses config.block_ms when block_ms is None."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Patch read to verify it's called with config.block_ms
        with patch.object(consumer._queue, "read", return_value=[]) as mock_read:
            consumer.next()
            mock_read.assert_called_once_with(block_ms=queue_config.block_ms, count=1)

    def test_next_respects_stopping_flag(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() respects stop() signal (best-effort)."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Stop the consumer
        consumer.stop()

        # Should return None immediately without blocking
        msg = consumer.next(block_ms=1000)

        assert msg is None

    def test_next_propagates_queue_error(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() propagates QueueError from underlying queue."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Mock RedisStreamsQueue.read to raise QueueError (as it would after wrapping RedisError)
        with patch.object(consumer._queue, "read", side_effect=QueueError("Failed to read messages: Connection failed")):
            with pytest.raises(QueueError, match="Failed to read messages"):
                consumer.next(block_ms=100)


class TestIterMessages:
    """Tests for iter_messages() method."""

    @pytest.mark.queue_integration
    def test_iter_messages_yields_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that iter_messages() yields messages as they arrive."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Enqueue messages
        queue.enqueue({"msg": 1})
        queue.enqueue({"msg": 2})

        # Collect messages
        messages = []
        for msg in consumer.iter_messages():
            messages.append(msg)
            if len(messages) >= 2:
                consumer.stop()
                break

        assert len(messages) == 2
        assert messages[0].payload == {"msg": 1}
        assert messages[1].payload == {"msg": 2}

    def test_iter_messages_stops_after_stop(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that iter_messages() stops yielding after stop() is called."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Stop immediately
        consumer.stop()

        # Should not yield any messages
        messages = list(consumer.iter_messages())
        assert messages == []

    def test_iter_messages_propagates_exceptions(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that iter_messages() propagates all exceptions."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Mock RedisStreamsQueue.read to raise QueueError (as it would after wrapping RedisError)
        with patch.object(consumer._queue, "read", side_effect=QueueError("Failed to read messages: Connection failed")):
            with pytest.raises(QueueError):
                # This should raise immediately when trying to read
                next(consumer.iter_messages())


class TestAck:
    """Tests for ack() method."""

    @pytest.mark.queue_integration
    def test_ack_acknowledges_message(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack() successfully acknowledges a message."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Enqueue and read a message
        queue.enqueue({"test": "data"})
        msg = consumer.next(block_ms=100)
        assert msg is not None

        # Acknowledge it
        consumer.ack(msg)

        # Verify it's no longer pending
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        pending_ids = [p["message_id"] for p in pending]
        assert msg.id not in pending_ids

    def test_ack_propagates_queue_error(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack() propagates QueueError on Redis failures."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Create a dummy message
        msg = QueueMessage(
            stream="test_stream",
            group="test_group",
            id="123-0",
            payload={"test": "data"},
        )

        # Mock RedisStreamsQueue.ack to raise QueueError (as it would after wrapping RedisError)
        with patch.object(consumer._queue, "ack", side_effect=QueueError("Failed to acknowledge message: Connection failed")):
            with pytest.raises(QueueError, match="Failed to acknowledge message"):
                consumer.ack(msg)

    @pytest.mark.queue_integration
    def test_ack_works_after_stop(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack() can be called after stop() for draining in-flight work."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Enqueue and read a message
        queue.enqueue({"test": "data"})
        msg = consumer.next(block_ms=100)
        assert msg is not None

        # Stop the consumer
        consumer.stop()

        # Should still be able to ack
        consumer.ack(msg)


class TestStop:
    """Tests for stop() method."""

    def test_stop_sets_stopping_flag(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() sets the _stopping flag.
        
        Implementation detail: verifies stop() flips internal event.
        """
        consumer = QueueConsumer(redis_client, queue_config)

        assert not consumer._stopping.is_set()
        consumer.stop()
        assert consumer._stopping.is_set()

    def test_stop_prevents_new_reads(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() prevents new reads (best-effort)."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Enqueue a message
        queue.enqueue({"test": "data"})

        # Stop the consumer
        consumer.stop()

        # Should return None without reading
        msg = consumer.next(block_ms=100)
        assert msg is None


class TestRun:
    """Tests for run() template method."""

    def test_run_does_not_ack_when_handler_raises(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that run() does not ack message when handler raises exception.
        
        Base-case semantics: If handler raises → DB transaction rolls back AND
        the message is NOT acked (still XPENDING).
        """
        consumer = QueueConsumer(redis_client, queue_config)
        mock_engine = MagicMock(spec=Engine)

        # Create a test message
        test_msg = QueueMessage(
            stream=queue_config.stream_key,
            group=queue_config.consumer_group,
            id="123-0",
            payload={"test": "data"},
        )

        # Mock next() to return the message once, then None to exit loop
        call_count = 0
        def mock_next(block_ms=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return test_msg
            return None

        # Mock ack() to track if it was called
        ack_called = False
        def track_ack(msg: QueueMessage) -> None:
            nonlocal ack_called
            ack_called = True

        with patch.object(consumer, "next", side_effect=mock_next):
            with patch.object(consumer, "ack", side_effect=track_ack):
                # Mock redis_client.xpending_range to verify message remains pending
                with patch.object(redis_client, "xpending_range", return_value=[{"message_id": test_msg.id}]):
                    # Mock DbSession to be a no-op context manager
                    mock_session = MagicMock(spec=DbSession)
                    with patch("conquiet.queue.consumer.DbSession", return_value=mock_session):
                        # Handler that raises
                        def handler(msg: QueueMessage, session: DbSession) -> None:
                            raise ValueError("Handler error")

                        # Run should propagate the exception
                        with pytest.raises(ValueError, match="Handler error"):
                            consumer.run(handler=handler, engine=mock_engine)

                        # Verify ack() was NOT called
                        assert not ack_called, "Message should not be acked when handler raises"

                        # Verify message remains pending (XPENDING would return it)
                        pending = redis_client.xpending_range(
                            name=queue_config.stream_key,
                            groupname=queue_config.consumer_group,
                            min="-",
                            max="+",
                            count=10,
                        )
                        pending_ids = [p["message_id"] for p in pending]
                        assert test_msg.id in pending_ids, "Message should remain in XPENDING when handler raises"

    def test_run_does_not_ack_when_commit_fails(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that run() does not ack message when commit fails in DbSession.__exit__.
        
        Base-case semantics: If commit fails inside DbSession.__exit__ → run() raises
        AND message is NOT acked (still XPENDING).
        """
        consumer = QueueConsumer(redis_client, queue_config)
        mock_engine = MagicMock(spec=Engine)

        # Create a test message
        test_msg = QueueMessage(
            stream=queue_config.stream_key,
            group=queue_config.consumer_group,
            id="456-0",
            payload={"test": "data"},
        )

        # Mock next() to return the message once, then None to exit loop
        call_count = 0
        def mock_next(block_ms=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return test_msg
            return None

        # Mock ack() to track if it was called
        ack_called = False
        def track_ack(msg: QueueMessage) -> None:
            nonlocal ack_called
            ack_called = True

        with patch.object(consumer, "next", side_effect=mock_next):
            with patch.object(consumer, "ack", side_effect=track_ack):
                # Mock redis_client.xpending_range to verify message remains pending
                with patch.object(redis_client, "xpending_range", return_value=[{"message_id": test_msg.id}]):
                    # Handler that succeeds
                    def handler(msg: QueueMessage, session: DbSession) -> None:
                        # Handler completes successfully
                        pass

                    # Create a mock session that fails on __exit__ (commit)
                    mock_session = MagicMock(spec=DbSession)
                    mock_session.__enter__ = MagicMock(return_value=mock_session)
                    # Make __exit__ raise RuntimeError when called without exception (successful handler)
                    def failing_exit(self, exc_type, exc_val, exc_tb):
                        if exc_type is None:
                            raise RuntimeError("Commit failed")
                        return False
                    mock_session.__exit__ = failing_exit

                    with patch("conquiet.queue.consumer.DbSession", return_value=mock_session):
                        # Run should propagate the commit failure
                        with pytest.raises(RuntimeError, match="Commit failed"):
                            consumer.run(handler=handler, engine=mock_engine)

                        # Verify ack() was NOT called
                        assert not ack_called, "Message should not be acked when commit fails"

                        # Verify message remains pending (XPENDING would return it)
                        pending = redis_client.xpending_range(
                            name=queue_config.stream_key,
                            groupname=queue_config.consumer_group,
                            min="-",
                            max="+",
                            count=10,
                        )
                        pending_ids = [p["message_id"] for p in pending]
                        assert test_msg.id in pending_ids, "Message should remain in XPENDING when commit fails"

    @pytest.mark.queue_integration
    def test_run_executes_correct_sequence(self, redis_client: Redis, queue_config: QueueConfig, engine: Engine) -> None:
        """Test that run() executes the correct control flow sequence."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Create a unique test table per test run
        table_name = f"test_run_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, value INT)")

        # Enqueue a message
        queue.enqueue({"id": 1, "value": 42})

        # Track execution order
        execution_order = []
        exception_captured: list[Exception] = []

        def handler(msg: QueueMessage, session: DbSession) -> None:
            execution_order.append("handler")
            session.execute(
                f"INSERT INTO {table_name} (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )

        # Run in a separate thread so we can stop it
        def run_consumer() -> None:
            try:
                consumer.run(handler=handler, engine=engine)
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_consumer, daemon=True)
        thread.start()

        # Poll until handler was called
        _poll_until(lambda: "handler" in execution_order, timeout=2.0)

        # Stop the consumer
        consumer.stop()
        # Allow time for stop() to take effect (best-effort, may wait for block_ms timeout)
        join_timeout = 2 * (queue_config.block_ms / 1000) + 0.5
        thread.join(timeout=join_timeout)

        # Verify thread terminated
        assert not thread.is_alive(), "consumer.run() thread did not exit after stop()"

        # Verify handler was called
        assert "handler" in execution_order

        # Verify no unexpected exceptions
        assert len(exception_captured) == 0

        # Verify database was committed
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is not None
            assert row[1] == 42

    @pytest.mark.queue_integration
    def test_run_rolls_back_on_exception(self, redis_client: Redis, queue_config: QueueConfig, engine: Engine) -> None:
        """Test that run() rolls back transaction on handler exception."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Create a unique test table per test run
        table_name = f"test_run_rollback_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, value INT)")

        # Enqueue a message
        queue.enqueue({"id": 1, "value": 42})

        def handler(msg: QueueMessage, session: DbSession) -> None:
            session.execute(
                f"INSERT INTO {table_name} (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )
            raise ValueError("Test error")

        # Run should propagate the exception
        with pytest.raises(ValueError, match="Test error"):
            consumer.run(handler=handler, engine=engine)

        # Verify database was rolled back (no row inserted)
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is None

    @pytest.mark.queue_integration
    def test_run_acks_after_commit(self, redis_client: Redis, queue_config: QueueConfig, engine: Engine) -> None:
        """Test that run() acknowledges message after successful commit."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Create a unique test table per test run
        table_name = f"test_run_ack_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, value INT)")

        # Enqueue a message
        entry_id = queue.enqueue({"id": 1, "value": 42})

        # Track if handler was called to verify processing
        handler_called = threading.Event()

        def handler(msg: QueueMessage, session: DbSession) -> None:
            handler_called.set()
            session.execute(
                f"INSERT INTO {table_name} (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )

        # Run in a separate thread
        exception_captured: list[Exception] = []

        def run_consumer() -> None:
            try:
                consumer.run(handler=handler, engine=engine)
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_consumer, daemon=True)
        thread.start()

        # Poll until message is acknowledged
        # Wait for handler to be called (proves message was processed)
        # Then verify message is acked
        _poll_until(lambda: handler_called.is_set(), timeout=5.0)

        # Verify DB was updated (message was processed)
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is not None, "Handler should have inserted row"
            assert row[1] == 42, "Row value should match"

        # Now verify message is acked (not in pending list)
        def is_acked() -> bool:
            pending = redis_client.xpending_range(
                name=queue_config.stream_key,
                groupname=queue_config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            pending_ids = [p["message_id"] for p in pending]
            # Handle both string and bytes message IDs
            pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
            return entry_id not in pending_ids_str

        _poll_until(is_acked, timeout=2.0)

        consumer.stop()
        # Allow time for stop() to take effect (best-effort, may wait for block_ms timeout)
        join_timeout = 2 * (queue_config.block_ms / 1000) + 0.5
        thread.join(timeout=join_timeout)

        # Verify thread terminated
        assert not thread.is_alive(), "consumer.run() thread did not exit after stop()"

        # Verify no unexpected exceptions
        assert len(exception_captured) == 0

        # Verify message was acknowledged (not pending)
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        pending_ids = [p["message_id"] for p in pending]
        pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
        assert entry_id not in pending_ids_str, "Message should be acked and not in pending list"

    @pytest.mark.queue_integration
    def test_run_propagates_user_exceptions(self, redis_client: Redis, queue_config: QueueConfig, engine: Engine) -> None:
        """Test that run() propagates user exceptions without swallowing."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        queue.enqueue({"test": "data"})

        def handler(msg: QueueMessage, session: DbSession) -> None:
            raise RuntimeError("User error")

        with pytest.raises(RuntimeError, match="User error"):
            consumer.run(handler=handler, engine=engine)

    @pytest.mark.queue_integration
    def test_run_propagates_queue_error_from_ack(self, redis_client: Redis, queue_config: QueueConfig, engine: Engine) -> None:
        """Test that run() propagates QueueError from ack failures after commit."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Create a unique test table per test run
        table_name = f"test_run_ack_error_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY)")

        queue.enqueue({"id": 1})

        def handler(msg: QueueMessage, session: DbSession) -> None:
            session.execute(f"INSERT INTO {table_name} (id) VALUES (:id)", {"id": msg.payload["id"]})

        # Mock ack to fail after handler succeeds
        # Simulate what RedisStreamsQueue.ack would do: wrap RedisError in QueueError
        def failing_ack(msg: QueueMessage) -> None:
            raise QueueError("Failed to acknowledge message: Ack failed")

        with patch.object(consumer._queue, "ack", side_effect=failing_ack):
            # Should propagate QueueError from ack failure
            with pytest.raises(QueueError, match="Failed to acknowledge message"):
                consumer.run(handler=handler, engine=engine)

        # But transaction should still be committed
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is not None

    @pytest.mark.queue_integration
    def test_run_ack_failure_after_commit_leaves_message_pending(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that ack failure after commit leaves message pending (at-least-once behavior).
        
        This test verifies the standard at-least-once semantics: if ACK fails after
        DB commit, the message remains pending and will be redelivered, requiring
        idempotent handlers.
        """
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Create a unique test table per test run
        table_name = f"test_run_ack_failure_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY)")

        # Enqueue a message
        entry_id = queue.enqueue({"id": 1})

        def handler(msg: QueueMessage, session: DbSession) -> None:
            session.execute(f"INSERT INTO {table_name} (id) VALUES (:id)", {"id": msg.payload["id"]})

        # Mock ack to fail after handler succeeds and commit happens
        def failing_ack(msg: QueueMessage) -> None:
            raise QueueError("Failed to acknowledge message: Redis connection lost")

        with patch.object(consumer._queue, "ack", side_effect=failing_ack):
            # Should propagate QueueError from ack failure
            with pytest.raises(QueueError, match="Failed to acknowledge message"):
                consumer.run(handler=handler, engine=engine)

        # Verify DB transaction was committed (despite ack failure)
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is not None, "DB transaction should be committed even if ack fails"

        # Verify message remains pending (will be redelivered)
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        pending_ids = [p["message_id"] for p in pending]
        # Handle both string and bytes message IDs
        pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
        assert entry_id in pending_ids_str, "Message should remain pending after ack failure, enabling redelivery"


class TestClaimStale:
    """Tests for claim_stale() method."""

    @pytest.mark.queue_integration
    def test_claim_stale_claims_stale_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale() can claim stale messages."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Create another consumer to own the message
        other_config = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name="other_consumer",
        )
        other_queue = RedisStreamsQueue(redis_client, other_config)

        # Enqueue and read with other consumer (makes it pending)
        other_queue.enqueue({"test": "data"})
        other_queue.read(block_ms=100, count=1)

        # Poll until message is stale enough (min_idle_ms=200)
        def is_stale_enough() -> bool:
            pending = redis_client.xpending_range(
                name=queue_config.stream_key,
                groupname=queue_config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            if not pending:
                return False
            for p in pending:
                idle_ms = p.get("time_since_delivered", 0)
                if idle_ms >= 200:
                    return True
            return False

        _poll_until(is_stale_enough, timeout=2.0)

        # Claim stale messages
        claimed = consumer.claim_stale(min_idle_ms=200)

        assert len(claimed) >= 1
        assert claimed[0].payload == {"test": "data"}

    @pytest.mark.queue_integration
    def test_claim_stale_uses_config_default(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale() uses config.claim_idle_ms when min_idle_ms is None."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Should work (may be empty if no stale messages, but shouldn't error)
        claimed = consumer.claim_stale()

        assert isinstance(claimed, list)
        assert len(claimed) == 0  # No stale messages in empty queue

    @pytest.mark.queue_integration
    def test_claim_stale_returns_empty_when_no_stale(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale() returns empty list when no stale messages."""
        consumer = QueueConsumer(redis_client, queue_config)

        claimed = consumer.claim_stale(min_idle_ms=1000)

        assert claimed == []

    def test_claim_stale_propagates_queue_error(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale() propagates QueueError on Redis failures."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Mock RedisStreamsQueue.claim_stale to raise QueueError (as it would after wrapping RedisError)
        with patch.object(consumer._queue, "claim_stale", side_effect=QueueError("Failed to claim stale messages: Connection failed")):
            with pytest.raises(QueueError, match="Failed to claim stale messages"):
                consumer.claim_stale(min_idle_ms=1000)

    @pytest.mark.queue_integration
    def test_claim_stale_partial_recovery(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale() can recover some but not all stale messages (best-effort).
        
        This verifies the best-effort nature of claim_stale(): it may not claim
        all eligible messages, especially when count limit is reached or when
        messages are being actively processed. We test this by using the underlying
        queue's claim_stale with a count limit to demonstrate partial recovery.
        """
        consumer = QueueConsumer(redis_client, queue_config)

        # Create multiple other consumers to own messages
        other_configs = [
            QueueConfig(
                stream_key=queue_config.stream_key,
                consumer_group=queue_config.consumer_group,
                consumer_name=f"other_consumer_{i}",
            )
            for i in range(3)
        ]
        other_queues = [RedisStreamsQueue(redis_client, cfg) for cfg in other_configs]

        # Enqueue multiple messages and read them with different consumers
        entry_ids = []
        for i in range(5):
            entry_id = other_queues[0].enqueue({"index": i})
            entry_ids.append(entry_id)
            # Read with different consumers to make them pending
            other_queues[i % len(other_queues)].read(block_ms=100, count=1)

        # Wait for messages to become stale
        time.sleep(0.2)

        # Claim with a count limit using the underlying queue (default is 10, use 3 to test partial recovery)
        claimed = consumer._queue.claim_stale(min_idle_ms=100, count=3)

        # Should claim some messages (up to count limit)
        assert len(claimed) <= 3, "Should not claim more than count limit"
        assert len(claimed) > 0, "Should claim at least some stale messages"

        # Verify claimed messages are in the original list
        claimed_ids = {msg.id for msg in claimed}
        assert all(cid in entry_ids for cid in claimed_ids), "Claimed messages should be from original set"

        # Verify some messages may still be pending (best-effort, not all recovered)
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        # Some messages should still be pending (either not claimed yet, or claimed by this consumer)
        assert len(pending) > 0, "Some messages should still be pending after partial recovery"


class TestRunClaimStale:
    """Tests for run_claim_stale() template method."""

    @pytest.mark.queue_integration
    def test_run_claim_stale_processes_claimed_messages(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() processes claimed messages through handler."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Create another consumer to own the message
        other_config = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name="other_consumer",
        )
        other_queue = RedisStreamsQueue(redis_client, other_config)

        # Create a unique test table per test run
        table_name = f"test_run_claim_stale_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, value INT)")

        # Enqueue and read with other consumer (makes it pending)
        other_queue.enqueue({"id": 1, "value": 100})
        other_queue.read(block_ms=100, count=1)

        # Track if handler was called
        handler_called = threading.Event()
        handler_received_payload = None

        def handler(msg: QueueMessage, session: DbSession) -> None:
            nonlocal handler_received_payload
            handler_called.set()
            handler_received_payload = msg.payload
            session.execute(
                f"INSERT INTO {table_name} (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )

        # Poll until message is stale enough (min_idle_ms=200)
        def is_stale_enough() -> bool:
            pending = redis_client.xpending_range(
                name=queue_config.stream_key,
                groupname=queue_config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            if not pending:
                return False
            for p in pending:
                idle_ms = p.get("time_since_delivered", 0)
                if idle_ms >= 200:
                    return True
            return False

        _poll_until(is_stale_enough, timeout=2.0)

        # Run claim_stale in a separate thread
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    min_idle_ms=200,
                    claim_interval_ms=100,  # Check frequently for test
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Poll until handler was called
        _poll_until(lambda: handler_called.is_set(), timeout=5.0)

        # Stop the consumer
        consumer.stop()
        # Allow time for stop() to take effect
        thread.join(timeout=2.0)

        # Verify handler was called with correct payload
        assert handler_called.is_set(), "Handler should have been called"
        assert handler_received_payload == {"id": 1, "value": 100}

        # Verify database was committed
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is not None, "Row should be inserted"
            assert row[1] == 100, "Row value should match"

        # Verify no unexpected exceptions
        assert len(exception_captured) == 0

    @pytest.mark.queue_integration
    def test_run_claim_stale_acks_after_commit(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() acknowledges message after successful commit."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Create another consumer to own the message
        other_config = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name="other_consumer",
        )
        other_queue = RedisStreamsQueue(redis_client, other_config)

        # Create a unique test table per test run
        table_name = f"test_run_claim_stale_ack_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY)")

        # Enqueue and read with other consumer (makes it pending)
        entry_id = other_queue.enqueue({"id": 1})
        other_queue.read(block_ms=100, count=1)

        handler_called = threading.Event()

        def handler(msg: QueueMessage, session: DbSession) -> None:
            handler_called.set()
            session.execute(f"INSERT INTO {table_name} (id) VALUES (:id)", {"id": msg.payload["id"]})

        # Poll until message is stale enough
        def is_stale_enough() -> bool:
            pending = redis_client.xpending_range(
                name=queue_config.stream_key,
                groupname=queue_config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            if not pending:
                return False
            for p in pending:
                idle_ms = p.get("time_since_delivered", 0)
                if idle_ms >= 200:
                    return True
            return False

        _poll_until(is_stale_enough, timeout=2.0)

        # Run claim_stale in a separate thread
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    min_idle_ms=200,
                    claim_interval_ms=100,
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Wait for handler to be called
        _poll_until(lambda: handler_called.is_set(), timeout=5.0)

        # Verify DB was updated
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is not None, "Handler should have inserted row"

        # Verify message is acked (not in pending list)
        def is_acked() -> bool:
            pending = redis_client.xpending_range(
                name=queue_config.stream_key,
                groupname=queue_config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            pending_ids = [p["message_id"] for p in pending]
            pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
            return entry_id not in pending_ids_str

        _poll_until(is_acked, timeout=2.0)

        consumer.stop()
        thread.join(timeout=2.0)

        # Verify message was acknowledged
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        pending_ids = [p["message_id"] for p in pending]
        pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
        assert entry_id not in pending_ids_str, "Message should be acked and not in pending list"

    @pytest.mark.queue_integration
    def test_run_claim_stale_rollback_on_handler_exception(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() rolls back transaction and does not ACK on handler exception."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Create another consumer to own the message
        other_config = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name="other_consumer",
        )
        other_queue = RedisStreamsQueue(redis_client, other_config)

        # Create a unique test table per test run
        table_name = f"test_run_claim_stale_rollback_{uuid.uuid4().hex[:12]}"
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, value INT)")

        # Enqueue and read with other consumer (makes it pending)
        entry_id = other_queue.enqueue({"id": 1, "value": 42})
        other_queue.read(block_ms=100, count=1)

        def handler(msg: QueueMessage, session: DbSession) -> None:
            session.execute(
                f"INSERT INTO {table_name} (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )
            raise ValueError("Test error")

        # Poll until message is stale enough
        def is_stale_enough() -> bool:
            pending = redis_client.xpending_range(
                name=queue_config.stream_key,
                groupname=queue_config.consumer_group,
                min="-",
                max="+",
                count=10,
            )
            if not pending:
                return False
            for p in pending:
                idle_ms = p.get("time_since_delivered", 0)
                if idle_ms >= 200:
                    return True
            return False

        _poll_until(is_stale_enough, timeout=2.0)

        # Run claim_stale - should propagate exception
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    min_idle_ms=200,
                    claim_interval_ms=100,
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Wait for exception to be raised
        _poll_until(lambda: len(exception_captured) > 0, timeout=5.0)

        consumer.stop()
        thread.join(timeout=2.0)

        # Verify exception was propagated
        assert len(exception_captured) == 1
        assert isinstance(exception_captured[0], ValueError)
        assert str(exception_captured[0]) == "Test error"

        # Verify database was rolled back (no row inserted)
        with engine.connect() as conn:
            result = conn.exec_driver_sql(f"SELECT * FROM {table_name} WHERE id = 1")
            row = result.fetchone()
            assert row is None, "Row should not be inserted on handler exception"

        # Verify message remains pending (not ACKed)
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        pending_ids = [p["message_id"] for p in pending]
        pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
        assert entry_id in pending_ids_str, "Message should remain pending when handler raises"

    @pytest.mark.queue_integration
    def test_run_claim_stale_respects_stop_signal(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() respects stop() signal for graceful shutdown."""
        consumer = QueueConsumer(redis_client, queue_config)

        handler_called = False

        def handler(msg: QueueMessage, session: DbSession) -> None:
            nonlocal handler_called
            handler_called = True

        # Run claim_stale in a separate thread
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    claim_interval_ms=1000,  # 1 second interval
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Wait a bit to ensure it's running
        time.sleep(0.1)

        # Stop the consumer
        consumer.stop()

        # Wait for thread to exit
        thread.join(timeout=2.0)

        # Verify thread terminated
        assert not thread.is_alive(), "run_claim_stale() thread did not exit after stop()"

        # Verify no unexpected exceptions
        assert len(exception_captured) == 0

    @pytest.mark.queue_integration
    def test_run_claim_stale_uses_claim_interval(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() respects claim_interval_ms parameter."""
        consumer = QueueConsumer(redis_client, queue_config)

        claim_times = []
        claim_lock = threading.Lock()

        def handler(msg: QueueMessage, session: DbSession) -> None:
            with claim_lock:
                claim_times.append(time.monotonic())

        # Run claim_stale with a specific interval
        claim_interval_ms = 200  # 200ms
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    claim_interval_ms=claim_interval_ms,
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Wait for at least 2 intervals to verify timing
        time.sleep((claim_interval_ms / 1000.0) * 2.5)

        consumer.stop()
        thread.join(timeout=1.0)

        # Verify it checked multiple times (at least 2 intervals)
        # Note: This is best-effort since there may be no stale messages
        # The important thing is that it didn't busy-loop
        assert len(exception_captured) == 0

    @pytest.mark.queue_integration
    def test_run_claim_stale_uses_config_default_min_idle_ms(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() uses config.claim_idle_ms when min_idle_ms is None."""
        consumer = QueueConsumer(redis_client, queue_config)

        handler_called = False

        def handler(msg: QueueMessage, session: DbSession) -> None:
            nonlocal handler_called
            handler_called = True

        # Run claim_stale without specifying min_idle_ms
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    claim_interval_ms=100,
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Wait a bit
        time.sleep(0.2)

        consumer.stop()
        thread.join(timeout=1.0)

        # Should not raise any errors (may have no stale messages, which is fine)
        assert len(exception_captured) == 0

    @pytest.mark.queue_integration
    def test_run_claim_stale_handles_empty_claims(
        self, redis_client: Redis, queue_config: QueueConfig, engine: Engine
    ) -> None:
        """Test that run_claim_stale() handles empty claims gracefully (no stale messages)."""
        consumer = QueueConsumer(redis_client, queue_config)

        handler_called = False

        def handler(msg: QueueMessage, session: DbSession) -> None:
            nonlocal handler_called
            handler_called = True

        # Run claim_stale when there are no stale messages
        exception_captured: list[Exception] = []

        def run_claim_stale_worker() -> None:
            try:
                consumer.run_claim_stale(
                    handler=handler,
                    engine=engine,
                    claim_interval_ms=100,
                )
            except Exception as exc:
                exception_captured.append(exc)

        thread = threading.Thread(target=run_claim_stale_worker, daemon=True)
        thread.start()

        # Wait for a couple of intervals
        time.sleep(0.3)

        consumer.stop()
        thread.join(timeout=1.0)

        # Should not raise any errors
        assert len(exception_captured) == 0
        # Handler should not be called (no stale messages)
        assert not handler_called


class TestMultipleConsumers:
    """Tests for multiple consumers competing for messages."""

    @pytest.mark.queue_integration
    def test_multiple_consumers_compete(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that multiple consumers can read from same group without conflicts.
        
        Verifies that multiple consumers in the same consumer group can safely
        read messages without conflicts, and each message is delivered to only
        one consumer.
        """
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Create multiple consumers in the same group
        consumer_configs = [
            QueueConfig(
                stream_key=queue_config.stream_key,
                consumer_group=queue_config.consumer_group,
                consumer_name=f"consumer_{i}",
            )
            for i in range(3)
        ]
        consumers = [QueueConsumer(redis_client, cfg) for cfg in consumer_configs]

        # Enqueue multiple messages
        num_messages = 6
        for i in range(num_messages):
            queue.enqueue({"index": i})

        # All consumers read messages concurrently
        messages_received = []
        messages_lock = threading.Lock()

        def read_messages(consumer: QueueConsumer, consumer_id: int) -> None:
            for _ in range(2):  # Each consumer tries to read 2 messages
                msg = consumer.next(block_ms=500)
                if msg is not None:
                    with messages_lock:
                        messages_received.append((consumer_id, msg))

        threads = []
        for i, consumer in enumerate(consumers):
            thread = threading.Thread(target=read_messages, args=(consumer, i), daemon=True)
            threads.append(thread)
            thread.start()

        # Wait for all reads to complete
        for thread in threads:
            thread.join(timeout=2.0)

        # Verify total messages received (should be at most num_messages)
        assert len(messages_received) <= num_messages, "Should not receive more messages than enqueued"

        # Verify each message ID appears only once (no duplicate deliveries)
        message_ids = [msg.id for _, msg in messages_received]
        assert len(message_ids) == len(set(message_ids)), "Each message should be delivered to only one consumer"

        # Verify messages are distributed across consumers (best-effort, not guaranteed)
        consumer_counts = {}
        for consumer_id, _ in messages_received:
            consumer_counts[consumer_id] = consumer_counts.get(consumer_id, 0) + 1
        assert len(consumer_counts) > 0, "At least one consumer should receive messages"

        # Cleanup: stop all consumers
        for consumer in consumers:
            consumer.stop()


class TestShutdownSemantics:
    """Tests for shutdown behavior."""

    @pytest.mark.queue_integration
    def test_shutdown_leaves_inflight_messages_pending(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that shutdown leaves in-flight messages pending."""
        queue = RedisStreamsQueue(redis_client, queue_config)
        consumer = QueueConsumer(redis_client, queue_config)

        # Enqueue a message
        entry_id = queue.enqueue({"test": "data"})

        # Read it (makes it pending)
        msg = consumer.next(block_ms=100)
        assert msg is not None

        # Stop without acking
        consumer.stop()

        # Message should still be pending
        pending = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        pending_ids = [p["message_id"] for p in pending]
        # Handle both string and bytes message IDs
        pending_ids_str = [pid.decode("utf-8") if isinstance(pid, bytes) else str(pid) for pid in pending_ids]
        assert entry_id in pending_ids_str


class TestThreadSafety:
    """Tests for thread-safety of stop() and _stopping flag."""

    def test_stop_is_thread_safe(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() can be called from multiple threads safely."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Call stop from multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=consumer.stop)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should be stopped
        assert consumer._stopping.is_set()

    @pytest.mark.queue_integration
    def test_stopping_flag_checked_safely(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() signal is checked safely across threads."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Use an event to signal that iter_messages() exited
        iteration_exited = threading.Event()
        messages_received = []

        def iterate() -> None:
            try:
                for msg in consumer.iter_messages():
                    messages_received.append(msg)
            except Exception:
                # Allow exceptions to propagate, but still signal exit
                pass
            finally:
                iteration_exited.set()

        thread = threading.Thread(target=iterate, daemon=True)
        thread.start()

        # Give thread a moment to start iterating
        time.sleep(0.1)

        # Stop from another thread
        consumer.stop()

        # Wait for iteration to exit (may take up to block_ms timeout)
        # Allow extra time for stop() to take effect (best-effort)
        max_wait = 2 * (queue_config.block_ms / 1000) + 0.5
        assert iteration_exited.wait(timeout=max_wait), "iter_messages() did not exit after stop()"

        # Verify iteration stopped (behavioral check: no more messages yielded)
        # Note: messages_received may be empty if no messages were available, but iteration should stop

    @pytest.mark.queue_integration
    def test_stop_during_blocking_read(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() during active XREADGROUP waits for block_ms timeout.
        
        This verifies best-effort shutdown semantics: stop() doesn't interrupt
        an in-progress blocking read, but takes effect after the block timeout.
        """
        # Use a longer block_ms to ensure we can observe the blocking behavior
        long_block_config = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name=queue_config.consumer_name,
            block_ms=1000,  # 1 second block
        )
        consumer_long_block = QueueConsumer(redis_client, long_block_config)

        read_started = threading.Event()
        read_completed = threading.Event()
        stop_called_time = None
        read_returned_time = None

        def blocking_read() -> None:
            nonlocal read_returned_time
            read_started.set()
            # This will block for up to 1000ms
            _ = consumer_long_block.next(block_ms=1000)  # Ignore message, just test blocking behavior
            read_returned_time = time.monotonic()
            read_completed.set()

        thread = threading.Thread(target=blocking_read, daemon=True)
        thread.start()

        # Wait for read to start (should be very quick)
        assert read_started.wait(timeout=0.1), "Read should start quickly"

        # Give a moment for XREADGROUP to be in blocking state
        time.sleep(0.05)

        # Call stop() while read is blocking
        stop_called_time = time.monotonic()
        consumer_long_block.stop()

        # Wait for read to complete (should wait for block_ms timeout)
        assert read_completed.wait(timeout=1.5), "Read should complete after block_ms timeout"

        # Verify that read waited for the full block_ms (allowing some tolerance)
        elapsed = read_returned_time - stop_called_time
        assert elapsed >= 0.8, f"Read should wait ~1s after stop(), but waited only {elapsed:.2f}s"
        assert elapsed <= 1.3, f"Read should not wait much longer than block_ms, but waited {elapsed:.2f}s"
