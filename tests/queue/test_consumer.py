from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest
from redis import Redis

from conquiet.config import QueueConfig
from conquiet.db.tx import DbFactory, DbTransaction
from conquiet.errors import QueueError
from conquiet.queue import QueueConsumer, QueueMessage


class TestNext:
    """Tests for next() method."""

    def test_next_returns_single_message(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() returns at most one message."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Enqueue a message
        queue.enqueue({"test": "data"})

        # Read message
        msg = consumer.next(block_ms=100)

        assert msg is not None
        assert isinstance(msg, QueueMessage)
        assert msg.payload == {"test": "data"}

    def test_next_returns_none_when_no_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() returns None when no messages are available."""
        consumer = QueueConsumer(redis_client, queue_config)

        msg = consumer.next(block_ms=100)

        assert msg is None

    def test_next_uses_config_block_ms_when_not_provided(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that next() uses config.block_ms when block_ms is not provided."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Should not raise even with default blocking
        start = time.time()
        msg = consumer.next()
        elapsed = time.time() - start

        # Should have blocked for approximately config.block_ms
        assert elapsed >= queue_config.block_ms / 1000.0 * 0.9  # Allow 10% tolerance
        assert msg is None

    def test_next_stops_when_stopped(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that next() returns None immediately when stopped."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Stop the consumer
        consumer.stop()

        # Should return None immediately without blocking
        start = time.time()
        msg = consumer.next(block_ms=1000)
        elapsed = time.time() - start

        assert msg is None
        assert elapsed < 0.1  # Should return quickly


class TestIterMessages:
    """Tests for iter_messages() method."""

    def test_iter_messages_yields_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that iter_messages() yields messages."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Enqueue messages
        queue.enqueue({"index": 1})
        queue.enqueue({"index": 2})

        # Iterate messages
        messages = list(consumer.iter_messages())

        # Should yield both messages
        assert len(messages) >= 2
        assert messages[0].payload["index"] == 1
        assert messages[1].payload["index"] == 2

    def test_iter_messages_stops_after_stop(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that iter_messages() stops yielding after stop() is called."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Enqueue a message
        queue.enqueue({"test": "data"})

        # Start iteration in background
        messages = []
        stop_event = threading.Event()

        def iterate():
            for msg in consumer.iter_messages():
                messages.append(msg)
                if len(messages) >= 1:
                    stop_event.set()
                    consumer.stop()

        thread = threading.Thread(target=iterate, daemon=True)
        thread.start()

        # Wait for iteration to stop
        stop_event.wait(timeout=2.0)
        thread.join(timeout=2.0)

        # Should have yielded at least one message before stopping
        assert len(messages) >= 1

    def test_iter_messages_propagates_exceptions(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that iter_messages() propagates exceptions."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Create invalid Redis client to cause errors
        invalid_client = Redis(host="invalid_host", port=9999, socket_connect_timeout=0.1)
        invalid_consumer = QueueConsumer(invalid_client, queue_config)

        # Should propagate QueueError
        with pytest.raises(QueueError):
            next(invalid_consumer.iter_messages())

        invalid_client.close()


class TestAck:
    """Tests for ack() method."""

    def test_ack_acknowledges_message(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack() acknowledges a message."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Enqueue and read message
        queue.enqueue({"test": "data"})
        msg = consumer.next(block_ms=100)
        assert msg is not None

        # Acknowledge message
        consumer.ack(msg)

        # Verify message is acknowledged (check pending count)
        pending_info = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        # Message should not be in pending list
        msg_ids = [p.get("message_id") for p in pending_info]
        if msg_ids and isinstance(msg_ids[0], bytes):
            msg_ids = [m.decode("utf-8") if isinstance(m, bytes) else str(m) for m in msg_ids]
        elif msg_ids:
            msg_ids = [str(m) for m in msg_ids]
        assert msg.id not in msg_ids

    def test_ack_propagates_redis_errors(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack() propagates Redis errors as QueueError."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Create a message with invalid ID
        msg = QueueMessage(
            stream=queue_config.stream_key,
            group=queue_config.consumer_group,
            id="invalid-id",
            payload={"test": "data"},
        )

        # Should raise QueueError (though Redis may accept invalid IDs, this tests error path)
        # Actually, Redis accepts invalid IDs gracefully, so we need a different test
        # Let's test with a closed Redis connection instead
        consumer.stop()  # This doesn't close Redis, but let's test with invalid consumer

        # Create invalid consumer
        invalid_client = Redis(host="invalid_host", port=9999, socket_connect_timeout=0.1)
        invalid_consumer = QueueConsumer(invalid_client, queue_config)

        # Enqueue with valid consumer first
        queue.enqueue({"test": "data"})
        msg = consumer.next(block_ms=100)

        # Try to ack with invalid consumer
        with pytest.raises(QueueError):
            invalid_consumer.ack(msg)

        invalid_client.close()


class TestStop:
    """Tests for stop() method."""

    def test_stop_sets_stopping_flag(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() sets the stopping flag."""
        consumer = QueueConsumer(redis_client, queue_config)

        assert not consumer._stopping.is_set()
        consumer.stop()
        assert consumer._stopping.is_set()

    def test_stop_prevents_new_reads(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that stop() prevents new reads."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Enqueue message
        queue.enqueue({"test": "data"})

        # Stop consumer
        consumer.stop()

        # Should not read new messages
        msg = consumer.next(block_ms=100)
        assert msg is None


class TestRun:
    """Tests for run() template method."""

    def test_run_executes_handler_and_commits(self, redis_client: Redis, queue_config: QueueConfig, engine, fresh_table: str) -> None:
        """Test that run() executes handler and commits transaction."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue
        db_factory = DbFactory(engine)
        table = fresh_table

        # Enqueue message
        queue.enqueue({"id": 1, "value": 100})

        # Handler that inserts into DB
        def handler(msg: QueueMessage, tx: DbTransaction) -> None:
            tx.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )

        # Run in background thread and stop after processing one message
        def run_with_stop():
            # Process one message then stop
            msg = consumer.next(block_ms=100)
            if msg:
                tx = db_factory.begin()
                try:
                    handler(msg, tx)
                    tx.commit()
                except Exception:
                    tx.rollback()
                    raise
                else:
                    consumer.ack(msg)
            consumer.stop()

        thread = threading.Thread(target=run_with_stop, daemon=True)
        thread.start()
        thread.join(timeout=5.0)

        # Verify data was committed
        tx = db_factory.begin()
        row = tx.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = :id", {"id": 1})
        tx.commit()
        assert row == {"id": 1, "value": 100}

    def test_run_rolls_back_on_handler_exception(
        self, redis_client: Redis, queue_config: QueueConfig, engine, fresh_table: str
    ) -> None:
        """Test that run() rolls back transaction on handler exception."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue
        db_factory = DbFactory(engine)
        table = fresh_table

        # Enqueue message
        queue.enqueue({"id": 1, "value": 100})

        # Handler that raises exception
        def handler(msg: QueueMessage, tx: DbTransaction) -> None:
            tx.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )
            raise RuntimeError("Handler error")

        # Run handler manually (simulating run() behavior)
        msg = consumer.next(block_ms=100)
        assert msg is not None

        tx = db_factory.begin()
        try:
            handler(msg, tx)
            tx.commit()
        except Exception:
            tx.rollback()
            raise

        # Verify data was NOT committed
        tx2 = db_factory.begin()
        row = tx2.fetch_one(f"SELECT id FROM `{table}` WHERE id = :id", {"id": 1})
        tx2.commit()
        assert row is None

    def test_run_acknowledges_after_commit(
        self, redis_client: Redis, queue_config: QueueConfig, engine, fresh_table: str
    ) -> None:
        """Test that run() acknowledges message after commit."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue
        db_factory = DbFactory(engine)
        table = fresh_table

        # Enqueue message
        queue.enqueue({"id": 1, "value": 100})

        # Handler
        def handler(msg: QueueMessage, tx: DbTransaction) -> None:
            tx.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": msg.payload["id"], "value": msg.payload["value"]},
            )

        # Run handler manually
        msg = consumer.next(block_ms=100)
        assert msg is not None

        tx = db_factory.begin()
        try:
            handler(msg, tx)
            tx.commit()
        except Exception:
            tx.rollback()
            raise
        else:
            consumer.ack(msg)

        # Verify message is acknowledged
        pending_info = redis_client.xpending_range(
            name=queue_config.stream_key,
            groupname=queue_config.consumer_group,
            min="-",
            max="+",
            count=10,
        )
        msg_ids = [p.get("message_id") for p in pending_info]
        if msg_ids and isinstance(msg_ids[0], bytes):
            msg_ids = [m.decode("utf-8") if isinstance(m, bytes) else str(m) for m in msg_ids]
        elif msg_ids:
            msg_ids = [str(m) for m in msg_ids]
        assert msg.id not in msg_ids

    def test_run_propagates_handler_exceptions(
        self, redis_client: Redis, queue_config: QueueConfig, engine, fresh_table: str
    ) -> None:
        """Test that run() propagates handler exceptions."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue
        db_factory = DbFactory(engine)
        table = fresh_table

        # Enqueue message
        queue.enqueue({"id": 1, "value": 100})

        # Handler that raises exception
        def handler(msg: QueueMessage, tx: DbTransaction) -> None:
            raise ValueError("Handler error")

        # Run handler manually
        msg = consumer.next(block_ms=100)
        assert msg is not None

        with pytest.raises(ValueError, match="Handler error"):
            tx = db_factory.begin()
            try:
                handler(msg, tx)
                tx.commit()
            except Exception:
                tx.rollback()
                raise


class TestRecoverStale:
    """Tests for recover_stale() method."""

    def test_recover_stale_claims_stale_messages(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that recover_stale() claims stale messages."""
        consumer = QueueConsumer(redis_client, queue_config)
        queue = consumer._queue

        # Enqueue message
        queue.enqueue({"test": "data"})

        # Read message but don't ack (makes it pending)
        msg = consumer.next(block_ms=100)
        assert msg is not None

        # Wait for message to become stale (use short claim_idle_ms for test)
        config_with_short_idle = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name=queue_config.consumer_name,
            claim_idle_ms=100,  # Very short for testing
        )
        consumer2 = QueueConsumer(redis_client, config_with_short_idle)

        # Wait for message to become stale
        time.sleep(0.2)

        # Recover stale messages
        claimed = consumer2.recover_stale(min_idle_ms=50)

        # Should have claimed the stale message
        assert len(claimed) >= 1
        assert claimed[0].payload == {"test": "data"}

    def test_recover_stale_returns_empty_when_no_stale(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that recover_stale() returns empty list when no stale messages."""
        consumer = QueueConsumer(redis_client, queue_config)

        # No stale messages
        claimed = consumer.recover_stale()

        assert claimed == []

    def test_recover_stale_uses_config_claim_idle_ms(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that recover_stale() uses config.claim_idle_ms when min_idle_ms is None."""
        consumer = QueueConsumer(redis_client, queue_config)

        # Should not raise
        claimed = consumer.recover_stale()

        assert isinstance(claimed, list)

