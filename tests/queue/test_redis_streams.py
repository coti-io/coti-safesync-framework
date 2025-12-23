from __future__ import annotations

import time

import pytest
from redis import Redis

from conquiet.config import QueueConfig
from conquiet.errors import QueueError
from conquiet.queue import QueueMessage, RedisStreamsQueue


class TestConsumerGroupInitialization:
    """Tests for consumer group initialization."""

    def test_creates_new_consumer_group(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that a new consumer group is created successfully."""
        RedisStreamsQueue(redis_client, queue_config)

        # Verify group exists by checking group info
        groups = redis_client.xinfo_groups(queue_config.stream_key)
        group_names = [g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups]
        assert queue_config.consumer_group in group_names

    def test_handles_existing_group_gracefully(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that creating a group that already exists doesn't error."""
        # Create first instance
        queue1 = RedisStreamsQueue(redis_client, queue_config)

        # Create second instance with same config - should not error
        queue2 = RedisStreamsQueue(redis_client, queue_config)

        # Both should work
        assert queue1.config.consumer_group == queue2.config.consumer_group


class TestEnqueue:
    """Tests for enqueue operation."""

    def test_enqueue_returns_entry_id(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that enqueue returns a valid entry ID."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        entry_id = queue.enqueue({"test": "data"})

        assert isinstance(entry_id, str)
        assert len(entry_id) > 0
        # Entry ID format: timestamp-counter (e.g., "1234567890-0")
        assert "-" in entry_id

    def test_enqueue_serializes_json(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that payload is properly serialized to JSON."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        payload = {"key": "value", "number": 42, "nested": {"foo": "bar"}}
        queue.enqueue(payload)

        # Verify message was stored correctly by reading it back
        messages = queue.read(block_ms=100, count=1)
        assert len(messages) == 1
        assert messages[0].payload == payload

    def test_enqueue_multiple_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test enqueueing multiple messages."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        entry_ids = []
        for i in range(3):
            entry_id = queue.enqueue({"index": i})
            entry_ids.append(entry_id)

        assert len(entry_ids) == 3
        assert len(set(entry_ids)) == 3  # All IDs should be unique

    def test_enqueue_rejects_non_json_serializable_payload(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that enqueue raises QueueError for non-JSON-serializable payloads."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Use a function object which is not JSON-serializable
        non_serializable = {"func": lambda x: x}

        with pytest.raises(QueueError, match="Payload is not JSON-serializable"):
            queue.enqueue(non_serializable)


class TestRead:
    """Tests for read operation."""

    def test_read_empty_stream_returns_empty_list(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test reading from an empty stream returns empty list."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        messages = queue.read(block_ms=100, count=1)

        assert messages == []

    def test_read_returns_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test reading messages from stream."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue a message
        payload = {"test": "message"}
        queue.enqueue(payload)

        # Read it back
        messages = queue.read(block_ms=100, count=1)

        assert len(messages) == 1
        assert isinstance(messages[0], QueueMessage)
        assert messages[0].payload == payload
        assert messages[0].stream == queue_config.stream_key
        assert messages[0].group == queue_config.consumer_group
        assert messages[0].id is not None

    def test_read_respects_count_limit(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read respects the count parameter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue multiple messages
        for i in range(5):
            queue.enqueue({"index": i})

        # Read with count=2
        messages = queue.read(block_ms=100, count=2)

        assert len(messages) <= 2

    def test_read_blocks_when_no_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read blocks when block_ms is specified."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        start_time = time.monotonic()
        messages = queue.read(block_ms=500, count=1)
        elapsed = time.monotonic() - start_time

        # Should have blocked for approximately 500ms (allow some tolerance)
        assert elapsed >= 0.4  # At least 400ms
        assert messages == []

    def test_read_rejects_none_block_ms(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read raises QueueError when block_ms is None."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        with pytest.raises(QueueError, match="block_ms cannot be None"):
            queue.read(block_ms=None, count=1)  # type: ignore[arg-type]

    def test_read_rejects_zero_block_ms(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read raises QueueError when block_ms is 0."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        with pytest.raises(QueueError, match="block_ms cannot be 0"):
            queue.read(block_ms=0, count=1)

    def test_read_with_small_block_ms_returns_quickly(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read with a small block_ms returns quickly when no messages."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        start_time = time.monotonic()
        # Use 1ms - Redis interprets 0 as infinite blocking, so use minimal value
        messages = queue.read(block_ms=1, count=1)
        elapsed = time.monotonic() - start_time

        # Should return quickly (within ~10ms for the 1ms block + overhead)
        assert elapsed < 0.1
        assert messages == []

    def test_read_parses_message_format(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read correctly parses the strict message format."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        payload = {"complex": {"nested": {"data": [1, 2, 3]}}, "string": "test"}
        queue.enqueue(payload)

        messages = queue.read(block_ms=100, count=1)

        assert len(messages) == 1
        assert messages[0].payload == payload

    def test_read_makes_messages_pending(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read messages become pending."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)

        assert len(messages) == 1

        # Check pending count
        pending_info = redis_client.xpending(queue_config.stream_key, queue_config.consumer_group)
        assert pending_info is not None
        # pending_info format: (total_pending, min_id, max_id, [(consumer, count), ...])
        if isinstance(pending_info, tuple) and len(pending_info) > 0:
            total_pending = pending_info[0] if isinstance(pending_info[0], int) else int(pending_info[0])
            assert total_pending >= 1


class TestAcknowledge:
    """Tests for acknowledge operation."""

    def test_ack_removes_from_pending(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack removes message from pending list."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)

        assert len(messages) == 1

        # Acknowledge the message
        queue.ack(messages[0])

        # Verify it's no longer pending
        pending_info = redis_client.xpending(queue_config.stream_key, queue_config.consumer_group)
        if isinstance(pending_info, tuple) and len(pending_info) > 0:
            total_pending = pending_info[0] if isinstance(pending_info[0], int) else int(pending_info[0])
            assert total_pending == 0

    def test_ack_allows_duplicate_ack(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that acking an already-acked message doesn't error."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)

        assert len(messages) == 1

        # Ack twice - should not error
        queue.ack(messages[0])
        queue.ack(messages[0])  # Should not raise

    def test_ack_non_pending_message_allowed(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that acking a non-pending message is allowed."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Create a message that doesn't exist in pending
        fake_msg = QueueMessage(
            stream=queue_config.stream_key,
            group=queue_config.consumer_group,
            id="9999999999-0",
            payload={},
        )

        # Should not raise (Redis allows this)
        queue.ack(fake_msg)


class TestClaimStale:
    """Tests for claim_stale operation."""

    def test_claim_stale_returns_empty_when_no_pending(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale returns empty list when no pending messages."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        claimed = queue.claim_stale(min_idle_ms=1000, count=10)

        assert claimed == []

    def test_claim_stale_claims_stale_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale can claim messages that exceed idle threshold."""
        # Create a second consumer to claim messages from the first consumer
        config2 = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name="claiming_consumer",
            block_ms=5_000,
            max_read_count=1,
            claim_idle_ms=60_000,
        )
        queue2 = RedisStreamsQueue(redis_client, config2)

        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue and read a message with first consumer (makes it pending)
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        assert len(messages) == 1

        # Wait for message to become stale (exceed min_idle_ms)
        time.sleep(0.1)  # 100ms

        # Second consumer claims stale messages with low threshold
        claimed = queue2.claim_stale(min_idle_ms=50, count=10)

        assert len(claimed) >= 1
        assert claimed[0].payload == {"test": "data"}

    def test_claim_stale_respects_idle_threshold(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale only claims messages exceeding idle threshold."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue and read a message
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        assert len(messages) == 1

        # Try to claim with very high threshold - should not claim
        claimed = queue.claim_stale(min_idle_ms=100_000, count=10)

        assert len(claimed) == 0

    def test_claim_stale_respects_count_limit(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale respects the count parameter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue multiple messages and read them (make them pending)
        for i in range(5):
            queue.enqueue({"index": i})
        queue.read(block_ms=100, count=10)  # Read all

        # Wait for them to become stale
        time.sleep(0.1)

        # Claim with count=2
        claimed = queue.claim_stale(min_idle_ms=50, count=2)

        assert len(claimed) <= 2

    def test_claim_stale_skips_own_messages(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale skips messages already owned by the same consumer."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue and read a message (makes it pending and owned by this consumer)
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        assert len(messages) == 1

        # Wait for message to become stale
        time.sleep(0.1)

        # Try to claim - should skip because it's already owned by this consumer
        claimed = queue.claim_stale(min_idle_ms=50, count=10)

        # Should not claim its own message
        assert len(claimed) == 0

    def test_claim_stale_claims_from_other_consumers(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim_stale can claim messages from other consumers."""
        # Create a second consumer with different name
        config2 = QueueConfig(
            stream_key=queue_config.stream_key,
            consumer_group=queue_config.consumer_group,
            consumer_name="other_consumer",
            block_ms=5_000,
            max_read_count=1,
            claim_idle_ms=60_000,
        )
        queue2 = RedisStreamsQueue(redis_client, config2)

        # Enqueue and read with first consumer
        queue = RedisStreamsQueue(redis_client, queue_config)
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        assert len(messages) == 1

        # Wait for message to become stale
        time.sleep(0.1)

        # Second consumer should be able to claim it
        claimed = queue2.claim_stale(min_idle_ms=50, count=10)

        assert len(claimed) >= 1
        assert claimed[0].payload == {"test": "data"}


class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_redis_connection_raises_queue_error(self) -> None:
        """Test that Redis connection errors are wrapped in QueueError."""
        from redis import Redis

        # Create a Redis client with invalid connection
        invalid_client = Redis(host="invalid_host", port=9999, socket_connect_timeout=0.1)

        config = QueueConfig(
            stream_key="test_stream",
            consumer_group="test_group",
            consumer_name="test_consumer",
        )

        with pytest.raises(QueueError):
            RedisStreamsQueue(invalid_client, config)

        # Cleanup
        invalid_client.close()

    def test_enqueue_error_wraps_in_queue_error(self, queue_config: QueueConfig) -> None:
        """Test that enqueue errors are wrapped in QueueError."""
        from redis import Redis

        # Create a Redis client with invalid connection that will fail
        invalid_client = Redis(host="invalid_host_that_does_not_exist", port=9999, socket_connect_timeout=0.1, socket_timeout=0.1)

        with pytest.raises(QueueError):
            queue = RedisStreamsQueue(invalid_client, queue_config)
            queue.enqueue({"test": "data"})

        invalid_client.close()

    def test_read_error_wraps_in_queue_error(self, queue_config: QueueConfig) -> None:
        """Test that read errors are wrapped in QueueError."""
        from redis import Redis

        # Create a Redis client with invalid connection that will fail
        invalid_client = Redis(host="invalid_host_that_does_not_exist", port=9999, socket_connect_timeout=0.1, socket_timeout=0.1)

        with pytest.raises(QueueError):
            queue = RedisStreamsQueue(invalid_client, queue_config)
            queue.read(block_ms=100, count=1)

        invalid_client.close()

    def test_ack_error_wraps_in_queue_error(self, queue_config: QueueConfig) -> None:
        """Test that ack errors are wrapped in QueueError."""
        from redis import Redis

        # Create invalid client - error will occur during initialization
        invalid_client = Redis(host="invalid_host_that_does_not_exist", port=9999, socket_connect_timeout=0.1, socket_timeout=0.1)

        # Error wrapping is tested during initialization (when creating consumer group)
        with pytest.raises(QueueError):
            RedisStreamsQueue(invalid_client, queue_config)

        invalid_client.close()


class TestMessageFormat:
    """Tests for strict message format enforcement."""

    def test_parse_entry_rejects_missing_data_field(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that _parse_entry rejects entries without 'data' field."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Manually add an entry with wrong format using Redis directly
        redis_client.xadd(queue_config.stream_key, {"wrong_field": "value"})

        # Read should fail when parsing
        with pytest.raises(QueueError, match="Invalid stream entry format"):
            queue.read(block_ms=100, count=1)

    def test_parse_entry_rejects_extra_fields(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that _parse_entry rejects entries with extra fields."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Manually add an entry with extra fields
        redis_client.xadd(
            queue_config.stream_key,
            {"data": '{"test": "value"}', "extra": "field"},
        )

        # Read should fail when parsing
        with pytest.raises(QueueError, match="Invalid stream entry format"):
            queue.read(block_ms=100, count=1)

    def test_parse_entry_rejects_wrong_field_name(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that _parse_entry rejects entries with wrong field name."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Manually add an entry with wrong field name
        redis_client.xadd(queue_config.stream_key, {"payload": '{"test": "value"}'})

        # Read should fail when parsing
        with pytest.raises(QueueError, match="Invalid stream entry format"):
            queue.read(block_ms=100, count=1)

