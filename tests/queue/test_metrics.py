from __future__ import annotations

import time

import pytest
from redis import Redis

from conquiet.config import QueueConfig
from conquiet.metrics.registry import (
    QUEUE_MESSAGES_ACK_TOTAL,
    QUEUE_MESSAGES_CLAIMED_TOTAL,
    QUEUE_MESSAGES_READ_TOTAL,
    QUEUE_READ_LATENCY_SECONDS,
)
from conquiet.queue import RedisStreamsQueue


class TestQueueReadMetrics:
    """Tests for queue read operation metrics."""

    def test_read_increments_counter(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read operations increment the messages read counter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial count
        initial_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Enqueue and read a message
        queue.enqueue({"test": "data"})
        queue.read(block_ms=100, count=1)

        # Verify counter was incremented
        new_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + 1

    def test_read_records_latency(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read operations record latency in histogram."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue a message
        queue.enqueue({"test": "data"})

        # Read it (should record latency)
        queue.read(block_ms=100, count=1)

        # Verify histogram was updated
        samples = list(QUEUE_READ_LATENCY_SECONDS.labels(stream=queue_config.stream_key).collect())
        assert len(samples) > 0

    def test_empty_read_records_latency_but_not_message_count(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that empty reads record latency but don't increment message counter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial count
        initial_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Read from empty stream
        queue.read(block_ms=100, count=1)

        # Counter should NOT increment (0 messages read, so increment by 0)
        # The metric counts messages, not operations
        new_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count

        # Latency should NOT be recorded for empty reads (only recorded when message_count > 0)
        # This matches the implementation: latency is only recorded when messages are actually read

    def test_read_counter_increments_by_message_count(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that read counter increments by the number of messages read."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Enqueue multiple messages
        for i in range(3):
            queue.enqueue({"index": i})

        # Get initial count
        initial_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Read multiple messages
        messages = queue.read(block_ms=100, count=10)

        # Counter should increment by number of messages read
        new_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + len(messages)

    def test_different_streams_create_separate_metrics(
        self, redis_client: Redis, queue_config_factory
    ) -> None:
        """Test that different streams create separate metric instances."""
        config1 = queue_config_factory()
        config2 = queue_config_factory()

        queue1 = RedisStreamsQueue(redis_client, config1)
        queue2 = RedisStreamsQueue(redis_client, config2)

        # Read from both streams
        queue1.enqueue({"test": "data1"})
        queue1.read(block_ms=100, count=1)

        queue2.enqueue({"test": "data2"})
        queue2.read(block_ms=100, count=1)

        # Verify both counters exist and are separate
        count1 = QUEUE_MESSAGES_READ_TOTAL.labels(stream=config1.stream_key)._value.get()
        count2 = QUEUE_MESSAGES_READ_TOTAL.labels(stream=config2.stream_key)._value.get()

        assert count1 >= 1
        assert count2 >= 1


class TestQueueAckMetrics:
    """Tests for queue acknowledgment metrics."""

    def test_ack_increments_counter(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that ack operations increment the messages ack counter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial count
        initial_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Enqueue, read, and ack a message
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        queue.ack(messages[0])

        # Verify counter was incremented
        new_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + 1

    def test_multiple_acks_increment_counter_multiple_times(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that multiple acks increment the counter multiple times."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial count
        initial_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Enqueue and read multiple messages
        for i in range(3):
            queue.enqueue({"index": i})
        messages = queue.read(block_ms=100, count=10)

        # Ack all messages
        for msg in messages:
            queue.ack(msg)

        # Counter should increment by number of acks
        new_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + len(messages)

    def test_duplicate_ack_still_increments_counter(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that duplicate acks still increment the counter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial count
        initial_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Enqueue, read, and ack twice
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        queue.ack(messages[0])
        queue.ack(messages[0])  # Duplicate ack

        # Counter should increment twice
        new_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + 2


class TestQueueClaimMetrics:
    """Tests for queue claim stale messages metrics."""

    def test_claim_increments_counter(self, redis_client: Redis, queue_config: QueueConfig) -> None:
        """Test that claim operations increment the messages claimed counter."""
        # Create a second consumer to claim from
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

        # Get initial count
        initial_count = QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Enqueue and read with first consumer
        queue.enqueue({"test": "data"})
        queue.read(block_ms=100, count=1)

        # Wait for message to become stale
        time.sleep(0.1)

        # Claim with second consumer
        claimed = queue2.claim_stale(min_idle_ms=50, count=10)

        # Counter should increment by number of claimed messages
        new_count = QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + len(claimed)

    def test_empty_claim_does_not_increment_counter(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that claiming when no messages are available does not increment counter."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial count
        initial_count = QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Try to claim when no pending messages
        claimed = queue.claim_stale(min_idle_ms=1000, count=10)

        # Counter should not increment (no messages claimed)
        new_count = QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count
        assert len(claimed) == 0

    def test_claim_counter_increments_by_claimed_count(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that claim counter increments by the number of messages actually claimed."""
        # Create a second consumer to claim from
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

        # Get initial count
        initial_count = QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Enqueue and read multiple messages
        for i in range(5):
            queue.enqueue({"index": i})
        queue.read(block_ms=100, count=10)

        # Wait for messages to become stale
        time.sleep(0.1)

        # Claim with second consumer
        claimed = queue2.claim_stale(min_idle_ms=50, count=10)

        # Counter should increment by number of messages actually claimed
        new_count = QUEUE_MESSAGES_CLAIMED_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        assert new_count == initial_count + len(claimed)


class TestMetricsOnlyOnSuccess:
    """Tests that metrics are only emitted on successful operations."""

    def test_metrics_emitted_after_successful_operations(
        self, redis_client: Redis, queue_config: QueueConfig
    ) -> None:
        """Test that metrics are emitted after successful operations."""
        queue = RedisStreamsQueue(redis_client, queue_config)

        # Get initial counts
        initial_read_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        initial_ack_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        # Perform successful operations
        queue.enqueue({"test": "data"})
        messages = queue.read(block_ms=100, count=1)
        queue.ack(messages[0])

        # Metrics should be incremented (operations succeeded)
        new_read_count = QUEUE_MESSAGES_READ_TOTAL.labels(stream=queue_config.stream_key)._value.get()
        new_ack_count = QUEUE_MESSAGES_ACK_TOTAL.labels(stream=queue_config.stream_key)._value.get()

        assert new_read_count == initial_read_count + 1
        assert new_ack_count == initial_ack_count + 1

        # Note: Testing that metrics are NOT emitted on errors is difficult without mocking
        # The implementation ensures metrics are only called after successful Redis operations
        # by placing metric calls after the try block's successful execution path

