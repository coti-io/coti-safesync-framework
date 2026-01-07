from __future__ import annotations

import json
import time
from typing import Any, Dict, List

from redis import Redis
from redis.exceptions import RedisError

from ..config import QueueConfig
from ..errors import QueueError
from .metrics import observe_queue_ack, observe_queue_claimed, observe_queue_read
from .models import QueueMessage


class RedisStreamsQueue:
    """
    Low-level API for Redis Streams operations.

    Provides direct interaction with Redis Streams using consumer groups.
    No looping, retries, or signal handling. No message buffering or business logic.

    This class is responsible for:
    - Consumer group initialization
    - Enqueueing messages
    - Reading messages via XREADGROUP
    - Acknowledging messages
    - Claiming stale messages

    All Redis errors (except BUSYGROUP during group creation) are wrapped in QueueError.
    """

    def __init__(self, redis: Redis, config: QueueConfig) -> None:
        """
        Initialize the queue with a Redis client and configuration.

        Args:
            redis: Redis client instance
            config: Queue configuration

        Raises:
            QueueError: If consumer group creation fails (except BUSYGROUP)
        """
        self.redis = redis
        self.config = config
        self._ensure_group_exists()

    def _ensure_group_exists(self) -> None:
        """
        Create the consumer group if it doesn't exist.

        Uses XGROUP CREATE with MKSTREAM to ensure both the group and stream exist.
        BUSYGROUP errors (group already exists) are ignored.
        All other errors are wrapped in QueueError and propagated.
        """
        try:
            self.redis.xgroup_create(
                name=self.config.stream_key,
                groupname=self.config.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except Exception as exc:
            # XGROUP CREATE fails if group exists; ignore that specific case.
            error_msg = str(exc)
            if "BUSYGROUP" in error_msg:
                return
            # Wrap all other errors in QueueError
            raise QueueError(f"Failed to create consumer group: {exc}") from exc

    def enqueue(self, payload: Dict[str, Any]) -> str:
        """
        Add a new entry to the stream.

        Serializes the payload to JSON and adds it to the stream with a single
        field named "data". Returns the auto-generated entry ID.

        Args:
            payload: Dictionary to serialize as JSON

        Returns:
            The Redis stream entry ID (e.g., "1234567890-0")

        Raises:
            QueueError: If Redis operation fails or payload is not JSON-serializable
        """
        try:
            data = {"data": json.dumps(payload)}
        except TypeError as exc:
            raise QueueError("Payload is not JSON-serializable") from exc
        except Exception as exc:
            raise QueueError(f"Failed to serialize payload: {exc}") from exc

        try:
            entry_id = self.redis.xadd(self.config.stream_key, data)
            # xadd returns bytes, convert to string
            if isinstance(entry_id, bytes):
                return entry_id.decode("utf-8")
            return str(entry_id)
        except Exception as exc:
            raise QueueError(f"Failed to enqueue message: {exc}") from exc

    def _parse_entry(self, entry_id: str, fields: Dict[bytes, bytes]) -> QueueMessage:
        """
        Parse a Redis stream entry into a QueueMessage.

        Enforces strict format: exactly one field named "data" containing JSON.

        Args:
            entry_id: The Redis stream entry ID
            fields: The field-value pairs from Redis (bytes keys/values)

        Returns:
            A QueueMessage instance

        Raises:
            QueueError: If format is invalid or JSON decoding fails
        """
        # Enforce strict format: exactly one field named "data"
        field_keys = set(fields.keys())
        expected_keys = {b"data"}

        if field_keys != expected_keys:
            raise QueueError(
                f"Invalid stream entry format: expected exactly one field 'data', "
                f"got {len(field_keys)} field(s): {[k.decode('utf-8', errors='replace') for k in field_keys]}"
            )

        # Extract and decode the "data" field
        raw = fields[b"data"]
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise QueueError(f"Failed to decode message payload: {exc}") from exc

        return QueueMessage(
            stream=self.config.stream_key,
            group=self.config.consumer_group,
            id=entry_id,
            payload=payload,
        )

    def read(
        self,
        block_ms: int,
        count: int = 1,
    ) -> List[QueueMessage]:
        """
        Read messages from the stream using XREADGROUP.

        Messages read become pending in the consumer group. The method blocks
        for up to block_ms milliseconds if no messages are available.

        Args:
            block_ms: Maximum time to block in milliseconds. Must be a positive integer (> 0).
                      Note: block_ms=0 means infinite blocking in Redis, so it is rejected.
            count: Maximum number of messages to read (advisory, may return fewer)

        Returns:
            List of QueueMessage objects (may be empty)

        Raises:
            QueueError: If Redis operation fails or block_ms is 0
        """
        if block_ms == 0:
            raise QueueError("block_ms cannot be 0; Redis interprets 0 as infinite blocking")

        start_time = time.monotonic()
        try:
            # Use XREADGROUP with ">" to read new messages
            # Format: XREADGROUP GROUP <group> <consumer> STREAMS <stream> >
            result = self.redis.xreadgroup(
                groupname=self.config.consumer_group,
                consumername=self.config.consumer_name,
                streams={self.config.stream_key: ">"},
                count=count,
                block=block_ms,
            )

            messages: List[QueueMessage] = []
            if result:
                # Result format: [(stream_name, [(entry_id, {field: value, ...}), ...])]
                for stream_name, entries in result:
                    for entry_id, fields in entries:
                        entry_id_str = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id)
                        msg = self._parse_entry(entry_id_str, fields)
                        messages.append(msg)

            # Emit metrics only on successful read
            # Note: Empty reads are intentional and valid; latency includes blocking time
            # Metrics are wrapped in try/except in the metrics module and never raise
            latency = time.monotonic() - start_time
            observe_queue_read(self.config.stream_key, len(messages), latency)

            return messages
        except QueueError:
            # Don't double-wrap QueueError
            raise
        except RedisError as exc:
            raise QueueError(f"Failed to read messages: {exc}") from exc
        except Exception as exc:
            # Catch any other unexpected exceptions and wrap them
            raise QueueError(f"Failed to read messages: {exc}") from exc

    def ack(self, msg: QueueMessage) -> None:
        """
        Acknowledge a message, removing it from the pending list.

        Acking a non-pending or already-acked ID is allowed by Redis and will not error.

        Args:
            msg: The QueueMessage to acknowledge

        Raises:
            QueueError: If Redis operation fails
        """
        try:
            self.redis.xack(
                self.config.stream_key,
                self.config.consumer_group,
                msg.id,
            )
            # Emit metrics only on successful ack
            # Metrics are wrapped in try/except in the metrics module and never raise
            observe_queue_ack(self.config.stream_key)
        except QueueError:
            # Don't double-wrap QueueError
            raise
        except RedisError as exc:
            raise QueueError(f"Failed to acknowledge message: {exc}") from exc
        except Exception as exc:
            # Catch any other unexpected exceptions and wrap them
            raise QueueError(f"Failed to acknowledge message: {exc}") from exc

    def claim_stale(
        self,
        min_idle_ms: int,
        count: int = 10,
    ) -> List[QueueMessage]:
        """
        Claim stale messages that have been pending longer than min_idle_ms.

        This is a best-effort operation. It uses XPENDING to find pending messages
        and XCLAIM to transfer ownership. Ordering is not preserved, and starvation
        is possible. Not all stale messages may be discovered or reclaimed.

        Args:
            min_idle_ms: Minimum idle time in milliseconds for a message to be claimed
            count: Maximum number of messages to claim (default 10)

        Returns:
            List of claimed QueueMessage objects (may be empty)

        Raises:
            QueueError: If Redis operation fails
        """
        try:
            # Get pending messages; returns list of dicts with keys: message_id, consumer, time_since_delivered, times_delivered
            pending_info = self.redis.xpending_range(
                name=self.config.stream_key,
                groupname=self.config.consumer_group,
                min="-",
                max="+",
                count=count,
            )

            if not pending_info:
                return []

            # Filter by idle time (Redis is authoritative), skip messages already owned by this consumer
            entry_ids_to_claim: List[str] = []
            for entry_info in pending_info:
                entry_id = entry_info.get("message_id")
                consumer = entry_info.get("consumer")
                idle_ms = entry_info.get("time_since_delivered", 0)

                consumer_str = consumer.decode("utf-8") if isinstance(consumer, bytes) else str(consumer)
                if consumer_str == self.config.consumer_name:
                    continue

                if entry_id and idle_ms >= min_idle_ms:
                    entry_id_str = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id)
                    entry_ids_to_claim.append(entry_id_str)

            if not entry_ids_to_claim:
                return []

            # Claim messages using XCLAIM
            # XCLAIM returns: [(entry_id, {field: value, ...}), ...]
            claimed_entries = self.redis.xclaim(
                name=self.config.stream_key,
                groupname=self.config.consumer_group,
                consumername=self.config.consumer_name,
                min_idle_time=min_idle_ms,
                message_ids=entry_ids_to_claim,
            )

            messages: List[QueueMessage] = []
            for entry_id, fields in claimed_entries:
                entry_id_str = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id)
                msg = self._parse_entry(entry_id_str, fields)
                messages.append(msg)

            # Emit metrics only on successful claim
            # Metrics are wrapped in try/except in the metrics module and never raise
            if messages:
                observe_queue_claimed(self.config.stream_key, len(messages))

            return messages
        except QueueError:
            # Don't double-wrap QueueError
            raise
        except RedisError as exc:
            raise QueueError(f"Failed to claim stale messages: {exc}") from exc
        except Exception as exc:
            # Catch any other unexpected exceptions and wrap them
            raise QueueError(f"Failed to claim stale messages: {exc}") from exc

