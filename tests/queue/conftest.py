from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterator

import pytest
from redis import Redis

from conquiet.config import QueueConfig

DEFAULT_TEST_REDIS_URL = "redis://127.0.0.1:6379/0"


@pytest.fixture(scope="session")
def redis_url() -> str:
    """
    Redis connection URL for unit tests.

    Prefer setting CONQUIET_TEST_REDIS_URL explicitly. If not set, we fall back to
    the docker-compose defaults in docker-compose.yml.
    """
    return os.environ.get("CONQUIET_TEST_REDIS_URL", DEFAULT_TEST_REDIS_URL)


@pytest.fixture(scope="session")
def redis_client(redis_url: str) -> Iterator[Redis]:
    """
    Session-scoped Redis client for tests.

    We fail fast if Redis is unreachable, so failures are actionable.
    """
    client = Redis.from_url(redis_url, decode_responses=False)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover
        pytest.fail(
            "Redis test server is not reachable.\n"
            f"- CONQUIET_TEST_REDIS_URL={redis_url!r}\n"
            "- If using docker-compose: run `docker compose up -d redis` and wait for healthcheck.\n"
            f"- Underlying error: {exc}",
            pytrace=False,
        )

    yield client

    # Cleanup: close connection
    client.close()


@pytest.fixture
def stream_key_factory(redis_client: Redis, request: pytest.FixtureRequest) -> Iterator[Callable[[], str]]:
    """
    Factory fixture creating per-test unique stream keys.

    Usage:
        stream_key = stream_key_factory()
    """
    created: list[str] = []

    def _create() -> str:
        base = f"test_stream_{request.node.name[:30]}"
        suffix = uuid.uuid4().hex[:10]
        stream_key = f"{base}_{suffix}"

        created.append(stream_key)
        return stream_key

    yield _create

    # Cleanup: delete all streams created during test
    for stream_key in created:
        try:
            redis_client.delete(stream_key)
        except Exception:
            # Ignore cleanup errors
            pass


@pytest.fixture
def queue_config_factory(
    stream_key_factory: Callable[[], str],
    request: pytest.FixtureRequest,
) -> Callable[[], QueueConfig]:
    """
    Factory fixture creating per-test QueueConfig instances.

    Each config uses a unique stream key, consumer group, and consumer name.

    Usage:
        config = queue_config_factory()
    """
    def _create() -> QueueConfig:
        stream_key = stream_key_factory()
        # Use test name and UUID for uniqueness
        test_id = uuid.uuid4().hex[:8]
        consumer_group = f"test_group_{request.node.name[:20]}_{test_id}"
        consumer_name = f"test_consumer_{request.node.name[:20]}_{test_id}"

        return QueueConfig(
            stream_key=stream_key,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
            block_ms=5_000,
            max_read_count=1,
            claim_idle_ms=60_000,
        )

    return _create


@pytest.fixture
def queue_config(queue_config_factory: Callable[[], QueueConfig]) -> QueueConfig:
    """
    A default QueueConfig instance for tests.

    Uses unique stream key, consumer group, and consumer name per test.
    """
    return queue_config_factory()

