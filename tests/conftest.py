from __future__ import annotations

import os
import re
import uuid
from collections.abc import Callable, Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


DEFAULT_TEST_DB_URL = "mysql+pymysql://test_user:test_password@127.0.0.1:3306/test_db"


@pytest.fixture(scope="session")
def mysql_url() -> str:
    """
    MySQL connection URL for unit tests.

    Prefer setting CONQUIET_TEST_DB_URL explicitly. If not set, we fall back to
    the docker-compose defaults in /Users/oded/Projects/COTI/conquiet/docker-compose.yml.
    """
    return os.environ.get("CONQUIET_TEST_DB_URL", DEFAULT_TEST_DB_URL)


@pytest.fixture(scope="session")
def engine(mysql_url: str) -> Iterator[Engine]:
    """
    Session-scoped SQLAlchemy engine for tests.

    We fail fast if MySQL is unreachable, so failures are actionable.
    """
    eng = create_engine(mysql_url, pool_pre_ping=True)
    try:
        with eng.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
    except Exception as exc:  # pragma: no cover
        pytest.fail(
            "MySQL test database is not reachable.\n"
            f"- CONQUIET_TEST_DB_URL={mysql_url!r}\n"
            "- If using docker-compose: run `docker compose up -d mysql` and wait for healthcheck.\n"
            f"- Underlying error: {exc}",
            pytrace=False,
        )

    yield eng
    eng.dispose()


def _sanitize_table_name(name: str) -> str:
    # MySQL table names are case-insensitive on many setups; keep it simple.
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_").lower()
    if not name:
        name = "t"
    return name[:48]


@pytest.fixture
def table_factory(engine: Engine, request: pytest.FixtureRequest) -> Iterator[Callable[[str], str]]:
    """
    Factory fixture creating per-test InnoDB tables.

    Usage:
        table = table_factory("id BIGINT PRIMARY KEY, value INT NOT NULL")
    """
    created: list[str] = []

    def _create(schema_sql: str) -> str:
        base = _sanitize_table_name(f"t_{request.node.name}")
        suffix = uuid.uuid4().hex[:10]
        table = f"{base}_{suffix}"

        with engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{table}`")
            conn.exec_driver_sql(f"CREATE TABLE `{table}` ({schema_sql}) ENGINE=InnoDB")

        created.append(table)
        return table

    yield _create

    with engine.begin() as conn:
        for table in created:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{table}`")


@pytest.fixture
def fresh_table(table_factory: Callable[[str], str]) -> str:
    """
    A default table schema used across DB unit tests.

    Includes:
    - PK `id` (duplicate-safe INSERT tests)
    - `version` (OCC tests)
    - a couple of extra indexed columns (multi-column WHERE determinism tests)
    """
    schema_sql = """
        id BIGINT NOT NULL,
        k1 BIGINT NOT NULL DEFAULT 0,
        k2 BIGINT NOT NULL DEFAULT 0,
        version INT NOT NULL DEFAULT 0,
        value INT NOT NULL DEFAULT 0,
        name VARCHAR(255) NULL,
        PRIMARY KEY (id),
        KEY idx_k1_k2 (k1, k2)
    """
    return table_factory(schema_sql)


