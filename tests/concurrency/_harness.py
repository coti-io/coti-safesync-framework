from __future__ import annotations

import os
import queue
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from conquiet.db.session import DbSession


DEFAULT_TEST_DB_URL = "mysql+pymysql://test_user:test_password@127.0.0.1:3306/test_db"


def mysql_url() -> str:
    return os.environ.get("CONQUIET_TEST_DB_URL", DEFAULT_TEST_DB_URL)


def make_engine() -> Engine:
    # Each process should have its own engine/pool.
    return create_engine(mysql_url(), pool_pre_ping=True)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def default_processes() -> int:
    return env_int("CONQUIET_CONCURRENCY_PROCESSES", 5)


def default_ops_per_process() -> int:
    return env_int("CONQUIET_CONCURRENCY_OPS", 50)


def unique_table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def create_counter_table(engine: Engine, table: str) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{table}`")
        conn.exec_driver_sql(
            f"""
            CREATE TABLE `{table}` (
                id BIGINT NOT NULL,
                value BIGINT NOT NULL DEFAULT 0,
                version BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB
            """
        )


def seed_counter(engine: Engine, table: str, *, id_value: int = 1, value: int = 0, version: int = 0) -> None:
    with DbSession(engine) as session:
        session.execute(
            f"INSERT INTO `{table}` (id, value, version) VALUES (:id, :value, :version)",
            {"id": id_value, "value": value, "version": version},
        )


def read_counter(engine: Engine, table: str, *, id_value: int = 1) -> dict[str, Any]:
    with DbSession(engine) as session:
        row = session.fetch_one(f"SELECT id, value, version FROM `{table}` WHERE id = :id", {"id": id_value})
        assert row is not None
        return row


@dataclass(frozen=True)
class WorkerError:
    worker_id: int
    exc_type: str
    message: str


def _worker_entrypoint(
    worker_id: int,
    table: str,
    start_barrier,
    err_queue,
    fn,
    fn_kwargs: dict[str, Any],
) -> None:
    try:
        engine = make_engine()
        try:
            start_barrier.wait()
        except Exception:
            # If barrier breaks, still attempt to run to surface root cause.
            pass

        fn(engine=engine, worker_id=worker_id, table=table, **fn_kwargs)
    except BaseException as exc:  # noqa: BLE001 - must propagate any failure
        err_queue.put(
            WorkerError(
                worker_id=worker_id,
                exc_type=type(exc).__name__,
                message=str(exc),
            )
        )


def run_workers(
    *,
    table: str,
    processes: int,
    worker_fn: Callable[..., None],
    worker_kwargs: dict[str, Any] | None = None,
    mp_ctx,
) -> None:
    worker_kwargs = worker_kwargs or {}

    start_barrier = mp_ctx.Barrier(processes)
    err_queue = mp_ctx.Queue()

    procs = []
    for wid in range(processes):
        p = mp_ctx.Process(
            target=_worker_entrypoint,
            kwargs={
                "worker_id": wid,
                "table": table,
                "start_barrier": start_barrier,
                "err_queue": err_queue,
                "fn": worker_fn,
                "fn_kwargs": worker_kwargs,
            },
        )
        p.start()
        procs.append(p)

    for p in procs:
        p.join(timeout=120)

    # Collect errors, if any.
    errors: list[WorkerError] = []
    while True:
        try:
            errors.append(err_queue.get_nowait())
        except queue.Empty:
            break

    # If any worker didn't exit cleanly, fail with diagnostics.
    bad = [p for p in procs if p.exitcode not in (0, None)]
    if errors or bad:
        details = []
        for e in errors:
            details.append(f"worker {e.worker_id}: {e.exc_type}: {e.message}")
        for p in bad:
            details.append(f"process pid={p.pid} exitcode={p.exitcode}")
        pytest.fail("Worker failures:\n" + "\n".join(details), pytrace=False)


def retry_until(
    *,
    attempts: int,
    fn: Callable[[], bool],
    on_fail_message: str,
) -> None:
    for _ in range(attempts):
        if fn():
            return
    pytest.fail(on_fail_message, pytrace=False)


