from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from conquiet.db.session import DbSession

from ._harness import (
    create_counter_table,
    default_ops_per_process,
    default_processes,
    make_engine,
    run_workers,
    unique_table,
)


def _idempotent_insert_worker(*, engine, worker_id: int, table: str, ops: int) -> None:
    """
    Worker that attempts to insert the same primary key multiple times.
    
    Duplicate key errors are expected and should be caught/handled.
    The invariant is that at most one row exists for the given primary key.
    """
    for _ in range(ops):
        with DbSession(engine) as session:
            try:
                session.execute(
                    f"INSERT INTO `{table}` (id, value, version) VALUES (:id, :value, :version)",
                    {"id": 1, "value": 100, "version": 0},
                )
            except IntegrityError:
                # Duplicate key error is expected - another worker already inserted
                # This is the idempotent behavior we're testing
                pass


@pytest.mark.concurrency
def test_invariant_idempotent_insert_correctness() -> None:
    """
    Invariant 5.4: Idempotent INSERT correctness.
    
    Multiple workers concurrently attempt to insert the same primary key.
    No explicit locks are used - duplicate key handling is sufficient.
    
    Invariant: At most one row exists for the given primary key.
    """
    engine = make_engine()
    table = unique_table("conquiet_conc_idempotent_insert")
    create_counter_table(engine, table)

    processes = default_processes()
    ops = default_ops_per_process()

    import multiprocessing as mp

    run_workers(
        table=table,
        processes=processes,
        worker_fn=_idempotent_insert_worker,
        worker_kwargs={"ops": ops},
        mp_ctx=mp.get_context("spawn"),
    )

    # Invariant: At most one row exists for the given primary key
    with DbSession(engine) as session:
        count = session.execute_scalar(
            f"SELECT COUNT(*) FROM `{table}` WHERE id = 1",
            {},
        )
        assert count == 1, f"Expected exactly 1 row, found {count}"

