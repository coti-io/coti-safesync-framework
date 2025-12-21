from __future__ import annotations

import pytest

from conquiet.db.locking.advisory_lock import AdvisoryLock
from conquiet.db.locking.row_lock import RowLock
from conquiet.db.session import DbSession

from ._harness import (
    create_counter_table,
    default_ops_per_process,
    default_processes,
    make_engine,
    read_counter,
    retry_until,
    run_workers,
    seed_counter,
    unique_table,
)


def _rmw_no_lock_worker(*, engine, worker_id: int, table: str, ops: int) -> None:
    for _ in range(ops):
        with DbSession(engine) as session:
            row = session.fetch_one(f"SELECT value FROM `{table}` WHERE id = 1", {})
            assert row is not None
            new_val = int(row["value"]) + 1
            session.execute(
                f"UPDATE `{table}` SET value = :v WHERE id = 1",
                {"v": new_val},
            )


def _rmg_with_row_lock_worker(*, engine, worker_id: int, table: str, ops: int) -> None:
    for _ in range(ops):
        with DbSession(engine) as session:
            row = RowLock(session, table, {"id": 1}).acquire()
            assert row is not None
            new_val = int(row["value"]) + 1
            session.execute(f"UPDATE `{table}` SET value = :v WHERE id = 1", {"v": new_val})


def _rmg_with_advisory_lock_worker(*, engine, worker_id: int, table: str, ops: int, key: str) -> None:
    for _ in range(ops):
        with DbSession(engine) as session:
            with AdvisoryLock(session, key, timeout=10):
                row = session.fetch_one(f"SELECT value FROM `{table}` WHERE id = 1", {})
                assert row is not None
                new_val = int(row["value"]) + 1
                session.execute(f"UPDATE `{table}` SET value = :v WHERE id = 1", {"v": new_val})


def _rmg_with_advisory_and_row_lock_worker(
    *, engine, worker_id: int, table: str, ops: int, key: str
) -> None:
    for _ in range(ops):
        with DbSession(engine) as session:
            with AdvisoryLock(session, key, timeout=10):
                row = RowLock(session, table, {"id": 1}).acquire()
                assert row is not None
                new_val = int(row["value"]) + 1
                session.execute(f"UPDATE `{table}` SET value = :v WHERE id = 1", {"v": new_val})


@pytest.mark.concurrency
def test_invariant_no_lost_updates_rowlock_correctness() -> None:
    engine = make_engine()
    table = unique_table("conquiet_conc_rmw_rowlock")
    create_counter_table(engine, table)
    seed_counter(engine, table, value=0)

    processes = default_processes()
    ops = default_ops_per_process()
    expected = processes * ops

    import multiprocessing as mp

    run_workers(
        table=table,
        processes=processes,
        worker_fn=_rmg_with_row_lock_worker,
        worker_kwargs={"ops": ops},
        mp_ctx=mp.get_context("spawn"),
    )

    final_row = read_counter(engine, table)
    assert final_row["value"] == expected


@pytest.mark.concurrency
def test_invariant_no_lost_updates_advisorylock_correctness() -> None:
    engine = make_engine()
    table = unique_table("conquiet_conc_rmw_advisory")
    create_counter_table(engine, table)
    seed_counter(engine, table, value=0)

    processes = default_processes()
    ops = default_ops_per_process()
    expected = processes * ops
    key = f"conquiet_demo_rmw:{table}:1"

    import multiprocessing as mp

    run_workers(
        table=table,
        processes=processes,
        worker_fn=_rmg_with_advisory_lock_worker,
        worker_kwargs={"ops": ops, "key": key},
        mp_ctx=mp.get_context("spawn"),
    )

    final_row = read_counter(engine, table)
    assert final_row["value"] == expected


@pytest.mark.concurrency
def test_invariant_no_lost_updates_advisory_plus_row_correctness() -> None:
    engine = make_engine()
    table = unique_table("conquiet_conc_rmw_advisory_row")
    create_counter_table(engine, table)
    seed_counter(engine, table, value=0)

    processes = default_processes()
    ops = default_ops_per_process()
    expected = processes * ops
    key = f"conquiet_demo_rmw:{table}:1"

    import multiprocessing as mp

    run_workers(
        table=table,
        processes=processes,
        worker_fn=_rmg_with_advisory_and_row_lock_worker,
        worker_kwargs={"ops": ops, "key": key},
        mp_ctx=mp.get_context("spawn"),
    )

    final_row = read_counter(engine, table)
    assert final_row["value"] == expected


@pytest.mark.concurrency
@pytest.mark.demo
def test_invariant_no_lost_updates_unsynchronized_demo() -> None:
    """
    Demonstration test: without synchronization, read-modify-write may lose updates.

    This is probabilistic; we retry a few times and pass if we observe *any* violation.
    """

    import multiprocessing as mp

    def attempt() -> bool:
        engine = make_engine()
        table = unique_table("conquiet_conc_rmw_nolock")
        create_counter_table(engine, table)
        seed_counter(engine, table, value=0)

        processes = max(6, default_processes())
        ops = max(75, default_ops_per_process())
        expected = processes * ops

        run_workers(
            table=table,
            processes=processes,
            worker_fn=_rmw_no_lock_worker,
            worker_kwargs={"ops": ops},
            mp_ctx=mp.get_context("spawn"),
        )

        final_row = read_counter(engine, table)
        return int(final_row["value"]) != expected

    retry_until(
        attempts=5,
        fn=attempt,
        on_fail_message=(
            "Unsafety was not demonstrated: unsynchronized read-modify-write preserved the invariant "
            "across all retries. Increase load or retries if needed."
        ),
    )


