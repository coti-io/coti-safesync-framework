from __future__ import annotations

import pytest

from conquiet.db.session import DbSession

from ._harness import (
    create_counter_table,
    default_ops_per_process,
    default_processes,
    make_engine,
    read_counter,
    run_workers,
    seed_counter,
    unique_table,
)


def _atomic_update_worker(*, engine, worker_id: int, table: str, ops: int) -> None:
    """
    Worker that performs atomic SQL updates without any locks.
    
    Each update is a single atomic statement: UPDATE ... SET value = value + 1
    """
    for _ in range(ops):
        with DbSession(engine) as session:
            session.execute(
                f"UPDATE `{table}` SET value = value + 1 WHERE id = 1",
                {},
            )
            print(f"Worker {worker_id} updated value to {session.execute_scalar(f'SELECT value FROM `{table}` WHERE id = 1', {})}")


@pytest.mark.concurrency
def test_invariant_atomic_sql_correctness() -> None:
    """
    Invariant 5.2: Atomic SQL correctness.
    
    Multiple workers perform atomic SQL updates (UPDATE ... SET value = value + 1).
    No explicit locks are used.
    
    Invariant: Final value equals number of UPDATE statements executed.
    """
    engine = make_engine()
    table = unique_table("conquiet_conc_atomic")
    create_counter_table(engine, table)
    seed_counter(engine, table, value=0)

    processes = default_processes()
    ops = default_ops_per_process()
    expected = processes * ops

    import multiprocessing as mp

    run_workers(
        table=table,
        processes=processes,
        worker_fn=_atomic_update_worker,
        worker_kwargs={"ops": ops},
        mp_ctx=mp.get_context("spawn"),
    )

    final_row = read_counter(engine, table)
    assert final_row["value"] == expected

