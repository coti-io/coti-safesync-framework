from __future__ import annotations

import random
import time

import pytest
from sqlalchemy.exc import OperationalError

from conquiet.db.helpers import occ_update
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


def _is_retryable_mysql_error(exc: Exception) -> bool:
    """
    Check if a MySQL error is retryable (deadlock or lock wait timeout).
    
    Error codes:
    - 1205: Lock wait timeout exceeded
    - 1213: Deadlock found when trying to get lock
    """
    if not isinstance(exc, OperationalError):
        return False
    
    # Check if it's a pymysql OperationalError with error code
    if hasattr(exc, "orig") and hasattr(exc.orig, "args") and len(exc.orig.args) > 0:
        error_code = exc.orig.args[0]
        return error_code in (1205, 1213)
    
    # Fallback: check error message
    error_msg = str(exc).lower()
    return "lock wait timeout" in error_msg or "deadlock" in error_msg


def _occ_with_retry_worker(*, engine, worker_id: int, table: str, ops: int) -> None:
    """
    Worker that performs OCC updates with retry on rowcount == 0.
    
    Each successful update increments both value and version.
    Retry loop is outside DbSession to avoid long-lived transactions.
    """
    MAX_RETRIES_PER_OP = 100
    
    for _ in range(ops):
        retries = 0
        while True:
            if retries >= MAX_RETRIES_PER_OP:
                raise RuntimeError(
                    f"OCC operation exceeded max retries ({MAX_RETRIES_PER_OP})"
                )
            
            try:
                with DbSession(engine) as session:
                    row = session.fetch_one(
                        f"SELECT id, value, version FROM `{table}` WHERE id = 1",
                        {},
                    )
                    assert row is not None
                    current_version = int(row["version"])
                    current_value = int(row["value"])

                    rc = occ_update(
                        session=session,
                        table=table,
                        id_column="id",
                        id_value=1,
                        version_column="version",
                        version_value=current_version,
                        updates={"value": current_value + 1},
                    )

                    if rc == 1:
                        break  # success; committed by DbSession exit

                # rc == 0 => version mismatch; retry with a new transaction
                retries += 1
                # Small randomized backoff to reduce contention
                time.sleep(random.uniform(0.001, 0.01))
            except Exception as e:
                # If it's a retryable MySQL operational error (deadlock/lock wait timeout), retry.
                # Otherwise re-raise.
                if _is_retryable_mysql_error(e):
                    retries += 1
                    # Small randomized backoff for retryable errors
                    time.sleep(random.uniform(0.01, 0.05))
                    continue
                raise


def _occ_without_retry_worker(*, engine, worker_id: int, table: str, ops: int) -> None:
    """
    Worker that performs OCC updates WITHOUT retry (ignores rowcount).
    
    This should lead to lost updates (demonstration test).
    """
    for _ in range(ops):
        with DbSession(engine) as session:
            row = session.fetch_one(f"SELECT id, value, version FROM `{table}` WHERE id = 1", {})
            assert row is not None
            
            current_version = int(row["version"])
            current_value = int(row["value"])
            
            # Attempt OCC update, but ignore rowcount (no retry)
            occ_update(
                session=session,
                table=table,
                id_column="id",
                id_value=1,
                version_column="version",
                version_value=current_version,
                updates={"value": current_value + 1},
            )
            # If rowcount == 0, we just continue (lost update)


@pytest.mark.concurrency
def test_invariant_occ_correctness_with_retry() -> None:
    """
    Invariant 5.3: OCC correctness (with retry).
    
    Multiple workers attempt OCC updates using a version column, with retry on rowcount == 0.
    
    Invariants:
    - Each successful update consumes a unique version value
    - No two successful updates may use the same version
    - Final version == total successful updates
    - Final value == initial + total successful updates
    """
    engine = make_engine()
    table = unique_table("conquiet_conc_occ_retry")
    create_counter_table(engine, table)
    seed_counter(engine, table, value=0, version=0)

    processes = default_processes()
    ops = default_ops_per_process()
    expected_total_updates = processes * ops

    import multiprocessing as mp

    run_workers(
        table=table,
        processes=processes,
        worker_fn=_occ_with_retry_worker,
        worker_kwargs={"ops": ops},
        mp_ctx=mp.get_context("spawn"),
    )

    final_row = read_counter(engine, table)
    
    # Invariant: final version equals total successful updates
    assert final_row["version"] == expected_total_updates
    
    # Invariant: final value equals initial + total successful updates
    assert final_row["value"] == expected_total_updates


@pytest.mark.concurrency
@pytest.mark.demo
def test_invariant_occ_demo_without_retry() -> None:
    """
    Demonstration test: OCC without retry may violate invariants.
    
    Workers perform OCC updates but ignore rowcount (no retry on version mismatch).
    This should lead to lost updates and version/value inconsistencies.
    
    This is probabilistic; we retry a few times and pass if we observe *any* violation.
    """
    import multiprocessing as mp

    def attempt() -> bool:
        engine = make_engine()
        table = unique_table("conquiet_conc_occ_noretry")
        create_counter_table(engine, table)
        seed_counter(engine, table, value=0, version=0)

        processes = max(6, default_processes())
        ops = max(75, default_ops_per_process())
        expected_total_updates = processes * ops

        run_workers(
            table=table,
            processes=processes,
            worker_fn=_occ_without_retry_worker,
            worker_kwargs={"ops": ops},
            mp_ctx=mp.get_context("spawn"),
        )

        final_row = read_counter(engine, table)
        
        # Check if invariants are violated:
        # - Version should equal total attempts if all succeeded
        # - Value should equal total successful updates
        # If version != value, or either != expected, we have a violation
        version = int(final_row["version"])
        value = int(final_row["value"])
        
        # Violation if version doesn't match expected (some updates failed silently)
        # or if value doesn't match version (lost updates occurred)
        return version != expected_total_updates or value != expected_total_updates or value != version

    retry_until(
        attempts=5,
        fn=attempt,
        on_fail_message=(
            "Unsafety was not demonstrated: OCC without retry preserved the invariant "
            "across all retries. Increase load or retries if needed."
        ),
    )

