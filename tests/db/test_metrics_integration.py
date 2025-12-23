from __future__ import annotations

import pytest
from sqlalchemy import text

from conquiet.db.locking.advisory_lock import AdvisoryLock
from conquiet.db.locking.row_lock import RowLock
from conquiet.db.session import DbSession
from conquiet.metrics.registry import (
    DB_LOCK_ACQUIRE_LATENCY_SECONDS,
    DB_WRITE_LATENCY_SECONDS,
    DB_WRITE_TOTAL,
)


def _get_counter_value(table: str, op_type: str, status: str) -> float:
    """Get current counter value for DB write metric."""
    return DB_WRITE_TOTAL.labels(table=table, op_type=op_type, status=status)._value.get()


def _get_histogram_sample_count(table: str, op_type: str) -> int:
    """Get current sample count for DB write latency histogram."""
    # Get the count sample from histogram (histograms have a _count metric)
    samples = list(DB_WRITE_LATENCY_SECONDS.labels(table=table, op_type=op_type).collect())
    # Find the count sample (usually the first one or one with name ending in _count)
    for sample_family in samples:
        for sample in sample_family.samples:
            if sample.name.endswith("_count"):
                return int(sample.value)
    return 0


def _get_lock_histogram_sample_count(strategy: str) -> int:
    """Get current sample count for lock acquisition latency histogram."""
    # Get the count sample from histogram
    samples = list(DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(strategy=strategy).collect())
    # Find the count sample
    for sample_family in samples:
        for sample in sample_family.samples:
            if sample.name.endswith("_count"):
                return int(sample.value)
    return 0


class TestDbSessionMetricsIntegration:
    """Integration tests for DB metrics emitted by DbSession."""

    def test_insert_emits_metrics(self, engine, fresh_table: str) -> None:
        """Test that INSERT operations emit metrics."""
        table = fresh_table
        
        # Get initial metric values
        initial_count = _get_counter_value(table=table, op_type="insert", status="success")
        initial_samples = _get_histogram_sample_count(table=table, op_type="insert")
        
        # Perform INSERT
        with DbSession(engine) as session:
            session.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": 1, "value": 100},
            )
        
        # Verify metrics were emitted
        new_count = _get_counter_value(table=table, op_type="insert", status="success")
        new_samples = _get_histogram_sample_count(table=table, op_type="insert")
        
        assert new_count == initial_count + 1
        assert new_samples > initial_samples

    def test_update_emits_metrics(self, engine, fresh_table: str) -> None:
        """Test that UPDATE operations emit metrics."""
        table = fresh_table
        
        # Insert a row first
        with DbSession(engine) as session:
            session.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": 1, "value": 100},
            )
        
        # Get initial metric values
        initial_count = _get_counter_value(table=table, op_type="update", status="success")
        initial_samples = _get_histogram_sample_count(table=table, op_type="update")
        
        # Perform UPDATE
        with DbSession(engine) as session:
            session.execute(
                f"UPDATE `{table}` SET value = :value WHERE id = :id",
                {"id": 1, "value": 200},
            )
        
        # Verify metrics were emitted
        new_count = _get_counter_value(table=table, op_type="update", status="success")
        new_samples = _get_histogram_sample_count(table=table, op_type="update")
        
        assert new_count == initial_count + 1
        assert new_samples > initial_samples

    def test_error_status_emitted_on_transaction_failure(self, engine, fresh_table: str) -> None:
        """Test that error status is emitted when transaction fails (rollback)."""
        table = fresh_table
        
        # Get initial metric values
        initial_success_count = _get_counter_value(table=table, op_type="insert", status="success")
        initial_error_count = _get_counter_value(table=table, op_type="insert", status="error")
        
        # Perform INSERT that succeeds but transaction fails due to exception
        with pytest.raises(RuntimeError):
            with DbSession(engine) as session:
                session.execute(
                    f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                    {"id": 1, "value": 100},
                )
                # Raise exception to cause rollback
                raise RuntimeError("Transaction failure")
        
        # Verify error metric was emitted (operation was tracked, then transaction failed)
        new_error_count = _get_counter_value(table=table, op_type="insert", status="error")
        new_success_count = _get_counter_value(table=table, op_type="insert", status="success")
        
        # The operation should be marked as error since transaction rolled back
        assert new_error_count == initial_error_count + 1
        # Success count should not have increased
        assert new_success_count == initial_success_count

    def test_multiple_operations_emit_multiple_metrics(self, engine, fresh_table: str) -> None:
        """Test that multiple operations in same session emit multiple metrics."""
        table = fresh_table
        
        # Get initial metric values
        initial_insert_count = _get_counter_value(table=table, op_type="insert", status="success")
        initial_update_count = _get_counter_value(table=table, op_type="update", status="success")
        
        # Perform multiple operations in one session
        with DbSession(engine) as session:
            # First INSERT
            session.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": 1, "value": 100},
            )
            # Second INSERT
            session.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": 2, "value": 200},
            )
            # UPDATE
            session.execute(
                f"UPDATE `{table}` SET value = :value WHERE id = :id",
                {"id": 1, "value": 150},
            )
        
        # Verify all metrics were emitted
        new_insert_count = _get_counter_value(table=table, op_type="insert", status="success")
        new_update_count = _get_counter_value(table=table, op_type="update", status="success")
        
        assert new_insert_count == initial_insert_count + 2
        assert new_update_count == initial_update_count + 1


class TestLockMetricsIntegration:
    """Integration tests for lock metrics emitted by lock primitives."""

    def test_advisory_lock_emits_metrics(self, engine, fresh_table: str) -> None:
        """Test that AdvisoryLock acquisition emits metrics."""
        table = fresh_table
        
        # Get initial metric values
        initial_samples = _get_lock_histogram_sample_count(strategy="advisory")
        
        # Acquire advisory lock
        with DbSession(engine) as session:
            with AdvisoryLock(session, f"{table}:1"):
                pass
        
        # Verify metrics were emitted
        new_samples = _get_lock_histogram_sample_count(strategy="advisory")
        assert new_samples > initial_samples

    def test_row_lock_emits_metrics(self, engine, fresh_table: str) -> None:
        """Test that RowLock acquisition emits metrics."""
        table = fresh_table
        
        # Insert a row first
        with DbSession(engine) as session:
            session.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": 1, "value": 100},
            )
        
        # Get initial metric values
        initial_samples = _get_lock_histogram_sample_count(strategy="row")
        
        # Acquire row lock
        with DbSession(engine) as session:
            row = RowLock(session, table, {"id": 1}).acquire()
            assert row is not None
        
        # Verify metrics were emitted
        new_samples = _get_lock_histogram_sample_count(strategy="row")
        assert new_samples > initial_samples

    def test_row_lock_emits_metrics_even_when_row_not_found(self, engine, fresh_table: str) -> None:
        """Test that RowLock emits metrics even when row doesn't exist."""
        table = fresh_table
        
        # Get initial metric values
        initial_samples = _get_lock_histogram_sample_count(strategy="row")
        
        # Acquire row lock for non-existent row
        with DbSession(engine) as session:
            row = RowLock(session, table, {"id": 999}).acquire()
            assert row is None
        
        # Verify metrics were still emitted (lock acquisition succeeds, row just doesn't exist)
        new_samples = _get_lock_histogram_sample_count(strategy="row")
        assert new_samples > initial_samples

    def test_advisory_lock_metrics_on_success(self, engine) -> None:
        """Test that successful AdvisoryLock acquisition emits metrics with success=True."""
        # Get initial metric values
        initial_samples = _get_lock_histogram_sample_count(strategy="advisory")
        
        # Acquire lock successfully
        with DbSession(engine) as session:
            with AdvisoryLock(session, "test_lock_key", timeout=10):
                pass
        
        # Verify metrics were emitted (successful locks are recorded)
        new_samples = _get_lock_histogram_sample_count(strategy="advisory")
        assert new_samples > initial_samples

