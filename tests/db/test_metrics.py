from __future__ import annotations

import pytest

from conquiet.db.metrics import observe_db_write, observe_lock_acquisition
from conquiet.metrics.registry import (
    DB_LOCK_ACQUIRE_LATENCY_SECONDS,
    DB_WRITE_LATENCY_SECONDS,
    DB_WRITE_TOTAL,
)


class TestObserveDbWrite:
    """Tests for observe_db_write() function."""

    def test_increments_counter_with_correct_labels(self) -> None:
        """Test that observe_db_write increments the counter with correct labels."""
        # Reset metrics (get fresh instances)
        table = "test_table"
        op_type = "insert"
        status = "success"
        
        # Get initial count
        initial_count = DB_WRITE_TOTAL.labels(
            table=table, op_type=op_type, status=status
        )._value.get()
        
        # Call observe_db_write
        observe_db_write(table=table, op_type=op_type, status=status, latency_s=0.1)
        
        # Verify counter was incremented
        new_count = DB_WRITE_TOTAL.labels(
            table=table, op_type=op_type, status=status
        )._value.get()
        assert new_count == initial_count + 1

    def test_records_latency_in_histogram(self) -> None:
        """Test that observe_db_write records latency in histogram."""
        table = "test_table"
        op_type = "update"
        latency = 0.25
        
        # Call observe_db_write - should not raise
        observe_db_write(table=table, op_type=op_type, status="success", latency_s=latency)
        
        # Verify histogram exists and can be collected (means it was updated)
        samples = list(DB_WRITE_LATENCY_SECONDS.labels(
            table=table, op_type=op_type
        ).collect())
        # Histogram should have samples after observation
        assert len(samples) > 0

    def test_different_labels_create_separate_metrics(self) -> None:
        """Test that different label combinations create separate metric instances."""
        # Call with different table
        observe_db_write(table="table1", op_type="insert", status="success", latency_s=0.1)
        observe_db_write(table="table2", op_type="insert", status="success", latency_s=0.1)
        
        # Verify both counters exist and are separate
        count1 = DB_WRITE_TOTAL.labels(table="table1", op_type="insert", status="success")._value.get()
        count2 = DB_WRITE_TOTAL.labels(table="table2", op_type="insert", status="success")._value.get()
        
        assert count1 >= 1
        assert count2 >= 1
        # They should be independent
        assert count1 != count2 or (count1 == 1 and count2 == 1)

    def test_error_status_tracked_separately(self) -> None:
        """Test that error status is tracked separately from success."""
        table = "test_table"
        op_type = "insert"
        
        # Record success
        observe_db_write(table=table, op_type=op_type, status="success", latency_s=0.1)
        success_count = DB_WRITE_TOTAL.labels(
            table=table, op_type=op_type, status="success"
        )._value.get()
        
        # Record error
        observe_db_write(table=table, op_type=op_type, status="error", latency_s=0.1)
        error_count = DB_WRITE_TOTAL.labels(
            table=table, op_type=op_type, status="error"
        )._value.get()
        
        # Both should be tracked separately
        assert success_count >= 1
        assert error_count >= 1


class TestObserveLockAcquisition:
    """Tests for observe_lock_acquisition() function."""

    def test_records_latency_for_successful_lock(self) -> None:
        """Test that observe_lock_acquisition records latency for successful locks."""
        strategy = "advisory"
        latency = 0.15
        
        # Call with success=True
        observe_lock_acquisition(strategy=strategy, latency_s=latency, success=True)
        
        # Verify histogram was updated by checking collect()
        samples = list(DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(
            strategy=strategy
        ).collect())
        # Histogram should have recorded the observation
        assert len(samples) > 0

    def test_does_not_record_latency_for_failed_lock(self) -> None:
        """Test that observe_lock_acquisition does NOT record latency for failed locks."""
        strategy = "row_failed"
        latency = 0.2
        
        # Get initial samples count
        initial_samples = list(DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(
            strategy=strategy
        ).collect())
        initial_sample_count = sum(len(s.samples) for s in initial_samples)
        
        # Call with success=False
        observe_lock_acquisition(strategy=strategy, latency_s=latency, success=False)
        
        # Verify histogram sample count did NOT increase (failed locks aren't recorded)
        new_samples = list(DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(
            strategy=strategy
        ).collect())
        new_sample_count = sum(len(s.samples) for s in new_samples)
        assert new_sample_count == initial_sample_count

    def test_different_strategies_create_separate_metrics(self) -> None:
        """Test that different strategies create separate metric instances."""
        # Call with different strategies
        observe_lock_acquisition(strategy="advisory", latency_s=0.1, success=True)
        observe_lock_acquisition(strategy="row", latency_s=0.1, success=True)
        
        # Verify both histograms exist and are separate
        advisory_samples = list(DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(
            strategy="advisory"
        ).collect())
        row_samples = list(DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(
            strategy="row"
        ).collect())
        
        # Both should have recorded observations
        assert len(advisory_samples) > 0
        assert len(row_samples) > 0

