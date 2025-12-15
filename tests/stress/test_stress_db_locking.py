"""
Pytest wrapper for stress tests - validates DB locking strategies under load.
"""
import pytest
import subprocess
import sys
import os
from pathlib import Path
from conquiet.db.locking.strategy import LockStrategy


# Stress test directory
STRESS_DIR = Path(__file__).parent


@pytest.fixture
def db_url():
    """Database URL for stress tests."""
    return os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://test_user:test_password@localhost:3306/test_db"
    )


@pytest.fixture
def cleanup_table(db_url):
    """Clean up stress table before and after test."""
    from sqlalchemy import create_engine, text
    
    engine = create_engine(db_url, echo=False)
    
    # Cleanup before
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS stress_table"))
    
    yield
    
    # Cleanup after
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS stress_table"))
    
    engine.dispose()


class TestStressDbLocking:
    """Stress tests for DB locking strategies."""
    
    @pytest.mark.stress
    @pytest.mark.parametrize("strategy,expect_races", [
        (LockStrategy.NONE, True),
        (LockStrategy.ROW, False),
        (LockStrategy.ADVISORY, False),
        (LockStrategy.ADVISORY_AND_ROW, False),
        (LockStrategy.TABLE, False),
    ])
    def test_stress_locking_strategy(
        self,
        strategy: LockStrategy,
        expect_races: bool,
        db_url: str,
        cleanup_table,
    ):
        """
        Run stress test for a specific locking strategy.
        
        Args:
            strategy: Lock strategy to test
            expect_races: Whether races are expected (True for NONE strategy)
            db_url: Database connection URL
            cleanup_table: Fixture to clean up test table
        """
        strategy_name = strategy.value
        
        # Run stress test
        run_cmd = [
            sys.executable,
            str(STRESS_DIR / "run_stress_test.py"),
            "--strategy", strategy_name,
            "--duration", "30",  # Shorter for pytest runs
            "--frequency", "0.1",  # 10 writes/sec (less aggressive for CI)
            "--id-range", "100",
            "--db-url", db_url,
        ]
        
        result = subprocess.run(
            run_cmd,
            cwd=STRESS_DIR,
            capture_output=True,
            text=True,
            timeout=60,  # Timeout after 60 seconds
        )
        
        assert result.returncode == 0, f"Stress test failed:\n{result.stderr}\n{result.stdout}"
        
        # Check results
        check_cmd = [
            sys.executable,
            str(STRESS_DIR / "check_results.py"),
            "--strategy", strategy_name,
            "--db-url", db_url,
        ]
        
        if expect_races:
            check_cmd.append("--expect-races")
        
        result = subprocess.run(
            check_cmd,
            cwd=STRESS_DIR,
            capture_output=True,
            text=True,
        )
        
        # For NONE strategy, races are expected, so exit code 0 is success
        # For other strategies, races should not occur, so exit code 0 means success
        assert result.returncode == 0, (
            f"Result validation failed for {strategy_name}:\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}"
        )
        
        # Verify report files were created
        json_report = STRESS_DIR / f"stress_report_{strategy_name}.json"
        txt_report = STRESS_DIR / f"stress_report_{strategy_name}.txt"
        
        assert json_report.exists(), f"JSON report not created: {json_report}"
        assert txt_report.exists(), f"Text report not created: {txt_report}"

