#!/usr/bin/env python3
"""
Run stress test with two processes competing for the same records.
"""
import argparse
import sys
import time
from multiprocessing import Process
from conquiet.db.locking.strategy import LockStrategy
from stress_test_process import launch_process


def run_stress_test(
    strategy: LockStrategy,
    duration: int = 60,
    frequency: float = 0.05,
    id_range: int = 200,
    db_url: str = "mysql+pymysql://test_user:test_password@localhost:3306/test_db",
):
    """Run stress test with two processes."""
    print(f"Starting stress test with strategy: {strategy.value}")
    print(f"Duration: {duration}s, Frequency: {1/frequency:.1f} writes/sec/process, ID range: 1-{id_range}")
    
    p1 = Process(target=launch_process, args=(1, strategy, duration, frequency, id_range, db_url))
    p2 = Process(target=launch_process, args=(2, strategy, duration, frequency, id_range, db_url))
    
    start_time = time.time()
    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
    
    elapsed = time.time() - start_time
    print(f"Stress test completed in {elapsed:.2f}s")
    
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DB stress test")
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["none", "row", "advisory", "advisory_and_row", "table"],
        default="none",
        help="Lock strategy to test",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds",
    )
    parser.add_argument(
        "--frequency",
        type=float,
        default=0.05,
        help="Write frequency in seconds (default 0.05 = 20 writes/sec)",
    )
    parser.add_argument(
        "--id-range",
        type=int,
        default=200,
        help="ID range for random record selection (default 200)",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default="mysql+pymysql://test_user:test_password@localhost:3306/test_db",
        help="Database URL",
    )
    
    args = parser.parse_args()
    
    strategy_map = {
        "none": LockStrategy.NONE,
        "row": LockStrategy.ROW,
        "advisory": LockStrategy.ADVISORY,
        "advisory_and_row": LockStrategy.ADVISORY_AND_ROW,
        "table": LockStrategy.TABLE,
    }
    
    strategy = strategy_map[args.strategy]
    
    sys.exit(run_stress_test(strategy, args.duration, args.frequency, args.id_range, args.db_url))

