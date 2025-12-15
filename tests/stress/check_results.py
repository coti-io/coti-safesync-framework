#!/usr/bin/env python3
"""
Check stress test results and generate report.
"""
import argparse
import json
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from conquiet.db.locking.strategy import LockStrategy


def check_results(
    strategy: LockStrategy,
    db_url: str = "mysql+pymysql://test_user:test_password@localhost:3306/test_db",
    expect_races: bool = False,
):
    """Check stress test results and generate report."""
    engine = create_engine(db_url, echo=False)
    
    with engine.begin() as conn:
        # Get all records
        result = conn.execute(text("SELECT * FROM stress_table ORDER BY id"))
        rows = result.fetchall()
        
        # Calculate statistics
        total_records = len(rows)
        duplicates = 0
        lost_updates = 0
        inconsistent_states = []
        
        # Check for issues
        for row in rows:
            id_val, counter, last_updated_by, write_count = row
            
            # Check for lost updates (counter should never decrease)
            # We'll track this by checking if counter matches expected increment pattern
            if write_count and counter < write_count:
                lost_updates += 1
                inconsistent_states.append({
                    "id": id_val,
                    "counter": counter,
                    "write_count": write_count,
                    "issue": "counter < write_count (possible lost update)",
                })
        
        # Check for duplicate IDs (shouldn't happen with primary key, but check anyway)
        id_counts = {}
        for row in rows:
            id_val = row[0]
            id_counts[id_val] = id_counts.get(id_val, 0) + 1
        
        duplicates = sum(1 for count in id_counts.values() if count > 1)
        
        # Generate report
        report = {
            "timestamp": datetime.now().isoformat(),
            "strategy": strategy.value,
            "expected_races": expect_races,
            "statistics": {
                "total_records": total_records,
                "duplicates": duplicates,
                "lost_updates": lost_updates,
                "inconsistent_states": len(inconsistent_states),
            },
            "issues": inconsistent_states,
        }
        
        # Write JSON report
        json_file = f"stress_report_{strategy.value}.json"
        with open(json_file, "w") as f:
            json.dump(report, f, indent=2)
        
        # Write text report
        txt_file = f"stress_report_{strategy.value}.txt"
        with open(txt_file, "w") as f:
            f.write(f"Stress Test Report - {strategy.value}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Timestamp: {report['timestamp']}\n")
            f.write(f"Strategy: {strategy.value}\n")
            f.write(f"Expected races: {expect_races}\n\n")
            f.write("Statistics:\n")
            f.write(f"  Total records: {total_records}\n")
            f.write(f"  Duplicates: {duplicates}\n")
            f.write(f"  Lost updates: {lost_updates}\n")
            f.write(f"  Inconsistent states: {len(inconsistent_states)}\n\n")
            
            if inconsistent_states:
                f.write("Issues Found:\n")
                for issue in inconsistent_states[:10]:  # Show first 10
                    f.write(f"  ID {issue['id']}: {issue['issue']}\n")
                if len(inconsistent_states) > 10:
                    f.write(f"  ... and {len(inconsistent_states) - 10} more\n")
        
        print(f"Report written to {json_file} and {txt_file}")
        
        # Validate results
        if expect_races:
            # With no locking, we expect issues
            if lost_updates == 0 and duplicates == 0:
                print("WARNING: Expected races but none detected!")
                return 1
            else:
                print(f"✓ Races detected as expected: {lost_updates} lost updates, {duplicates} duplicates")
                return 0
        else:
            # With locking, we should have no issues
            if lost_updates > 0 or duplicates > 0:
                print(f"ERROR: Found {lost_updates} lost updates and {duplicates} duplicates!")
                return 1
            else:
                print("✓ No races detected - locking strategy working correctly")
                return 0
    
    engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check stress test results")
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["none", "row", "advisory", "advisory_and_row", "table"],
        required=True,
        help="Lock strategy that was tested",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default="mysql+pymysql://test_user:test_password@localhost:3306/test_db",
        help="Database URL",
    )
    parser.add_argument(
        "--expect-races",
        action="store_true",
        help="Expect races (for NONE strategy)",
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
    
    sys.exit(check_results(strategy, args.db_url, args.expect_races))

