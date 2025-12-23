from __future__ import annotations

import time

from sqlalchemy import text
from ..session import DbSession
from ..helpers import _validate_identifier
from ..metrics import observe_lock_acquisition


class RowLock:
    """
    Pessimistic read-modify-write protection using SELECT ... FOR UPDATE.

    Row locks are held for the entire duration of the surrounding transaction
    and are released only when the transaction commits or rolls back.

    This is NOT a context manager - locks are transaction-scoped, not method-scoped.

    ⚠️ IMPORTANT USAGE RULES ⚠️
    - Do NOT use RowLock inside retry loops.
    - Do NOT hold transactions open longer than necessary.
    - Each RowLock acquisition should be followed by exactly one
      read-modify-write sequence and then committed.

    **Important: Indexing Requirements**

    The WHERE clause predicates should match indexed columns. Non-indexed predicates
    may cause:
    - Full table scans (performance degradation)
    - Gap locks (especially under REPEATABLE READ isolation)
    - Unexpected contention and deadlocks

    Ensure your WHERE clause columns are indexed for optimal performance and
    predictable locking behavior.

    Use RowLock when you need strict serialization.

    ⚠️ If you want optimistic concurrency with retries, use occ_update instead.
    See docs/occ.md for the correct OCC pattern.

    ⚠️ SECURITY CONTRACT ⚠️
    Table name and column names in the where dictionary MUST be trusted identifiers
    - hardcoded strings or validated at application boundaries. They MUST NOT come
    directly from user input. This class validates identifier format to prevent SQL
    injection, but identifiers are still interpolated into SQL strings.

    Usage:
        with DbSession(engine) as session:
            row = RowLock(session, "orders", {"id": 42}).acquire()
            if row is None:
                return  # row doesn't exist

            # Exactly one read-modify-write per transaction
            session.execute(
                "UPDATE orders SET status = :status WHERE id = :id",
                {"id": row["id"], "status": "processed"}
            )
    """

    def __init__(self, session: DbSession, table: str, where: dict) -> None:
        """
        Initialize a row lock.
        
        Args:
            session: Active DbSession instance
            table: Table name (must be a trusted identifier, not user-controlled)
            where: Dictionary of column -> value for WHERE clause
                   (e.g., {"id": 42} or {"order_id": 123, "item_id": 456})
                   Column names must be trusted identifiers, not user-controlled
        """
        # Validate table name to prevent SQL injection
        self.table = _validate_identifier(table, "table")
        self.session = session
        self.where = where

    def acquire(self) -> dict | None:
        """
        Acquire a row-level lock and return the row data.
        
        Executes SELECT * FROM table WHERE ... FOR UPDATE.
        If the row doesn't exist, returns None.
        If the row exists, returns it as a dict and holds the lock until
        the transaction commits or rolls back.
        
        Returns:
            dict with row data if row exists, None otherwise
            
        Raises:
            RuntimeError: If DbSession is not active (not within a context manager)
        """
        # Track lock acquisition start time for metrics
        start_time = time.monotonic()
        
        # Enforce active DbSession
        if self.session._conn is None:
            raise RuntimeError(
                "RowLock.acquire() requires an active DbSession. "
                "Use RowLock within a 'with db.session() as session:' block."
            )
        
        # Build WHERE clause from where dict (sorted keys for deterministic SQL)
        # Validate all column names to prevent SQL injection
        where_clauses = []
        params = {}
        for i, (col, val) in enumerate(sorted(self.where.items())):
            col = _validate_identifier(col, "column name")  # Validate each column name
            param_name = f"where_{i}"
            where_clauses.append(f"{col} = :{param_name}")
            params[param_name] = val

        where_sql = " AND ".join(where_clauses)
        sql = f"SELECT * FROM {self.table} WHERE {where_sql} FOR UPDATE"
        
        stmt = text(sql)
        result = self.session.fetch_one(stmt, params)
        
        # Emit metrics for lock acquisition (always success since SELECT ... FOR UPDATE succeeds)
        try:
            latency = time.monotonic() - start_time
            observe_lock_acquisition(
                strategy="row",
                latency_s=latency,
                success=True,
            )
        except Exception:
            pass
        
        return result

