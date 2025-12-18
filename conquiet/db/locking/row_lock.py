from __future__ import annotations

from sqlalchemy import text
from ..session import DbSession


class RowLock:
    """
    Pessimistic read-modify-write protection using SELECT ... FOR UPDATE.
    
    Row locks are held for the entire duration of the surrounding transaction
    and are released only when the transaction commits or rolls back.
    
    This is NOT a context manager - locks are transaction-scoped, not method-scoped.
    
    **Important: Indexing Requirements**
    
    The WHERE clause predicates should match indexed columns. Non-indexed predicates
    may cause:
    - Full table scans (performance degradation)
    - Gap locks (especially under REPEATABLE READ isolation)
    - Unexpected contention and deadlocks
    
    Ensure your WHERE clause columns are indexed for optimal performance and
    predictable locking behavior.
    
    Usage:
        with db.session() as session:
            row = RowLock(session, "orders", {"id": 42}).acquire()
            if row is None:
                return  # row doesn't exist
            
            # Use row data for read-modify-write
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
            table: Table name
            where: Dictionary of column -> value for WHERE clause
                   (e.g., {"id": 42} or {"order_id": 123, "item_id": 456})
        """
        self.session = session
        self.table = table
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
        # Enforce active DbSession
        if self.session._conn is None:
            raise RuntimeError(
                "RowLock.acquire() requires an active DbSession. "
                "Use RowLock within a 'with db.session() as session:' block."
            )
        
        # Build WHERE clause from where dict (sorted keys for deterministic SQL)
        where_clauses = []
        params = {}
        for i, (col, val) in enumerate(sorted(self.where.items())):
            param_name = f"where_{i}"
            where_clauses.append(f"{col} = :{param_name}")
            params[param_name] = val

        where_sql = " AND ".join(where_clauses)
        sql = f"SELECT * FROM {self.table} WHERE {where_sql} FOR UPDATE"
        
        stmt = text(sql)
        return self.session.fetch_one(stmt, params)

