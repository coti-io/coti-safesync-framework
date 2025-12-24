from __future__ import annotations

import time
from typing import Any, Mapping, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.elements import TextClause

from .helpers import _parse_sql_operation
from .metrics import observe_db_write


class DbTx(Protocol):
    """
    Protocol for database transactions with explicit commit/rollback.
    
    Used by QueueConsumer's run() template method to provide
    explicit transaction control flow.
    """

    def commit(self) -> None:
        """Commit the transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the transaction."""
        ...

    def execute(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a non-SELECT statement and return affected row count."""
        ...

    def execute_scalar(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute a statement expected to return a single scalar value."""
        ...

    def fetch_one(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Execute a SELECT expected to return 0 or 1 row."""
        ...

    def fetch_all(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a SELECT returning multiple rows."""
        ...


class DbTransaction:
    """
    Database transaction with explicit commit/rollback methods.
    
    This class provides the same SQL execution interface as DbSession
    but with explicit commit() and rollback() methods instead of
    context manager semantics. This is required by QueueConsumer's
    run() template method.
    
    The transaction begins on construction and must be explicitly
    committed or rolled back. After commit or rollback, the connection
    is closed and the transaction cannot be used again.
    
    ⚠️ IMPORTANT: Do NOT perform retry loops inside a single DbTransaction.
    Each retry must use a new transaction via DbFactory.begin().
    
    Usage:
        factory = DbFactory(engine)
        tx = factory.begin()
        try:
            tx.execute("INSERT INTO ...", {...})
            tx.commit()
        except Exception:
            tx.rollback()
            raise
    """

    def __init__(self, engine: Engine) -> None:
        """
        Initialize and begin a new transaction.
        
        Args:
            engine: SQLAlchemy Engine instance
        """
        self.engine = engine
        self._conn: Connection | None = None
        self._tx = None
        self._closed = False
        # Track execute() operations for metrics
        self._execute_operations: list[dict[str, Any]] = []
        
        # Begin transaction immediately
        self._conn = self.engine.connect()
        self._tx = self._conn.begin()

    def _connection(self) -> Connection:
        """Get the active connection, raising if closed."""
        if self._closed or self._conn is None:
            raise RuntimeError("Transaction is closed")
        return self._conn

    def commit(self) -> None:
        """
        Commit the transaction and close the connection.
        
        Raises:
            RuntimeError: If transaction is already closed
        """
        if self._closed:
            raise RuntimeError("Transaction is already closed")
        
        status = "success"
        end_time = time.monotonic()
        
        try:
            if self._tx is not None:
                self._tx.commit()
        except Exception:
            status = "error"
            # Best-effort rollback on commit failure
            try:
                if self._tx is not None:
                    self._tx.rollback()
            except Exception:
                pass
            raise
        finally:
            self._closed = True
            if self._conn is not None:
                self._conn.close()
            
            self._conn = None
            self._tx = None
            
            # Emit metrics for all tracked execute() operations
            # Wrap in try-except to ensure metrics don't mask real errors
            try:
                for op in self._execute_operations:
                    # Only emit metrics for INSERT/UPDATE operations (skip "unknown")
                    if op["op_type"] not in ("insert", "update"):
                        continue
                    
                    latency = end_time - op["start_time"]
                    observe_db_write(
                        table=op["table"],
                        op_type=op["op_type"],
                        status=status,
                        latency_s=latency,
                    )
            except Exception:
                # Silently ignore metric errors to avoid masking real exceptions
                pass

    def rollback(self) -> None:
        """
        Rollback the transaction and close the connection.
        
        Raises:
            RuntimeError: If transaction is already closed
        """
        if self._closed:
            raise RuntimeError("Transaction is already closed")
        
        status = "error"
        end_time = time.monotonic()
        
        try:
            if self._tx is not None:
                self._tx.rollback()
        finally:
            # Cleanup and emit metrics even if rollback fails
            # Re-raise rollback exceptions after cleanup
            self._closed = True
            if self._conn is not None:
                self._conn.close()
            
            self._conn = None
            self._tx = None
            
            # Emit metrics for all tracked execute() operations
            # Wrap in try-except to ensure metrics don't mask real errors
            try:
                for op in self._execute_operations:
                    # Only emit metrics for INSERT/UPDATE operations (skip "unknown")
                    if op["op_type"] not in ("insert", "update"):
                        continue
                    
                    latency = end_time - op["start_time"]
                    observe_db_write(
                        table=op["table"],
                        op_type=op["op_type"],
                        status=status,
                        latency_s=latency,
                    )
            except Exception:
                # Silently ignore metric errors to avoid masking real exceptions
                pass

    def execute(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> int:
        """
        Execute a non-SELECT statement and return affected row count.
        
        Args:
            sql: SQL statement string or TextClause
            params: Optional parameters for the SQL statement
            
        Returns:
            Number of affected rows
            
        Raises:
            RuntimeError: If transaction is closed or rowcount is None
        """
        # Track operation start time and metadata for metrics
        start_time = time.monotonic()
        table_name, op_type = _parse_sql_operation(sql)
        
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        try:
            if result.rowcount is None:
                raise RuntimeError(
                    "execute() received None rowcount for statement. "
                    "This may indicate a DDL statement or unsupported operation type."
                )
            
            rowcount = int(result.rowcount)
        finally:
            result.close()
        
        # Store operation metadata for metrics emission in commit/rollback
        self._execute_operations.append({
            "start_time": start_time,
            "table": table_name,
            "op_type": op_type,
        })
        
        return rowcount

    def execute_scalar(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Execute a statement expected to return a single scalar value.
        Intended for control primitives (e.g., GET_LOCK).
        
        Args:
            sql: SQL statement string or TextClause
            params: Optional parameters for the SQL statement
            
        Returns:
            Scalar value or None
            
        Raises:
            RuntimeError: If transaction is closed
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        try:
            return result.scalar_one_or_none()
        finally:
            result.close()

    def fetch_one(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """
        Execute a SELECT expected to return 0 or 1 row. Raises if more than one row.
        
        Args:
            sql: SQL statement string or TextClause
            params: Optional parameters for the SQL statement
            
        Returns:
            Dictionary representing the row, or None if no row found
            
        Raises:
            RuntimeError: If transaction is closed
            MultipleResultsFound: If more than one row is returned
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        try:
            row = result.mappings().one_or_none()
            if row is None:
                return None
            return dict(row)
        finally:
            result.close()

    def fetch_all(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a SELECT returning multiple rows.
        
        Args:
            sql: SQL statement string or TextClause
            params: Optional parameters for the SQL statement
            
        Returns:
            List of dictionaries, one per row
            
        Raises:
            RuntimeError: If transaction is closed
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        try:
            return [dict(row) for row in result.mappings()]
        finally:
            result.close()


class DbFactory:
    """
    Factory for creating database transactions.
    
    Used by QueueConsumer's run() template method to create
    transactions for each message processing cycle.
    
    Usage:
        factory = DbFactory(engine)
        tx = factory.begin()
        try:
            # Use tx...
            tx.commit()
        except Exception:
            tx.rollback()
            raise
    """

    def __init__(self, engine: Engine) -> None:
        """
        Initialize the factory with a SQLAlchemy Engine.
        
        Args:
            engine: SQLAlchemy Engine instance
        """
        self.engine = engine

    def begin(self) -> DbTransaction:
        """
        Begin a new transaction.
        
        Returns:
            A new DbTransaction instance with an active transaction
        """
        return DbTransaction(self.engine)

