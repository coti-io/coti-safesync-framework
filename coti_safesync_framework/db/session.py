from __future__ import annotations

import time
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.elements import TextClause

from .helpers import _parse_sql_operation
from .metrics import observe_db_write


class DbSession:
    """
    Transactional wrapper around a SQLAlchemy Engine connection.

    A DbSession represents a *single database transaction*.
    The transaction begins on __enter__ and commits or rolls back on __exit__.

    ⚠️ Do NOT perform retry loops (e.g. OCC retries) inside a single DbSession.
    Each retry must use a new DbSession / transaction.

    See docs/occ.md for details on correct OCC transaction boundaries.

    Use as:
        with DbSession(engine) as session:
            session.execute(...)
            row = session.fetch_one(...)
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._conn: Connection | None = None
        self._tx = None
        # Track execute() operations for metrics
        self._execute_operations: list[dict[str, Any]] = []

    def __enter__(self) -> "DbSession":
        if self._conn is not None:
            raise RuntimeError("DbSession is already active; nested sessions are not allowed")
        self._conn = self.engine.connect()
        self._tx = self._conn.begin()
        # Reset operation tracking for new session
        self._execute_operations = []
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        end_time = time.monotonic()
        commit_failed = False
        
        try:
            if self._tx is not None:
                if exc_type:
                    self._tx.rollback()
                else:
                    try:
                        self._tx.commit()
                    except Exception:
                        commit_failed = True
                        raise
        finally:
            # Determine final status
            status = "error" if (exc_type or commit_failed) else "success"
            
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

        # propagate exceptions (if any)
        return False

    def _connection(self) -> Connection:
        if self._conn is None:
            raise RuntimeError("DbSession is not active; use within a context manager")
        return self._conn

    def execute(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> int:
        """
        Execute a non-SELECT statement and return affected row count.
        """
        # Track operation start time and metadata for metrics
        start_time = time.monotonic()
        table_name, op_type = _parse_sql_operation(sql)
        
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params)
        if result.rowcount is None:
            raise RuntimeError(
                "execute() received None rowcount for statement. "
                "This may indicate a DDL statement or unsupported operation type."
            )
        
        # Store operation metadata for metrics emission in __exit__()
        self._execute_operations.append({
            "start_time": start_time,
            "table": table_name,
            "op_type": op_type,
        })
        
        return int(result.rowcount)

    def execute_scalar(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> Any:
        """
        Execute a statement expected to return a single scalar value.
        Intended for control primitives (e.g., GET_LOCK).
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params)
        return result.scalar_one_or_none()

    def fetch_one(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """
        Execute a SELECT expected to return 0 or 1 row. Raises if more than one row.
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params)
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return dict(row)

    def fetch_all(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Execute a SELECT expected to return multiple rows.
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params)
        return [dict(row) for row in result.mappings()]

