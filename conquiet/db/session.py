from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql import TextClause


class DbSession:
    """
    Transactional wrapper around a SQLAlchemy Engine connection.

    Use as:
        with DbSession(engine) as session:
            session.execute(...)
            row = session.fetch_one(...)
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._conn: Connection | None = None
        self._tx = None

    def __enter__(self) -> "DbSession":
        if self._conn is not None:
            raise RuntimeError("DbSession is already active; nested sessions are not allowed")
        self._conn = self.engine.connect()
        self._tx = self._conn.begin()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self._tx is not None:
                if exc_type:
                    self._tx.rollback()
                else:
                    self._tx.commit()
        finally:
            if self._conn is not None:
                self._conn.close()

            self._conn = None
            self._tx = None

        # propagate exceptions (if any)
        return False

    def _connection(self) -> Connection:
        if self._conn is None:
            raise RuntimeError("DbSession is not active; use within a context manager")
        return self._conn

    def execute(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> int:
        """
        Execute a non-SELECT statement and return affected row count.
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        if result.rowcount is None:
            raise RuntimeError(
                "execute() received None rowcount for statement. "
                "This may indicate a DDL statement or unsupported operation type."
            )
        return int(result.rowcount)

    def execute_scalar(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Execute a statement expected to return a single scalar value.
        Intended for control primitives (e.g., GET_LOCK).
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        return result.scalar_one_or_none()

    def fetch_one(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """
        Execute a SELECT expected to return 0 or 1 row. Raises if more than one row.
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return dict(row)

    def fetch_all(
        self,
        sql: str | TextClause,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a SELECT expected to return multiple rows.
        """
        conn = self._connection()
        stmt = text(sql) if isinstance(sql, str) else sql
        result = conn.execute(stmt, params or {})
        return [dict(row) for row in result.mappings()]

