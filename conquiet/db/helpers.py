from __future__ import annotations

from typing import Any, Mapping
from sqlalchemy import text
from .session import DbSession


def occ_update(
    session: DbSession,
    table: str,
    id_column: str,
    id_value: Any,
    version_column: str,
    version_value: Any,
    updates: Mapping[str, Any],
) -> int:
    """
    Execute an optimistic concurrency control (OCC) update using version checking.
    
    This is a convenience helper for the common OCC pattern where updates are
    conditional on a version column matching the expected value.
    
    The version column is automatically incremented as part of the update.
    
    Args:
        session: Active DbSession instance
        table: Table name
        id_column: Primary key column name
        id_value: Primary key value
        version_column: Version column name (typically "version")
        version_value: Expected version value
        updates: Dictionary of column -> value to update (version_column is handled automatically)
    
    Returns:
        Affected row count:
        - 1: Update succeeded (version matched)
        - 0: Update failed (version mismatch, missing row, or no-op)
    
    Example:
        with db.session() as session:
            row = session.fetch_one("SELECT * FROM orders WHERE id = :id", {"id": 42})
            if not row:
                return
            
            rows = occ_update(
                session,
                table="orders",
                id_column="id",
                id_value=42,
                version_column="version",
                version_value=row["version"],
                updates={"amount": 100.50, "status": "processed"}
            )
            
            if rows == 0:
                # Version mismatch or row missing - handle accordingly
                # (retry, re-read, abort, etc.)
                return
    """
    # Build SET clause from updates (excluding version column)
    set_clauses = []
    params: dict[str, Any] = {
        "id_value": id_value,
        "version_value": version_value,
    }
    
    for col, val in sorted(updates.items()):
        if col != version_column:
            set_clauses.append(f"{col} = :{col}")
            params[col] = val
    
    # Always increment version column
    set_clauses.append(f"{version_column} = {version_column} + 1")
    
    set_sql = ", ".join(set_clauses)
    sql = (
        f"UPDATE {table} SET {set_sql} "
        f"WHERE {id_column} = :id_value AND {version_column} = :version_value"
    )
    
    stmt = text(sql)
    return session.execute(stmt, params)


