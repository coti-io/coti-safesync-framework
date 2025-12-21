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

⚠️ IMPORTANT USAGE CONTRACT ⚠️
This helper is a *low-level primitive*. Correctness depends on how it is used.

You MUST:
- Run each OCC attempt in its own DbSession / transaction
- Retry when rowcount == 0 (version mismatch)
- Re-read the row before retrying
- Keep transactions short (no retry loops inside a single session)

Incorrect usage can cause lost updates, deadlocks, or lock wait timeouts.

See docs/occ.md for the full, authoritative usage guide and examples.

Args:
    session: Active DbSession instance
    table: Table name
    id_column: Primary key column name
    id_value: Primary key value
    version_column: Version column name (typically "version")
    version_value: Expected version value
    updates: Dictionary of column -> value to update
             (version_column is handled automatically)

Returns:
    Affected row count:
    - 1: Update succeeded (version matched)
    - 0: Update failed (version mismatch, missing row, or no-op)

Example (CORRECT USAGE):

    MAX_RETRIES = 10

    for _ in range(MAX_RETRIES):
        with DbSession(engine) as session:
            row = session.fetch_one(
                "SELECT amount, version FROM orders WHERE id = :id",
                {"id": 42},
            )
            if not row:
                break

            rc = occ_update(
                session=session,
                table="orders",
                id_column="id",
                id_value=42,
                version_column="version",
                version_value=row["version"],
                updates={"amount": row["amount"] + 10},
            )

            if rc == 1:
                break  # success; commit happens on session exit

        # rc == 0 → version mismatch; retry with a NEW transaction
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


