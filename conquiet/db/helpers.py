from __future__ import annotations

import re
from typing import Any, Mapping
from sqlalchemy import text
from .session import DbSession


def _validate_identifier(name: str, identifier_type: str = "identifier") -> str:
    """
    Validate that an identifier (table/column name) is safe for SQL interpolation.
    
    MySQL identifiers can contain letters, digits, underscores, and dollar signs,
    but we restrict to alphanumeric + underscore for security and simplicity.
    
    ⚠️ SECURITY CONTRACT ⚠️
    This function validates identifier format but does NOT prevent SQL injection
    if identifiers come from untrusted user input. Identifiers MUST be trusted
    (hardcoded or validated at application boundaries, not directly from user input).
    
    Args:
        name: The identifier to validate
        identifier_type: Description of the identifier (for error messages)
    
    Returns:
        The validated identifier (unchanged if valid)
    
    Raises:
        TypeError: If identifier is not a string
        ValueError: If identifier contains unsafe characters or is invalid
    
    Example:
        >>> _validate_identifier("orders", "table")
        'orders'
        >>> _validate_identifier("user_id", "column")
        'user_id'
        >>> _validate_identifier("'; DROP TABLE--", "table")
        ValueError: Invalid table '; DROP TABLE--': ...
    """
    if not isinstance(name, str):
        raise TypeError(f"{identifier_type} must be a string, got {type(name).__name__}")
    
    if not name:
        raise ValueError(f"{identifier_type} cannot be empty")
    
    # MySQL identifier rules:
    # - Can be up to 64 characters
    # - Can contain letters, digits, underscore, dollar sign
    # - We restrict to alphanumeric + underscore for security
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Invalid {identifier_type} {name!r}: "
            "must start with letter/underscore and contain only alphanumeric characters and underscores"
        )
    
    if len(name) > 64:
        raise ValueError(f"{identifier_type} {name!r} exceeds MySQL's 64-character limit")
    
    return name


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

⚠️ SECURITY CONTRACT ⚠️
Table and column names (table, id_column, version_column, and keys in updates)
MUST be trusted identifiers - hardcoded strings or validated at application
boundaries. They MUST NOT come directly from user input. This function validates
identifier format to prevent SQL injection, but identifiers are still interpolated
into SQL strings. If you need user-controlled identifiers, use a whitelist approach.

Args:
    session: Active DbSession instance
    table: Table name (must be a trusted identifier, not user-controlled)
    id_column: Primary key column name (must be a trusted identifier)
    id_value: Primary key value
    version_column: Version column name (typically "version", must be trusted)
    version_value: Expected version value
    updates: Dictionary of column -> value to update
             (column names must be trusted identifiers; version_column is handled automatically)

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

    # Validate all identifiers to prevent SQL injection
    table = _validate_identifier(table, "table")
    id_column = _validate_identifier(id_column, "id_column")
    version_column = _validate_identifier(version_column, "version_column")

    # Build SET clause from updates (excluding version column)
    set_clauses = []
    params: dict[str, Any] = {
        "id_value": id_value,
        "version_value": version_value,
    }
    
    for col, val in sorted(updates.items()):
        col = _validate_identifier(col, "column name")  # Validate each update column
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


