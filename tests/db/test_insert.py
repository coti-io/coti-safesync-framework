from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from conquiet.config import DbConfig
from conquiet.db.models import DbOperation, DbOperationType
from conquiet.db.session import DbSession
from conquiet.db.writer import DbWriter


def test_dbsession_duplicate_insert_raises_integrity_error(engine, fresh_table: str) -> None:
    """
    DbSession is a thin transactional primitive; it does not suppress duplicate-key errors.
    Callers that want idempotent INSERTs must handle IntegrityError themselves.
    """
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 10)")
        with pytest.raises(IntegrityError):
            session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 999)")


def test_dbwriter_duplicate_pk_insert_is_suppressed_and_row_is_unchanged(
    engine, fresh_table: str
) -> None:
    """
    DbWriter (deprecated) provides idempotent INSERT semantics by suppressing
    duplicate-key errors; existing rows are not modified by the suppressed INSERT.
    """
    table = fresh_table

    with pytest.warns(DeprecationWarning):
        writer = DbWriter(engine, DbConfig(table_name=table, id_column="id"))

    writer.execute(
        DbOperation(
            table=f"`{table}`",
            op_type=DbOperationType.INSERT,
            id_value=1,
            payload={"id": 1, "value": 10, "name": "a"},
        )
    )

    # Same PK, different payload: must not raise, and must not overwrite existing data.
    writer.execute(
        DbOperation(
            table=f"`{table}`",
            op_type=DbOperationType.INSERT,
            id_value=1,
            payload={"id": 1, "value": 999, "name": "b"},
        )
    )

    with DbSession(engine) as session:
        row = session.fetch_one(f"SELECT id, value, name FROM `{table}` WHERE id = 1")
        assert row == {"id": 1, "value": 10, "name": "a"}


def test_dbwriter_duplicate_unique_key_insert_is_suppressed(engine, table_factory) -> None:
    """
    Duplicate-key suppression should apply to UNIQUE constraints too (MySQL 1062).
    """
    table = table_factory(
        """
        id BIGINT NOT NULL,
        name VARCHAR(255) NOT NULL,
        value INT NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE KEY uq_name (name)
        """
    )

    with pytest.warns(DeprecationWarning):
        writer = DbWriter(engine, DbConfig(table_name=table, id_column="id"))

    writer.execute(
        DbOperation(
            table=f"`{table}`",
            op_type=DbOperationType.INSERT,
            id_value=1,
            payload={"id": 1, "name": "dup", "value": 10},
        )
    )

    # Different PK but same UNIQUE(name) -> duplicate key; must be suppressed.
    writer.execute(
        DbOperation(
            table=f"`{table}`",
            op_type=DbOperationType.INSERT,
            id_value=2,
            payload={"id": 2, "name": "dup", "value": 999},
        )
    )

    with DbSession(engine) as session:
        assert session.execute_scalar(f"SELECT COUNT(*) FROM `{table}` WHERE name = 'dup'") == 1
        assert session.fetch_one(f"SELECT id, value FROM `{table}` WHERE name = 'dup'") == {
            "id": 1,
            "value": 10,
        }


