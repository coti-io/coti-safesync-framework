from __future__ import annotations

import pytest

from conquiet.config import DbConfig
from conquiet.db.models import DbOperation, DbOperationType
from conquiet.db.writer import DbWriter


def test_dbwriter_emits_deprecation_warning_on_construction(engine) -> None:
    with pytest.warns(DeprecationWarning):
        DbWriter(engine, DbConfig(table_name="unused_for_this_test"))


def test_insert_suppresses_duplicate_key_errors(engine, fresh_table: str) -> None:
    table = fresh_table

    with pytest.warns(DeprecationWarning):
        writer = DbWriter(engine, DbConfig(table_name=table, id_column="id"))

    op = DbOperation(
        table=f"`{table}`",
        op_type=DbOperationType.INSERT,
        id_value=1,
        payload={"id": 1, "value": 10},
    )

    # First insert should succeed, second should be suppressed (no raise).
    writer.execute(op)
    writer.execute(op)

    # Row exists once.
    from conquiet.db.session import DbSession

    with DbSession(engine) as session:
        count = session.execute_scalar(f"SELECT COUNT(*) FROM `{table}` WHERE id = 1")
        assert count == 1


def test_update_executes_successfully_when_row_exists(engine, fresh_table: str) -> None:
    table = fresh_table

    with pytest.warns(DeprecationWarning):
        writer = DbWriter(engine, DbConfig(table_name=table, id_column="id"))

    # Seed row
    insert_op = DbOperation(
        table=f"`{table}`",
        op_type=DbOperationType.INSERT,
        id_value=1,
        payload={"id": 1, "value": 10},
    )
    writer.execute(insert_op)

    update_op = DbOperation(
        table=f"`{table}`",
        op_type=DbOperationType.UPDATE,
        id_value=1,
        payload={"id": 1, "value": 11},
    )
    writer.execute(update_op)

    from conquiet.db.session import DbSession

    with DbSession(engine) as session:
        row = session.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = 1")
        assert row == {"id": 1, "value": 11}


