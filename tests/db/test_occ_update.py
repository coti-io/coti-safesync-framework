from __future__ import annotations

import pytest

from conquiet.db.helpers import occ_update
from conquiet.db.session import DbSession


def test_occ_update_returns_1_and_increments_version_on_match(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(
            f"INSERT INTO `{table}` (id, value, version, name) VALUES (:id, :value, :version, :name)",
            {"id": 1, "value": 10, "version": 5, "name": "a"},
        )

        rc = occ_update(
            session=session,
            table=f"`{table}`",
            id_column="id",
            id_value=1,
            version_column="version",
            version_value=5,
            updates={"value": 11, "name": "b"},
        )
        assert rc == 1

        row = session.fetch_one(f"SELECT id, value, version, name FROM `{table}` WHERE id = 1", {})
        assert row == {"id": 1, "value": 11, "version": 6, "name": "b"}


def test_occ_update_returns_0_and_does_not_modify_row_on_version_mismatch(
    engine, fresh_table: str
) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(
            f"INSERT INTO `{table}` (id, value, version, name) VALUES (:id, :value, :version, :name)",
            {"id": 1, "value": 10, "version": 5, "name": "a"},
        )

        rc = occ_update(
            session=session,
            table=f"`{table}`",
            id_column="id",
            id_value=1,
            version_column="version",
            version_value=999,
            updates={"value": 999, "name": "zzz"},
        )
        assert rc == 0

        row = session.fetch_one(f"SELECT id, value, version, name FROM `{table}` WHERE id = 1", {})
        assert row == {"id": 1, "value": 10, "version": 5, "name": "a"}


def test_occ_update_requires_active_dbsession(engine, fresh_table: str) -> None:
    session = DbSession(engine)  # not entered
    with pytest.raises(RuntimeError):
        occ_update(
            session=session,
            table=f"`{fresh_table}`",
            id_column="id",
            id_value=1,
            version_column="version",
            version_value=0,
            updates={"value": 1},
        )


