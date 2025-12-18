from __future__ import annotations

import pytest
from sqlalchemy import text

from conquiet.db.session import DbSession


def _insert_two_rows(session: DbSession, table: str) -> None:
    session.execute(
        f"INSERT INTO `{table}` (id, value, version, k1, k2, name) "
        "VALUES (:id, :value, :version, :k1, :k2, :name)",
        {"id": 1, "value": 10, "version": 0, "k1": 7, "k2": 9, "name": "a"},
    )
    session.execute(
        f"INSERT INTO `{table}` (id, value, version, k1, k2, name) "
        "VALUES (:id, :value, :version, :k1, :k2, :name)",
        {"id": 2, "value": 20, "version": 0, "k1": 7, "k2": 10, "name": "b"},
    )


def test_transaction_commits_on_success(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        rc = session.execute(
            f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
            {"id": 1, "value": 123},
        )
        assert rc == 1

    with DbSession(engine) as session2:
        row = session2.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = :id", {"id": 1})
        assert row == {"id": 1, "value": 123}


def test_transaction_rolls_back_on_exception(engine, fresh_table: str) -> None:
    table = fresh_table

    with pytest.raises(RuntimeError):
        with DbSession(engine) as session:
            session.execute(
                f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
                {"id": 1, "value": 123},
            )
            raise RuntimeError("boom")

    with DbSession(engine) as session2:
        row = session2.fetch_one(f"SELECT id FROM `{table}` WHERE id = :id", {"id": 1})
        assert row is None


def test_connection_is_closed_after_exit(engine, fresh_table: str) -> None:
    table = fresh_table

    conn = None
    with DbSession(engine) as session:
        conn = session._conn  # behavior we care about: connection closes after exit
        assert conn is not None
        session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 1)")

    assert conn is not None
    assert conn.closed is True


def test_nested_usage_raises_runtime_error(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 1)")
        with pytest.raises(RuntimeError):
            with session:
                pass


def test_execute_returns_correct_rowcount_for_update(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 10)")
        rc = session.execute(
            f"UPDATE `{table}` SET value = :value WHERE id = :id",
            {"id": 1, "value": 11},
        )
        assert rc == 1


def test_execute_binds_parameters(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(
            f"INSERT INTO `{table}` (id, value, name) VALUES (:id, :value, :name)",
            {"id": 1, "value": 10, "name": "hello"},
        )

    with DbSession(engine) as session2:
        row = session2.fetch_one(
            f"SELECT id, value, name FROM `{table}` WHERE name = :name",
            {"name": "hello"},
        )
        assert row == {"id": 1, "value": 10, "name": "hello"}


def test_execute_insert_rowcount_and_visibility(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        rc = session.execute(
            f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
            {"id": 1, "value": 10},
        )
        assert rc == 1

    with DbSession(engine) as session2:
        row = session2.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = 1")
        assert row == {"id": 1, "value": 10}


def test_execute_raises_if_rowcount_is_none_probe_or_skip(engine) -> None:
    """
    We prefer a real-MySQL reproduction of `result.rowcount is None`.
    If we can't reproduce it with the current driver, we skip with a clear reason.
    """
    probe_sqls = [
        # DDL and utility statements that might produce driver-specific rowcount semantics.
        "CREATE TEMPORARY TABLE IF NOT EXISTS tmp_conquiet_probe (id INT) ENGINE=InnoDB",
        "DROP TEMPORARY TABLE IF EXISTS tmp_conquiet_probe",
        "DO 1",
        "SET @conquiet_probe := 1",
    ]

    found_none_rowcount = False
    none_rowcount_sql = None

    with engine.connect() as conn:
        for sql in probe_sqls:
            try:
                res = conn.execute(text(sql))
            except Exception:
                continue
            if res.rowcount is None:
                found_none_rowcount = True
                none_rowcount_sql = sql
                break

    if not found_none_rowcount:
        pytest.skip(
            "Could not reproduce SQLAlchemy Result.rowcount == None with the current "
            "MySQL driver/SQLAlchemy combo; skipping the DbSession.execute() None-rowcount assertion."
        )

    assert none_rowcount_sql is not None
    with DbSession(engine) as session:
        with pytest.raises(RuntimeError):
            session.execute(none_rowcount_sql)


def test_fetch_one_returns_dict_when_exactly_one_row(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 10)")
        row = session.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = 1")
        assert row == {"id": 1, "value": 10}


def test_fetch_one_returns_none_when_no_rows(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        row = session.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = 999")
        assert row is None


def test_fetch_one_raises_if_more_than_one_row(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        _insert_two_rows(session, table)
        with pytest.raises(Exception):
            session.fetch_one(f"SELECT id FROM `{table}`")


def test_fetch_all_returns_empty_list_when_no_rows(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        rows = session.fetch_all(f"SELECT id FROM `{table}` WHERE id = 999")
        assert rows == []


def test_fetch_all_returns_list_of_dicts_when_rows_exist(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        _insert_two_rows(session, table)
        rows = session.fetch_all(f"SELECT id, value FROM `{table}` ORDER BY id")
        assert rows == [{"id": 1, "value": 10}, {"id": 2, "value": 20}]


def test_execute_scalar_returns_scalar_value(engine) -> None:
    with DbSession(engine) as session:
        val = session.execute_scalar("SELECT 123")
        assert val == 123


def test_execute_scalar_returns_none_for_null(engine) -> None:
    with DbSession(engine) as session:
        val = session.execute_scalar("SELECT NULL")
        assert val is None


def test_execute_scalar_does_not_interfere_with_transaction_state(engine, fresh_table: str) -> None:
    table = fresh_table

    with pytest.raises(RuntimeError):
        with DbSession(engine) as session:
            session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 10)")
            assert session.execute_scalar("SELECT 1") == 1
            raise RuntimeError("force rollback")

    with DbSession(engine) as session2:
        row = session2.fetch_one(f"SELECT id FROM `{table}` WHERE id = 1")
        assert row is None


