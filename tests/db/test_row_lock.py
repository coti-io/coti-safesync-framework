from __future__ import annotations

import pytest

from coti_safesync_framework.db.locking.row_lock import RowLock
from coti_safesync_framework.db.session import DbSession


def test_raises_runtime_error_if_dbsession_inactive(engine, fresh_table: str) -> None:
    session = DbSession(engine)  # not entered
    lock = RowLock(session, fresh_table, {"id": 1})
    with pytest.raises(RuntimeError):
        lock.acquire()


def test_returns_row_as_dict_if_exists(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(
            f"INSERT INTO `{table}` (id, value, k1, k2) VALUES (:id, :value, :k1, :k2)",
            {"id": 1, "value": 10, "k1": 7, "k2": 9},
        )
        row = RowLock(session, table, {"id": 1}).acquire()
        assert row is not None
        assert row["id"] == 1
        assert row["value"] == 10


def test_returns_none_if_row_does_not_exist(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        row = RowLock(session, table, {"id": 999}).acquire()
        assert row is None


def test_does_not_commit_or_rollback_implicitly(engine, fresh_table: str) -> None:
    """
    RowLock.acquire() must not commit.

    We verify this by inserting a row, acquiring a row lock, then raising to force
    rollback. If RowLock committed implicitly, the row would remain.
    """
    table = fresh_table

    with pytest.raises(RuntimeError):
        with DbSession(engine) as session:
            session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 10)", {})
            assert RowLock(session, table, {"id": 1}).acquire() is not None
            raise RuntimeError("force rollback")

    with DbSession(engine) as session2:
        assert session2.fetch_one(f"SELECT id FROM `{table}` WHERE id = 1", {}) is None


def test_where_dict_order_is_deterministic(engine, fresh_table: str) -> None:
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(
            f"INSERT INTO `{table}` (id, value, k1, k2) VALUES (:id, :value, :k1, :k2)",
            {"id": 1, "value": 10, "k1": 7, "k2": 9},
        )

        # Same predicate, different key order.
        row_a = RowLock(session, table, {"k1": 7, "k2": 9}).acquire()
        row_b = RowLock(session, table, {"k2": 9, "k1": 7}).acquire()

        assert row_a is not None and row_b is not None
        assert row_a["id"] == row_b["id"] == 1
        assert row_a["k1"] == row_b["k1"] == 7
        assert row_a["k2"] == row_b["k2"] == 9


