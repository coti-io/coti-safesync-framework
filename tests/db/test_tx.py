from __future__ import annotations

import pytest
from sqlalchemy import text

from conquiet.db.tx import DbFactory, DbTransaction


def test_transaction_commits_on_explicit_commit(engine, fresh_table: str) -> None:
    """Test that transaction commits when commit() is called."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx.commit()

    # Verify data was committed
    tx2 = factory.begin()
    row = tx2.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = :id", {"id": 1})
    tx2.commit()
    assert row == {"id": 1, "value": 123}


def test_transaction_rolls_back_on_explicit_rollback(engine, fresh_table: str) -> None:
    """Test that transaction rolls back when rollback() is called."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx.rollback()

    # Verify data was not committed
    tx2 = factory.begin()
    row = tx2.fetch_one(f"SELECT id FROM `{table}` WHERE id = :id", {"id": 1})
    tx2.commit()
    assert row is None


def test_transaction_cannot_be_reused_after_commit(engine, fresh_table: str) -> None:
    """Test that transaction cannot be used after commit."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx.commit()

    # Attempting to use closed transaction should raise
    with pytest.raises(RuntimeError, match="Transaction is already closed"):
        tx.execute(f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)", {"id": 2, "value": 456})


def test_transaction_cannot_be_reused_after_rollback(engine, fresh_table: str) -> None:
    """Test that transaction cannot be used after rollback."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx.rollback()

    # Attempting to use closed transaction should raise
    with pytest.raises(RuntimeError, match="Transaction is already closed"):
        tx.execute(f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)", {"id": 2, "value": 456})


def test_transaction_cannot_commit_twice(engine, fresh_table: str) -> None:
    """Test that commit() cannot be called twice."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx.commit()

    with pytest.raises(RuntimeError, match="Transaction is already closed"):
        tx.commit()


def test_transaction_cannot_rollback_twice(engine, fresh_table: str) -> None:
    """Test that rollback() cannot be called twice."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx.rollback()

    with pytest.raises(RuntimeError, match="Transaction is already closed"):
        tx.rollback()


def test_transaction_execute_returns_rowcount(engine, fresh_table: str) -> None:
    """Test that execute() returns affected row count."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    rc = tx.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    assert rc == 1
    tx.commit()


def test_transaction_fetch_one_returns_dict(engine, fresh_table: str) -> None:
    """Test that fetch_one() returns a dictionary."""
    table = fresh_table
    factory = DbFactory(engine)

    # Insert data
    tx1 = factory.begin()
    tx1.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx1.commit()

    # Fetch data
    tx2 = factory.begin()
    row = tx2.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = :id", {"id": 1})
    tx2.commit()
    assert row == {"id": 1, "value": 123}


def test_transaction_fetch_one_returns_none_when_not_found(engine, fresh_table: str) -> None:
    """Test that fetch_one() returns None when no row is found."""
    table = fresh_table
    factory = DbFactory(engine)

    tx = factory.begin()
    row = tx.fetch_one(f"SELECT id FROM `{table}` WHERE id = :id", {"id": 999})
    tx.commit()
    assert row is None


def test_transaction_fetch_all_returns_list(engine, fresh_table: str) -> None:
    """Test that fetch_all() returns a list of dictionaries."""
    table = fresh_table
    factory = DbFactory(engine)

    # Insert multiple rows
    tx1 = factory.begin()
    tx1.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 100},
    )
    tx1.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 2, "value": 200},
    )
    tx1.commit()

    # Fetch all rows
    tx2 = factory.begin()
    rows = tx2.fetch_all(f"SELECT id, value FROM `{table}` ORDER BY id")
    tx2.commit()
    assert len(rows) == 2
    assert rows[0] == {"id": 1, "value": 100}
    assert rows[1] == {"id": 2, "value": 200}


def test_transaction_execute_scalar_returns_value(engine, fresh_table: str) -> None:
    """Test that execute_scalar() returns a scalar value."""
    table = fresh_table
    factory = DbFactory(engine)

    # Insert data
    tx1 = factory.begin()
    tx1.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 123},
    )
    tx1.commit()

    # Fetch scalar
    tx2 = factory.begin()
    count = tx2.execute_scalar(f"SELECT COUNT(*) FROM `{table}`")
    tx2.commit()
    assert count == 1


def test_factory_creates_new_transaction_each_time(engine, fresh_table: str) -> None:
    """Test that factory.begin() creates a new transaction each time."""
    table = fresh_table
    factory = DbFactory(engine)

    tx1 = factory.begin()
    tx2 = factory.begin()

    # Both transactions should be independent
    tx1.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 1, "value": 100},
    )
    tx2.execute(
        f"INSERT INTO `{table}` (id, value) VALUES (:id, :value)",
        {"id": 2, "value": 200},
    )

    tx1.commit()
    tx2.rollback()

    # Only tx1's data should be committed
    tx3 = factory.begin()
    row1 = tx3.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = :id", {"id": 1})
    row2 = tx3.fetch_one(f"SELECT id, value FROM `{table}` WHERE id = :id", {"id": 2})
    tx3.commit()

    assert row1 == {"id": 1, "value": 100}
    assert row2 is None

