from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from conquiet.db.session import DbSession


def test_dbsession_duplicate_insert_raises_integrity_error(engine, fresh_table: str) -> None:
    """
    DbSession is a thin transactional primitive; it does not suppress duplicate-key errors.
    Callers that want idempotent INSERTs must handle IntegrityError themselves.
    """
    table = fresh_table

    with DbSession(engine) as session:
        session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 10)", {})
        with pytest.raises(IntegrityError):
            session.execute(f"INSERT INTO `{table}` (id, value) VALUES (1, 999)", {})


