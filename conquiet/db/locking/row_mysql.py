from sqlalchemy import text
from .base import LockBackend
from ..models import DbOperation
from ..session import DbSession


class MySQLRowLockBackend(LockBackend):
    def __init__(self, id_column: str = "id"):
        self.id_column = id_column

    def acquire(self, session: DbSession, op: DbOperation) -> dict | None:
        stmt = text(
            f"SELECT {self.id_column} FROM {op.table} "
            f"WHERE {self.id_column} = :id FOR UPDATE"
        )
        # Acquire the row lock and return the selected row (if any)
        return session.fetch_one(stmt, {"id": op.id_value})

    def release(self, session: DbSession, op: DbOperation) -> None:
        # Row locks are released when the surrounding transaction commits/rolls back.
        # Nothing special to do here.
        return None

