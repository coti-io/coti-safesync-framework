from sqlalchemy import text
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation


class MySQLRowLockBackend(LockBackend):
    def __init__(self, id_column: str = "id"):
        self.id_column = id_column

    def acquire(self, conn: Connection, op: DbOperation) -> None:
        stmt = text(
            f"SELECT {self.id_column} FROM {op.table} "
            f"WHERE {self.id_column} = :id FOR UPDATE"
        )
        conn.execute(stmt, {"id": op.id_value})

    def release(self, conn: Connection, op: DbOperation) -> None:
        # Row locks are released when the surrounding transaction commits/rolls back.
        # Nothing special to do here.
        return None

