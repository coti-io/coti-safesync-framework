from sqlalchemy import text
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation


class MySQLTableLockBackend(LockBackend):
    def acquire(self, conn: Connection, op: DbOperation) -> None:
        stmt = text(f"LOCK TABLES {op.table} WRITE")
        conn.execute(stmt)

    def release(self, conn: Connection, op: DbOperation) -> None:
        conn.execute(text("UNLOCK TABLES"))

