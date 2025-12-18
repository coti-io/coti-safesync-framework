from sqlalchemy import text
from .base import LockBackend
from ..models import DbOperation
from ..session import DbSession


class MySQLTableLockBackend(LockBackend):
    def acquire(self, session: DbSession, op: DbOperation) -> None:
        stmt = text(f"LOCK TABLES {op.table} WRITE")
        session.execute(stmt)

    def release(self, session: DbSession, op: DbOperation) -> None:
        session.execute(text("UNLOCK TABLES"))

