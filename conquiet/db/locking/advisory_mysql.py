from sqlalchemy import text
from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation
from ...errors import LockAcquisitionError


class MySQLAdvisoryLockBackend(LockBackend):
    def __init__(self, lock_timeout: int = 10, prefix: str = "conquiet_lock"):
        self.lock_timeout = lock_timeout
        self.prefix = prefix

    def _lock_name(self, op: DbOperation) -> str:
        return f"{self.prefix}:{op.table}:{op.id_value}"

    def acquire(self, conn: Connection, op: DbOperation) -> None:
        lock_name = self._lock_name(op)
        stmt = text("SELECT GET_LOCK(:lock_name, :timeout)")
        res = conn.execute(stmt, {"lock_name": lock_name, "timeout": self.lock_timeout}).scalar()

        if res != 1:
            raise LockAcquisitionError(f"Failed to acquire advisory lock '{lock_name}'")

    def release(self, conn: Connection, op: DbOperation) -> None:
        lock_name = self._lock_name(op)
        stmt = text("SELECT RELEASE_LOCK(:lock_name)")
        conn.execute(stmt, {"lock_name": lock_name})

