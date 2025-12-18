from sqlalchemy import text
from .base import LockBackend
from ..models import DbOperation
from ..session import DbSession
from ...errors import LockAcquisitionError


class MySQLAdvisoryLockBackend(LockBackend):
    def __init__(self, lock_timeout: int = 10, prefix: str = "conquiet_lock"):
        self.lock_timeout = lock_timeout
        self.prefix = prefix

    def _lock_name(self, op: DbOperation) -> str:
        return f"{self.prefix}:{op.table}:{op.id_value}"

    def acquire(self, session: DbSession, op: DbOperation) -> None:
        lock_name = self._lock_name(op)
        stmt = text("SELECT GET_LOCK(:lock_name, :timeout) AS acquired")
        res = session.execute_scalar(
            stmt, {"lock_name": lock_name, "timeout": self.lock_timeout}
        )

        if res != 1:
            raise LockAcquisitionError(f"Failed to acquire advisory lock '{lock_name}'")

    def release(self, session: DbSession, op: DbOperation) -> None:
        lock_name = self._lock_name(op)
        stmt = text("SELECT RELEASE_LOCK(:lock_name)")
        # RELEASE_LOCK returns a scalar; we ignore the result
        session.execute_scalar(stmt, {"lock_name": lock_name})

