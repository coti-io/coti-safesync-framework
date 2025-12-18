from __future__ import annotations

from sqlalchemy import text
from ..session import DbSession
from ...errors import LockTimeoutError


class AdvisoryLock:
    """
    General-purpose mutex using MySQL advisory locks.
    
    Lock is connection-scoped and released on context exit.
    Does not own the transaction - must be used within a DbSession.
    
    Usage:
        with db.session() as session:
            with AdvisoryLock(session, "order:42"):
                # protected logic
                row = session.fetch_one(...)
                session.execute(...)
    """

    def __init__(self, session: DbSession, key: str, timeout: int = 10) -> None:
        """
        Initialize an advisory lock.
        
        Args:
            session: Active DbSession instance
            key: Lock key (will be used directly in GET_LOCK)
            timeout: Maximum seconds to wait for lock acquisition
        """
        self.session = session
        self.key = key
        self.timeout = timeout
        self._acquired = False

    def __enter__(self) -> "AdvisoryLock":
        """
        Acquire the advisory lock.
        
        Raises:
            LockTimeoutError: If lock cannot be acquired within timeout
        """
        stmt = text("SELECT GET_LOCK(:lock_name, :timeout)")
        res = self.session.execute_scalar(
            stmt, {"lock_name": self.key, "timeout": self.timeout}
        )

        if res != 1:
            raise LockTimeoutError(
                f"Failed to acquire advisory lock '{self.key}' within {self.timeout} seconds"
            )

        self._acquired = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Release the advisory lock.
        
        Lock is released regardless of whether an exception occurred.
        """
        if self._acquired:
            stmt = text("SELECT RELEASE_LOCK(:lock_name)")
            # RELEASE_LOCK returns a scalar; we ignore the result
            self.session.execute_scalar(stmt, {"lock_name": self.key})
            self._acquired = False

        # Propagate exceptions
        return False

