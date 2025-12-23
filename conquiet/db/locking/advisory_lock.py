from __future__ import annotations

import time

from sqlalchemy import text
from ..session import DbSession
from ..metrics import observe_lock_acquisition
from ...errors import LockTimeoutError


class AdvisoryLock:
    """
    General-purpose mutex using MySQL advisory locks.

    MySQL advisory locks are **connection-scoped**, not transaction-scoped.

    This lock is intentionally NOT released in __exit__.
    It is released only when the surrounding DbSession closes
    (after commit or rollback).

    ⚠️ IMPORTANT SEMANTICS ⚠️
    - The lock is held across transaction commit.
    - This guarantees that no other connection can observe
      intermediate or stale state.

    ⚠️ USAGE RULES ⚠️
    - Do NOT use AdvisoryLock inside retry loops.
    - Do NOT use AdvisoryLock to "protect" OCC.
    - Each lock acquisition should guard a single logical operation.

    AdvisoryLock is NOT a substitute for optimistic concurrency control (OCC).
    If you need retries based on version mismatch, use occ_update instead.

    See docs/occ.md for when OCC is the correct tool.

    Usage:
        with DbSession(engine) as session:
            with AdvisoryLock(session, "order:42"):
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
        # Track lock acquisition start time for metrics
        start_time = time.monotonic()
        
        stmt = text("SELECT GET_LOCK(:lock_name, :timeout)")
        # Enforce active DbSession
        if self.session._conn is None:
            raise RuntimeError(
                "AdvisoryLock.__enter__() requires an active DbSession. "
            )
        res = self.session.execute_scalar(
            stmt, {"lock_name": self.key, "timeout": self.timeout}
        )

        if res != 1:
            # Emit metrics for failed lock acquisition
            try:
                latency = time.monotonic() - start_time
                observe_lock_acquisition(
                    strategy="advisory",
                    latency_s=latency,
                    success=False,
                )
            except Exception:
                pass
            raise LockTimeoutError(
                f"Failed to acquire advisory lock '{self.key}' within {self.timeout} seconds"
            )

        self._acquired = True
        
        # Emit metrics for successful lock acquisition
        try:
            latency = time.monotonic() - start_time
            observe_lock_acquisition(
                strategy="advisory",
                latency_s=latency,
                success=True,
            )
        except Exception:
            pass
        
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Exit the advisory lock context.
    
        Note: The lock is NOT released here. It is released when the surrounding
        DbSession connection closes (after commit/rollback). This ensures the lock
        is held across the transaction commit, preventing other connections from
        observing stale state.
        """
        # NOTE:
        # MySQL advisory locks are connection-scoped. Releasing the lock here would
        # happen *before* the surrounding DbSession commits/rolls back (because the
        # lock context exits before DbSession.__exit__). That ordering can allow
        # another session to acquire the lock and observe stale state, violating the
        # invariants validated by bootstrap_concurrency_tests.md.
        #
        # We therefore rely on the DbSession connection closing (after commit/rollback)
        # to release the lock deterministically.

        # Propagate exceptions
        return False

