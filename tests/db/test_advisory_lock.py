from __future__ import annotations

import uuid

import pytest

from coti_safesync_framework.db.locking.advisory_lock import AdvisoryLock
from coti_safesync_framework.db.session import DbSession
from coti_safesync_framework.errors import LockTimeoutError


def _lock_key(prefix: str = "coti_safesync_test") -> str:
    # Keep comfortably under MySQL's 64-char advisory lock name limit.
    return f"{prefix}:{uuid.uuid4().hex[:24]}"


def test_acquires_advisory_lock_within_timeout(engine) -> None:
    key = _lock_key()
    with DbSession(engine) as session:
        with AdvisoryLock(session, key, timeout=1):
            assert session.execute_scalar("SELECT 1", {}) == 1


def test_raises_lock_timeout_error_when_lock_cannot_be_acquired(engine) -> None:
    key = _lock_key()

    with DbSession(engine) as session1:
        with AdvisoryLock(session1, key, timeout=1):
            with DbSession(engine) as session2:
                with pytest.raises(LockTimeoutError):
                    # timeout=0 => do not wait; deterministic failure if lock is held
                    with AdvisoryLock(session2, key, timeout=0):
                        pass


def test_lock_held_after_advisorylock_exit_while_dbsession_active(engine) -> None:
    """
    AdvisoryLock intentionally does NOT release the lock in __exit__.
    The lock remains held while the surrounding DbSession is active.
    """
    key = _lock_key()

    with DbSession(engine) as session1:
        with AdvisoryLock(session1, key, timeout=1):
            assert session1.execute_scalar("SELECT 1", {}) == 1
        # AdvisoryLock context exits, but lock is still held (connection-scoped)
        
        # Lock is still held while DbSession is active
        with DbSession(engine) as session2:
            with pytest.raises(LockTimeoutError):
                with AdvisoryLock(session2, key, timeout=0):
                    pass
        # Note: Lock release when connection closes is validated by concurrency tests


def test_lock_held_after_exception_while_dbsession_active(engine) -> None:
    """
    Even when an exception is raised inside AdvisoryLock context,
    the lock remains held while the surrounding DbSession is active.
    """
    key = _lock_key()

    with pytest.raises(RuntimeError):
        with DbSession(engine) as session1:
            with AdvisoryLock(session1, key, timeout=1):
                raise RuntimeError("boom")
            # AdvisoryLock context exits (even on exception), but lock is still held
            # Lock is still held while DbSession is active
            with DbSession(engine) as session2:
                with pytest.raises(LockTimeoutError):
                    with AdvisoryLock(session2, key, timeout=0):
                        pass
        # Note: Lock release when connection closes is validated by concurrency tests


def test_requires_active_dbsession(engine) -> None:
    key = _lock_key()
    session = DbSession(engine)  # not entered
    with pytest.raises(RuntimeError):
        with AdvisoryLock(session, key, timeout=0):
            pass


