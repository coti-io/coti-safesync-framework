from __future__ import annotations

import uuid

import pytest

from conquiet.db.locking.advisory_lock import AdvisoryLock
from conquiet.db.session import DbSession
from conquiet.errors import LockTimeoutError


def _lock_key(prefix: str = "conquiet_test") -> str:
    # Keep comfortably under MySQL's 64-char advisory lock name limit.
    return f"{prefix}:{uuid.uuid4().hex[:24]}"


def test_acquires_advisory_lock_within_timeout(engine) -> None:
    key = _lock_key()
    with DbSession(engine) as session:
        with AdvisoryLock(session, key, timeout=1):
            assert session.execute_scalar("SELECT 1") == 1


def test_raises_lock_timeout_error_when_lock_cannot_be_acquired(engine) -> None:
    key = _lock_key()

    with DbSession(engine) as session1:
        with AdvisoryLock(session1, key, timeout=1):
            with DbSession(engine) as session2:
                with pytest.raises(LockTimeoutError):
                    # timeout=0 => do not wait; deterministic failure if lock is held
                    with AdvisoryLock(session2, key, timeout=0):
                        pass


def test_releases_lock_on_normal_context_exit(engine) -> None:
    key = _lock_key()

    with DbSession(engine) as session1:
        with AdvisoryLock(session1, key, timeout=1):
            assert session1.execute_scalar("SELECT 1") == 1

    with DbSession(engine) as session2:
        with AdvisoryLock(session2, key, timeout=0):
            assert session2.execute_scalar("SELECT 1") == 1


def test_releases_lock_when_exception_is_raised_inside_context(engine) -> None:
    key = _lock_key()

    with pytest.raises(RuntimeError):
        with DbSession(engine) as session1:
            with AdvisoryLock(session1, key, timeout=1):
                raise RuntimeError("boom")

    with DbSession(engine) as session2:
        with AdvisoryLock(session2, key, timeout=0):
            assert session2.execute_scalar("SELECT 1") == 1


def test_requires_active_dbsession(engine) -> None:
    key = _lock_key()
    session = DbSession(engine)  # not entered
    with pytest.raises(RuntimeError):
        with AdvisoryLock(session, key, timeout=0):
            pass


