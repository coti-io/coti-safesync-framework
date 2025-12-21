from .strategy import LockStrategy
from .base import LockBackend
from ...config import DbConfig


def make_lock_backend(strategy: LockStrategy, db_config: DbConfig) -> LockBackend | None:
    """
    Create a lock backend for the deprecated DbWriter.
    
    .. deprecated:: 2.0
        This function is only used by the deprecated DbWriter class.
        For new code, use AdvisoryLock and RowLock directly with DbSession.
        See bootstrap.md for migration guidance.
    """
    if strategy == LockStrategy.NONE:
        return None

    # All locking strategies are deprecated via DbWriter.
    # Use AdvisoryLock and RowLock directly with DbSession instead.
    if strategy == LockStrategy.ADVISORY:
        raise NotImplementedError(
            "Advisory locks are not supported via DbWriter. "
            "Use AdvisoryLock directly with DbSession instead. "
            "See bootstrap.md for migration guidance."
        )

    if strategy == LockStrategy.ROW:
        raise NotImplementedError(
            "Row locks are not supported via DbWriter. "
            "Use RowLock directly with DbSession instead. "
            "See bootstrap.md for migration guidance."
        )

    if strategy == LockStrategy.ADVISORY_AND_ROW:
        raise NotImplementedError(
            "Advisory+Row locks are not supported via DbWriter. "
            "Use AdvisoryLock and RowLock directly with DbSession instead. "
            "See bootstrap.md for migration guidance."
        )

    if strategy == LockStrategy.TABLE:
        raise NotImplementedError(
            "Table locks are not supported via DbWriter. "
            "Table locks are generally not recommended. "
            "If needed, use LOCK TABLES directly with DbSession.execute()."
        )

    raise ValueError(f"Unknown lock strategy: {strategy}")

