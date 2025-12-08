from .strategy import LockStrategy
from .base import LockBackend
from .advisory_mysql import MySQLAdvisoryLockBackend
from .row_mysql import MySQLRowLockBackend
from .table_mysql import MySQLTableLockBackend
from .composite import CompositeLockBackend
from ...config import DbConfig


def make_lock_backend(strategy: LockStrategy, db_config: DbConfig) -> LockBackend | None:
    if strategy == LockStrategy.NONE:
        return None

    if strategy == LockStrategy.ADVISORY:
        return MySQLAdvisoryLockBackend()

    if strategy == LockStrategy.ROW:
        return MySQLRowLockBackend(id_column=db_config.id_column)

    if strategy == LockStrategy.ADVISORY_AND_ROW:
        return CompositeLockBackend([
            MySQLAdvisoryLockBackend(),
            MySQLRowLockBackend(id_column=db_config.id_column),
        ])

    if strategy == LockStrategy.TABLE:
        return MySQLTableLockBackend()

    raise ValueError(f"Unknown lock strategy: {strategy}")

