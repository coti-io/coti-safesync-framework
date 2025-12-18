import logging
import time
import warnings
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from .models import DbOperation, DbOperationType
from .locking import make_lock_backend
from .locking.strategy import LockStrategy
from .metrics import observe_db_write, observe_lock_acquisition
from .session import DbSession
from ..config import DbConfig
from ..errors import DbWriteError

logger = logging.getLogger(__name__)


class DbWriter:
    """
    Thin wrapper for idempotent INSERT and UPDATE operations.
    
    .. deprecated:: 2.0
        DbWriter is deprecated. Prefer using DbSession directly with explicit locks:
        
        - For INSERT: Use DbSession.execute() directly and handle duplicate keys
        - For UPDATE: Use DbSession.execute() with AdvisoryLock or RowLock as needed
        - For OCC: Use DbSession.execute() with conditional WHERE clauses
        
        DbWriter will be removed in a future version.
    """
    def __init__(
        self,
        engine: Engine,
        db_config: DbConfig,
        lock_strategy: LockStrategy = LockStrategy.NONE,
    ) -> None:
        warnings.warn(
            "DbWriter is deprecated. Use DbSession with explicit locks instead. "
            "See bootstrap.md for migration guidance.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.engine = engine
        self.db_config = db_config
        self.lock_strategy = lock_strategy
        self.lock_backend = make_lock_backend(lock_strategy, db_config)

    def execute(self, op: DbOperation) -> None:
        """
        Perform the DB operation with appropriate locking.
        Raises DbWriteError on failure.
        
        INSERT operations never acquire locks, regardless of configured LockStrategy.
        Duplicate key errors for INSERT are caught, logged, and suppressed.
        """
        start_time = time.monotonic()
        status = "success"

        try:
            with DbSession(self.engine) as session:
                # Lock (if any) - only for UPDATE operations
                if self.lock_backend and op.op_type == DbOperationType.UPDATE:
                    lock_start = time.monotonic()
                    self.lock_backend.acquire(session, op)
                    lock_latency = time.monotonic() - lock_start
                    observe_lock_acquisition(self.lock_strategy.value, lock_latency, True)

                # Perform operation
                if op.op_type == DbOperationType.INSERT:
                    self._insert(session, op)
                elif op.op_type == DbOperationType.UPDATE:
                    self._update(session, op)
                else:
                    raise DbWriteError(f"Unsupported operation type: {op.op_type}")

                # Release lock (if any) - only for UPDATE operations
                if self.lock_backend and op.op_type == DbOperationType.UPDATE:
                    self.lock_backend.release(session, op)

        except Exception as exc:
            status = "error"
            raise DbWriteError(str(exc)) from exc
        finally:
            latency = time.monotonic() - start_time
            observe_db_write(op.table, op.op_type.value, status, latency)

    def _insert(self, session: DbSession, op: DbOperation) -> None:
        """
        Execute INSERT operation.
        Duplicate key errors are caught, logged, and suppressed.
        INSERT is considered successful if the row exists after execution.
        """
        # Only plain INSERT (no INSERT IGNORE or UPSERT)
        cols = list(op.payload.keys())
        col_names = ", ".join(cols)
        placeholders = ", ".join(f":{c}" for c in cols)

        sql = f"INSERT INTO {op.table} ({col_names}) VALUES ({placeholders})"
        stmt = text(sql)
        
        try:
            session.execute(stmt, op.payload)
        except IntegrityError as exc:
            # Check if this is a MySQL duplicate key error
            # MySQL error code 1062 is ER_DUP_ENTRY
            # Also check error message for "Duplicate entry"
            error_msg = str(exc.orig) if hasattr(exc, 'orig') else str(exc)
            error_code = getattr(exc.orig, 'args', [None])[0] if hasattr(exc, 'orig') else None
            
            is_duplicate_key = (
                error_code == 1062 or
                "Duplicate entry" in error_msg or
                "duplicate key" in error_msg.lower()
            )
            
            if is_duplicate_key:
                logger.info(
                    "Duplicate key error suppressed for INSERT into %s with id=%s: %s",
                    op.table,
                    op.id_value,
                    error_msg,
                )
            else:
                # Not a duplicate key error, re-raise
                raise

    def _update(self, session: DbSession, op: DbOperation) -> None:
        # UPDATE based on primary key
        cols = [c for c in op.payload.keys() if c != self.db_config.id_column]
        if not cols:
            return  # nothing to update

        set_clause = ", ".join(f"{c} = :{c}" for c in cols)
        sql = (
            f"UPDATE {op.table} SET {set_clause} "
            f"WHERE {self.db_config.id_column} = :id_value"
        )

        params = dict(op.payload)
        params["id_value"] = op.id_value

        stmt = text(sql)
        session.execute(stmt, params)

