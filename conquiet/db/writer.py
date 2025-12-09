import time
from sqlalchemy.engine import Engine
from sqlalchemy import text
from .models import DbOperation, DbOperationType
from .locking import make_lock_backend
from .locking.strategy import LockStrategy
from .metrics import observe_db_write, observe_lock_acquisition
from ..config import DbConfig
from ..errors import DbWriteError


class DbWriter:
    def __init__(
        self,
        engine: Engine,
        db_config: DbConfig,
        lock_strategy: LockStrategy = LockStrategy.NONE,
    ) -> None:
        self.engine = engine
        self.db_config = db_config
        self.lock_strategy = lock_strategy
        self.lock_backend = make_lock_backend(lock_strategy, db_config)

    def execute(self, op: DbOperation) -> None:
        """
        Perform the DB operation with appropriate locking.
        Raises DbWriteError on failure.
        """
        start_time = time.monotonic()
        status = "success"

        try:
            with self.engine.begin() as conn:
                # Lock (if any)
                if self.lock_backend:
                    lock_start = time.monotonic()
                    self.lock_backend.acquire(conn, op)
                    lock_latency = time.monotonic() - lock_start
                    observe_lock_acquisition(self.lock_strategy.value, lock_latency, True)

                # Perform operation
                if op.op_type == DbOperationType.INSERT:
                    self._insert(conn, op)
                elif op.op_type == DbOperationType.UPDATE:
                    self._update(conn, op)
                else:
                    raise DbWriteError(f"Unsupported operation type: {op.op_type}")

                # Release lock (if any)
                if self.lock_backend:
                    self.lock_backend.release(conn, op)

        except Exception as exc:
            status = "error"
            raise DbWriteError(str(exc)) from exc
        finally:
            latency = time.monotonic() - start_time
            observe_db_write(op.table, op.op_type.value, status, latency)

    def _insert(self, conn, op: DbOperation) -> None:
        # Only plain INSERT (no INSERT IGNORE or UPSERT)
        cols = list(op.payload.keys())
        col_names = ", ".join(cols)
        placeholders = ", ".join(f":{c}" for c in cols)

        sql = f"INSERT INTO {op.table} ({col_names}) VALUES ({placeholders})"
        stmt = text(sql)
        conn.execute(stmt, op.payload)

    def _update(self, conn, op: DbOperation) -> None:
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
        conn.execute(stmt, params)

