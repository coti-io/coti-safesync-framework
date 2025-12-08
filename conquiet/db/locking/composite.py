from sqlalchemy.engine import Connection
from .base import LockBackend
from ..models import DbOperation


class CompositeLockBackend(LockBackend):
    """
    Applies multiple lock backends in sequence.
    Only used for the ADVISORY_AND_ROW strategy.
    """

    def __init__(self, backends: list[LockBackend]):
        self.backends = backends

    def acquire(self, conn: Connection, op: DbOperation) -> None:
        for backend in self.backends:
            backend.acquire(conn, op)

    def release(self, conn: Connection, op: DbOperation) -> None:
        for backend in reversed(self.backends):
            backend.release(conn, op)

