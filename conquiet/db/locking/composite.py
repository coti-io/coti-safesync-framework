from .base import LockBackend
from ..models import DbOperation
from ..session import DbSession


class CompositeLockBackend(LockBackend):
    """
    Applies multiple lock backends in sequence.
    Only used for the ADVISORY_AND_ROW strategy.
    """

    def __init__(self, backends: list[LockBackend]):
        self.backends = backends

    def acquire(self, session: DbSession, op: DbOperation) -> None:
        for backend in self.backends:
            backend.acquire(session, op)

    def release(self, session: DbSession, op: DbOperation) -> None:
        for backend in reversed(self.backends):
            backend.release(session, op)

