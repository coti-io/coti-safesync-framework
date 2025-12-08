from abc import ABC, abstractmethod
from sqlalchemy.engine import Connection
from ..models import DbOperation


class LockBackend(ABC):
    """
    Abstract base for MySQL lock backends.
    All locking is performed using the provided Connection.
    """

    @abstractmethod
    def acquire(self, conn: Connection, op: DbOperation) -> None:
        """Acquire the lock for this operation on the given connection."""
        ...

    @abstractmethod
    def release(self, conn: Connection, op: DbOperation) -> None:
        """Release the lock for this operation on the given connection."""
        ...

