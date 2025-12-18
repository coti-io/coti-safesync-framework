from abc import ABC, abstractmethod
from ..models import DbOperation
from ..session import DbSession


class LockBackend(ABC):
    """
    Abstract base for MySQL lock backends.
    All locking is performed using the provided Connection.
    """

    @abstractmethod
    def acquire(self, session: DbSession, op: DbOperation):
        """Acquire the lock for this operation using the given DbSession."""
        ...

    @abstractmethod
    def release(self, session: DbSession, op: DbOperation) -> None:
        """Release the lock for this operation using the given DbSession."""
        ...

