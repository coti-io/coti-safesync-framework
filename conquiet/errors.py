class ConquietError(Exception):
    """Base exception for conquiet errors."""


class DbWriteError(ConquietError):
    """Any failure during DB write."""


class LockAcquisitionError(ConquietError):
    """Failed to acquire a lock within the expected constraints."""


class LockTimeoutError(ConquietError):
    """Failed to acquire a lock within the timeout period."""


class QueueError(ConquietError):
    """General queue-related issues."""

