class SafeSyncError(Exception):
    """Base exception for coti_safesync_framework errors."""


class DbWriteError(SafeSyncError):
    """Any failure during DB write."""


class LockAcquisitionError(SafeSyncError):
    """Failed to acquire a lock within the expected constraints."""


class LockTimeoutError(SafeSyncError):
    """Failed to acquire a lock within the timeout period."""


class QueueError(SafeSyncError):
    """General queue-related issues."""

