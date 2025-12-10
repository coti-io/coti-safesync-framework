from ..metrics.registry import (
    DB_WRITE_TOTAL,
    DB_WRITE_LATENCY_SECONDS,
    DB_LOCK_ACQUIRE_LATENCY_SECONDS,
)


def observe_db_write(table: str, op_type: str, status: str, latency_s: float) -> None:
    """
    Record DB write operation metrics.
    
    Args:
        table: Table name
        op_type: Operation type ("insert" or "update")
        status: Operation status ("success" or "error")
        latency_s: Operation latency in seconds
    """
    DB_WRITE_TOTAL.labels(table=table, op_type=op_type, status=status).inc()
    DB_WRITE_LATENCY_SECONDS.labels(table=table, op_type=op_type).observe(latency_s)


def observe_lock_acquisition(strategy: str, latency_s: float, success: bool) -> None:
    """
    Record lock acquisition metrics.
    
    Args:
        strategy: Lock strategy name
        latency_s: Lock acquisition latency in seconds
        success: Whether lock acquisition succeeded
    """
    if success:
        DB_LOCK_ACQUIRE_LATENCY_SECONDS.labels(strategy=strategy).observe(latency_s)

