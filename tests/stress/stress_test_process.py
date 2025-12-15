import time
import random
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text
from conquiet.db import DbWriter, DbOperation, DbOperationType, LockStrategy
from conquiet.config import DbConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

process_stats = {
    "writes_attempted": 0,
    "writes_succeeded": 0,
    "writes_failed": 0,
    "errors": [],
}


def writer_job(process_id: int, writer: DbWriter, id_range: int):
    """Writer job that runs at high frequency."""
    process_stats["writes_attempted"] += 1
    
    try:
        # Create overlapping IDs intentionally
        record_id = random.randint(1, id_range)
        
        # Read current counter, increment, write
        # NOTE: This read-modify-write gap intentionally exposes race conditions
        with writer.engine.begin() as conn:
            result = conn.execute(
                text("SELECT counter FROM stress_table WHERE id = :id"),
                {"id": record_id}
            )
            row = result.fetchone()
            
            if row:
                current_counter = row[0]
                new_counter = current_counter + 1
            else:
                # First write to this ID
                new_counter = 1
        
        # Perform update (or insert if doesn't exist)
        op = DbOperation(
            table="stress_table",
            op_type=DbOperationType.UPDATE,
            id_value=record_id,
            payload={
                "id": record_id,
                "counter": new_counter,
                "last_updated_by": process_id,
                "write_count": None,  # Will be handled separately
            },
        )
        
        writer.execute(op)
        
        # Track write count separately
        with writer.engine.begin() as conn:
            conn.execute(
                text("UPDATE stress_table SET write_count = COALESCE(write_count, 0) + 1 WHERE id = :id"),
                {"id": record_id}
            )
        
        process_stats["writes_succeeded"] += 1
        
    except Exception as e:
        process_stats["writes_failed"] += 1
        process_stats["errors"].append(str(e))
        logger.error(f"Process {process_id} write failed: {e}")


def launch_process(
    process_id: int,
    strategy: LockStrategy,
    duration: int = 60,
    frequency: float = 0.05,
    id_range: int = 200,
    db_url: str = "mysql+pymysql://test_user:test_password@localhost:3306/test_db",
):
    """Launch a single stress test process."""
    global process_stats
    process_stats = {
        "writes_attempted": 0,
        "writes_succeeded": 0,
        "writes_failed": 0,
        "errors": [],
    }
    
    engine = create_engine(db_url, echo=False)
    db_config = DbConfig(table_name="stress_table", id_column="id")
    writer = DbWriter(engine, db_config, lock_strategy=strategy)
    
    # Ensure table exists
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stress_table (
                id INT PRIMARY KEY,
                counter INT DEFAULT 0,
                last_updated_by INT,
                write_count INT DEFAULT 0
            )
        """))
    
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        writer_job,
        "interval",
        seconds=frequency,
        args=[process_id, writer, id_range],
        id=f"writer_job_{process_id}",
    )
    
    scheduler.start()
    logger.info(f"Process {process_id} started with strategy {strategy.value}")
    
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        logger.info(f"Process {process_id} interrupted")
    finally:
        scheduler.shutdown(wait=True)
        engine.dispose()
        logger.info(f"Process {process_id} stats: {process_stats}")
    
    return process_stats

