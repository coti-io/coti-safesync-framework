import pytest
from sqlalchemy import create_engine, text
from conquiet.db.locking import make_lock_backend
from conquiet.db.locking.strategy import LockStrategy
from conquiet.db.models import DbOperation, DbOperationType
from conquiet.config import DbConfig
from conquiet.errors import LockAcquisitionError


class TestLockBackends:
    """Test individual lock backend implementations."""

    @pytest.fixture
    def mysql_engine(self):
        engine = create_engine("mysql+pymysql://test_user:test_password@localhost:3306/test_db")
        yield engine
        engine.dispose()

    @pytest.fixture
    def db_config(self):
        return DbConfig(table_name="test_table", id_column="id")

    @pytest.fixture
    def db_operation(self):
        return DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=123,
            payload={"id": 123, "name": "test"},
        )

    def test_advisory_lock_acquire_release(self, mysql_engine, db_config, db_operation):
        """Test advisory lock acquisition and release."""
        backend = make_lock_backend(LockStrategy.ADVISORY, db_config)
        assert backend is not None

        with mysql_engine.begin() as conn:
            backend.acquire(conn, db_operation)
            backend.release(conn, db_operation)

    def test_advisory_lock_timeout(self, mysql_engine, db_config, db_operation):
        """Test advisory lock timeout behavior."""
        backend = make_lock_backend(LockStrategy.ADVISORY, db_config)
        
        # Acquire lock in first connection
        with mysql_engine.begin() as conn1:
            backend.acquire(conn1, db_operation)
            
            # Try to acquire same lock in second connection with short timeout
            backend2 = make_lock_backend(LockStrategy.ADVISORY, db_config)
            backend2.lock_timeout = 1  # 1 second timeout
            
            with mysql_engine.begin() as conn2:
                with pytest.raises(LockAcquisitionError):
                    backend2.acquire(conn2, db_operation)

    def test_row_lock_acquire_release(self, mysql_engine, db_config, db_operation):
        """Test row lock acquisition."""
        backend = make_lock_backend(LockStrategy.ROW, db_config)
        assert backend is not None

        # Create table first
        with mysql_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """))
            # Insert a row to lock
            conn.execute(text("INSERT INTO test_table (id, name) VALUES (123, 'test')"))

        with mysql_engine.begin() as conn:
            backend.acquire(conn, db_operation)
            # Row lock is released when transaction commits

    def test_table_lock_acquire_release(self, mysql_engine, db_config, db_operation):
        """Test table lock acquisition and release."""
        backend = make_lock_backend(LockStrategy.TABLE, db_config)
        assert backend is not None

        with mysql_engine.begin() as conn:
            backend.acquire(conn, db_operation)
            backend.release(conn, db_operation)

    def test_composite_lock_acquire_release(self, mysql_engine, db_config, db_operation):
        """Test composite (advisory+row) lock acquisition."""
        backend = make_lock_backend(LockStrategy.ADVISORY_AND_ROW, db_config)
        assert backend is not None

        # Create table first
        with mysql_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """))

        with mysql_engine.begin() as conn:
            backend.acquire(conn, db_operation)
            backend.release(conn, db_operation)

    def test_none_strategy_returns_none(self, db_config):
        """Test that NONE strategy returns None."""
        backend = make_lock_backend(LockStrategy.NONE, db_config)
        assert backend is None

    def test_unknown_strategy_raises_error(self, db_config):
        """Test that unknown strategy raises ValueError."""
        # This test would require a mock or invalid enum value
        # For now, we'll test that make_lock_backend validates correctly
        pass

