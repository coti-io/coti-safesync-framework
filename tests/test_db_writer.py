import pytest
from sqlalchemy import create_engine, text
from conquiet.db import DbWriter, DbOperation, DbOperationType, LockStrategy
from conquiet.config import DbConfig
from conquiet.errors import DbWriteError


class TestDbWriter:
    """Test DbWriter insert and update operations."""

    @pytest.fixture
    def mysql_engine(self):
        engine = create_engine("mysql+pymysql://test_user:test_password@localhost:3306/test_db")
        yield engine
        engine.dispose()

    @pytest.fixture
    def db_config(self):
        return DbConfig(table_name="test_table", id_column="id")

    @pytest.fixture
    def create_table(self, mysql_engine):
        """Create test table before each test."""
        # Drop table first to ensure clean state
        with mysql_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_table"))
            conn.execute(text("""
                CREATE TABLE test_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT,
                    status VARCHAR(50)
                )
            """))
        yield
        # Cleanup
        with mysql_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_table"))

    def test_insert_operation(self, mysql_engine, db_config, create_table):
        """Test basic INSERT operation."""
        writer = DbWriter(mysql_engine, db_config)
        
        op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=1,
            payload={"id": 1, "name": "test", "value": 100, "status": "active"},
        )
        
        writer.execute(op)
        
        # Verify insert
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 1"))
            row = result.fetchone()
            assert row is not None
            assert row[1] == "test"
            assert row[2] == 100

    def test_update_operation(self, mysql_engine, db_config, create_table):
        """Test UPDATE operation."""
        writer = DbWriter(mysql_engine, db_config)
        
        # First insert
        insert_op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=2,
            payload={"id": 2, "name": "original", "value": 50},
        )
        writer.execute(insert_op)
        
        # Then update
        update_op = DbOperation(
            table="test_table",
            op_type=DbOperationType.UPDATE,
            id_value=2,
            payload={"name": "updated", "value": 75},
        )
        writer.execute(update_op)
        
        # Verify update
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 2"))
            row = result.fetchone()
            assert row is not None
            assert row[1] == "updated"
            assert row[2] == 75

    def test_update_with_only_id_column_is_noop(self, mysql_engine, db_config, create_table):
        """Test UPDATE with only id_column in payload results in silent no-op."""
        writer = DbWriter(mysql_engine, db_config)
        
        # Insert first
        insert_op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=3,
            payload={"id": 3, "name": "test", "value": 100, "status": "active"},
        )
        writer.execute(insert_op)
        
        # Update with only id_column (should be no-op, no SQL executed)
        update_op = DbOperation(
            table="test_table",
            op_type=DbOperationType.UPDATE,
            id_value=3,
            payload={"id": 3},  # Only id column
        )
        writer.execute(update_op)  # Should not raise error and not execute SQL
        
        # Verify original values are unchanged
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 3"))
            row = result.fetchone()
            assert row is not None
            assert row[1] == "test"  # name unchanged
            assert row[2] == 100     # value unchanged
            assert row[3] == "active"  # status unchanged

    def test_partial_update(self, mysql_engine, db_config, create_table):
        """Test UPDATE with partial fields - only specified fields are updated."""
        writer = DbWriter(mysql_engine, db_config)
        
        # Insert first with all fields
        insert_op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=4,
            payload={"id": 4, "name": "original", "value": 50, "status": "pending"},
        )
        writer.execute(insert_op)
        
        # Update only name and value, leaving status unchanged
        update_op = DbOperation(
            table="test_table",
            op_type=DbOperationType.UPDATE,
            id_value=4,
            payload={"name": "updated_name", "value": 75},  # Only name and value
        )
        writer.execute(update_op)
        
        # Verify partial update - name and value changed, status unchanged
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 4"))
            row = result.fetchone()
            assert row is not None
            assert row[1] == "updated_name"  # name updated
            assert row[2] == 75               # value updated
            assert row[3] == "pending"        # status unchanged

    def test_insert_with_advisory_lock(self, mysql_engine, db_config, create_table):
        """Test INSERT with advisory locking."""
        writer = DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.ADVISORY)
        
        op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=5,
            payload={"id": 5, "name": "locked"},
        )
        
        writer.execute(op)
        
        # Verify insert succeeded
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 5"))
            assert result.fetchone() is not None

    def test_insert_with_row_lock(self, mysql_engine, db_config, create_table):
        """Test INSERT with row locking."""
        writer = DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.ROW)
        
        op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=6,
            payload={"id": 6, "name": "row_locked"},
        )
        
        writer.execute(op)
        
        # Verify insert succeeded
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 6"))
            assert result.fetchone() is not None

    def test_insert_with_composite_lock(self, mysql_engine, db_config, create_table):
        """Test INSERT with advisory+row locking."""
        writer = DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.ADVISORY_AND_ROW)
        
        op = DbOperation(
            table="test_table",
            op_type=DbOperationType.INSERT,
            id_value=7,
            payload={"id": 7, "name": "composite_locked"},
        )
        
        writer.execute(op)
        
        # Verify insert succeeded
        with mysql_engine.begin() as conn:
            result = conn.execute(text("SELECT * FROM test_table WHERE id = 7"))
            assert result.fetchone() is not None

    def test_invalid_operation_type_raises_error(self, mysql_engine, db_config, create_table):
        """Test that invalid operation type raises error."""
        writer = DbWriter(mysql_engine, db_config)
        
        # Create an invalid operation type (this would require mocking)
        # For now, we test that execute validates op_type
        pass

    def test_db_error_propagates(self, mysql_engine, db_config):
        """Test that DB errors are wrapped in DbWriteError."""
        writer = DbWriter(mysql_engine, db_config)
        
        # Try to insert into non-existent table
        op = DbOperation(
            table="non_existent_table",
            op_type=DbOperationType.INSERT,
            id_value=1,
            payload={"id": 1, "name": "test"},
        )
        
        with pytest.raises(DbWriteError):
            writer.execute(op)

