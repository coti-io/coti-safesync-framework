import pytest
from sqlalchemy import create_engine, text
from conquiet.config import DbConfig
from conquiet.db import DbWriter, LockStrategy


@pytest.fixture
def mysql_engine():
    """Create a SQLAlchemy engine for MySQL testing."""
    # Default to local MySQL for testing
    # Can be overridden with environment variables
    import os
    db_url = os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://test_user:test_password@localhost:3306/test_db"
    )
    engine = create_engine(db_url, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture
def db_config():
    """Default DB config for testing."""
    return DbConfig(table_name="test_table", id_column="id")


@pytest.fixture
def db_writer(mysql_engine, db_config):
    """Create a DbWriter instance for testing."""
    return DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.NONE)


@pytest.fixture
def db_writer_with_advisory_lock(mysql_engine, db_config):
    """Create a DbWriter with advisory locking."""
    return DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.ADVISORY)


@pytest.fixture
def db_writer_with_row_lock(mysql_engine, db_config):
    """Create a DbWriter with row locking."""
    return DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.ROW)


@pytest.fixture
def db_writer_with_composite_lock(mysql_engine, db_config):
    """Create a DbWriter with advisory+row locking."""
    return DbWriter(mysql_engine, db_config, lock_strategy=LockStrategy.ADVISORY_AND_ROW)


@pytest.fixture
def create_test_table(mysql_engine):
    """Create a test table and clean it up after."""
    with mysql_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                status VARCHAR(50)
            )
        """))
    yield
    with mysql_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_table"))

