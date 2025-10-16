"""
Pytest configuration and fixtures for Iceberg Snapshots tests
"""

import os
import sys
from datetime import datetime

import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from bcn.config import Config
from bcn.spark_client import SparkClient
from tests.infrastructure import InfrastructureManager


@pytest.fixture(scope="session")
def infrastructure():
    """
    Session-scoped fixture to ensure infrastructure is running

    Note: When running via test_runner.sh, infrastructure check is skipped
    since it's already verified before running tests.
    """
    manager = InfrastructureManager()

    # Only check infrastructure if NOT running inside a container
    # (test_runner.sh already checks infrastructure before running tests)
    if os.getenv("RUNNING_IN_CONTAINER") != "true" and not manager.ensure_infrastructure_ready():
        pytest.skip("Infrastructure is not ready. Start with: docker-compose up -d")

    yield manager


@pytest.fixture(scope="session")
def spark_session(infrastructure):
    """
    Session-scoped Spark client fixture
    """
    client = SparkClient(app_name="bcn-test")
    yield client
    client.close()


@pytest.fixture(scope="function")
def clean_database(spark_session):
    """
    Function-scoped fixture that provides a clean database for each test
    """
    database = "default"

    # Ensure database exists
    spark_session.create_database(database)

    # Clean up before test
    spark_session.cleanup_database(database)

    yield database

    # Optional: cleanup after test as well (disabled by default to allow inspection)
    # spark_session.cleanup_database(database)


@pytest.fixture(scope="function")
def test_table_name():
    """
    Fixture that provides a unique test table name
    """
    return "my_table"


@pytest.fixture(scope="function")
def backup_name():
    """
    Fixture that provides a unique backup name
    """
    return "iceberg_backup"


@pytest.fixture(scope="function")
def sample_data():
    """
    Fixture that provides sample test data
    """
    return [
        (1, "Alice", 100.5, datetime(2024, 1, 1, 10, 0, 0)),
        (2, "Bob", 200.75, datetime(2024, 1, 2, 11, 30, 0)),
        (3, "Charlie", 300.25, datetime(2024, 1, 3, 14, 15, 0)),
        (4, "David", 400.0, datetime(2024, 1, 4, 9, 45, 0)),
        (5, "Eve", 500.99, datetime(2024, 1, 5, 16, 20, 0)),
    ]


@pytest.fixture(scope="function")
def table_schema():
    """
    Fixture that provides table schema
    """
    return "id INT, name STRING, value DOUBLE, created_at TIMESTAMP"


@pytest.fixture(scope="function")
def source_table(spark_session, clean_database, test_table_name, sample_data, table_schema):
    """
    Fixture that creates and populates a source table for testing
    """
    # Create table location
    table_location = f"s3a://{Config.WAREHOUSE_BUCKET}/{clean_database}/{test_table_name}"

    # Explicitly drop the table if it exists to ensure a fresh start
    spark_session.drop_table(clean_database, test_table_name)

    # Create table
    success = spark_session.create_table(
        clean_database, test_table_name, table_schema, table_location
    )

    if not success:
        pytest.fail(f"Failed to create source table {clean_database}.{test_table_name}")

    # Insert sample data
    success = spark_session.insert_data(clean_database, test_table_name, sample_data)

    if not success:
        pytest.fail(f"Failed to insert data into {clean_database}.{test_table_name}")

    # Return table info
    return {
        "database": clean_database,
        "table": test_table_name,
        "location": table_location,
        "data": sample_data,
    }


def pytest_configure(config):
    """
    Pytest configuration hook
    """
    # Add custom markers
    config.addinivalue_line("markers", "e2e: mark test as end-to-end test")
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "unit: mark test as unit test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to add markers based on test location
    """
    for item in items:
        # Auto-mark E2E tests
        if "test_backup_restore" in item.nodeid:
            item.add_marker(pytest.mark.e2e)
            item.add_marker(pytest.mark.slow)
