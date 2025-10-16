"""
End-to-End tests for Iceberg backup and restore
"""

import pytest

from bcn.backup import IcebergBackup
from bcn.config import Config
from bcn.restore import IcebergRestore


@pytest.mark.e2e
@pytest.mark.slow
class TestBackupRestore:
    """End-to-end tests for backup and restore workflow"""

    def test_complete_backup_restore_workflow(self, spark_session, source_table, backup_name):
        """
        Test the complete backup and restore workflow

        This test:
        1. Uses a pre-populated source table (from fixture)
        2. Creates a backup
        3. Restores to a new table
        4. Validates data integrity
        """
        database = source_table["database"]
        table = source_table["table"]
        expected_data = source_table["data"]

        # Step 1: Verify source table has data
        source_data = spark_session.query_table(database, table)
        assert source_data is not None, "Failed to query source table"
        assert len(source_data) == len(expected_data), (
            f"Source table has {len(source_data)} rows, expected {len(expected_data)}"
        )

        print(f"\n✓ Source table verified: {len(source_data)} rows")

        # Step 2: Create backup
        print(f"\nCreating backup '{backup_name}'...")
        backup = IcebergBackup(database, table, backup_name)
        backup_success = backup.create_backup()

        assert backup_success, "Backup creation failed"
        print(f"✓ Backup '{backup_name}' created successfully")

        # Step 3: Restore to new table
        target_table = f"{table}_restored"
        target_location = f"s3a://{Config.WAREHOUSE_BUCKET}/{database}/{target_table}"

        print(f"\nRestoring to '{target_table}'...")
        restore = IcebergRestore(
            backup_name=backup_name,
            target_database=database,
            target_table=target_table,
            target_location=target_location,
            catalog_type="hive",
        )
        restore_success = restore.restore_backup()

        assert restore_success, "Restore failed"
        print(f"✓ Backup restored to '{target_table}' successfully")

        # Step 4: Verify restored table
        print("\nVerifying restored table...")
        restored_data = spark_session.query_table(database, target_table)

        assert restored_data is not None, "Failed to query restored table"
        assert len(restored_data) == len(source_data), (
            f"Restored table has {len(restored_data)} rows, expected {len(source_data)}"
        )

        print(f"✓ Row count matches: {len(restored_data)} rows")

        # Step 5: Compare data row by row
        print("\nComparing data integrity...")
        self._compare_table_data(source_data, restored_data)

        print("\n✓ All data matches - backup and restore successful!")

    def test_backup_preserves_schema(self, spark_session, source_table, backup_name):
        """
        Test that backup preserves table schema correctly
        """
        database = source_table["database"]
        table = source_table["table"]

        # Create backup
        backup = IcebergBackup(database, table, f"{backup_name}_schema")
        assert backup.create_backup(), "Backup creation failed"

        # Restore
        target_table = f"{table}_schema_test"
        target_location = f"s3a://{Config.WAREHOUSE_BUCKET}/{database}/{target_table}"

        restore = IcebergRestore(
            backup_name=f"{backup_name}_schema",
            target_database=database,
            target_table=target_table,
            target_location=target_location,
        )
        assert restore.restore_backup(), "Restore failed"

        # Query both tables to verify schema matches
        source_data = spark_session.query_table(database, table)
        restored_data = spark_session.query_table(database, target_table)

        assert source_data is not None and restored_data is not None

        # Compare column names
        if len(source_data) > 0 and len(restored_data) > 0:
            source_columns = set(source_data[0].keys())
            restored_columns = set(restored_data[0].keys())

            assert source_columns == restored_columns, (
                f"Schema mismatch: source={source_columns}, restored={restored_columns}"
            )

            print(f"✓ Schema preserved: {source_columns}")

    def test_multiple_backups(self, spark_session, source_table):
        """
        Test creating multiple backups from the same source table
        """
        database = source_table["database"]
        table = source_table["table"]

        # Create two backups
        backup1 = IcebergBackup(database, table, "backup_1")
        backup2 = IcebergBackup(database, table, "backup_2")

        assert backup1.create_backup(), "First backup failed"
        assert backup2.create_backup(), "Second backup failed"

        # Restore both
        for i, backup_name in enumerate(["backup_1", "backup_2"], 1):
            target_table = f"{table}_multi_{i}"
            target_location = f"s3a://{Config.WAREHOUSE_BUCKET}/{database}/{target_table}"

            restore = IcebergRestore(
                backup_name=backup_name,
                target_database=database,
                target_table=target_table,
                target_location=target_location,
            )
            assert restore.restore_backup(), f"Restore of {backup_name} failed"

        # Verify both restored tables have the same data
        data1 = spark_session.query_table(database, f"{table}_multi_1")
        data2 = spark_session.query_table(database, f"{table}_multi_2")

        assert data1 is not None and data2 is not None
        assert len(data1) == len(data2)

        print("✓ Multiple backups work correctly")

    def _compare_table_data(self, source_data: list, restored_data: list):
        """
        Compare two datasets row by row

        Args:
            source_data: Source table data
            restored_data: Restored table data
        """
        mismatches = []

        for i, (source_row, restored_row) in enumerate(zip(source_data, restored_data)):
            if not self._compare_rows(source_row, restored_row):
                mismatches.append(
                    {"row_number": i + 1, "source": source_row, "restored": restored_row}
                )

        if mismatches:
            print(f"\n✗ Found {len(mismatches)} mismatched rows:")
            for mismatch in mismatches[:5]:  # Show first 5
                print(f"  Row {mismatch['row_number']}:")
                print(f"    Source:   {mismatch['source']}")
                print(f"    Restored: {mismatch['restored']}")

            pytest.fail(f"Data mismatch: {len(mismatches)} rows differ")

        print(f"  ✓ All {len(source_data)} rows match exactly")

    def _compare_rows(self, row1: dict, row2: dict) -> bool:
        """
        Compare two rows for equality

        Args:
            row1: First row
            row2: Second row

        Returns:
            True if rows match, False otherwise
        """
        if len(row1) != len(row2):
            return False

        for key in row1:
            if key not in row2:
                return False

            val1 = row1[key]
            val2 = row2[key]

            # Handle None values
            if val1 is None and val2 is None:
                continue

            # Convert timestamps to strings for comparison
            if hasattr(val1, "isoformat"):
                val1 = val1.isoformat()
            if hasattr(val2, "isoformat"):
                val2 = val2.isoformat()

            # Compare values
            if val1 != val2:
                return False

        return True


@pytest.mark.e2e
class TestBackupErrors:
    """Test error handling in backup operations"""

    def test_backup_nonexistent_table(self):
        """Test backing up a table that doesn't exist"""
        backup = IcebergBackup("default", "nonexistent_table", "should_fail")
        result = backup.create_backup()

        # Should fail gracefully
        assert not result, "Backup of nonexistent table should fail"
        print("✓ Properly handles nonexistent table")


@pytest.mark.e2e
class TestRestoreErrors:
    """Test error handling in restore operations"""

    def test_restore_nonexistent_backup(self):
        """Test restoring from a backup that doesn't exist"""
        restore = IcebergRestore(
            backup_name="nonexistent_backup",
            target_database="default",
            target_table="should_fail",
            target_location="s3a://warehouse/default/should_fail",
        )
        result = restore.restore_backup()

        # Should fail gracefully
        assert not result, "Restore of nonexistent backup should fail"
        print("✓ Properly handles nonexistent backup")
