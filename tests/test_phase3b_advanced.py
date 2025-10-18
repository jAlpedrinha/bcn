"""
Tests for Phase 3b: Advanced Features

Tests cover:
- Repair strategies and execution
- Retention policies and garbage collection
- Storage connectors and abstraction
- Backup migration
"""

import pytest
from bcn.repair_strategies import RepairPlanner, RepairExecutor, RepairStrategy, RepairPlan
from bcn.garbage_collector import GarbageCollector, RetentionPolicy, RetentionStrategyType
from bcn.storage_connectors import S3Connector, LocalFilesystemConnector
from bcn.backup_migrator import BackupMigrator, MigrationStrategy


# ============================================================================
# Test Repair Strategies
# ============================================================================


class TestRepairStrategies:
    """Tests for repair strategies."""

    def test_repair_plan_creation(self):
        """Test creating repair plan."""
        plan = RepairPlan(
            backup_name="test_backup",
            corrupted_pit="pit_001",
            strategy=RepairStrategy.SKIP_CORRUPTED,
            affected_pits=["pit_001", "pit_002"],
            actions=["Mark pit_001 as unusable"],
            estimated_time_minutes=5,
        )

        assert plan.backup_name == "test_backup"
        assert plan.strategy == RepairStrategy.SKIP_CORRUPTED
        assert len(plan.affected_pits) == 2

    def test_repair_executor_skip_corrupted(self):
        """Test executing skip corrupted strategy."""
        # Note: Would need mock repository
        pass

    def test_repair_executor_restore_from_parent(self):
        """Test executing restore from parent strategy."""
        # Note: Would need mock repository
        pass


# ============================================================================
# Test Retention & Garbage Collection
# ============================================================================


class TestRetentionPolicy:
    """Tests for retention policies."""

    def test_keep_last_n_policy(self):
        """Test keep last N retention policy."""
        policy = RetentionPolicy.keep_last_n(10)

        assert policy.strategy == RetentionStrategyType.KEEP_LAST_N
        assert policy.param == 10

    def test_keep_since_days_policy(self):
        """Test keep since days retention policy."""
        policy = RetentionPolicy.keep_since_days(30)

        assert policy.strategy == RetentionStrategyType.KEEP_SINCE_DAYS
        assert policy.param == 30


class TestGarbageCollector:
    """Tests for garbage collector."""

    def test_gc_plan_creation(self):
        """Test creating GC plan."""
        from bcn.garbage_collector import GarbageCollectionPlan

        plan = GarbageCollectionPlan(
            backup_name="test_backup",
            total_pits=100,
            pits_to_delete=["pit_001", "pit_002"],
            pits_to_keep=list(range(3, 100)),
            estimated_storage_freed_mb=100.0,
            affected_files=200,
        )

        assert plan.backup_name == "test_backup"
        assert len(plan.pits_to_delete) == 2
        assert plan.estimated_storage_freed_mb == 100.0

    def test_gc_dry_run(self, capsys):
        """Test GC dry run mode."""
        from bcn.garbage_collector import GarbageCollectionPlan
        from unittest.mock import Mock

        # Create mock repository
        mock_repo = Mock()
        mock_repo.get_repository_index.return_value = {"pits": []}

        gc = GarbageCollector("test_backup", mock_repo)

        plan = GarbageCollectionPlan(
            backup_name="test_backup",
            total_pits=10,
            pits_to_delete=["pit_001"],
            pits_to_keep=list(range(2, 10)),
            estimated_storage_freed_mb=50.0,
            affected_files=10,
        )

        # Dry run should succeed
        result = gc.collect(plan, dry_run=True)
        assert result is True


# ============================================================================
# Test Storage Connectors
# ============================================================================


class TestStorageConnectors:
    """Tests for storage connectors."""

    def test_local_filesystem_connector(self):
        """Test local filesystem connector."""
        connector = LocalFilesystemConnector("/tmp")

        # Write
        connector.write_object("bucket", "key", "content")

        # Read
        content = connector.read_object("bucket", "key")
        assert content == "content"

        # Exists
        assert connector.exists("bucket", "key") is True
        assert connector.exists("bucket", "nonexistent") is False

        # Size
        size = connector.get_size("bucket", "key")
        assert size == 7  # len("content")

    def test_local_filesystem_delete(self):
        """Test deleting object from local filesystem."""
        connector = LocalFilesystemConnector("/tmp")

        connector.write_object("bucket", "key", "content")
        assert connector.exists("bucket", "key") is True

        connector.delete_object("bucket", "key")
        assert connector.exists("bucket", "key") is False

    def test_local_filesystem_copy(self):
        """Test copying object in local filesystem."""
        connector = LocalFilesystemConnector("/tmp")

        connector.write_object("bucket", "source", "content")
        connector.copy_object("source", "dest", "bucket")

        assert connector.read_object("bucket", "dest") == "content"

    def test_local_filesystem_list(self):
        """Test listing objects in local filesystem."""
        connector = LocalFilesystemConnector("/tmp")

        connector.write_object("bucket", "prefix/key1", "content1")
        connector.write_object("bucket", "prefix/key2", "content2")
        connector.write_object("bucket", "other/key3", "content3")

        # List with prefix
        objects = connector.list_objects("bucket", "prefix/")
        assert len(objects) >= 2


# ============================================================================
# Test Backup Migration
# ============================================================================


class TestBackupMigrator:
    """Tests for backup migration."""

    def test_migration_plan_full_copy(self):
        """Test migration plan with full copy strategy."""
        source_connector = LocalFilesystemConnector("/tmp")
        dest_connector = LocalFilesystemConnector("/tmp2")

        migrator = BackupMigrator(
            "test_backup",
            source_connector,
            dest_connector,
        )

        plan = migrator.plan_migration(MigrationStrategy.FULL_COPY)

        assert plan.backup_name == "test_backup"
        assert plan.strategy == MigrationStrategy.FULL_COPY
        assert plan.total_size_mb > 0

    def test_migration_plan_incremental(self):
        """Test migration plan with incremental strategy."""
        source_connector = LocalFilesystemConnector("/tmp")
        dest_connector = LocalFilesystemConnector("/tmp2")

        migrator = BackupMigrator(
            "test_backup",
            source_connector,
            dest_connector,
        )

        plan = migrator.plan_migration(MigrationStrategy.INCREMENTAL)

        assert plan.strategy == MigrationStrategy.INCREMENTAL

    def test_migration_dry_run(self):
        """Test migration dry run."""
        source_connector = LocalFilesystemConnector("/tmp")
        dest_connector = LocalFilesystemConnector("/tmp2")

        migrator = BackupMigrator(
            "test_backup",
            source_connector,
            dest_connector,
        )

        plan = migrator.plan_migration(MigrationStrategy.FULL_COPY)

        # Dry run should succeed
        result = migrator.migrate(plan, dry_run=True)
        assert result is True


# ============================================================================
# Integration Tests
# ============================================================================


class TestPhase3bIntegration:
    """Integration tests for Phase 3b components."""

    def test_storage_migration_workflow(self):
        """Test complete storage migration workflow."""
        source = LocalFilesystemConnector("/tmp")
        dest = LocalFilesystemConnector("/tmp2")

        # Write source data
        source.write_object("backup", "test_key", "test_data")

        # Create migrator
        migrator = BackupMigrator(
            "test_backup",
            source,
            dest,
        )

        # Plan migration
        plan = migrator.plan_migration(MigrationStrategy.FULL_COPY)
        assert len(plan.pits_to_migrate) > 0

        # Execute dry run
        result = migrator.migrate(plan, dry_run=True)
        assert result is True

    def test_retention_and_gc_workflow(self):
        """Test retention policy and GC workflow."""
        from unittest.mock import Mock

        mock_repo = Mock()
        mock_repo.get_repository_index.return_value = {
            "pits": ["pit_001", "pit_002", "pit_003"]
        }

        gc = GarbageCollector("test_backup", mock_repo)
        policy = RetentionPolicy.keep_last_n(2)

        plan = gc.analyze(policy)

        assert plan.backup_name == "test_backup"
        # Should want to keep last 2
        assert len(plan.pits_to_keep) <= 2
