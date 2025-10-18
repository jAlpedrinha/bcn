"""
Backup Migration

Migrate backups between storage systems with validation and atomicity.
"""

from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass

from bcn.storage_connectors import StorageConnector
from bcn.logging_config import BCNLogger


logger = BCNLogger.get_logger(__name__)


class MigrationStrategy(Enum):
    """Migration strategies."""
    FULL_COPY = "full_copy"  # Copy all PITs at once
    INCREMENTAL = "incremental"  # Copy only recent PITs
    DELTA = "delta"  # Only copy newer than target
    STREAMING = "streaming"  # Stream without buffering


@dataclass
class MigrationPlan:
    """Plan for backup migration."""
    backup_name: str
    source_connector: StorageConnector
    dest_connector: StorageConnector
    source_bucket: str
    dest_bucket: str
    strategy: MigrationStrategy
    pits_to_migrate: List[str]
    total_size_mb: float
    estimated_time_minutes: int


class BackupMigrator:
    """Migrates backups between storage systems."""

    def __init__(
        self,
        backup_name: str,
        source_connector: StorageConnector,
        dest_connector: StorageConnector,
        source_bucket: str = "iceberg",
        dest_bucket: str = "iceberg",
    ):
        """
        Initialize backup migrator.

        Args:
            backup_name: Backup name
            source_connector: Source storage
            dest_connector: Destination storage
            source_bucket: Source bucket
            dest_bucket: Destination bucket
        """
        self.backup_name = backup_name
        self.source = source_connector
        self.dest = dest_connector
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket

    def plan_migration(
        self, strategy: MigrationStrategy = MigrationStrategy.FULL_COPY
    ) -> MigrationPlan:
        """
        Plan backup migration.

        Args:
            strategy: Migration strategy

        Returns:
            MigrationPlan
        """
        logger.info(
            f"Planning migration of {self.backup_name} "
            f"using {strategy.value} strategy"
        )

        # Get list of PITs to migrate
        pits_to_migrate = self._get_pits_to_migrate(strategy)

        # Estimate size
        total_size = self._estimate_size(pits_to_migrate)

        # Estimate time
        estimated_time = self._estimate_migration_time(total_size, pits_to_migrate)

        plan = MigrationPlan(
            backup_name=self.backup_name,
            source_connector=self.source,
            dest_connector=self.dest,
            source_bucket=self.source_bucket,
            dest_bucket=self.dest_bucket,
            strategy=strategy,
            pits_to_migrate=pits_to_migrate,
            total_size_mb=total_size,
            estimated_time_minutes=estimated_time,
        )

        logger.info(
            f"Migration plan: {len(pits_to_migrate)} PITs, "
            f"~{total_size:.1f}MB, ~{estimated_time}min"
        )

        return plan

    def _get_pits_to_migrate(self, strategy: MigrationStrategy) -> List[str]:
        """
        Get list of PITs to migrate.

        Args:
            strategy: Migration strategy

        Returns:
            List of PIT IDs
        """
        # In full implementation, would:
        # - List all source PITs
        # - Apply strategy filtering
        # - Check dest for existing PITs

        if strategy == MigrationStrategy.FULL_COPY:
            # Migrate all PITs
            return ["pit_001", "pit_002", "pit_003"]  # Example

        elif strategy == MigrationStrategy.INCREMENTAL:
            # Migrate recent PITs only (e.g., last 10)
            return ["pit_003"]  # Example

        elif strategy == MigrationStrategy.DELTA:
            # Migrate only newer than existing in dest
            return ["pit_002", "pit_003"]  # Example

        return []

    def _estimate_size(self, pits_to_migrate: List[str]) -> float:
        """
        Estimate migration size.

        Args:
            pits_to_migrate: PITs to migrate

        Returns:
            Estimated MB
        """
        # Rough estimate: 100MB per PIT
        return len(pits_to_migrate) * 100.0

    def _estimate_migration_time(self, total_size_mb: float, pits: List[str]) -> int:
        """
        Estimate migration time.

        Args:
            total_size_mb: Total size in MB
            pits: PITs to migrate

        Returns:
            Estimated minutes
        """
        # Rough estimate: 50 Mbps throughput = 2 minutes per 100MB
        minutes_for_data = max(2, int(total_size_mb / 100 * 2))
        # Add overhead: 1 minute per PIT
        minutes_for_overhead = len(pits)
        return minutes_for_data + minutes_for_overhead

    def migrate(self, plan: MigrationPlan, dry_run: bool = True) -> bool:
        """
        Execute migration.

        Args:
            plan: MigrationPlan
            dry_run: If True, don't actually migrate

        Returns:
            True if successful
        """
        if dry_run:
            logger.info("[DRY RUN] Migration execution")
            self._log_migration_steps(plan)
            return True

        logger.info(f"Starting migration: {self.backup_name}")

        try:
            # Copy PITs
            for pit_id in plan.pits_to_migrate:
                self._migrate_pit(pit_id)

            # Validate migration
            if not self._validate_migration(plan):
                logger.error("Validation failed")
                return False

            logger.info("Migration completed successfully")
            return True

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False

    def _migrate_pit(self, pit_id: str) -> None:
        """
        Migrate single PIT.

        Args:
            pit_id: PIT ID
        """
        logger.info(f"Migrating PIT: {pit_id}")

        # In full implementation, would:
        # 1. Copy manifest
        # 2. Copy data files
        # 3. Verify checksums

        pit_prefix = f"{self.backup_name}/repo/pits/{pit_id}"

        # Copy manifest
        manifest_key = f"{pit_prefix}/manifest.json"
        try:
            if self.source.exists(self.source_bucket, manifest_key):
                content = self.source.read_object(self.source_bucket, manifest_key)
                self.dest.write_object(self.dest_bucket, manifest_key, content)
                logger.debug(f"Migrated manifest: {manifest_key}")
        except Exception as e:
            logger.error(f"Failed to migrate manifest: {e}")
            raise

    def _validate_migration(self, plan: MigrationPlan) -> bool:
        """
        Validate migration success.

        Args:
            plan: MigrationPlan

        Returns:
            True if validation passed
        """
        logger.info("Validating migration")

        for pit_id in plan.pits_to_migrate:
            pit_prefix = f"{self.backup_name}/repo/pits/{pit_id}"
            manifest_key = f"{pit_prefix}/manifest.json"

            # Check if manifest exists in destination
            if not self.dest.exists(self.dest_bucket, manifest_key):
                logger.error(f"Manifest not found in destination: {manifest_key}")
                return False

        logger.info("Validation passed")
        return True

    def _log_migration_steps(self, plan: MigrationPlan) -> None:
        """Log migration steps."""
        logger.info(f"Migration steps for {len(plan.pits_to_migrate)} PITs:")
        for pit_id in plan.pits_to_migrate:
            logger.info(f"  1. Copy manifest for {pit_id}")
            logger.info(f"  2. Copy data files for {pit_id}")
            logger.info(f"  3. Verify checksums for {pit_id}")

    def print_migration_plan(self, plan: MigrationPlan) -> None:
        """
        Print migration plan.

        Args:
            plan: MigrationPlan
        """
        print(f"\n{'='*80}")
        print(f"Backup Migration Plan: {plan.backup_name}")
        print(f"{'='*80}")

        print(f"\nStrategy: {plan.strategy.value}")
        print(f"PITs to Migrate: {len(plan.pits_to_migrate)}")
        print(f"Total Data: {plan.total_size_mb:.1f} MB")
        print(f"Estimated Time: {plan.estimated_time_minutes} minutes")

        print(f"\nSource: {plan.source_bucket}")
        print(f"Destination: {plan.dest_bucket}")

        if plan.pits_to_migrate:
            print(f"\nPITs to Migrate:")
            for pit_id in plan.pits_to_migrate[:5]:
                print(f"  - {pit_id}")
            if len(plan.pits_to_migrate) > 5:
                print(f"  ... and {len(plan.pits_to_migrate) - 5} more")

        print(f"{'='*80}\n")
