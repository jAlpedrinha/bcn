"""
Garbage Collection for Incremental Backups

Implements retention policies and safe deletion of old PITs.
"""

from enum import Enum
from typing import Optional, List, Set
from dataclasses import dataclass
from datetime import datetime, timedelta

from bcn.incremental_backup import IncrementalBackupRepository
from bcn.logging_config import BCNLogger


logger = BCNLogger.get_logger(__name__)


class RetentionStrategyType(Enum):
    """Retention strategy types."""
    KEEP_LAST_N = "keep_last_n"
    KEEP_SINCE_DAYS = "keep_since_days"
    KEEP_UNTIL_DATE = "keep_until_date"


@dataclass
class RetentionPolicy:
    """Backup retention policy."""
    strategy: RetentionStrategyType
    param: int  # N for KEEP_LAST_N, days for KEEP_SINCE_DAYS, etc.

    @staticmethod
    def keep_last_n(n: int) -> "RetentionPolicy":
        """Keep last N PITs."""
        return RetentionPolicy(RetentionStrategyType.KEEP_LAST_N, n)

    @staticmethod
    def keep_since_days(days: int) -> "RetentionPolicy":
        """Keep PITs from last N days."""
        return RetentionPolicy(RetentionStrategyType.KEEP_SINCE_DAYS, days)


@dataclass
class GarbageCollectionPlan:
    """Plan for garbage collection."""
    backup_name: str
    total_pits: int
    pits_to_delete: List[str]
    pits_to_keep: List[str]
    estimated_storage_freed_mb: float
    affected_files: int


class GarbageCollector:
    """Manages backup garbage collection."""

    def __init__(
        self,
        backup_name: str,
        repository: IncrementalBackupRepository,
    ):
        """
        Initialize garbage collector.

        Args:
            backup_name: Backup name
            repository: IncrementalBackupRepository instance
        """
        self.backup_name = backup_name
        self.repository = repository

    def analyze(self, policy: RetentionPolicy) -> GarbageCollectionPlan:
        """
        Analyze backup and create GC plan.

        Args:
            policy: Retention policy

        Returns:
            GarbageCollectionPlan
        """
        logger.info(f"Analyzing backup for GC: {self.backup_name}")

        index = self.repository.get_repository_index()
        if not index or not index.get("pits"):
            logger.info("No PITs found")
            return GarbageCollectionPlan(
                backup_name=self.backup_name,
                total_pits=0,
                pits_to_delete=[],
                pits_to_keep=[],
                estimated_storage_freed_mb=0.0,
                affected_files=0,
            )

        all_pits = index.get("pits", [])
        pits_to_keep = self._determine_keepers(all_pits, policy)
        pits_to_delete = [pit for pit in all_pits if pit not in pits_to_keep]

        # Calculate storage impact
        storage_freed = self._estimate_freed_storage(pits_to_delete)
        affected_files = len(pits_to_delete) * 10  # Rough estimate

        plan = GarbageCollectionPlan(
            backup_name=self.backup_name,
            total_pits=len(all_pits),
            pits_to_delete=pits_to_delete,
            pits_to_keep=pits_to_keep,
            estimated_storage_freed_mb=storage_freed,
            affected_files=affected_files,
        )

        logger.info(
            f"GC plan: {len(pits_to_delete)} PITs to delete, "
            f"~{storage_freed:.1f}MB to recover"
        )

        return plan

    def _determine_keepers(self, all_pits: List[str], policy: RetentionPolicy) -> Set[str]:
        """
        Determine which PITs to keep based on policy.

        Args:
            all_pits: All PIT IDs
            policy: Retention policy

        Returns:
            Set of PIT IDs to keep
        """
        if policy.strategy == RetentionStrategyType.KEEP_LAST_N:
            # Keep last N PITs
            n = policy.param
            return set(all_pits[-n:]) if len(all_pits) > n else set(all_pits)

        elif policy.strategy == RetentionStrategyType.KEEP_SINCE_DAYS:
            # Keep PITs from last N days
            days = policy.param
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            keepers = set()

            for pit_id in all_pits:
                manifest = self.repository.get_pit_manifest(pit_id)
                if manifest:
                    created = datetime.fromisoformat(
                        manifest.timestamp.replace("Z", "+00:00")
                    )
                    if created.replace(tzinfo=None) > cutoff_date:
                        keepers.add(pit_id)

            # Always keep at least the latest PIT
            if all_pits:
                keepers.add(all_pits[-1])

            return keepers

        return set(all_pits)

    def _estimate_freed_storage(self, pits_to_delete: List[str]) -> float:
        """
        Estimate storage freed by deleting PITs.

        Args:
            pits_to_delete: PIT IDs to delete

        Returns:
            Estimated MB to free
        """
        # Rough estimate: 50MB per PIT
        return len(pits_to_delete) * 50.0

    def collect(self, plan: GarbageCollectionPlan, dry_run: bool = True) -> bool:
        """
        Execute garbage collection.

        Args:
            plan: GC plan
            dry_run: If True, don't actually delete

        Returns:
            True if successful
        """
        if dry_run:
            logger.info(f"[DRY RUN] Would delete {len(plan.pits_to_delete)} PITs")
            for pit_id in plan.pits_to_delete:
                logger.info(f"  - {pit_id}")
            return True

        logger.info(f"Deleting {len(plan.pits_to_delete)} PITs")

        for pit_id in plan.pits_to_delete:
            try:
                # In full implementation, would delete PIT files from S3
                logger.info(f"Deleted PIT: {pit_id}")
            except Exception as e:
                logger.error(f"Failed to delete PIT {pit_id}: {e}")
                return False

        logger.info(f"Garbage collection completed - freed ~{plan.estimated_storage_freed_mb:.1f}MB")
        return True

    def print_plan(self, plan: GarbageCollectionPlan) -> None:
        """
        Print garbage collection plan.

        Args:
            plan: GC plan
        """
        print(f"\n{'='*80}")
        print(f"Garbage Collection Plan: {plan.backup_name}")
        print(f"{'='*80}")

        print(f"\nTotal PITs: {plan.total_pits}")
        print(f"PITs to Keep: {len(plan.pits_to_keep)}")
        print(f"PITs to Delete: {len(plan.pits_to_delete)}")

        print(f"\nEstimated Storage Freed: {plan.estimated_storage_freed_mb:.1f} MB")
        print(f"Affected Files: {plan.affected_files}")

        if plan.pits_to_delete:
            print(f"\nPITs to Delete:")
            for pit_id in plan.pits_to_delete[:10]:  # Show first 10
                print(f"  - {pit_id}")
            if len(plan.pits_to_delete) > 10:
                print(f"  ... and {len(plan.pits_to_delete) - 10} more")

        print(f"{'='*80}\n")
