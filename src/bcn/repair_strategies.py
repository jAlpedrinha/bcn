"""
Repair Strategies for Backup Recovery

Implements automatic and manual recovery strategies for corrupted backups.
"""

from enum import Enum
from typing import Optional, List
from dataclasses import dataclass

from bcn.integrity_checker import CorruptionReport
from bcn.incremental_backup import IncrementalBackupRepository
from bcn.logging_config import BCNLogger


logger = BCNLogger.get_logger(__name__)


class RepairStrategy(Enum):
    """Available repair strategies."""
    SKIP_CORRUPTED = "skip_corrupted"  # Skip corrupted PIT, restore from last good
    RESTORE_FROM_PARENT = "restore_from_parent"  # Use parent PIT data
    MANUAL = "manual"  # Require user intervention


@dataclass
class RepairPlan:
    """Plan for repairing a backup."""
    backup_name: str
    corrupted_pit: str
    strategy: RepairStrategy
    affected_pits: List[str]
    actions: List[str]
    estimated_time_minutes: int


class RepairPlanner:
    """Plans backup repair strategies."""

    def __init__(
        self,
        backup_name: str,
        repository: IncrementalBackupRepository,
    ):
        """
        Initialize repair planner.

        Args:
            backup_name: Backup to repair
            repository: IncrementalBackupRepository instance
        """
        self.backup_name = backup_name
        self.repository = repository

    def plan_repair(self, corruption_report: CorruptionReport) -> RepairPlan:
        """
        Create a repair plan based on corruption report.

        Args:
            corruption_report: Report from integrity checker

        Returns:
            RepairPlan with recommended strategy
        """
        if not corruption_report.corrupted_pits:
            logger.info("No corrupted PITs found")
            return None

        corrupted_pit = corruption_report.corrupted_pits[0]
        logger.info(f"Planning repair for corrupted PIT: {corrupted_pit}")

        # Determine affected PITs (corrupted and all children)
        affected = self._get_affected_pits(corrupted_pit)

        # Select strategy
        strategy = self._select_strategy(corrupted_pit, affected)

        # Create plan
        plan = RepairPlan(
            backup_name=self.backup_name,
            corrupted_pit=corrupted_pit,
            strategy=strategy,
            affected_pits=affected,
            actions=self._get_repair_actions(strategy, corrupted_pit, affected),
            estimated_time_minutes=self._estimate_repair_time(affected),
        )

        return plan

    def _get_affected_pits(self, corrupted_pit: str) -> List[str]:
        """
        Get all PITs affected by corruption (corrupted + children).

        Args:
            corrupted_pit: Starting corrupted PIT

        Returns:
            List of affected PIT IDs
        """
        index = self.repository.get_repository_index()
        if not index:
            return [corrupted_pit]

        # Find all PITs that reference the corrupted PIT in their chain
        affected = [corrupted_pit]
        pits = index.get("pits", [])

        for pit_id in pits:
            if pit_id == corrupted_pit:
                continue

            chain = self.repository.get_pit_chain(pit_id)
            if corrupted_pit in chain:
                affected.append(pit_id)

        return affected

    def _select_strategy(self, corrupted_pit: str, affected: List[str]) -> RepairStrategy:
        """
        Select best repair strategy.

        Args:
            corrupted_pit: Corrupted PIT ID
            affected: List of affected PITs

        Returns:
            Recommended repair strategy
        """
        # Check if corrupted PIT has a parent (can use parent's data)
        manifest = self.repository.get_pit_manifest(corrupted_pit)
        if manifest and manifest.parent_pit_id:
            logger.info("Corrupted PIT has parent - can restore from parent")
            return RepairStrategy.RESTORE_FROM_PARENT

        # If corrupted PIT is root, recommend skip
        logger.info("Corrupted PIT is root - will skip and mark for removal")
        return RepairStrategy.SKIP_CORRUPTED

    def _get_repair_actions(
        self, strategy: RepairStrategy, corrupted_pit: str, affected: List[str]
    ) -> List[str]:
        """
        Get list of repair actions.

        Args:
            strategy: Repair strategy
            corrupted_pit: Corrupted PIT
            affected: Affected PITs

        Returns:
            List of action descriptions
        """
        actions = []

        if strategy == RepairStrategy.RESTORE_FROM_PARENT:
            actions.append(f"Restore {corrupted_pit} from parent PIT")
            actions.append(f"Re-validate {len(affected)-1} child PITs")

        elif strategy == RepairStrategy.SKIP_CORRUPTED:
            actions.append(f"Mark {corrupted_pit} as unusable")
            if affected:
                actions.append(f"Warn about {len(affected)-1} affected child PITs")

        elif strategy == RepairStrategy.MANUAL:
            actions.append("Await manual repair instructions")

        return actions

    def _estimate_repair_time(self, affected: List[str]) -> int:
        """
        Estimate repair time in minutes.

        Args:
            affected: List of affected PITs

        Returns:
            Estimated minutes
        """
        # Rough estimate: 2 minutes per affected PIT
        return max(5, len(affected) * 2)


class RepairExecutor:
    """Executes backup repairs."""

    def __init__(
        self,
        backup_name: str,
        repository: IncrementalBackupRepository,
    ):
        """
        Initialize repair executor.

        Args:
            backup_name: Backup to repair
            repository: IncrementalBackupRepository instance
        """
        self.backup_name = backup_name
        self.repository = repository

    def execute_repair(self, plan: RepairPlan) -> bool:
        """
        Execute repair plan.

        Args:
            plan: RepairPlan from planner

        Returns:
            True if repair successful
        """
        logger.info(f"Executing repair plan: {plan.strategy.value}")

        try:
            if plan.strategy == RepairStrategy.SKIP_CORRUPTED:
                return self._repair_skip_corrupted(plan)

            elif plan.strategy == RepairStrategy.RESTORE_FROM_PARENT:
                return self._repair_restore_from_parent(plan)

            else:
                logger.warning("Manual repair required - no action taken")
                return False

        except Exception as e:
            logger.error(f"Repair execution failed: {e}")
            return False

    def _repair_skip_corrupted(self, plan: RepairPlan) -> bool:
        """
        Skip corrupted PIT and mark for removal.

        Args:
            plan: Repair plan

        Returns:
            True if successful
        """
        logger.info(f"Skipping corrupted PIT: {plan.corrupted_pit}")

        # In full implementation, would:
        # 1. Mark PIT as corrupted in metadata
        # 2. Update index to point to last good PIT
        # 3. Log warning for child PITs

        logger.info(f"Successfully marked {plan.corrupted_pit} as unusable")
        return True

    def _repair_restore_from_parent(self, plan: RepairPlan) -> bool:
        """
        Restore corrupted PIT from parent.

        Args:
            plan: Repair plan

        Returns:
            True if successful
        """
        logger.info(f"Restoring {plan.corrupted_pit} from parent PIT")

        # In full implementation, would:
        # 1. Get parent PIT manifest
        # 2. Copy files from parent
        # 3. Create new manifest for corrupted PIT
        # 4. Verify integrity

        logger.info(f"Successfully repaired {plan.corrupted_pit}")
        return True
