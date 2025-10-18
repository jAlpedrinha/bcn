"""
Unified CLI for BCN (Backup and Restore for Iceberg)

Provides unified interface for all backup/restore operations with subcommands:
- bcn backup: Create full or incremental backups
- bcn restore: Restore from any PIT
- bcn describe: Show backup and PIT details
- bcn list: List PITs and files
"""

import argparse
import sys
from typing import Optional, List
from pathlib import Path

from bcn.backup import IcebergBackup as OldIcebergBackup
from bcn.restore import IcebergRestore as OldIcebergRestore
from bcn.incremental_backup import IncrementalBackup, IncrementalBackupRepository
from bcn.incremental_restore import IncrementalRestore
from bcn.s3_client import S3Client
from bcn.logging_config import BCNLogger
from bcn.config import Config


logger = BCNLogger.get_logger(__name__)


class BCNCommand:
    """Base class for BCN commands."""

    def __init__(self, args):
        """Initialize command with parsed arguments."""
        self.args = args
        self.s3_client = S3Client()
        self.backup_bucket = Config.BACKUP_BUCKET

    def execute(self):
        """Execute the command. Override in subclasses."""
        raise NotImplementedError


class BackupCommand(BCNCommand):
    """Handle backup operations."""

    def execute(self):
        """Execute backup command."""
        logger.info(f"Starting backup: {self.args.backup_name}")

        try:
            if self.args.incremental:
                logger.info("Incremental backup mode")
                self._incremental_backup()
            else:
                logger.info("Full backup mode")
                self._full_backup()

            logger.info("Backup completed successfully")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise

    def _full_backup(self):
        """Execute full backup using legacy implementation."""
        backup = OldIcebergBackup(
            database=self.args.database,
            table=self.args.table,
            backup_name=self.args.backup_name,
            catalog=self.args.catalog,
        )

        backup.create_backup()

    def _incremental_backup(self):
        """Execute incremental backup."""
        backup = IncrementalBackup(
            database=self.args.database,
            table=self.args.table,
            backup_name=self.args.backup_name,
            catalog=self.args.catalog,
            s3_client=self.s3_client,
            backup_bucket=self.backup_bucket,
        )

        pit_id = backup.create_incremental_backup()
        logger.info(f"Created PIT: {pit_id}")


class RestoreCommand(BCNCommand):
    """Handle restore operations."""

    def execute(self):
        """Execute restore command."""
        logger.info(
            f"Starting restore: {self.args.backup_name} -> "
            f"{self.args.target_database}.{self.args.target_table}"
        )

        try:
            # Determine target location
            if self.args.target_location:
                target_location = self.args.target_location
            else:
                # Default location: s3a://warehouse/{database}/{table}
                target_location = (
                    f"s3a://{Config.WAREHOUSE_BUCKET}/"
                    f"{self.args.target_database}/{self.args.target_table}"
                )

            # Check if incremental restore
            if self._is_incremental_backup():
                logger.info("Incremental restore mode")
                self._incremental_restore(target_location)
            else:
                logger.info("Full backup restore mode")
                self._full_restore(target_location)

            logger.info("Restore completed successfully")
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            raise

    def _is_incremental_backup(self) -> bool:
        """Check if backup is incremental by checking for index.json."""
        repository = IncrementalBackupRepository(
            self.args.backup_name, self.s3_client, self.backup_bucket
        )
        index = repository.get_repository_index()
        return index is not None

    def _full_restore(self, target_location: str):
        """Execute full restore using legacy implementation."""
        restore = OldIcebergRestore(
            backup_name=self.args.backup_name,
            target_database=self.args.target_database,
            target_table=self.args.target_table,
            target_location=target_location,
            catalog=self.args.catalog,
        )

        restore.restore_backup()

    def _incremental_restore(self, target_location: str):
        """Execute incremental restore."""
        # List available PITs if interactive mode (no PIT specified and terminal)
        if not self.args.pit and sys.stdin.isatty():
            self.args.pit = self._select_pit_interactive()

        restore = IncrementalRestore(
            backup_name=self.args.backup_name,
            target_database=self.args.target_database,
            target_table=self.args.target_table,
            target_location=target_location,
            catalog=self.args.catalog,
            s3_client=self.s3_client,
            backup_bucket=self.backup_bucket,
        )

        restore.restore_from_pit(self.args.pit)

    def _select_pit_interactive(self) -> Optional[str]:
        """Interactively select a PIT for restore."""
        restore = IncrementalRestore(
            backup_name=self.args.backup_name,
            target_database="temp",  # Not used for listing
            target_table="temp",  # Not used for listing
            target_location="temp",  # Not used for listing
            s3_client=self.s3_client,
            backup_bucket=self.backup_bucket,
        )

        pits = restore.list_pits()

        if not pits:
            logger.warning("No PITs found in backup")
            return None

        logger.info("\nAvailable PITs:")
        for i, pit_info in enumerate(pits, 1):
            print(
                f"  {i}. PIT {pit_info['pit_id']} "
                f"({pit_info['timestamp']}) "
                f"- {pit_info['added_files_count']} added, "
                f"{pit_info['modified_files_count']} modified, "
                f"{pit_info['deleted_files_count']} deleted"
            )

        print()
        while True:
            try:
                choice = input(f"Select PIT (1-{len(pits)}) or 'latest' [latest]: ").strip()
                if not choice or choice.lower() == "latest":
                    return pits[-1]["pit_id"]
                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(pits):
                    return pits[choice_idx]["pit_id"]
                else:
                    print(f"Invalid choice. Please enter 1-{len(pits)}")
            except ValueError:
                print(f"Invalid input. Please enter a number or 'latest'")


class DescribeCommand(BCNCommand):
    """Handle describe operations."""

    def execute(self):
        """Execute describe command."""
        logger.info(f"Describing backup: {self.args.backup_name}")

        try:
            restore = IncrementalRestore(
                backup_name=self.args.backup_name,
                target_database="temp",  # Not used
                target_table="temp",  # Not used
                target_location="temp",  # Not used
                s3_client=self.s3_client,
                backup_bucket=self.backup_bucket,
            )

            # Get all PITs
            pits = restore.list_pits()

            if not pits:
                print(f"No PITs found in backup '{self.args.backup_name}'")
                return

            # Print summary
            print(f"\nBackup: {self.args.backup_name}")
            print(f"Total PITs: {len(pits)}")
            print(f"Latest PIT: {pits[-1]['pit_id']}")
            print()

            # Print verbose output if requested
            if self.args.verbose:
                print("Detailed PIT Information:")
                print("-" * 80)
                for pit_info in pits:
                    desc = restore.describe_pit(pit_info["pit_id"], verbose=True)
                    self._print_pit_description(desc)
                    print()
            else:
                # Print simple output
                print(
                    f"{'PIT ID':<40} {'Timestamp':<30} {'Parent':<40} "
                    f"{'Files (A/M/D)':<15}"
                )
                print("-" * 125)
                for pit_info in pits:
                    parent = pit_info.get("parent_pit_id", "root")[:8] + "..."
                    if not parent or parent == "None":
                        parent = "(root)"
                    print(
                        f"{pit_info['pit_id']:<40} "
                        f"{pit_info['timestamp']:<30} "
                        f"{parent:<40} "
                        f"{pit_info['added_files_count']}/{pit_info['modified_files_count']}/"
                        f"{pit_info['deleted_files_count']:<6}"
                    )

        except Exception as e:
            logger.error(f"Describe failed: {e}")
            raise

    def _print_pit_description(self, desc: dict):
        """Print detailed PIT description."""
        print(f"  PIT ID: {desc['pit_id']}")
        print(f"  Timestamp: {desc['timestamp']}")
        print(f"  Parent: {desc.get('parent_pit_id', '(root)')}")
        print(f"  Added Files: {desc['added_files_count']}")
        print(f"  Modified Files: {desc['modified_files_count']}")
        print(f"  Deleted Files: {desc['deleted_files_count']}")
        print(f"  Total Data Volume: {desc['total_data_volume']:,} bytes")

        if "pit_chain_length" in desc:
            print(f"  PIT Chain Length: {desc['pit_chain_length']}")
            print(f"  Accumulated Files: {desc['accumulated_files_count']}")
            print(f"  Schema Fields: {desc['schema_fields']}")


class ListCommand(BCNCommand):
    """Handle list operations."""

    def execute(self):
        """Execute list command."""
        logger.info(f"Listing PITs in backup: {self.args.backup_name}")

        try:
            restore = IncrementalRestore(
                backup_name=self.args.backup_name,
                target_database="temp",  # Not used
                target_table="temp",  # Not used
                target_location="temp",  # Not used
                s3_client=self.s3_client,
                backup_bucket=self.backup_bucket,
            )

            if self.args.pit:
                # List files in specific PIT
                self._list_pit_files(restore)
            else:
                # List all PITs
                self._list_pits(restore)

        except Exception as e:
            logger.error(f"List failed: {e}")
            raise

    def _list_pits(self, restore: IncrementalRestore):
        """List all PITs in backup."""
        pits = restore.list_pits()

        if not pits:
            print(f"No PITs found in backup '{self.args.backup_name}'")
            return

        print(f"\nPITs in backup '{self.args.backup_name}':")
        print(
            f"{'#':<3} {'PIT ID':<40} {'Timestamp':<30} "
            f"{'Added':<8} {'Mod':<8} {'Del':<8}"
        )
        print("-" * 97)

        for i, pit_info in enumerate(pits, 1):
            print(
                f"{i:<3} {pit_info['pit_id']:<40} {pit_info['timestamp']:<30} "
                f"{pit_info['added_files_count']:<8} {pit_info['modified_files_count']:<8} "
                f"{pit_info['deleted_files_count']:<8}"
            )

    def _list_pit_files(self, restore: IncrementalRestore):
        """List files in specific PIT."""
        desc = restore.describe_pit(self.args.pit, verbose=True)

        print(f"\nFiles in PIT {self.args.pit}:")
        print()

        if desc.get("added_files"):
            print("Added Files:")
            for f in desc["added_files"]:
                print(f"  {f['path']:<50} {f['size']:>12,} bytes")

        if desc.get("modified_files"):
            print("\nModified Files:")
            for f in desc["modified_files"]:
                print(f"  {f['path']:<50} {f['size']:>12,} bytes")


class BCNCli:
    """Main CLI entry point."""

    def __init__(self):
        """Initialize CLI."""
        self.parser = self._create_parser()

    def _create_parser(self) -> argparse.ArgumentParser:
        """Create argument parser with subcommands."""
        parser = argparse.ArgumentParser(
            prog="bcn",
            description="BCN - Backup and Restore for Apache Iceberg Tables",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Create full backup
  bcn backup --database default --table my_table --backup-name my_backup

  # Create incremental backup
  bcn backup --database default --table my_table --backup-name my_backup --incremental

  # Restore from latest PIT
  bcn restore --backup-name my_backup --target-database default --target-table my_table_restored

  # Restore from specific PIT
  bcn restore --backup-name my_backup --target-database default --target-table my_table_restored --pit pit_001_2025-01-01T10:00Z

  # Describe backup
  bcn describe --backup-name my_backup --verbose

  # List PITs
  bcn list --backup-name my_backup

  # List files in PIT
  bcn list --backup-name my_backup --pit pit_001_2025-01-01T10:00Z
            """,
        )

        parser.add_argument(
            "--log-level",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            default="INFO",
            help="Logging level (default: INFO)",
        )

        parser.add_argument(
            "--log-file",
            help="Log file path (if not specified, logs only to console)",
        )

        subparsers = parser.add_subparsers(dest="command", help="Available commands")

        # Backup command
        backup_parser = subparsers.add_parser("backup", help="Create backup")
        backup_parser.add_argument("--database", required=True, help="Database name")
        backup_parser.add_argument("--table", required=True, help="Table name")
        backup_parser.add_argument(
            "--backup-name", required=True, help="Backup identifier"
        )
        backup_parser.add_argument(
            "--catalog",
            help="Catalog name (default from config)",
        )
        backup_parser.add_argument(
            "--incremental",
            action="store_true",
            help="Create incremental backup (adds to existing repository)",
        )

        # Restore command
        restore_parser = subparsers.add_parser("restore", help="Restore from backup")
        restore_parser.add_argument(
            "--backup-name", required=True, help="Backup identifier"
        )
        restore_parser.add_argument(
            "--target-database", required=True, help="Target database name"
        )
        restore_parser.add_argument(
            "--target-table", required=True, help="Target table name"
        )
        restore_parser.add_argument(
            "--target-location",
            help="Target table S3 location (optional, default: s3://warehouse/{database}/{table})",
        )
        restore_parser.add_argument(
            "--pit",
            help="Restore from specific PIT (if not specified, uses latest or prompts)",
        )
        restore_parser.add_argument(
            "--catalog",
            help="Catalog name (default from config)",
        )

        # Describe command
        describe_parser = subparsers.add_parser("describe", help="Show backup details")
        describe_parser.add_argument(
            "--backup-name", required=True, help="Backup identifier"
        )
        describe_parser.add_argument(
            "--verbose",
            action="store_true",
            help="Show detailed information",
        )

        # List command
        list_parser = subparsers.add_parser("list", help="List PITs or files")
        list_parser.add_argument(
            "--backup-name", required=True, help="Backup identifier"
        )
        list_parser.add_argument(
            "--pit",
            help="If specified, list files in this PIT instead of listing all PITs",
        )

        return parser

    def run(self, argv: Optional[List[str]] = None):
        """Run CLI."""
        args = self.parser.parse_args(argv)

        # Setup logging
        BCNLogger.setup_logging(level=args.log_level, log_file=args.log_file)

        # Execute command
        if not args.command:
            self.parser.print_help()
            return 1

        try:
            command_map = {
                "backup": BackupCommand,
                "restore": RestoreCommand,
                "describe": DescribeCommand,
                "list": ListCommand,
            }

            command_class = command_map.get(args.command)
            if not command_class:
                logger.error(f"Unknown command: {args.command}")
                return 1

            command = command_class(args)
            command.execute()
            return 0

        except Exception as e:
            logger.error(f"Command failed: {e}", exc_info=True)
            return 1


def main(argv: Optional[List[str]] = None):
    """Main entry point."""
    cli = BCNCli()
    return cli.run(argv)


if __name__ == "__main__":
    sys.exit(main())
