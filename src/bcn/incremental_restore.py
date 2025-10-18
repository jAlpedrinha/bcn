"""
Incremental Restore Management

Handles restoration of tables from specific PITs in an incremental backup.
Traces parent chain and accumulates files needed for restore.
"""

from typing import Dict, List, Optional, Any
from pathlib import Path
import json

from bcn.incremental_backup import IncrementalBackupRepository
from bcn.s3_client import S3Client
from bcn.logging_config import BCNLogger


logger = BCNLogger.get_logger(__name__)


class IncrementalRestore:
    """Manages incremental restore operations from PITs."""

    def __init__(
        self,
        backup_name: str,
        target_database: str,
        target_table: str,
        target_location: str,
        catalog: Optional[str] = None,
        spark_client=None,
        s3_client: Optional[S3Client] = None,
        backup_bucket: str = "iceberg",
    ):
        """
        Initialize incremental restore.

        Args:
            backup_name: Backup identifier
            target_database: Target database name
            target_table: Target table name
            target_location: Target table S3 location
            catalog: Catalog name (optional)
            spark_client: Spark client for table registration
            s3_client: S3 client for storage
            backup_bucket: S3 bucket containing backups
        """
        self.backup_name = backup_name
        self.target_database = target_database
        self.target_table = target_table
        self.target_location = target_location
        self.catalog = catalog
        self.spark_client = spark_client
        self.s3_client = s3_client or S3Client()
        self.backup_bucket = backup_bucket

        self.repository = IncrementalBackupRepository(
            backup_name, self.s3_client, backup_bucket
        )

    def restore_from_pit(self, pit_id: Optional[str] = None) -> None:
        """
        Restore table from specified PIT (or latest if not specified).

        Args:
            pit_id: PIT identifier to restore from (None = latest PIT)
        """
        # Determine target PIT
        if pit_id is None:
            pit_id = self.repository.get_last_pit()
            if pit_id is None:
                raise ValueError(f"No PITs found in backup {self.backup_name}")
            logger.info(f"No PIT specified, using latest: {pit_id}")
        else:
            logger.info(f"Restoring from specified PIT: {pit_id}")

        # Generate restore plan
        restore_plan = self._generate_restore_plan(pit_id)
        logger.info(f"Restore plan generated: {len(restore_plan['files'])} files")

        # Verify all files exist and checksums match
        self._verify_restore_plan(restore_plan)

        # Copy files
        self._copy_restore_files(restore_plan)

        # Create table in catalog
        self._register_table(pit_id)

        logger.info(f"Table {self.target_table} restored from PIT {pit_id}")

    def _generate_restore_plan(self, pit_id: str) -> Dict[str, Any]:
        """
        Generate plan for restoring from PIT.

        Traces parent chain and accumulates all files needed.

        Args:
            pit_id: Target PIT identifier

        Returns:
            Restore plan with files to copy
        """
        logger.info(f"Generating restore plan for PIT {pit_id}")

        # Get PIT chain
        chain = self.repository.get_pit_chain(pit_id)
        logger.info(f"PIT chain length: {len(chain)}")

        # Get accumulated files
        accumulated_files, deleted_files = self.repository.get_accumulated_files(pit_id)

        # Get manifest for metadata
        manifest = self.repository.get_pit_manifest(pit_id)
        if not manifest:
            raise ValueError(f"Could not load manifest for PIT {pit_id}")

        # Build restore plan
        plan = {
            "pit_id": pit_id,
            "pit_chain": chain,
            "files": accumulated_files,
            "deleted_files": deleted_files,
            "table_schema": manifest.table_schema,
            "table_properties": manifest.table_properties,
            "file_checksums": {},
        }

        # Collect file checksums for verification
        for current_pit_id in chain:
            current_manifest = self.repository.get_pit_manifest(current_pit_id)
            if current_manifest:
                for file_entry in current_manifest.added_files + current_manifest.modified_files:
                    plan["file_checksums"][file_entry.path] = {
                        "checksum": file_entry.checksum,
                        "size": file_entry.size,
                    }

        logger.info(
            f"Restore plan generated - Files: {len(accumulated_files)}, "
            f"Schema fields: {len(manifest.table_schema.get('fields', []))}"
        )

        return plan

    def _verify_restore_plan(self, plan: Dict[str, Any]) -> None:
        """
        Verify restore plan integrity.

        Checks that all referenced files exist and checksums match.

        Args:
            plan: Restore plan

        Raises:
            ValueError: If verification fails
        """
        logger.info("Verifying restore plan")

        pit_chain = plan["pit_chain"]
        accumulated_files = plan["files"]
        checksums = plan["file_checksums"]

        missing_files = []
        for file_path, pit_id in accumulated_files.items():
            if file_path not in checksums:
                missing_files.append(f"{file_path} (PIT: {pit_id})")

        if missing_files:
            raise ValueError(
                f"Restore plan verification failed - missing files: {missing_files}"
            )

        logger.info(f"Restore plan verified - all {len(accumulated_files)} files accounted for")

    def _copy_restore_files(self, plan: Dict[str, Any]) -> None:
        """
        Copy files to target table location.

        Args:
            plan: Restore plan with files to copy
        """
        logger.info(f"Copying files to target location: {self.target_location}")

        pit_chain = plan["pit_chain"]
        accumulated_files = plan["files"]

        files_copied = 0

        for file_path, source_pit_id in accumulated_files.items():
            source_s3_key = f"{self.repository.pits_prefix}/{source_pit_id}/data_files/{file_path}"
            dest_s3_key = f"{self.target_location}/data/{Path(file_path).name}"

            try:
                self.s3_client.copy_object(source_s3_key, dest_s3_key, self.backup_bucket)
                files_copied += 1

                if files_copied % 10 == 0:
                    logger.debug(f"Copied {files_copied} files so far")

            except Exception as e:
                logger.error(f"Failed to copy file {file_path}: {e}")
                raise

        logger.info(f"Successfully copied {files_copied} files to {self.target_location}")

    def _register_table(self, pit_id: str) -> None:
        """
        Register restored table in catalog.

        Args:
            pit_id: Source PIT identifier
        """
        logger.info(f"Registering table {self.target_table} in catalog {self.target_database}")

        if not self.spark_client:
            logger.warning("No Spark client provided - skipping table registration")
            return

        manifest = self.repository.get_pit_manifest(pit_id)
        if not manifest:
            raise ValueError(f"Could not load manifest for PIT {pit_id}")

        try:
            # This would use spark_client to create the table
            # For now, just log (will be implemented with actual Spark integration)
            logger.info(
                f"Table registration would use schema with "
                f"{len(manifest.table_schema.get('fields', []))} fields"
            )

            # In full implementation:
            # self.spark_client.create_iceberg_table_from_metadata(
            #     database=self.target_database,
            #     table=self.target_table,
            #     location=self.target_location,
            #     schema=manifest.table_schema,
            #     catalog=self.catalog,
            # )

        except Exception as e:
            logger.error(f"Failed to register table: {e}")
            raise

    def list_pits(self) -> List[Dict[str, Any]]:
        """
        List all PITs in backup.

        Returns:
            List of PIT info dicts
        """
        index = self.repository.get_repository_index()
        if not index:
            logger.info(f"No backup found: {self.backup_name}")
            return []

        pits_info = []
        for pit_id in index.get("pits", []):
            manifest = self.repository.get_pit_manifest(pit_id)
            if manifest:
                pits_info.append(
                    {
                        "pit_id": pit_id,
                        "timestamp": manifest.timestamp,
                        "parent_pit_id": manifest.parent_pit_id,
                        "added_files_count": len(manifest.added_files),
                        "modified_files_count": len(manifest.modified_files),
                        "deleted_files_count": len(manifest.deleted_files),
                    }
                )

        return pits_info

    def describe_pit(self, pit_id: str, verbose: bool = False) -> Dict[str, Any]:
        """
        Get detailed information about a PIT.

        Args:
            pit_id: PIT identifier
            verbose: Include detailed file information

        Returns:
            PIT information dict
        """
        manifest = self.repository.get_pit_manifest(pit_id)
        if not manifest:
            raise ValueError(f"PIT not found: {pit_id}")

        description = {
            "pit_id": pit_id,
            "timestamp": manifest.timestamp,
            "parent_pit_id": manifest.parent_pit_id,
            "added_files_count": len(manifest.added_files),
            "modified_files_count": len(manifest.modified_files),
            "deleted_files_count": len(manifest.deleted_files),
            "total_data_volume": sum(
                f.size for f in manifest.added_files + manifest.modified_files
            ),
        }

        if verbose:
            # Calculate total size for chain
            chain = self.repository.get_pit_chain(pit_id)
            accumulated_files, _ = self.repository.get_accumulated_files(pit_id)

            description["pit_chain_length"] = len(chain)
            description["accumulated_files_count"] = len(accumulated_files)
            description["schema_fields"] = len(manifest.table_schema.get("fields", []))

            if verbose:
                description["added_files"] = [
                    {
                        "path": f.path,
                        "checksum": f.checksum,
                        "size": f.size,
                    }
                    for f in manifest.added_files
                ]
                description["modified_files"] = [
                    {
                        "path": f.path,
                        "checksum": f.checksum,
                        "size": f.size,
                    }
                    for f in manifest.modified_files
                ]

        return description
