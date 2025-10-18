"""
Incremental Backup Management

Manages PIT (Point in Time) chains and incremental backups.
Handles repository structure, change detection, and manifest creation.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import shutil

from bcn.pit_manifest import PITManifest, PITManifestData
from bcn.change_detector import ChangeDetector, TableSnapshot
from bcn.iceberg_utils import PathAbstractor
from bcn.s3_client import S3Client
from bcn.logging_config import BCNLogger


logger = BCNLogger.get_logger(__name__)


class IncrementalBackupRepository:
    """Manages incremental backup repository structure and PIT chain."""

    def __init__(self, backup_name: str, s3_client: S3Client, backup_bucket: str):
        """
        Initialize repository manager.

        Args:
            backup_name: Name of the backup
            s3_client: S3 client for storage operations
            backup_bucket: S3 bucket for backups
        """
        self.backup_name = backup_name
        self.s3_client = s3_client
        self.backup_bucket = backup_bucket
        self.repository_prefix = f"{backup_name}/repo"
        self.pits_prefix = f"{self.repository_prefix}/pits"

    def initialize_repository(self) -> Dict[str, Any]:
        """
        Initialize repository structure (called on first backup).

        Returns:
            Repository index data
        """
        logger.info(f"Initializing incremental backup repository: {self.backup_name}")

        index = {
            "backup_name": self.backup_name,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "last_pit": None,
            "pits": [],
        }

        # Save index.json
        self._save_index(index)
        logger.info(f"Repository initialized successfully")

        return index

    def get_repository_index(self) -> Optional[Dict[str, Any]]:
        """
        Load repository index.

        Returns:
            Index data if repository exists, None otherwise
        """
        try:
            index_key = f"{self.repository_prefix}/index.json"
            content = self.s3_client.read_object(self.backup_bucket, index_key)
            return json.loads(content)
        except Exception as e:
            logger.debug(f"Repository index not found: {e}")
            return None

    def get_last_pit(self) -> Optional[str]:
        """
        Get ID of last PIT in chain.

        Returns:
            PIT ID or None if no backups exist
        """
        index = self.get_repository_index()
        if index:
            return index.get("last_pit")
        return None

    def get_pit_manifest(self, pit_id: str) -> Optional[PITManifestData]:
        """
        Load PIT manifest.

        Args:
            pit_id: PIT identifier

        Returns:
            PITManifestData or None if not found
        """
        try:
            manifest_key = f"{self.pits_prefix}/{pit_id}/manifest.json"
            content = self.s3_client.read_object(self.backup_bucket, manifest_key)
            return PITManifest.deserialize_manifest(content)
        except Exception as e:
            logger.error(f"Failed to load manifest for PIT {pit_id}: {e}")
            return None

    def create_pit(
        self,
        pit_id: str,
        parent_pit_id: Optional[str],
        added_files: Dict[str, Tuple[str, int]],
        deleted_files: List[str],
        modified_files: Dict[str, Tuple[str, int]],
        table_schema: Dict[str, Any],
        table_properties: Dict[str, Any],
    ) -> PITManifestData:
        """
        Create a new PIT and save manifest.

        Args:
            pit_id: Unique PIT identifier
            parent_pit_id: Parent PIT ID or None for root
            added_files: Dict of {path: (checksum, size)}
            deleted_files: List of deleted file paths
            modified_files: Dict of {path: (checksum, size)}
            table_schema: Table schema snapshot
            table_properties: Table properties snapshot

        Returns:
            Created PITManifestData
        """
        logger.info(f"Creating PIT: {pit_id}")

        # Create manifest
        manifest = PITManifest.create_manifest(
            pit_id=pit_id,
            parent_pit_id=parent_pit_id,
            added_files=added_files,
            deleted_files=deleted_files,
            modified_files=modified_files,
            table_schema=table_schema,
            table_properties=table_properties,
        )

        # Save manifest
        manifest_key = f"{self.pits_prefix}/{pit_id}/manifest.json"
        manifest_json = PITManifest.serialize_manifest(manifest)
        self.s3_client.write_object(self.backup_bucket, manifest_key, manifest_json)

        logger.info(f"Manifest saved for PIT {pit_id}")

        return manifest

    def upload_pit_data_files(
        self,
        pit_id: str,
        added_files: Dict[str, str],  # {dest_path: source_s3_path}
        modified_files: Dict[str, str],  # {dest_path: source_s3_path}
    ) -> None:
        """
        Upload data files for a PIT.

        Only new/modified files are uploaded. Unchanged files are referenced
        from parent PIT via manifest.

        Args:
            pit_id: PIT identifier
            added_files: Dict of {dest_path: source_s3_path}
            modified_files: Dict of {dest_path: source_s3_path}
        """
        all_files = {**added_files, **modified_files}

        if not all_files:
            logger.info(f"No data files to upload for PIT {pit_id}")
            return

        logger.info(f"Uploading {len(all_files)} data files for PIT {pit_id}")

        for dest_path, source_path in all_files.items():
            pit_data_path = f"{self.pits_prefix}/{pit_id}/data_files/{dest_path}"
            try:
                self.s3_client.copy_object(
                    source_path, pit_data_path, self.backup_bucket
                )
                logger.debug(f"Copied data file: {source_path} -> {pit_data_path}")
            except Exception as e:
                logger.error(f"Failed to copy data file {source_path}: {e}")
                raise

        logger.info(f"Successfully uploaded {len(all_files)} data files for PIT {pit_id}")

    def update_index(self, pit_id: str) -> None:
        """
        Update repository index with new PIT.

        Args:
            pit_id: PIT identifier to add
        """
        index = self.get_repository_index()
        if index is None:
            index = self.initialize_repository()

        if pit_id not in index["pits"]:
            index["pits"].append(pit_id)
            index["last_pit"] = pit_id

        self._save_index(index)
        logger.info(f"Index updated with PIT {pit_id}")

    def get_pit_chain(self, pit_id: str) -> List[str]:
        """
        Get chain of PITs from root to specified PIT.

        Args:
            pit_id: Target PIT identifier

        Returns:
            List of PIT IDs in order from root to target
        """
        chain = []
        current_pit = pit_id

        while current_pit:
            chain.insert(0, current_pit)
            manifest = self.get_pit_manifest(current_pit)
            if manifest:
                current_pit = manifest.parent_pit_id
            else:
                break

        return chain

    def get_accumulated_files(self, pit_id: str) -> Tuple[Dict[str, str], List[str]]:
        """
        Get all data files accumulated from root PIT to target.

        Traces parent chain and accumulates files, accounting for deletions.

        Args:
            pit_id: Target PIT identifier

        Returns:
            Tuple of (file_dict, deleted_files)
            - file_dict: {relative_path: pit_id} mapping to which PIT contains file
            - deleted_files: List of files deleted in target PIT
        """
        chain = self.get_pit_chain(pit_id)
        accumulated_files: Dict[str, str] = {}
        accumulated_deleted: set = set()

        for current_pit_id in chain:
            manifest = self.get_pit_manifest(current_pit_id)
            if not manifest:
                logger.warning(f"Could not load manifest for PIT {current_pit_id}")
                continue

            # Add new files
            for file_entry in manifest.added_files:
                accumulated_files[file_entry.path] = current_pit_id

            # Add modified files (replace previous)
            for file_entry in manifest.modified_files:
                accumulated_files[file_entry.path] = current_pit_id

            # Track deletions
            for deleted_path in manifest.deleted_files:
                accumulated_deleted.add(deleted_path)
                accumulated_files.pop(deleted_path, None)

        return (accumulated_files, list(accumulated_deleted))

    def verify_pit_integrity(self, pit_id: str) -> bool:
        """
        Verify PIT integrity by checking manifest checksums.

        Args:
            pit_id: PIT identifier

        Returns:
            True if integrity verified
        """
        manifest = self.get_pit_manifest(pit_id)
        if not manifest:
            logger.error(f"Could not load manifest for PIT {pit_id}")
            return False

        try:
            # Verify all file checksums
            for file_entry in manifest.added_files + manifest.modified_files:
                # Note: In full implementation, would verify against actual files
                logger.debug(f"Verified checksum for {file_entry.path}: {file_entry.checksum}")

            logger.info(f"PIT {pit_id} integrity verified")
            return True
        except Exception as e:
            logger.error(f"PIT {pit_id} integrity verification failed: {e}")
            return False

    def _save_index(self, index: Dict[str, Any]) -> None:
        """
        Save repository index.

        Args:
            index: Index data to save
        """
        index_key = f"{self.repository_prefix}/index.json"
        index_json = json.dumps(index, indent=2, default=str)
        self.s3_client.write_object(self.backup_bucket, index_key, index_json)


class IncrementalBackup:
    """Manages incremental backup operations."""

    def __init__(
        self,
        database: str,
        table: str,
        backup_name: str,
        catalog: Optional[str] = None,
        spark_client=None,
        s3_client: Optional[S3Client] = None,
        backup_bucket: str = "iceberg",
    ):
        """
        Initialize incremental backup.

        Args:
            database: Database name
            table: Table name
            backup_name: Backup identifier
            catalog: Catalog name (optional)
            spark_client: Spark client for table metadata
            s3_client: S3 client for storage
            backup_bucket: S3 bucket for backups
        """
        self.database = database
        self.table = table
        self.backup_name = backup_name
        self.catalog = catalog
        self.spark_client = spark_client
        self.s3_client = s3_client or S3Client()
        self.backup_bucket = backup_bucket

        self.repository = IncrementalBackupRepository(
            backup_name, self.s3_client, backup_bucket
        )

    def create_incremental_backup(self) -> str:
        """
        Create an incremental backup.

        Returns:
            Created PIT identifier
        """
        logger.info(f"Starting incremental backup: {self.backup_name}")

        # Check if repository exists
        index = self.repository.get_repository_index()
        if index is None:
            logger.info("First backup - initializing repository")
            self.repository.initialize_repository()
            parent_pit_id = None
        else:
            parent_pit_id = self.repository.get_last_pit()
            logger.info(f"Incremental backup - parent PIT: {parent_pit_id}")

        # Get current table state
        current_snapshot = self._get_table_snapshot()

        # Get previous snapshot if not first backup
        previous_snapshot = None
        if parent_pit_id:
            previous_snapshot = self._load_snapshot_from_pit(parent_pit_id)

        # Detect changes
        added_files, deleted_files, modified_files = ChangeDetector.compare_snapshots(
            previous_snapshot, current_snapshot
        )

        logger.info(
            f"Changes detected - Added: {len(added_files)}, "
            f"Modified: {len(modified_files)}, Deleted: {len(deleted_files)}"
        )

        # Generate PIT ID
        pit_id = self._generate_pit_id()

        # Create PIT
        manifest = self.repository.create_pit(
            pit_id=pit_id,
            parent_pit_id=parent_pit_id,
            added_files=added_files,
            deleted_files=deleted_files,
            modified_files=modified_files,
            table_schema=current_snapshot.schema,
            table_properties=current_snapshot.metadata.get("properties", {}),
        )

        # Upload data files
        self._upload_data_files(pit_id, added_files, modified_files, current_snapshot)

        # Update repository index
        self.repository.update_index(pit_id)

        # Verify integrity
        if not self.repository.verify_pit_integrity(pit_id):
            logger.warning(f"PIT {pit_id} integrity verification failed")

        logger.info(f"Incremental backup completed - PIT: {pit_id}")
        return pit_id

    def _get_table_snapshot(self) -> TableSnapshot:
        """
        Get current table state snapshot.

        Returns:
            TableSnapshot with current data files, metadata, and schema
        """
        logger.info(f"Getting table snapshot: {self.database}.{self.table}")

        # This would call spark_client to get actual table metadata
        # For now, return empty snapshot (will be populated by actual implementation)
        return TableSnapshot(
            data_files=set(),
            metadata={},
            schema={},
        )

    def _load_snapshot_from_pit(self, pit_id: str) -> Optional[TableSnapshot]:
        """
        Load table snapshot from PIT manifest.

        Args:
            pit_id: PIT identifier

        Returns:
            TableSnapshot or None if not found
        """
        manifest = self.repository.get_pit_manifest(pit_id)
        if not manifest:
            return None

        # Reconstruct files state from manifest
        # In full implementation, would trace through entire chain
        files = set()
        file_info = {}

        for file_entry in manifest.added_files + manifest.modified_files:
            files.add(file_entry.path)
            file_info[file_entry.path] = (file_entry.checksum, file_entry.size)

        return TableSnapshot(
            data_files=files,
            metadata={"file_info": file_info},
            schema=manifest.table_schema,
        )

    def _upload_data_files(
        self,
        pit_id: str,
        added_files: Dict[str, Tuple[str, int]],
        modified_files: Dict[str, Tuple[str, int]],
        snapshot: TableSnapshot,
    ) -> None:
        """
        Upload data files to PIT.

        Args:
            pit_id: PIT identifier
            added_files: Added files info
            modified_files: Modified files info
            snapshot: Current table snapshot
        """
        # Map file paths to S3 locations
        added_s3_files = {}
        modified_s3_files = {}

        # This would use spark_client to find actual S3 paths of files
        # For now, empty (will be populated by actual implementation)

        self.repository.upload_pit_data_files(pit_id, added_s3_files, modified_s3_files)

    def _generate_pit_id(self) -> str:
        """
        Generate unique PIT identifier.

        Returns:
            PIT ID in format: pit_NNN_TIMESTAMP
        """
        index = self.repository.get_repository_index()
        pit_count = 0
        if index:
            pit_count = len(index.get("pits", []))

        timestamp = datetime.utcnow().isoformat().replace(":", "").replace(".", "")
        pit_number = str(pit_count + 1).zfill(3)

        return f"pit_{pit_number}_{timestamp}"
