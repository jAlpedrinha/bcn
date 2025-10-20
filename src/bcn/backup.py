#!/usr/bin/env python3
"""
Iceberg Table Backup Script

Creates a backup of an Iceberg table by copying metadata and data files
to a backup location with abstracted paths.
"""

import argparse
import json
import os
import re
import shutil
import sys
import time
import uuid
from typing import Dict, List, Optional

from bcn.config import Config
from bcn.iceberg_utils import ManifestFileHandler, PathAbstractor
from bcn.logging_config import BCNLogger
from bcn.s3_client import S3Client
from bcn.spark_client import SparkClient

logger = BCNLogger.get_logger(__name__)


class BackupRepository:
    """Manages PIT (Point-in-Time) chain structure and repository state.

    Handles PIT indexing, manifest storage, and chain traversal for incremental
    backup support. Each PIT is a backup snapshot with optional parent reference.
    """

    def __init__(self, backup_name: str, s3_client: S3Client):
        """
        Initialize repository manager.

        Args:
            backup_name: Name of the backup
            s3_client: S3 client for storage operations
        """
        self.backup_name = backup_name
        self.s3_client = s3_client
        self.backup_bucket = Config.BACKUP_BUCKET

    def get_or_create_index(self) -> Dict:
        """Get or create repository index (list of PITs).

        Returns:
            Dictionary with structure:
            {
                "backup_name": str,
                "pits": [
                    {
                        "pit_id": str,
                        "created_at": int (unix timestamp),
                        "parent_pit_id": Optional[str]
                    },
                    ...
                ]
            }
        """
        try:
            index_key = f"{self.backup_name}/index.json"
            content = self.s3_client.read_object(self.backup_bucket, index_key)

            if content:
                return json.loads(content.decode("utf-8"))

            # Create new index
            return {
                "backup_name": self.backup_name,
                "pits": []
            }
        except Exception as e:
            logger.debug(f"Could not read repository index: {e}")
            return {
                "backup_name": self.backup_name,
                "pits": []
            }

    def get_last_pit(self) -> Optional[str]:
        """Get latest PIT ID in chain.

        Returns:
            PIT ID (string) if PITs exist, None otherwise
        """
        index = self.get_or_create_index()
        pits = index.get("pits", [])

        if not pits:
            return None

        # PITs are ordered by creation, last one is most recent
        return pits[-1]["pit_id"]

    def save_pit(self, pit_id: str, manifest: Dict) -> bool:
        """Store PIT manifest.

        Args:
            pit_id: Unique PIT identifier
            manifest: PIT manifest dictionary

        Returns:
            True if successful, False otherwise
        """
        try:
            manifest_key = f"{self.backup_name}/pits/{pit_id}/manifest.json"
            manifest_content = json.dumps(manifest, indent=2).encode("utf-8")

            return self.s3_client.write_object(
                self.backup_bucket, manifest_key, manifest_content
            )
        except Exception as e:
            logger.error(f"Error saving PIT manifest: {e}")
            return False

    def get_pit_manifest(self, pit_id: str) -> Optional[Dict]:
        """Load PIT manifest.

        Args:
            pit_id: PIT identifier

        Returns:
            Manifest dictionary if found, None otherwise
        """
        try:
            manifest_key = f"{self.backup_name}/pits/{pit_id}/manifest.json"
            content = self.s3_client.read_object(self.backup_bucket, manifest_key)

            if not content:
                return None

            return json.loads(content.decode("utf-8"))
        except Exception as e:
            logger.debug(f"Could not read PIT manifest: {e}")
            return None

    def update_index(self, pit_id: str, parent_pit_id: Optional[str] = None) -> bool:
        """Add PIT to index.

        Args:
            pit_id: New PIT identifier
            parent_pit_id: Parent PIT ID (None for first backup)

        Returns:
            True if successful, False otherwise
        """
        try:
            index = self.get_or_create_index()

            # Add new PIT entry
            pit_entry = {
                "pit_id": pit_id,
                "created_at": int(time.time() * 1000),  # milliseconds since epoch
                "parent_pit_id": parent_pit_id
            }

            index["pits"].append(pit_entry)

            # Save updated index
            index_key = f"{self.backup_name}/index.json"
            index_content = json.dumps(index, indent=2).encode("utf-8")

            return self.s3_client.write_object(
                self.backup_bucket, index_key, index_content
            )
        except Exception as e:
            logger.error(f"Error updating repository index: {e}")
            return False


class IcebergBackup:
    """Orchestrates the backup process for an Iceberg table"""

    def __init__(self, database: str, table: str, backup_name: str, catalog: str = None):
        """
        Initialize backup process

        Args:
            database: Database name
            table: Table name
            backup_name: Name for this backup
            catalog: Catalog name (optional). Uses fallback: parameter -> env var -> default

        Raises:
            ValueError: If database, table, or backup_name are empty or contain invalid characters
        """
        # Validate inputs
        if not database or not database.strip():
            raise ValueError("Database name cannot be empty")
        if not table or not table.strip():
            raise ValueError("Table name cannot be empty")
        if not backup_name or not backup_name.strip():
            raise ValueError("Backup name cannot be empty")

        # Check for invalid characters in backup_name
        if not re.match(r'^[a-zA-Z0-9_-]+$', backup_name):
            raise ValueError(
                "Backup name must contain only letters, numbers, hyphens, and underscores"
            )

        self.database = database.strip()
        self.table = table.strip()
        self.backup_name = backup_name.strip()

        # Catalog resolution: parameter -> environment variable -> default
        if catalog:
            self.catalog = catalog
        else:
            self.catalog = os.getenv("CATALOG_NAME", Config.CATALOG_NAME)

        self.s3_client = S3Client()
        self.spark_client = SparkClient(app_name=f"iceberg-backup-{backup_name}", catalog=self.catalog)
        self.repository = BackupRepository(backup_name, self.s3_client)
        self.work_dir = os.path.join(Config.WORK_DIR, backup_name)

    def create_backup(self) -> Optional[str]:
        """
        Create a backup of the Iceberg table using unified algorithm.

        Works for first backup (creates initial PIT with all files as delta)
        and subsequent backups (creates new PIT with only changed files + parent ref).
        Same algorithm - just different input state.

        Returns:
            PIT ID (string) if successful, None otherwise
        """
        try:
            logger.info(f"Starting backup of {self.database}.{self.table} as '{self.backup_name}'")

            # Step 1: Get table metadata from Spark catalog
            logger.info("Step 1: Retrieving table metadata from catalog...")
            table_metadata = self.spark_client.get_table_metadata(self.database, self.table)
            if not table_metadata:
                logger.error(f"Could not retrieve metadata for {self.database}.{self.table}")
                return None

            table_location = table_metadata["location"]
            metadata_location = table_metadata.get("metadata_location")

            if not metadata_location:
                logger.error(f"No metadata_location found for {self.database}.{self.table}")
                logger.error("This may not be an Iceberg table")
                return None

            logger.debug(f"Table location: {table_location}")
            logger.debug(f"Metadata location: {metadata_location}")

            # Step 2: Download and parse main metadata file
            logger.info("Step 2: Downloading and parsing main metadata file...")
            bucket, key = self.s3_client.parse_s3_uri(metadata_location)
            metadata_content = self.s3_client.read_object(bucket, key)
            if not metadata_content:
                logger.error(f"Could not read metadata file from {metadata_location}")
                return None

            metadata = json.loads(metadata_content.decode("utf-8"))
            logger.debug(f"Current snapshot ID: {metadata.get('current-snapshot-id')}")

            # Step 3: Abstract paths in metadata
            logger.info("Step 3: Abstracting paths in metadata...")
            abstracted_metadata = PathAbstractor.abstract_metadata_file(metadata, table_location)

            # Step 4: Process snapshots and manifests
            logger.info("Step 4: Processing snapshots and manifest files...")
            manifest_files = self._collect_manifest_files(metadata, table_location)
            logger.debug(f"Found {len(manifest_files)} manifest files to process")

            # Step 5: Download manifest list AND individual manifest files as raw Avro
            logger.info("Step 5: Downloading manifest files...")
            os.makedirs(self.work_dir, exist_ok=True)

            manifest_list_paths = []
            individual_manifest_paths = []

            for manifest_list_path in manifest_files:
                logger.debug(f"Downloading manifest list: {manifest_list_path}")
                relative_path = PathAbstractor.abstract_path(manifest_list_path, table_location)
                manifest_list_paths.append(relative_path)

                # Read the manifest list to get individual manifest file paths
                try:
                    bucket, key = self.s3_client.parse_s3_uri(manifest_list_path)
                    content = self.s3_client.read_object(bucket, key)
                    if content:
                        entries, _ = ManifestFileHandler.read_manifest_file(content)
                        for entry in entries:
                            manifest_path = entry.get("manifest_path")
                            if not manifest_path:
                                continue

                            # Convert relative manifest path to full S3 URI
                            if not manifest_path.startswith(
                                "s3://"
                            ) and not manifest_path.startswith("s3a://"):
                                full_manifest_path = f"{table_location}/{manifest_path}"
                            else:
                                full_manifest_path = manifest_path

                            # Store individual manifest path
                            logger.debug(f"Found individual manifest: {manifest_path}")
                            manifest_relative_path = PathAbstractor.abstract_path(
                                full_manifest_path, table_location
                            )
                            if manifest_relative_path not in individual_manifest_paths:
                                individual_manifest_paths.append(manifest_relative_path)
                except Exception as e:
                    logger.warning(f"Could not read manifest list {manifest_list_path}: {e}")

            # Step 6: Collect data file references
            logger.info("Step 6: Collecting data file references...")
            data_files = self._collect_data_files(manifest_files, table_location)
            logger.debug(f"Found {len(data_files)} data files")

            # Step 7: Check if previous backup exists (unified algorithm decision point)
            logger.info("Step 7: Checking for previous backups...")
            previous_pit_id = self.repository.get_last_pit()

            if previous_pit_id is None:
                logger.info("  This is the first backup for this backup name")
                parent_pit_id = None
            else:
                logger.info(f"  Found previous PIT: {previous_pit_id}")
                parent_pit_id = previous_pit_id

            # Step 8: Generate new PIT ID
            pit_id = self._generate_pit_id()
            logger.info(f"  Generated new PIT ID: {pit_id}")

            # Step 9: Create PIT manifest
            logger.info("Step 9: Creating PIT manifest...")
            pit_manifest = self._create_pit_manifest(
                pit_id=pit_id,
                parent_pit_id=parent_pit_id,
                database=self.database,
                table=self.table,
                table_location=table_location,
                abstracted_metadata=abstracted_metadata,
                manifest_lists=manifest_list_paths,
                individual_manifests=individual_manifest_paths,
                data_files=[PathAbstractor.abstract_path(f, table_location) for f in data_files],
            )

            # Step 10: Save PIT manifest
            if not self.repository.save_pit(pit_id, pit_manifest):
                logger.error("Failed to save PIT manifest")
                return None

            # Step 11: Update repository index
            if not self.repository.update_index(pit_id, parent_pit_id):
                logger.error("Failed to update repository index")
                return None

            # Step 12: Upload manifest files to S3
            logger.info("Step 12: Uploading manifest files to S3...")
            success = self._upload_backup_to_s3(pit_manifest, table_location)

            if not success:
                logger.error("Failed to upload backup to S3")
                return None

            logger.info(f"Successfully created backup PIT: {pit_id}")
            logger.info(f"  Backup name: {self.backup_name}")
            logger.info(f"  Parent PIT: {parent_pit_id or 'None (first backup)'}")
            logger.info(f"  Location: s3://{Config.BACKUP_BUCKET}/{self.backup_name}/pits/{pit_id}/")

            # Cleanup
            logger.info("Cleaning up temporary files...")
            shutil.rmtree(self.work_dir, ignore_errors=True)

            return pit_id

        except Exception as e:
            logger.error(f"Error during backup: {e}", exc_info=True)
            return None
        # Note: Not closing spark_client here as it may be shared with other processes
        # The caller or session fixture is responsible for closing the Spark session

    def _generate_pit_id(self) -> str:
        """Generate a unique PIT ID using timestamp and UUID.

        Returns:
            PIT ID in format: pit-{timestamp}-{uuid}
        """
        timestamp = int(time.time() * 1000)  # milliseconds since epoch
        unique_id = str(uuid.uuid4())[:8]  # First 8 chars of UUID
        return f"pit-{timestamp}-{unique_id}"

    def _create_pit_manifest(
        self,
        pit_id: str,
        parent_pit_id: Optional[str],
        database: str,
        table: str,
        table_location: str,
        abstracted_metadata: Dict,
        manifest_lists: List[str],
        individual_manifests: List[str],
        data_files: List[str],
    ) -> Dict:
        """Create a PIT manifest with parent reference and file information.

        Args:
            pit_id: Unique PIT identifier
            parent_pit_id: Parent PIT ID (None for first backup)
            database: Source database name
            table: Source table name
            table_location: Original table location
            abstracted_metadata: Abstracted Iceberg metadata
            manifest_lists: List of manifest list paths
            individual_manifests: List of individual manifest paths
            data_files: List of data file paths

        Returns:
            PIT manifest dictionary with structure:
            {
                "pit_id": str,
                "parent_pit_id": Optional[str],
                "created_at": int,
                "original_database": str,
                "original_table": str,
                "original_location": str,
                "backup_name": str,
                "abstracted_metadata": Dict,
                "manifest_lists": [str],
                "individual_manifests": [str],
                "data_files": [str]
            }
        """
        return {
            "pit_id": pit_id,
            "parent_pit_id": parent_pit_id,
            "created_at": int(time.time() * 1000),  # milliseconds since epoch
            "original_database": database,
            "original_table": table,
            "original_location": table_location,
            "backup_name": self.backup_name,
            "abstracted_metadata": abstracted_metadata,
            "manifest_lists": manifest_lists,
            "individual_manifests": individual_manifests,
            "data_files": data_files,
        }

    def _collect_manifest_files(self, metadata: Dict, table_location: str) -> List[str]:
        """
        Collect all manifest file paths from Iceberg metadata.

        Extracts manifest-list paths from all snapshots in the metadata and converts
        relative paths to full S3 URIs for consistent processing.

        Args:
            metadata: Parsed Iceberg metadata JSON containing snapshots
            table_location: Base S3 location of the table for resolving relative paths

        Returns:
            List of full S3 URIs pointing to manifest list files (snap-*.avro)
        """
        manifest_files = []

        for snapshot in metadata.get("snapshots", []):
            if "manifest-list" in snapshot:
                manifest_list_path = snapshot["manifest-list"]
                # Convert relative paths to full S3 URIs
                if not manifest_list_path.startswith("s3://") and not manifest_list_path.startswith(
                    "s3a://"
                ):
                    # Relative path - combine with table location
                    manifest_list_path = f"{table_location}/{manifest_list_path}"
                manifest_files.append(manifest_list_path)

        return manifest_files

    def _collect_data_files(self, manifest_list_files: List[str], table_location: str) -> List[str]:
        """
        Collect all data file paths from manifest list and manifest files.

        Traverses the manifest hierarchy: manifest lists -> individual manifests -> data files.
        Gracefully handles errors in individual files without failing the entire operation.

        Args:
            manifest_list_files: List of full S3 URIs to manifest list files (snap-*.avro)
            table_location: Base S3 location of the table for relative path resolution

        Returns:
            List of S3 paths to data files referenced in the manifests
        """
        data_files = []

        # manifest_list_files are actually manifest list files (snap-*.avro)
        # which contain entries pointing to manifest files
        for manifest_list_path in manifest_list_files:
            try:
                # Read the manifest list file
                manifest_list_entries, _ = ManifestFileHandler.read_manifest_from_s3(
                    self.s3_client, manifest_list_path, table_location
                )
                if not manifest_list_entries:
                    continue

                # Now read each manifest file to get data files
                for entry in manifest_list_entries:
                    # Manifest list entries have a 'manifest_path' field
                    manifest_path = entry.get("manifest_path")
                    if not manifest_path:
                        continue

                    try:
                        # Read the actual manifest file
                        manifest_entries, _ = ManifestFileHandler.read_manifest_from_s3(
                            self.s3_client, manifest_path, table_location
                        )
                        if not manifest_entries:
                            continue

                        # Get data files from the manifest
                        for m_entry in manifest_entries:
                            if "data_file" in m_entry and "file_path" in m_entry["data_file"]:
                                data_files.append(m_entry["data_file"]["file_path"])
                    except Exception as e:
                        logger.warning(f"Could not read manifest file {manifest_path}: {e}")
                        continue

            except Exception as e:
                logger.warning(f"Could not read manifest list {manifest_list_path}: {e}")
                continue

        return data_files

    def _upload_backup_to_s3(self, backup_metadata: Dict, table_location: str) -> bool:
        """
        Upload all backup files to the S3 backup bucket.

        Uploads metadata files (JSON) and manifest files (Avro) for the backup.
        Data files are not copied during backup; they remain in the original location
        and are copied during restore if needed.

        Args:
            backup_metadata: Dictionary containing backup metadata including manifest lists,
                           individual manifests, and abstracted metadata
            table_location: Original table location for constructing full S3 paths

        Returns:
            True if all uploads succeed, False if any critical upload fails

        Note:
            Individual file upload failures are logged as warnings but do not fail the
            entire backup operation unless metadata uploads fail.
        """
        try:
            backup_prefix = f"{self.backup_name}/"

            # Upload backup metadata
            metadata_key = f"{backup_prefix}backup_metadata.json"
            metadata_content = json.dumps(backup_metadata, indent=2).encode("utf-8")
            if not self.s3_client.write_object(
                Config.BACKUP_BUCKET, metadata_key, metadata_content
            ):
                return False
            logger.info("Uploaded backup metadata")

            # Upload abstracted metadata file
            iceberg_metadata_key = f"{backup_prefix}metadata.json"
            iceberg_content = json.dumps(backup_metadata["abstracted_metadata"], indent=2).encode(
                "utf-8"
            )
            if not self.s3_client.write_object(
                Config.BACKUP_BUCKET, iceberg_metadata_key, iceberg_content
            ):
                return False
            logger.info("Uploaded Iceberg metadata")

            # Copy manifest list files as raw Avro
            manifest_lists = backup_metadata.get("manifest_lists", [])
            for relative_path in manifest_lists:
                full_path = f"{table_location}/{relative_path}"
                try:
                    # Download raw Avro from source
                    bucket, key = self.s3_client.parse_s3_uri(full_path)
                    content = self.s3_client.read_object(bucket, key)
                    if content:
                        # Upload raw Avro to backup
                        manifest_key = f"{backup_prefix}{relative_path}"
                        if not self.s3_client.write_object(
                            Config.BACKUP_BUCKET, manifest_key, content
                        ):
                            logger.warning(f"Failed to upload manifest list {relative_path}")
                except Exception as e:
                    logger.warning(f"Could not copy manifest list {relative_path}: {e}")
            logger.info(f"Uploaded {len(manifest_lists)} manifest list files")

            # Copy individual manifest files as raw Avro
            individual_manifests = backup_metadata.get("individual_manifests", [])
            for relative_path in individual_manifests:
                full_path = f"{table_location}/{relative_path}"
                try:
                    # Download raw Avro from source
                    bucket, key = self.s3_client.parse_s3_uri(full_path)
                    content = self.s3_client.read_object(bucket, key)
                    if content:
                        # Upload raw Avro to backup
                        manifest_key = f"{backup_prefix}{relative_path}"
                        if not self.s3_client.write_object(
                            Config.BACKUP_BUCKET, manifest_key, content
                        ):
                            logger.warning(
                                f"Failed to upload individual manifest {relative_path}"
                            )
                except Exception as e:
                    logger.warning(f"Could not copy individual manifest {relative_path}: {e}")
            logger.info(f"Uploaded {len(individual_manifests)} individual manifest files")

            # Note: Data files remain in original location and are not copied during backup
            # They will be copied during restore operation

            return True

        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return False


def main():
    """Main entry point for backup script"""
    parser = argparse.ArgumentParser(description="Create a backup of an Iceberg table")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--backup-name", required=True, help="Name for this backup")
    parser.add_argument(
        "--catalog",
        default=None,
        help="Catalog name (optional). Falls back to CATALOG_NAME env var, then Config default",
    )
    parser.add_argument("--log-level", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--log-file", default=None, help="Optional log file path")

    args = parser.parse_args()

    # Setup logging with CLI arguments
    BCNLogger.setup_logging(level=args.log_level, log_file=args.log_file)

    # Create backup
    backup = IcebergBackup(args.database, args.table, args.backup_name, catalog=args.catalog)
    pit_id = backup.create_backup()

    if pit_id:
        print(f"✓ Backup created successfully: PIT {pit_id}")
        sys.exit(0)
    else:
        print("✗ Backup failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
