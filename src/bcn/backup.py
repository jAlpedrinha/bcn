#!/usr/bin/env python3
"""
Iceberg Table Backup Script

Creates a complete, independent backup of an Iceberg table by copying:
- Metadata files (Iceberg metadata JSON)
- Manifest files (manifest lists and individual manifests in Avro format)
- Data files (Parquet/ORC/Avro data files)

All paths are abstracted (table location prefixes removed) for portability.
The backup is stored in a backup bucket and is fully independent of the
original table, allowing safe deletion of the source table after backup.
"""

import argparse
import json
import os
import re
import shutil
import sys
from typing import Dict, List

from bcn.config import Config
from bcn.iceberg_utils import ManifestFileHandler, PathAbstractor
from bcn.logging_config import BCNLogger
from bcn.s3_client import S3Client
from bcn.spark_client import SparkClient

logger = BCNLogger.get_logger(__name__)


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
        self.work_dir = os.path.join(Config.WORK_DIR, backup_name)

    def create_backup(self) -> bool:
        """
        Create a backup of the Iceberg table

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting backup of {self.database}.{self.table} as '{self.backup_name}'")

            # Step 1: Get table metadata from Spark catalog
            logger.info("Step 1: Retrieving table metadata from catalog...")
            table_metadata = self.spark_client.get_table_metadata(self.database, self.table)
            if not table_metadata:
                logger.error(f"Could not retrieve metadata for {self.database}.{self.table}")
                return False

            table_location = table_metadata["location"]
            metadata_location = table_metadata.get("metadata_location")

            if not metadata_location:
                logger.error(f"No metadata_location found for {self.database}.{self.table}")
                logger.error("This may not be an Iceberg table")
                return False

            logger.debug(f"Table location: {table_location}")
            logger.debug(f"Metadata location: {metadata_location}")

            # Step 2: Download and parse main metadata file
            logger.info("Step 2: Downloading and parsing main metadata file...")
            bucket, key = self.s3_client.parse_s3_uri(metadata_location)
            metadata_content = self.s3_client.read_object(bucket, key)
            if not metadata_content:
                logger.error(f"Could not read metadata file from {metadata_location}")
                return False

            metadata = json.loads(metadata_content.decode("utf-8"))
            logger.debug(f"Current snapshot ID: {metadata.get('current-snapshot-id')}")

            # Step 3: Abstract paths in metadata
            logger.info("Step 3: Abstracting paths in metadata...")
            abstracted_metadata = PathAbstractor.abstract_metadata_file(metadata, table_location)

            # Step 4: Process snapshots and manifests
            logger.info("Step 4: Processing snapshots and manifest files...")
            # Use abstracted_metadata which contains only the current snapshot
            manifest_files = self._collect_manifest_files(abstracted_metadata, table_location)
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

            # Step 7: Save backup metadata
            logger.info("Step 7: Saving backup metadata...")
            backup_metadata = {
                "original_database": self.database,
                "original_table": self.table,
                "original_location": table_location,
                "backup_name": self.backup_name,
                "abstracted_metadata": abstracted_metadata,
                "manifest_lists": manifest_list_paths,
                "individual_manifests": individual_manifest_paths,
                "data_files": [PathAbstractor.abstract_path(f, table_location) for f in data_files],
            }

            # Save to local work directory
            backup_metadata_path = os.path.join(self.work_dir, "backup_metadata.json")
            with open(backup_metadata_path, "w") as f:
                json.dump(backup_metadata, f, indent=2)

            # Step 8: Upload to backup bucket
            logger.info("Step 8: Uploading backup to S3...")
            success = self._upload_backup_to_s3(backup_metadata, table_location)

            if success:
                logger.info(f"Backup '{self.backup_name}' created successfully!")
                # Include prefix in location if present
                if Config.BACKUP_PREFIX:
                    backup_location = f"s3://{Config.BACKUP_BUCKET}/{Config.BACKUP_PREFIX}/{self.backup_name}/"
                else:
                    backup_location = f"s3://{Config.BACKUP_BUCKET}/{self.backup_name}/"
                logger.info(f"Location: {backup_location}")
            else:
                logger.error("Failed to upload backup to S3")
                return False

            # Cleanup
            logger.info("Cleaning up temporary files...")
            shutil.rmtree(self.work_dir, ignore_errors=True)

            return True

        except Exception as e:
            logger.error(f"Error during backup: {e}", exc_info=True)
            return False
        # Note: Not closing spark_client here as it may be shared with other processes
        # The caller or session fixture is responsible for closing the Spark session

    def _collect_manifest_files(self, metadata: Dict, table_location: str) -> List[str]:
        """
        Collect manifest file paths from current snapshot only.

        For backups, we only need the current snapshot's manifest files, not the entire
        snapshot history. This method extracts the manifest-list path from snapshots
        (which should only contain the current snapshot after abstraction) and converts
        relative paths to full S3 URIs.

        Args:
            metadata: Parsed Iceberg metadata JSON containing snapshots (should be abstracted
                     metadata with only current snapshot)
            table_location: Base S3 location of the table for resolving relative paths

        Returns:
            List of full S3 URIs pointing to manifest list files (snap-*.avro)
        """
        manifest_files = []

        # After abstraction, metadata should only contain current snapshot
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
        Collect all active data file paths from manifest list and manifest files.

        Traverses the manifest hierarchy: manifest lists -> individual manifests -> data files.
        Only collects ACTIVE files (status 0=EXISTING or 1=ADDED), skipping DELETED files (status 2).
        This ensures we only back up data that is part of the current snapshot.

        Args:
            manifest_list_files: List of full S3 URIs to manifest list files (snap-*.avro)
            table_location: Base S3 location of the table for relative path resolution

        Returns:
            List of S3 paths to active data files referenced in the manifests
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
                        # Only include ACTIVE files (status 0=EXISTING or 1=ADDED)
                        for m_entry in manifest_entries:
                            # Check entry status: 0=EXISTING, 1=ADDED, 2=DELETED
                            status = m_entry.get("status", 1)  # Default to ADDED if missing
                            if status == 2:
                                # Skip deleted entries
                                logger.info(f"Skiping manifest entry {m_entry}")
                                continue

                            if "data_file" in m_entry and "file_path" in m_entry["data_file"]:
                                logger.info(f"Adding data file {m_entry}")
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

        Uploads metadata files (JSON), manifest files (Avro), and data files (Parquet/ORC/Avro)
        to create a complete, independent backup.

        Args:
            backup_metadata: Dictionary containing backup metadata including manifest lists,
                           individual manifests, abstracted metadata, and data file paths
            table_location: Original table location for constructing full S3 paths

        Returns:
            True if all uploads succeed, False if any critical upload fails

        Note:
            Individual file upload failures are logged as warnings but do not fail the
            entire backup operation unless metadata uploads fail.
        """
        try:
            # Construct backup prefix including any configured prefix from BACKUP_BUCKET
            if Config.BACKUP_PREFIX:
                backup_prefix = f"{Config.BACKUP_PREFIX}/{self.backup_name}/"
            else:
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

            # Copy manifest list files (manifest lists don't have deleted entries, copy as-is)
            manifest_lists = backup_metadata.get("manifest_lists", [])
            for relative_path in manifest_lists:
                full_path = f"{table_location}/{relative_path}"
                try:
                    # Download raw Avro from source
                    bucket, key = self.s3_client.parse_s3_uri(full_path)
                    content = self.s3_client.read_object(bucket, key)
                    if content:
                        # Upload raw Avro to backup (manifest lists don't need filtering)
                        manifest_key = f"{backup_prefix}{relative_path}"
                        if not self.s3_client.write_object(
                            Config.BACKUP_BUCKET, manifest_key, content
                        ):
                            logger.warning(f"Failed to upload manifest list {relative_path}")
                except Exception as e:
                    logger.warning(f"Could not copy manifest list {relative_path}: {e}")
            logger.info(f"Uploaded {len(manifest_lists)} manifest list files")

            # Copy individual manifest files, filtering out deleted entries
            individual_manifests = backup_metadata.get("individual_manifests", [])
            for relative_path in individual_manifests:
                full_path = f"{table_location}/{relative_path}"
                try:
                    # Download and parse Avro from source
                    bucket, key = self.s3_client.parse_s3_uri(full_path)
                    content = self.s3_client.read_object(bucket, key)
                    if content:
                        # Read entries and schema
                        entries, schema = ManifestFileHandler.read_manifest_file(content)

                        # Filter out deleted entries (status=2)
                        # Only keep EXISTING (0) and ADDED (1) entries
                        active_entries = [e for e in entries if e.get("status", 1) != 2]

                        # Rewrite Avro with only active entries
                        filtered_content = ManifestFileHandler.write_manifest_list(
                            active_entries, schema
                        )

                        # Upload filtered Avro to backup
                        manifest_key = f"{backup_prefix}{relative_path}"
                        if not self.s3_client.write_object(
                            Config.BACKUP_BUCKET, manifest_key, filtered_content
                        ):
                            logger.warning(
                                f"Failed to upload individual manifest {relative_path}"
                            )
                except Exception as e:
                    logger.warning(f"Could not copy individual manifest {relative_path}: {e}")
            logger.info(f"Uploaded {len(individual_manifests)} individual manifest files")

            # Copy data files (Parquet/ORC/Avro files)
            data_files = backup_metadata.get("data_files", [])
            if data_files:
                logger.info(f"Copying {len(data_files)} data files...")
                copied_count = 0
                failed_count = 0

                for i, relative_path in enumerate(data_files, 1):
                    # Log progress every 10 files or at the end
                    if i % 10 == 0 or i == len(data_files):
                        logger.info(f"  Progress: {i}/{len(data_files)} data files processed...")

                    full_path = f"{table_location}/{relative_path}"
                    try:
                        # Parse source S3 URI
                        source_bucket, source_key = self.s3_client.parse_s3_uri(full_path)

                        # Construct destination key
                        dest_key = f"{backup_prefix}{relative_path}"

                        # Copy the file using S3 copy operation (more efficient than download/upload)
                        if self.s3_client.copy_object(
                            source_bucket, source_key, Config.BACKUP_BUCKET, dest_key
                        ):
                            copied_count += 1
                        else:
                            failed_count += 1
                            logger.warning(f"Failed to copy data file: {relative_path}")
                    except Exception as e:
                        failed_count += 1
                        logger.warning(f"Could not copy data file {relative_path}: {e}")

                logger.info(
                    f"Data file copy complete: {copied_count} succeeded, {failed_count} failed"
                )

                # If too many failures, consider the backup failed
                if failed_count > copied_count:
                    logger.error("More than half of data files failed to copy")
                    return False
            else:
                logger.info("No data files to copy")

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
    success = backup.create_backup()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
