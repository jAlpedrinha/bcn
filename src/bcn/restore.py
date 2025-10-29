#!/usr/bin/env python3
"""
Iceberg Table Restore Script

Restores an Iceberg table from a backup to a new table location.
"""

import argparse
import json
import os
import re
import shutil
import sys
import time
import uuid
from typing import List

from bcn.config import Config
from bcn.iceberg_utils import ManifestFileHandler, PathAbstractor
from bcn.logging_config import BCNLogger
from bcn.s3_client import S3Client
from bcn.spark_client import SparkClient

logger = BCNLogger.get_logger(__name__)


class IcebergRestore:
    """Orchestrates the restore process for an Iceberg table"""

    def __init__(
        self,
        backup_name: str,
        target_database: str,
        target_table: str,
        target_location: str,
        catalog: str = None,
    ):
        """
        Initialize restore process

        Args:
            backup_name: Name of the backup to restore
            target_database: Target database name
            target_table: Target table name
            target_location: Target S3 location for the table
            catalog: Catalog name (optional). Uses fallback: parameter -> env var -> default

        Raises:
            ValueError: If any required parameter is empty or invalid
        """
        # Validate inputs
        if not backup_name or not backup_name.strip():
            raise ValueError("Backup name cannot be empty")
        if not target_database or not target_database.strip():
            raise ValueError("Target database name cannot be empty")
        if not target_table or not target_table.strip():
            raise ValueError("Target table name cannot be empty")
        if not target_location or not target_location.strip():
            raise ValueError("Target location cannot be empty")

        # Check for invalid characters in backup_name and table names
        if not re.match(r'^[a-zA-Z0-9_-]+$', backup_name):
            raise ValueError(
                "Backup name must contain only letters, numbers, hyphens, and underscores"
            )

        self.backup_name = backup_name.strip()
        self.target_database = target_database.strip()
        self.target_table = target_table.strip()
        self.target_location = target_location.rstrip("/").strip()

        # Catalog resolution: parameter -> environment variable -> default
        if catalog:
            self.catalog = catalog
        else:
            self.catalog = os.getenv("CATALOG_NAME", Config.CATALOG_NAME)

        self.s3_client = S3Client()
        self.spark_client = SparkClient(app_name=f"iceberg-restore-{backup_name}", catalog=self.catalog)
        self.work_dir = os.path.join(Config.WORK_DIR, f"restore_{backup_name}")
        self.backup_metadata = None

    def restore_backup(self) -> bool:
        """
        Restore a backup to a new table

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(
                f"Starting restore of backup '{self.backup_name}' to {self.target_database}.{self.target_table}"
            )

            # Step 1: Download backup metadata
            logger.info("Step 1: Downloading backup metadata...")
            if not self._download_backup_metadata():
                logger.error("Could not download backup metadata")
                return False

            original_location = self.backup_metadata["original_location"]
            logger.info(f"  Original location: {original_location}")
            logger.info(f"  Target location: {self.target_location}")

            # Step 2: Restore paths in metadata
            logger.info("Step 2: Restoring paths in metadata...")
            abstracted_metadata = self.backup_metadata["abstracted_metadata"]

            # The backup already contains abstracted metadata with the complete snapshot ancestry
            # We just need to copy it and restore paths, not abstract it again
            restored_metadata = abstracted_metadata.copy()

            # Set new location
            restored_metadata["location"] = self.target_location

            # Restore snapshot manifest paths
            for snapshot in restored_metadata.get("snapshots", []):
                if "manifest-list" in snapshot:
                    snapshot["manifest-list"] = PathAbstractor.restore_path(
                        snapshot["manifest-list"], self.target_location
                    )

            # Restore metadata log paths
            if "metadata-log" in restored_metadata:
                for entry in restored_metadata["metadata-log"]:
                    if "metadata-file" in entry:
                        entry["metadata-file"] = PathAbstractor.restore_path(
                            entry["metadata-file"], self.target_location
                        )

            # Step 3: Process and restore manifest files
            logger.info("Step 3: Processing manifest files...")

            # Handle both old format (manifest_files) and new format (manifest_lists + individual_manifests)
            manifest_lists = self.backup_metadata.get(
                "manifest_lists", self.backup_metadata.get("manifest_files", [])
            )
            individual_manifests = self.backup_metadata.get("individual_manifests", [])

            logger.info(
                f"  Found {len(manifest_lists)} manifest lists and {len(individual_manifests)} individual manifests to process"
            )

            restored_manifest_lists = {}
            for relative_path in manifest_lists:
                logger.debug(f"  Restoring manifest list: {relative_path}")
                entries, schema = self._restore_manifest_file(relative_path)
                if entries is not None and schema is not None:
                    restored_manifest_lists[relative_path] = {"entries": entries, "schema": schema}

            restored_individual_manifests = {}
            for relative_path in individual_manifests:
                logger.debug(f"  Restoring individual manifest: {relative_path}")
                entries, schema = self._restore_manifest_file(relative_path)
                if entries is not None and schema is not None:
                    restored_individual_manifests[relative_path] = {
                        "entries": entries,
                        "schema": schema,
                    }

            # Step 4: Copy data files to new location
            logger.info("Step 4: Copying data files to new location...")
            data_files = self.backup_metadata.get("data_files", [])
            logger.info(f"  Found {len(data_files)} data files to copy")

            self._copy_data_files(data_files, original_location)

            # Step 5: Upload restored metadata to new location
            logger.info("Step 5: Uploading restored metadata to new location...")
            metadata_filename = self._generate_metadata_filename()
            metadata_path = f"{Config.METADATA_DIR}/{metadata_filename}"
            new_metadata_location = f"{self.target_location}/{metadata_path}"

            # Upload metadata file
            bucket, _ = self.s3_client.parse_s3_uri(self.target_location)
            _, key_prefix = self.s3_client.parse_s3_uri(self.target_location)
            metadata_key = f"{key_prefix}/{metadata_path}"

            metadata_content = json.dumps(restored_metadata, indent=2).encode("utf-8")
            if not self.s3_client.write_object(bucket, metadata_key, metadata_content):
                logger.error("Could not upload metadata file")
                return False
            logger.info(f"  Uploaded metadata to {new_metadata_location}")

            # Upload manifest files as Avro
            logger.info("Step 6: Uploading manifest files...")

            # Upload manifest lists
            for relative_path, manifest_data in restored_manifest_lists.items():
                full_path = f"{self.target_location}/{relative_path}"
                bucket, key = self.s3_client.parse_s3_uri(full_path)

                entries = manifest_data["entries"]
                schema = manifest_data["schema"]

                try:
                    # Write Avro with restored paths
                    manifest_content = ManifestFileHandler.write_manifest_list(entries, schema)

                    if not self.s3_client.write_object(bucket, key, manifest_content):
                        logger.warning(f"  Could not upload manifest list {relative_path}")
                    else:
                        logger.debug(f"  Uploaded manifest list: {relative_path}")
                except Exception as e:
                    logger.warning(f"  Could not write manifest list {relative_path}: {e}")
                    logger.error("Exception details:", exc_info=True)

            # Upload individual manifests
            for relative_path, manifest_data in restored_individual_manifests.items():
                full_path = f"{self.target_location}/{relative_path}"
                bucket, key = self.s3_client.parse_s3_uri(full_path)

                entries = manifest_data["entries"]
                schema = manifest_data["schema"]

                try:
                    # Write Avro with restored paths
                    manifest_content = ManifestFileHandler.write_manifest_list(entries, schema)

                    if not self.s3_client.write_object(bucket, key, manifest_content):
                        logger.warning(f"  Could not upload individual manifest {relative_path}")
                    else:
                        logger.debug(f"  Uploaded individual manifest: {relative_path}")
                except Exception as e:
                    logger.warning(f"  Could not write individual manifest {relative_path}: {e}")
                    logger.error("Exception details:", exc_info=True)

            # Step 7: Register table in catalog
            logger.info("Step 7: Registering table in catalog...")
            if not self._register_table(new_metadata_location):
                logger.error("Could not register table in catalog")
                return False

            logger.info(
                f"Successfully restored backup '{self.backup_name}' to {self.target_database}.{self.target_table}"
            )
            logger.info(f"  Table location: {self.target_location}")
            logger.info(f"  Metadata location: {new_metadata_location}")

            # Cleanup
            logger.info("Cleaning up temporary files...")
            shutil.rmtree(self.work_dir, ignore_errors=True)

            return True

        except Exception as e:
            logger.error(f"Error during restore: {e}", exc_info=True)
            return False
        # Note: Not closing spark_client here as it may be shared with other processes
        # The caller or session fixture is responsible for closing the Spark session

    def _download_backup_metadata(self) -> bool:
        """
        Download and parse backup metadata from the backup bucket.

        Retrieves the backup_metadata.json file that contains information about the backup
        including original location, manifest lists, individual manifests, and abstracted metadata.

        Returns:
            True if metadata is successfully downloaded and parsed, False otherwise

        Side Effects:
            Sets self.backup_metadata with the parsed metadata dictionary
        """
        try:
            # Construct backup key including any configured prefix from BACKUP_BUCKET
            if Config.BACKUP_PREFIX:
                backup_key = f"{Config.BACKUP_PREFIX}/{self.backup_name}/backup_metadata.json"
            else:
                backup_key = f"{self.backup_name}/backup_metadata.json"
            content = self.s3_client.read_object(Config.BACKUP_BUCKET, backup_key)

            if not content:
                return False

            self.backup_metadata = json.loads(content.decode("utf-8"))
            return True

        except Exception as e:
            logger.error(f"Error downloading backup metadata: {e}", exc_info=True)
            return False

    def _restore_manifest_file(self, relative_path: str) -> tuple:
        """
        Download and restore paths in a manifest file.

        Downloads a manifest file (in Avro format) from the backup bucket, parses it,
        abstracts paths from the original location, and restores them to the new location.
        Handles both manifest list files and individual manifest files with different path structures.

        Note: Deleted entries (status=2) were already filtered out during backup, so the
        manifests in backup only contain active entries.

        Args:
            relative_path: Relative path to the manifest file within the backup (from backup_metadata.json)

        Returns:
            Tuple of (entries, schema) where:
            - entries: List of manifest entries with restored paths, or None if error
            - schema: Avro schema of the manifest file, or None if error
        """
        try:
            # Download raw Avro from backup
            # Construct backup key including any configured prefix from BACKUP_BUCKET
            if Config.BACKUP_PREFIX:
                backup_key = f"{Config.BACKUP_PREFIX}/{self.backup_name}/{relative_path}"
            else:
                backup_key = f"{self.backup_name}/{relative_path}"
            content = self.s3_client.read_object(Config.BACKUP_BUCKET, backup_key)

            if not content:
                logger.warning(f"  Could not read manifest {relative_path}")
                return None, None

            # Read the Avro file to get entries and schema
            entries, schema = ManifestFileHandler.read_manifest_file(content)

            # The manifest files in backup have absolute paths to the original location
            # First, abstract them to relative paths, then restore them to the new location
            original_location = self.backup_metadata["original_location"]

            # Check if this is a manifest list (has manifest_path) or individual manifest (has data_file)
            if entries and "manifest_path" in entries[0]:
                # This is a manifest list - abstract then restore manifest_path
                # Step 1: Abstract the paths from original location
                abstracted_entries = ManifestFileHandler.abstract_manifest_paths_avro(
                    entries, original_location
                )
                # Step 2: Restore the paths to new location
                restored_entries = ManifestFileHandler.restore_manifest_paths(
                    abstracted_entries, self.target_location
                )
            elif entries and "data_file" in entries[0]:
                # This is an individual manifest - abstract then restore data_file paths
                # Note: Deleted entries were already filtered out during backup
                # Step 1: Abstract the paths from original location
                abstracted_entries = ManifestFileHandler.abstract_manifest_data_paths_avro(
                    entries, original_location
                )
                # Step 2: Restore the paths to new location
                restored_entries = ManifestFileHandler.restore_manifest_data_paths(
                    abstracted_entries, self.target_location
                )
            else:
                restored_entries = entries

            return restored_entries, schema

        except Exception as e:
            logger.error(f"  Error restoring manifest {relative_path}: {e}", exc_info=True)
            return None, None

    def _copy_data_files(self, data_files: List[str], original_location: str) -> None:
        """
        Copy data files from backup location to new target location.

        Copies all data files from the backup bucket to the new table location in S3.
        The data files were copied to the backup during the backup operation, making
        the backup independent of the original table.

        Args:
            data_files: List of relative data file paths (from manifest files)
            original_location: Original S3 location of the table (unused, kept for compatibility)

        Raises:
            RuntimeError: If any data files fail to copy, includes list of up to 5 failed files
        """
        try:
            # Data files are now in the backup location, not the original location
            # Construct source prefix in backup bucket
            if Config.BACKUP_PREFIX:
                backup_source_prefix = f"{Config.BACKUP_PREFIX}/{self.backup_name}"
            else:
                backup_source_prefix = self.backup_name

            target_bucket, target_prefix = self.s3_client.parse_s3_uri(self.target_location)

            copied = 0
            failed_files = []

            for relative_path in data_files:
                # Build source path from backup location
                source_key = f"{backup_source_prefix}/{relative_path}"
                # Build destination path at target location
                dest_key = f"{target_prefix}/{relative_path}".lstrip("/")

                # Copy the file from backup to target
                if self.s3_client.copy_object(
                    Config.BACKUP_BUCKET, source_key, target_bucket, dest_key
                ):
                    copied += 1
                    if copied % 10 == 0:  # Progress update every 10 files
                        logger.debug(f"  Copied {copied}/{len(data_files)} files...")
                else:
                    failed_files.append(relative_path)

            if failed_files:
                error_msg = f"Failed to copy {len(failed_files)} data files: {failed_files[:5]}"
                if len(failed_files) > 5:
                    error_msg += f" ... and {len(failed_files) - 5} more"
                raise RuntimeError(error_msg)

            logger.info(f"  Copied {copied} data files from backup")

        except RuntimeError:
            # Re-raise RuntimeError from failed copies
            raise
        except Exception as e:
            raise RuntimeError(f"Error copying data files: {e}") from e

    def _generate_metadata_filename(self) -> str:
        """
        Generate metadata filename using Iceberg format.

        Iceberg metadata files use the format: {version}-{uuid}.metadata.json
        Example: 00001-bcebdc51-8f37-461d-a300-f6ab0759be07.metadata.json

        For a restored table, we start with version 00001 and generate a new UUID.

        Returns:
            Metadata filename in format '{version}-{uuid}.metadata.json'
        """
        version = "00001"  # Start with version 1 for restored table
        file_uuid = str(uuid.uuid4())
        return f"{version}-{file_uuid}.metadata.json"

    def _register_table(self, metadata_location: str) -> bool:
        """
        Register the restored table in the Iceberg catalog.

        Uses Spark's register_table system procedure to create a new table entry that points
        to the restored metadata file. This makes the table accessible in the catalog.

        Args:
            metadata_location: Full S3 URI to the restored metadata.json file

        Returns:
            True if table registration succeeds, False otherwise
        """
        try:
            # Use Spark to create the table pointing to the restored metadata
            success = self.spark_client.create_iceberg_table_from_metadata(
                database=self.target_database,
                table=self.target_table,
                location=self.target_location,
                metadata_location=metadata_location,
            )

            return success

        except Exception as e:
            logger.error(f"Error registering table in catalog: {e}", exc_info=True)
            return False


def main():
    """Main entry point for restore script"""
    parser = argparse.ArgumentParser(description="Restore an Iceberg table from backup")
    parser.add_argument("--backup-name", required=True, help="Name of the backup to restore")
    parser.add_argument("--target-database", required=True, help="Target database name")
    parser.add_argument("--target-table", required=True, help="Target table name")
    parser.add_argument(
        "--target-location",
        required=True,
        help="Target S3 location for the table (e.g., s3://bucket/warehouse/db/table)",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Catalog name (optional). Falls back to CATALOG_NAME env var, then Config default",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Log level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path",
    )

    args = parser.parse_args()

    # Setup logging
    BCNLogger.setup_logging(level=args.log_level, log_file=args.log_file)

    # Create restore
    restore = IcebergRestore(
        backup_name=args.backup_name,
        target_database=args.target_database,
        target_table=args.target_table,
        target_location=args.target_location,
        catalog=args.catalog,
    )

    success = restore.restore_backup()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
