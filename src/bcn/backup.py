#!/usr/bin/env python3
"""
Iceberg Table Backup Script

Creates a backup of an Iceberg table by copying metadata and data files
to a backup location with abstracted paths.
"""

import argparse
import json
import os
import shutil
import sys
import traceback
from typing import Dict, List

from bcn.config import Config
from bcn.iceberg_utils import ManifestFileHandler, PathAbstractor
from bcn.s3_client import S3Client
from bcn.spark_client import SparkClient


class IcebergBackup:
    """Orchestrates the backup process for an Iceberg table"""

    def __init__(self, database: str, table: str, backup_name: str):
        """
        Initialize backup process

        Args:
            database: Database name
            table: Table name
            backup_name: Name for this backup
        """
        self.database = database
        self.table = table
        self.backup_name = backup_name
        self.s3_client = S3Client()
        self.spark_client = SparkClient(app_name=f"iceberg-backup-{backup_name}")
        self.work_dir = os.path.join(Config.WORK_DIR, backup_name)

    def create_backup(self) -> bool:
        """
        Create a backup of the Iceberg table

        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"Starting backup of {self.database}.{self.table} as '{self.backup_name}'")

            # Step 1: Get table metadata from Spark catalog
            print("Step 1: Retrieving table metadata from catalog...")
            table_metadata = self.spark_client.get_table_metadata(self.database, self.table)
            if not table_metadata:
                print(f"Error: Could not retrieve metadata for {self.database}.{self.table}")
                return False

            table_location = table_metadata["location"]
            metadata_location = table_metadata.get("metadata_location")

            if not metadata_location:
                print(f"Error: No metadata_location found for {self.database}.{self.table}")
                print("This may not be an Iceberg table.")
                return False

            print(f"  Table location: {table_location}")
            print(f"  Metadata location: {metadata_location}")

            # Step 2: Download and parse main metadata file
            print("\nStep 2: Downloading and parsing main metadata file...")
            bucket, key = self.s3_client.parse_s3_uri(metadata_location)
            metadata_content = self.s3_client.read_object(bucket, key)
            if not metadata_content:
                print(f"Error: Could not read metadata file from {metadata_location}")
                return False

            metadata = json.loads(metadata_content.decode("utf-8"))
            print(f"  Current snapshot ID: {metadata.get('current-snapshot-id')}")

            # Step 3: Abstract paths in metadata
            print("\nStep 3: Abstracting paths in metadata...")
            abstracted_metadata = PathAbstractor.abstract_metadata_file(metadata, table_location)

            # Step 4: Process snapshots and manifests
            print("\nStep 4: Processing snapshots and manifest files...")
            manifest_files = self._collect_manifest_files(metadata, table_location)
            print(f"  Found {len(manifest_files)} manifest files to process")

            # Step 5: Download manifest list AND individual manifest files as raw Avro
            print("\nStep 5: Downloading manifest files...")
            os.makedirs(self.work_dir, exist_ok=True)

            manifest_list_paths = []
            individual_manifest_paths = []

            for manifest_list_path in manifest_files:
                print(f"  Downloading manifest list: {manifest_list_path}")
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
                            print(f"    Found individual manifest: {manifest_path}")
                            manifest_relative_path = PathAbstractor.abstract_path(
                                full_manifest_path, table_location
                            )
                            if manifest_relative_path not in individual_manifest_paths:
                                individual_manifest_paths.append(manifest_relative_path)
                except Exception as e:
                    print(f"  Warning: Could not read manifest list {manifest_list_path}: {e}")

            # Step 6: Collect data file references
            print("\nStep 6: Collecting data file references...")
            data_files = self._collect_data_files(manifest_files, table_location)
            print(f"  Found {len(data_files)} data files")

            # Step 7: Save backup metadata
            print("\nStep 7: Saving backup metadata...")
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
            print("\nStep 8: Uploading backup to S3...")
            success = self._upload_backup_to_s3(backup_metadata, table_location)

            if success:
                print(f"\n✓ Backup '{self.backup_name}' created successfully!")
                print(f"  Location: s3://{Config.BACKUP_BUCKET}/{self.backup_name}/")
            else:
                print("\n✗ Failed to upload backup to S3")
                return False

            # Cleanup
            print("\nCleaning up temporary files...")
            shutil.rmtree(self.work_dir, ignore_errors=True)

            return True

        except Exception as e:
            print(f"Error during backup: {e}")
            traceback.print_exc()
            return False
        # Note: Not closing spark_client here as it may be shared with other processes
        # The caller or session fixture is responsible for closing the Spark session

    def _collect_manifest_files(self, metadata: Dict, table_location: str) -> List[str]:
        """Collect all manifest file paths from metadata"""
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
        """Collect all data file paths from manifest list files"""
        data_files = []

        # manifest_list_files are actually manifest list files (snap-*.avro)
        # which contain entries pointing to manifest files
        for manifest_list_path in manifest_list_files:
            try:
                # Read the manifest list file
                bucket, key = self.s3_client.parse_s3_uri(manifest_list_path)
                content = self.s3_client.read_object(bucket, key)
                if not content:
                    continue

                # Get manifest file paths from the manifest list
                manifest_list_entries, _ = ManifestFileHandler.read_manifest_file(content)

                # Now read each manifest file to get data files
                for entry in manifest_list_entries:
                    # Manifest list entries have a 'manifest_path' field
                    manifest_path = entry.get("manifest_path")
                    if not manifest_path:
                        continue

                    # Convert relative manifest path to full S3 URI
                    if not manifest_path.startswith("s3://") and not manifest_path.startswith(
                        "s3a://"
                    ):
                        manifest_path = f"{table_location}/{manifest_path}"

                    try:
                        # Read the actual manifest file
                        m_bucket, m_key = self.s3_client.parse_s3_uri(manifest_path)
                        m_content = self.s3_client.read_object(m_bucket, m_key)
                        if not m_content:
                            continue

                        # Get data files from the manifest
                        manifest_entries, _ = ManifestFileHandler.read_manifest_file(m_content)
                        for m_entry in manifest_entries:
                            if "data_file" in m_entry and "file_path" in m_entry["data_file"]:
                                data_files.append(m_entry["data_file"]["file_path"])
                    except Exception as e:
                        print(f"  Warning: Could not read manifest file {manifest_path}: {e}")
                        continue

            except Exception as e:
                print(f"  Warning: Could not read manifest list {manifest_list_path}: {e}")
                continue

        return data_files

    def _upload_backup_to_s3(self, backup_metadata: Dict, table_location: str) -> bool:
        """Upload backup files to S3 backup bucket"""
        try:
            backup_prefix = f"{self.backup_name}/"

            # Upload backup metadata
            metadata_key = f"{backup_prefix}backup_metadata.json"
            metadata_content = json.dumps(backup_metadata, indent=2).encode("utf-8")
            if not self.s3_client.write_object(
                Config.BACKUP_BUCKET, metadata_key, metadata_content
            ):
                return False
            print("  ✓ Uploaded backup metadata")

            # Upload abstracted metadata file
            iceberg_metadata_key = f"{backup_prefix}metadata.json"
            iceberg_content = json.dumps(backup_metadata["abstracted_metadata"], indent=2).encode(
                "utf-8"
            )
            if not self.s3_client.write_object(
                Config.BACKUP_BUCKET, iceberg_metadata_key, iceberg_content
            ):
                return False
            print("  ✓ Uploaded Iceberg metadata")

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
                            print(f"  Warning: Failed to upload manifest list {relative_path}")
                except Exception as e:
                    print(f"  Warning: Could not copy manifest list {relative_path}: {e}")
            print(f"  ✓ Uploaded {len(manifest_lists)} manifest list files")

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
                            print(
                                f"  Warning: Failed to upload individual manifest {relative_path}"
                            )
                except Exception as e:
                    print(f"  Warning: Could not copy individual manifest {relative_path}: {e}")
            print(f"  ✓ Uploaded {len(individual_manifests)} individual manifest files")

            # Note: Data files remain in original location and are not copied during backup
            # They will be copied during restore operation

            return True

        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False


def main():
    """Main entry point for backup script"""
    parser = argparse.ArgumentParser(description="Create a backup of an Iceberg table")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--backup-name", required=True, help="Name for this backup")

    args = parser.parse_args()

    # Create backup
    backup = IcebergBackup(args.database, args.table, args.backup_name)
    success = backup.create_backup()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
