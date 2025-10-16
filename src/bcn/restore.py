#!/usr/bin/env python3
"""
Iceberg Table Restore Script

Restores an Iceberg table from a backup to a new table location.
"""
import argparse
import json
import os
import shutil
import sys
from typing import List

from bcn.config import Config
from bcn.iceberg_utils import ManifestFileHandler, PathAbstractor
from bcn.s3_client import S3Client
from bcn.spark_client import SparkClient


class IcebergRestore:
    """Orchestrates the restore process for an Iceberg table"""

    def __init__(self, backup_name: str, target_database: str, target_table: str,
                 target_location: str, catalog_type: str = 'hive'):
        """
        Initialize restore process

        Args:
            backup_name: Name of the backup to restore
            target_database: Target database name
            target_table: Target table name
            target_location: Target S3 location for the table
            catalog_type: Type of catalog (hive or glue) - currently only hive supported
        """
        self.backup_name = backup_name
        self.target_database = target_database
        self.target_table = target_table
        self.target_location = target_location.rstrip('/')
        self.catalog_type = catalog_type
        self.s3_client = S3Client()
        self.spark_client = SparkClient(app_name=f"iceberg-restore-{backup_name}")
        self.work_dir = os.path.join(Config.WORK_DIR, f"restore_{backup_name}")
        self.backup_metadata = None

    def restore_backup(self) -> bool:
        """
        Restore a backup to a new table

        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"Starting restore of backup '{self.backup_name}' to {self.target_database}.{self.target_table}")

            # Step 1: Download backup metadata
            print("\nStep 1: Downloading backup metadata...")
            if not self._download_backup_metadata():
                print("Error: Could not download backup metadata")
                return False

            original_location = self.backup_metadata['original_location']
            print(f"  Original location: {original_location}")
            print(f"  Target location: {self.target_location}")

            # Step 2: Restore paths in metadata
            print("\nStep 2: Restoring paths in metadata...")
            abstracted_metadata = self.backup_metadata['abstracted_metadata']
            restored_metadata = PathAbstractor.abstract_metadata_file(
                abstracted_metadata.copy(), ''
            )

            # Set new location
            restored_metadata['location'] = self.target_location

            # Restore snapshot manifest paths
            for snapshot in restored_metadata.get('snapshots', []):
                if 'manifest-list' in snapshot:
                    snapshot['manifest-list'] = PathAbstractor.restore_path(
                        snapshot['manifest-list'], self.target_location
                    )

            # Restore metadata log paths
            if 'metadata-log' in restored_metadata:
                for entry in restored_metadata['metadata-log']:
                    if 'metadata-file' in entry:
                        entry['metadata-file'] = PathAbstractor.restore_path(
                            entry['metadata-file'], self.target_location
                        )

            # Step 3: Process and restore manifest files
            print("\nStep 3: Processing manifest files...")

            # Handle both old format (manifest_files) and new format (manifest_lists + individual_manifests)
            manifest_lists = self.backup_metadata.get('manifest_lists', self.backup_metadata.get('manifest_files', []))
            individual_manifests = self.backup_metadata.get('individual_manifests', [])

            print(f"  Found {len(manifest_lists)} manifest lists and {len(individual_manifests)} individual manifests to process")

            restored_manifest_lists = {}
            for relative_path in manifest_lists:
                print(f"  Restoring manifest list: {relative_path}")
                entries, schema = self._restore_manifest_file(relative_path)
                if entries is not None and schema is not None:
                    restored_manifest_lists[relative_path] = {'entries': entries, 'schema': schema}

            restored_individual_manifests = {}
            for relative_path in individual_manifests:
                print(f"  Restoring individual manifest: {relative_path}")
                entries, schema = self._restore_manifest_file(relative_path)
                if entries is not None and schema is not None:
                    restored_individual_manifests[relative_path] = {'entries': entries, 'schema': schema}

            # Step 4: Copy data files to new location
            print("\nStep 4: Copying data files to new location...")
            data_files = self.backup_metadata.get('data_files', [])
            print(f"  Found {len(data_files)} data files to copy")

            if not self._copy_data_files(data_files, original_location):
                print("Warning: Some data files could not be copied")

            # Step 5: Upload restored metadata to new location
            print("\nStep 5: Uploading restored metadata to new location...")
            metadata_filename = self._generate_metadata_filename()
            metadata_path = f"metadata/{metadata_filename}"
            new_metadata_location = f"{self.target_location}/{metadata_path}"

            # Upload metadata file
            bucket, _ = self.s3_client.parse_s3_uri(self.target_location)
            _, key_prefix = self.s3_client.parse_s3_uri(self.target_location)
            metadata_key = f"{key_prefix}/{metadata_path}"

            metadata_content = json.dumps(restored_metadata, indent=2).encode('utf-8')
            if not self.s3_client.write_object(bucket, metadata_key, metadata_content):
                print("Error: Could not upload metadata file")
                return False
            print(f"  ✓ Uploaded metadata to {new_metadata_location}")

            # Upload manifest files as Avro
            print("\nStep 6: Uploading manifest files...")

            # Upload manifest lists
            for relative_path, manifest_data in restored_manifest_lists.items():
                full_path = f"{self.target_location}/{relative_path}"
                bucket, key = self.s3_client.parse_s3_uri(full_path)

                entries = manifest_data['entries']
                schema = manifest_data['schema']

                try:
                    # Write Avro with restored paths
                    manifest_content = ManifestFileHandler.write_manifest_list(entries, schema)

                    if not self.s3_client.write_object(bucket, key, manifest_content):
                        print(f"  Warning: Could not upload manifest list {relative_path}")
                    else:
                        print(f"  ✓ Uploaded manifest list: {relative_path}")
                except Exception as e:
                    print(f"  Warning: Could not write manifest list {relative_path}: {e}")
                    import traceback
                    traceback.print_exc()

            # Upload individual manifests
            for relative_path, manifest_data in restored_individual_manifests.items():
                full_path = f"{self.target_location}/{relative_path}"
                bucket, key = self.s3_client.parse_s3_uri(full_path)

                entries = manifest_data['entries']
                schema = manifest_data['schema']

                try:
                    # Write Avro with restored paths
                    manifest_content = ManifestFileHandler.write_manifest_list(entries, schema)

                    if not self.s3_client.write_object(bucket, key, manifest_content):
                        print(f"  Warning: Could not upload individual manifest {relative_path}")
                    else:
                        print(f"  ✓ Uploaded individual manifest: {relative_path}")
                except Exception as e:
                    print(f"  Warning: Could not write individual manifest {relative_path}: {e}")
                    import traceback
                    traceback.print_exc()

            # Step 7: Register table in catalog
            print("\nStep 7: Registering table in catalog...")
            if not self._register_table(new_metadata_location):
                print("Error: Could not register table in catalog")
                return False

            print(f"\n✓ Successfully restored backup '{self.backup_name}' to {self.target_database}.{self.target_table}")
            print(f"  Table location: {self.target_location}")
            print(f"  Metadata location: {new_metadata_location}")

            # Cleanup
            print("\nCleaning up temporary files...")
            shutil.rmtree(self.work_dir, ignore_errors=True)

            return True

        except Exception as e:
            print(f"Error during restore: {e}")
            import traceback
            traceback.print_exc()
            return False
        # Note: Not closing spark_client here as it may be shared with other processes
        # The caller or session fixture is responsible for closing the Spark session

    def _download_backup_metadata(self) -> bool:
        """Download and parse backup metadata from S3"""
        try:
            backup_key = f"{self.backup_name}/backup_metadata.json"
            content = self.s3_client.read_object(Config.BACKUP_BUCKET, backup_key)

            if not content:
                return False

            self.backup_metadata = json.loads(content.decode('utf-8'))
            return True

        except Exception as e:
            print(f"Error downloading backup metadata: {e}")
            return False

    def _restore_manifest_file(self, relative_path: str) -> tuple:
        """Download and restore paths in a manifest file (raw Avro)"""
        try:
            # Download raw Avro from backup
            backup_key = f"{self.backup_name}/{relative_path}"
            content = self.s3_client.read_object(Config.BACKUP_BUCKET, backup_key)

            if not content:
                print(f"  Warning: Could not read manifest {relative_path}")
                return None, None

            # Read the Avro file to get entries and schema
            entries, schema = ManifestFileHandler.read_manifest_file(content)

            # The manifest files in backup have absolute paths to the original location
            # First, abstract them to relative paths, then restore them to the new location
            original_location = self.backup_metadata['original_location']

            # Check if this is a manifest list (has manifest_path) or individual manifest (has data_file)
            if entries and 'manifest_path' in entries[0]:
                # This is a manifest list - abstract then restore manifest_path
                # Step 1: Abstract the paths from original location
                abstracted_entries = ManifestFileHandler.abstract_manifest_paths_avro(
                    entries, original_location
                )
                # Step 2: Restore the paths to new location
                restored_entries = ManifestFileHandler.restore_manifest_paths(
                    abstracted_entries, self.target_location
                )
            elif entries and 'data_file' in entries[0]:
                # This is an individual manifest - abstract then restore data_file paths
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
            print(f"  Error restoring manifest {relative_path}: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    def _copy_data_files(self, data_files: List[str], original_location: str) -> bool:
        """Copy data files from original location to new location"""
        try:
            # Parse original location
            orig_bucket, orig_prefix = self.s3_client.parse_s3_uri(original_location)
            target_bucket, target_prefix = self.s3_client.parse_s3_uri(self.target_location)

            copied = 0
            failed = 0

            for relative_path in data_files:
                # Build source and destination paths
                source_key = f"{orig_prefix}/{relative_path}".lstrip('/')
                dest_key = f"{target_prefix}/{relative_path}".lstrip('/')

                # Copy the file
                if self.s3_client.copy_object(orig_bucket, source_key, target_bucket, dest_key):
                    copied += 1
                    if copied % 10 == 0:  # Progress update every 10 files
                        print(f"  Copied {copied}/{len(data_files)} files...")
                else:
                    failed += 1
                    print(f"  Warning: Could not copy {relative_path}")

            print(f"  ✓ Copied {copied} data files ({failed} failed)")
            return failed == 0

        except Exception as e:
            print(f"Error copying data files: {e}")
            return False

    def _generate_metadata_filename(self) -> str:
        """Generate a metadata filename"""
        import time
        timestamp = int(time.time() * 1000)
        return f"v{timestamp}.metadata.json"

    def _register_table(self, metadata_location: str) -> bool:
        """Register the restored table in the catalog using Spark"""
        try:
            # Use Spark to create the table pointing to the restored metadata
            success = self.spark_client.create_iceberg_table_from_metadata(
                database=self.target_database,
                table=self.target_table,
                location=self.target_location,
                metadata_location=metadata_location
            )

            return success

        except Exception as e:
            print(f"Error registering table in catalog: {e}")
            return False


def main():
    """Main entry point for restore script"""
    parser = argparse.ArgumentParser(
        description='Restore an Iceberg table from backup'
    )
    parser.add_argument(
        '--backup-name',
        required=True,
        help='Name of the backup to restore'
    )
    parser.add_argument(
        '--target-database',
        required=True,
        help='Target database name'
    )
    parser.add_argument(
        '--target-table',
        required=True,
        help='Target table name'
    )
    parser.add_argument(
        '--target-location',
        required=True,
        help='Target S3 location for the table (e.g., s3://bucket/warehouse/db/table)'
    )
    parser.add_argument(
        '--catalog-type',
        default='hive',
        choices=['hive', 'glue'],
        help='Catalog type (default: hive)'
    )

    args = parser.parse_args()

    # Create restore
    restore = IcebergRestore(
        backup_name=args.backup_name,
        target_database=args.target_database,
        target_table=args.target_table,
        target_location=args.target_location,
        catalog_type=args.catalog_type
    )

    success = restore.restore_backup()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
