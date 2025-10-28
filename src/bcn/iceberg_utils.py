"""
Iceberg metadata utilities for parsing and modifying Iceberg table files
"""

import copy
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import avro.datafile
import avro.io
import avro.schema

if TYPE_CHECKING:
    from bcn.s3_client import S3Client


class PathAbstractor:
    """Handles path abstraction and restoration for backup/restore"""

    @staticmethod
    def abstract_path(full_path: str, table_location: str) -> str:
        """
        Abstract a full path by removing the table location prefix

        Args:
            full_path: Full path (e.g., s3://bucket/warehouse/db/table/metadata/snap-123.avro)
            table_location: Table location (e.g., s3://bucket/warehouse/db/table)

        Returns:
            Abstracted relative path (e.g., metadata/snap-123.avro)
        """
        # Normalize paths - remove trailing slashes
        table_location = table_location.rstrip("/")
        full_path = full_path.rstrip("/")

        # If the path starts with the table location, remove it
        if full_path.startswith(table_location):
            relative_path = full_path[len(table_location) :].lstrip("/")
            return relative_path

        # If paths don't match, return as-is (might be a relative path already)
        return full_path

    @staticmethod
    def restore_path(relative_path: str, new_table_location: str) -> str:
        """
        Restore a full path from relative path and new table location

        Args:
            relative_path: Relative path (e.g., metadata/snap-123.avro)
            new_table_location: New table location (e.g., s3://bucket/warehouse/db/new_table)

        Returns:
            Full path with new table location
        """
        new_table_location = new_table_location.rstrip("/")
        relative_path = relative_path.lstrip("/")
        return f"{new_table_location}/{relative_path}"

    @staticmethod
    def resolve_path(path: str, base_location: str) -> str:
        """
        Resolve a potentially relative path to a full S3 URI

        Args:
            path: Path that may be relative or absolute
            base_location: Base location for resolving relative paths

        Returns:
            Full S3 URI
        """
        # Normalize s3a:// to s3://
        if path.startswith("s3a://"):
            return "s3://" + path[6:]

        # Already absolute
        if path.startswith("s3://"):
            return path

        # Relative path - combine with base
        base_location = base_location.rstrip("/")
        path = path.lstrip("/")
        return f"{base_location}/{path}"

    @staticmethod
    def abstract_metadata_file(metadata_content: Dict, table_location: str) -> Dict:
        """
        Abstract paths in main metadata file and keep only current snapshot.

        For a restored table, we treat it as a new table with only the current snapshot,
        not the entire snapshot history. This ensures the restored table is clean and
        doesn't reference snapshots or metadata files that weren't migrated.

        Args:
            metadata_content: Parsed metadata JSON content
            table_location: Table location to abstract

        Returns:
            Modified metadata with abstracted paths and only current snapshot
        """
        abstracted = copy.deepcopy(metadata_content)

        # Abstract location
        if "location" in abstracted:
            abstracted["location"] = ""  # Will be set during restore

        # Keep only the current snapshot, discard snapshot history
        current_snapshot_id = abstracted.get("current-snapshot-id")
        if current_snapshot_id and "snapshots" in abstracted:
            # Find the current snapshot
            current_snapshot = None
            for snapshot in abstracted["snapshots"]:
                if snapshot.get("snapshot-id") == current_snapshot_id:
                    current_snapshot = snapshot
                    break

            if current_snapshot:
                # Abstract the manifest-list path in current snapshot
                if "manifest-list" in current_snapshot:
                    current_snapshot["manifest-list"] = PathAbstractor.abstract_path(
                        current_snapshot["manifest-list"], table_location
                    )
                # Keep only the current snapshot
                abstracted["snapshots"] = [current_snapshot]
            else:
                # If we can't find current snapshot, keep all and abstract paths
                # This shouldn't happen but is a safety fallback
                for snapshot in abstracted["snapshots"]:
                    if "manifest-list" in snapshot:
                        snapshot["manifest-list"] = PathAbstractor.abstract_path(
                            snapshot["manifest-list"], table_location
                        )

        # Clear snapshot log (no history for restored table)
        abstracted["snapshot-log"] = []

        # Clear metadata log (no history for restored table)
        abstracted["metadata-log"] = []

        return abstracted


class ManifestFileHandler:
    """Handles reading and writing Avro manifest files"""

    @staticmethod
    def write_manifest_list(entries: List[Dict], schema: Any) -> bytes:
        """
        Write manifest entries to Avro format

        Args:
            entries: List of manifest entries
            schema: Avro schema

        Returns:
            Binary content of manifest list file
        """
        output = BytesIO()
        writer = avro.datafile.DataFileWriter(output, avro.io.DatumWriter(), schema)
        for entry in entries:
            writer.append(entry)
        writer.flush()
        # Get the value before closing
        result = output.getvalue()
        writer.close()
        return result

    @staticmethod
    def _convert_bytes_to_str(obj: Any) -> Any:
        """
        Recursively convert bytes to base64 strings for JSON serialization
        """
        import base64

        if isinstance(obj, bytes):
            return {"__type__": "bytes", "__value__": base64.b64encode(obj).decode("utf-8")}
        elif isinstance(obj, dict):
            return {k: ManifestFileHandler._convert_bytes_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ManifestFileHandler._convert_bytes_to_str(item) for item in obj]
        else:
            return obj

    @staticmethod
    def restore_manifest_paths(entries: List[Dict], new_table_location: str) -> List[Dict]:
        """
        Restore paths in manifest list entries

        Args:
            entries: List of manifest entries with abstracted paths
            new_table_location: New table location

        Returns:
            Modified manifest entries with restored paths
        """
        import copy

        restored = []
        for entry in entries:
            entry_copy = copy.deepcopy(entry)
            if "manifest_path" in entry_copy:
                entry_copy["manifest_path"] = PathAbstractor.restore_path(
                    entry_copy["manifest_path"], new_table_location
                )
            restored.append(entry_copy)
        return restored

    @staticmethod
    def abstract_manifest_paths_avro(entries: List[Dict], table_location: str) -> List[Dict]:
        """
        Abstract paths in manifest list entries (Avro format, no bytes conversion)

        Args:
            entries: List of manifest entries
            table_location: Table location to abstract

        Returns:
            Modified manifest entries with abstracted paths
        """
        import copy

        abstracted = []
        for entry in entries:
            entry_copy = copy.deepcopy(entry)
            if "manifest_path" in entry_copy:
                entry_copy["manifest_path"] = PathAbstractor.abstract_path(
                    entry_copy["manifest_path"], table_location
                )
            abstracted.append(entry_copy)
        return abstracted

    @staticmethod
    def read_manifest_file(content: bytes) -> Tuple[List[Dict], Any]:
        """
        Read an Avro manifest file

        Args:
            content: Binary content of manifest file

        Returns:
            Tuple of (list of data file entries, schema)
        """
        entries = []
        schema = None
        with BytesIO(content) as bio:
            reader = avro.datafile.DataFileReader(bio, avro.io.DatumReader())
            # Get the schema from the Avro file's metadata
            # The schema is stored in the meta dictionary with key 'avro.schema'
            schema_str = reader.meta.get("avro.schema")
            if schema_str:
                schema = avro.schema.parse(
                    schema_str.decode("utf-8") if isinstance(schema_str, bytes) else schema_str
                )
            for record in reader:
                entries.append(record)
            reader.close()
        return entries, schema

    @staticmethod
    def read_manifest_from_s3(
        s3_client: "S3Client",
        manifest_path: str,
        table_location: str
    ) -> Tuple[Optional[List[Dict]], Optional[Any]]:
        """
        Read manifest file from S3

        Args:
            s3_client: S3 client instance
            manifest_path: Full or relative manifest path
            table_location: Table location for resolving relative paths

        Returns:
            Tuple of (entries, schema) or (None, None) on error
        """
        from bcn.logging_config import BCNLogger

        logger = BCNLogger.get_logger(__name__)

        # Convert relative paths to full S3 URIs
        if not manifest_path.startswith("s3://") and not manifest_path.startswith("s3a://"):
            full_path = PathAbstractor.resolve_path(manifest_path, table_location)
        else:
            full_path = manifest_path

        try:
            bucket, key = s3_client.parse_s3_uri(full_path)
            content = s3_client.read_object(bucket, key)
            if not content:
                logger.warning(f"Empty content for manifest: {manifest_path}")
                return None, None

            return ManifestFileHandler.read_manifest_file(content)
        except Exception as e:
            logger.error(f"Error reading manifest {manifest_path}: {e}")
            return None, None

    @staticmethod
    def abstract_manifest_data_paths(entries: List[Dict], table_location: str) -> List[Dict]:
        """
        Abstract data file paths in manifest entries

        Args:
            entries: List of manifest data entries
            table_location: Table location to abstract

        Returns:
            Modified manifest entries with abstracted data file paths
        """
        abstracted = []
        for entry in entries:
            # Convert bytes to base64 strings for JSON serialization
            entry_copy = ManifestFileHandler._convert_bytes_to_str(entry.copy())

            if "data_file" in entry_copy and "file_path" in entry_copy["data_file"]:
                entry_copy["data_file"]["file_path"] = PathAbstractor.abstract_path(
                    entry_copy["data_file"]["file_path"], table_location
                )
            abstracted.append(entry_copy)
        return abstracted

    @staticmethod
    def restore_manifest_data_paths(entries: List[Dict], new_table_location: str) -> List[Dict]:
        """
        Restore data file paths in manifest entries

        Args:
            entries: List of manifest data entries with abstracted paths
            new_table_location: New table location

        Returns:
            Modified manifest entries with restored data file paths
        """
        import copy

        restored = []
        for entry in entries:
            entry_copy = copy.deepcopy(entry)
            if "data_file" in entry_copy and "file_path" in entry_copy["data_file"]:
                entry_copy["data_file"]["file_path"] = PathAbstractor.restore_path(
                    entry_copy["data_file"]["file_path"], new_table_location
                )
            restored.append(entry_copy)
        return restored

    @staticmethod
    def abstract_manifest_data_paths_avro(entries: List[Dict], table_location: str) -> List[Dict]:
        """
        Abstract data file paths in manifest entries (Avro format, no bytes conversion)

        Args:
            entries: List of manifest data entries
            table_location: Table location to abstract

        Returns:
            Modified manifest entries with abstracted data file paths
        """
        import copy

        abstracted = []
        for entry in entries:
            entry_copy = copy.deepcopy(entry)
            if "data_file" in entry_copy and "file_path" in entry_copy["data_file"]:
                entry_copy["data_file"]["file_path"] = PathAbstractor.abstract_path(
                    entry_copy["data_file"]["file_path"], table_location
                )
            abstracted.append(entry_copy)
        return abstracted
