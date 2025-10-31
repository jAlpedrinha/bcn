"""
Utility for rewriting paths in Iceberg manifest files.
"""

import io
from typing import Optional
import fastavro
from bcn.logging_config import BCNLogger

logger = BCNLogger.get_logger(__name__)

class ManifestRewriter:
    """Rewrites path references in Iceberg manifest Avro files."""

    @staticmethod
    def rewrite_manifest_paths(
        manifest_content: bytes, old_location: str, new_location: str, deleted_files_sizes: dict
    ) -> Optional[bytes]:
        """
        Rewrite path references in a manifest Avro file.

        This updates path references in:
        - lower_bounds (for file_path column field ID 134)
        - upper_bounds (for file_path column field ID 134)
        - data_file.file_path field

        Args:
            manifest_content: Raw bytes of the manifest Avro file
            old_location: Old location prefix (e.g., "s3://bucket/ref_participant")
            new_location: New location prefix (e.g., "s3://bucket/ref_participant_copy")
            deleted_files_sizes: Dictionary mapping deleted file paths to their sizes

        Returns:
            Rewritten manifest content as bytes, or None if no rewriting needed
        """
        try:
            # Read the manifest Avro file
            reader = fastavro.reader(io.BytesIO(manifest_content))
            schema = reader.writer_schema
            records = list(reader)

            if not records:
                return manifest_content

            # Track if any changes were made
            changes_made = False

            # Process each record - each record IS an entry (not wrapped in an "entries" field)
            for record in records:
                # Check if this is a manifest list (has manifest_path) or individual manifest (has data_file)
                if "manifest_path" in record:
                    # This is a manifest list entry - update manifest_path
                    manifest_path = record.get("manifest_path", "")
                    if manifest_path and manifest_path.startswith(old_location):
                        new_path = new_location + manifest_path[len(old_location) :]
                        record["manifest_path"] = new_path
                        changes_made = True
                    continue

                # This is an individual manifest entry - process data_file
                data_file = record.get("data_file", {})

                # Update file_path in data_file
                file_path = data_file.get("file_path", "")
                logger.debug(f"Original file_path: {file_path}")
                if file_path and file_path.startswith(old_location):
                    new_path = new_location + file_path[len(old_location) :]
                    data_file["file_path"] = new_path

                    name_without_prefix = file_path[len(old_location) + 1 :]
                    # Update file_size in data_file if it matches a deleted file
                    logger.debug(f"Is file {name_without_prefix} deleted: {name_without_prefix in deleted_files_sizes}")
                    logger.debug(f"Deleted files sizes: {deleted_files_sizes}")
                    if name_without_prefix in deleted_files_sizes:
                        logger.debug(f"Updating file size for deleted file: {name_without_prefix}")
                        logger.debug(f"\n\tPrevious size {data_file.get('file_size_in_bytes')}, New size: {deleted_files_sizes[name_without_prefix]}")
                        data_file["file_size_in_bytes"] = deleted_files_sizes[name_without_prefix]
                        # Remove statistics fields that are no longer valid after rewriting
                        for key in ["column_sizes", "value_counts", "null_value_counts", "lower_bounds", "upper_bounds", "nan_value_counts"]:
                            data_file.pop(key, None)
                    changes_made = True
                    
                # Update lower_bounds - check all fields for paths (not just field ID 134)
                # Some schemas use different field IDs (e.g., 2147483546)
                lower_bounds = data_file.get("lower_bounds")
                if lower_bounds:
                    for bound in lower_bounds:
                        value = bound.get("value")
                        if value:
                            try:
                                # Decode as UTF-8 string
                                path_str = value.decode("utf-8")
                                if path_str.startswith(old_location):
                                    new_path = new_location + path_str[len(old_location) :]
                                    bound["value"] = new_path.encode("utf-8")
                                    changes_made = True
                            except (UnicodeDecodeError, AttributeError):
                                # If it's already a string, handle it
                                if isinstance(value, str) and value.startswith(old_location):
                                    new_path = new_location + value[len(old_location) :]
                                    bound["value"] = new_path.encode("utf-8")
                                    changes_made = True

                # Update upper_bounds - check all fields for paths (not just field ID 134)
                # Some schemas use different field IDs (e.g., 2147483546)
                upper_bounds = data_file.get("upper_bounds")
                if upper_bounds:
                    for bound in upper_bounds:
                        value = bound.get("value")
                        if value:
                            try:
                                # Decode as UTF-8 string
                                path_str = value.decode("utf-8")
                                if path_str.startswith(old_location):
                                    new_path = new_location + path_str[len(old_location) :]
                                    bound["value"] = new_path.encode("utf-8")
                                    changes_made = True
                            except (UnicodeDecodeError, AttributeError):
                                # If it's already a string, handle it
                                if isinstance(value, str) and value.startswith(old_location):
                                    new_path = new_location + value[len(old_location) :]
                                    bound["value"] = new_path.encode("utf-8")
                                    changes_made = True

            if not changes_made:
                return manifest_content

            # Write the updated manifest back to bytes
            output = io.BytesIO()
            fastavro.writer(output, schema, records)
            return output.getvalue()

        except Exception as e:
            logger.error(f"Error rewriting manifest paths: {e}", exc_info=True)
            return None
