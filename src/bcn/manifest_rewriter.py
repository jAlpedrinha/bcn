"""
Utility for rewriting paths in Iceberg manifest files.
"""

import io
from typing import Optional
import fastavro


class ManifestRewriter:
    """Rewrites path references in Iceberg manifest Avro files."""

    @staticmethod
    def rewrite_manifest_paths(
        manifest_content: bytes, old_location: str, new_location: str
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
                if file_path and file_path.startswith(old_location):
                    new_path = new_location + file_path[len(old_location) :]
                    data_file["file_path"] = new_path
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
            print(f"Error rewriting manifest paths: {e}")
            return None
