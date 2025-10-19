"""
Change Detection for Incremental Backups

Detects what changed between two table snapshots by comparing
metadata, schemas, and data files.
"""

from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass
import json


@dataclass
class TableSnapshot:
    """Snapshot of table state at a point in time."""
    data_files: Set[str]  # Set of file paths
    metadata: Dict[str, Any]  # Iceberg metadata
    schema: Dict[str, Any]  # Table schema


class ChangeDetector:
    """Detects changes between table snapshots."""

    @staticmethod
    def compare_snapshots(
        previous_snapshot: Optional[TableSnapshot],
        current_snapshot: TableSnapshot,
    ) -> Tuple[Dict[str, Tuple[str, int]], List[str], Dict[str, Tuple[str, int]]]:
        """
        Compare two table snapshots and identify changes.

        Args:
            previous_snapshot: Previous snapshot (None for first backup)
            current_snapshot: Current snapshot

        Returns:
            Tuple of (added_files, deleted_files, modified_files)
            where:
            - added_files: Dict {path: (checksum, size)}
            - deleted_files: List of paths
            - modified_files: Dict {path: (checksum, size)}
        """
        if previous_snapshot is None:
            # First backup: all files are added
            added_files = ChangeDetector._extract_file_info(current_snapshot)
            return (added_files, [], {})

        previous_files = previous_snapshot.data_files
        current_files = current_snapshot.data_files

        # Identify added and deleted files
        added_file_paths = current_files - previous_files
        deleted_file_paths = previous_files - current_files
        potentially_modified = current_files & previous_files

        # Extract info for added and deleted
        added_files = ChangeDetector._extract_file_info_for_paths(
            current_snapshot, added_file_paths
        )
        deleted_files = list(deleted_file_paths)

        # Detect modifications (same path, different checksum)
        modified_files = ChangeDetector._detect_modified_files(
            previous_snapshot, current_snapshot, potentially_modified
        )

        return (added_files, deleted_files, modified_files)

    @staticmethod
    def compare_schemas(
        previous_schema: Optional[Dict[str, Any]],
        current_schema: Dict[str, Any],
    ) -> bool:
        """
        Check if schemas have changed.

        Args:
            previous_schema: Previous schema (None for first backup)
            current_schema: Current schema

        Returns:
            True if schemas are different
        """
        if previous_schema is None:
            return False  # No change for first backup

        # Normalize and compare
        prev_normalized = json.dumps(previous_schema, sort_keys=True)
        curr_normalized = json.dumps(current_schema, sort_keys=True)

        return prev_normalized != curr_normalized

    @staticmethod
    def compare_properties(
        previous_props: Optional[Dict[str, Any]],
        current_props: Dict[str, Any],
    ) -> bool:
        """
        Check if table properties have changed.

        Args:
            previous_props: Previous properties (None for first backup)
            current_props: Current properties

        Returns:
            True if properties are different
        """
        if previous_props is None:
            return False  # No change for first backup

        # Normalize and compare
        prev_normalized = json.dumps(previous_props, sort_keys=True)
        curr_normalized = json.dumps(current_props, sort_keys=True)

        return prev_normalized != curr_normalized

    @staticmethod
    def _extract_file_info(
        snapshot: TableSnapshot,
    ) -> Dict[str, Tuple[str, int]]:
        """
        Extract file info (checksum, size) from snapshot.

        Args:
            snapshot: Table snapshot

        Returns:
            Dict {path: (checksum, size)}
        """
        result = {}
        metadata = snapshot.metadata

        # Extract from Iceberg metadata structure
        # This assumes metadata has been processed to include file info
        if "file_info" in metadata:
            result = metadata["file_info"].copy()

        return result

    @staticmethod
    def _extract_file_info_for_paths(
        snapshot: TableSnapshot,
        file_paths: Set[str],
    ) -> Dict[str, Tuple[str, int]]:
        """
        Extract file info for specific file paths.

        Args:
            snapshot: Table snapshot
            file_paths: Set of file paths to extract

        Returns:
            Dict {path: (checksum, size)} for files in file_paths
        """
        result = {}
        metadata = snapshot.metadata

        if "file_info" in metadata:
            all_file_info = metadata["file_info"]
            for path in file_paths:
                if path in all_file_info:
                    result[path] = all_file_info[path]

        return result

    @staticmethod
    def _detect_modified_files(
        previous_snapshot: TableSnapshot,
        current_snapshot: TableSnapshot,
        potentially_modified: Set[str],
    ) -> Dict[str, Tuple[str, int]]:
        """
        Detect which files have been modified (same path, different content).

        Args:
            previous_snapshot: Previous snapshot
            current_snapshot: Current snapshot
            potentially_modified: Set of file paths that could be modified

        Returns:
            Dict {path: (checksum, size)} for modified files
        """
        modified = {}

        prev_file_info = previous_snapshot.metadata.get("file_info", {})
        curr_file_info = current_snapshot.metadata.get("file_info", {})

        for file_path in potentially_modified:
            prev_checksum = prev_file_info.get(file_path, {}).get("checksum") if isinstance(
                prev_file_info.get(file_path), dict
            ) else None
            curr_checksum = curr_file_info.get(file_path, {}).get("checksum") if isinstance(
                curr_file_info.get(file_path), dict
            ) else None

            if prev_checksum and curr_checksum and prev_checksum != curr_checksum:
                curr_info = curr_file_info.get(file_path)
                if isinstance(curr_info, tuple) and len(curr_info) >= 2:
                    modified[file_path] = curr_info
                elif isinstance(curr_info, dict):
                    modified[file_path] = (curr_info.get("checksum"), curr_info.get("size"))

        return modified
