"""
PIT Manifest Management

Handles creation and parsing of immutable PIT (Point in Time) manifests.
Each manifest is versioned per PIT and documents what changed.
"""

import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class FileEntry:
    """Represents a file entry in a manifest."""
    path: str
    checksum: str  # SHA-256 hex digest
    size: int
    timestamp: str  # ISO format


@dataclass
class PITManifestData:
    """Data structure for a PIT manifest."""
    pit_id: str
    timestamp: str  # ISO format, creation time of this PIT
    parent_pit_id: Optional[str]  # Reference to parent PIT, None for root

    # Files in this PIT
    added_files: List[FileEntry]
    deleted_files: List[str]  # Paths of files deleted from previous PIT
    modified_files: List[FileEntry]

    # Metadata snapshot
    table_schema: Dict[str, Any]
    table_properties: Dict[str, Any]

    # Integrity
    manifest_checksum: Optional[str] = None  # Checksum of manifest itself


class PITManifest:
    """Manages PIT manifest creation and verification."""

    @staticmethod
    def compute_file_checksum(file_path: str, file_content: Optional[bytes] = None) -> str:
        """
        Compute SHA-256 checksum of a file.

        Args:
            file_path: Path to the file (used for error messages)
            file_content: Optional bytes content. If provided, compute checksum directly.
                         Otherwise, read from file_path.

        Returns:
            Hex digest of SHA-256 checksum
        """
        sha256_hash = hashlib.sha256()

        if file_content is not None:
            sha256_hash.update(file_content)
        else:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()

    @staticmethod
    def compute_manifest_checksum(manifest_data: PITManifestData) -> str:
        """
        Compute checksum of manifest (excluding the checksum field itself).

        Args:
            manifest_data: The manifest data to checksum

        Returns:
            Hex digest of SHA-256 checksum
        """
        # Create a copy and remove checksum field for computation
        manifest_dict = asdict(manifest_data)
        manifest_dict["manifest_checksum"] = None

        # Serialize consistently
        manifest_json = json.dumps(manifest_dict, sort_keys=True, default=str)
        return hashlib.sha256(manifest_json.encode()).hexdigest()

    @staticmethod
    def create_manifest(
        pit_id: str,
        parent_pit_id: Optional[str],
        added_files: Dict[str, tuple],  # {path: (checksum, size)}
        deleted_files: List[str],
        modified_files: Dict[str, tuple],  # {path: (checksum, size)}
        table_schema: Dict[str, Any],
        table_properties: Dict[str, Any],
    ) -> PITManifestData:
        """
        Create a new PIT manifest.

        Args:
            pit_id: Unique PIT identifier
            parent_pit_id: Parent PIT ID or None for root
            added_files: Dict of {path: (checksum, size)}
            deleted_files: List of deleted file paths
            modified_files: Dict of {path: (checksum, size)}
            table_schema: Table schema snapshot
            table_properties: Table properties snapshot

        Returns:
            PITManifestData with computed checksums
        """
        now = datetime.utcnow().isoformat() + "Z"

        # Create file entries
        added_entries = [
            FileEntry(
                path=path,
                checksum=checksum,
                size=size,
                timestamp=now
            )
            for path, (checksum, size) in added_files.items()
        ]

        modified_entries = [
            FileEntry(
                path=path,
                checksum=checksum,
                size=size,
                timestamp=now
            )
            for path, (checksum, size) in modified_files.items()
        ]

        manifest = PITManifestData(
            pit_id=pit_id,
            timestamp=now,
            parent_pit_id=parent_pit_id,
            added_files=added_entries,
            deleted_files=deleted_files,
            modified_files=modified_entries,
            table_schema=table_schema,
            table_properties=table_properties,
        )

        # Compute and set manifest checksum
        manifest.manifest_checksum = PITManifest.compute_manifest_checksum(manifest)

        return manifest

    @staticmethod
    def serialize_manifest(manifest: PITManifestData) -> str:
        """
        Serialize manifest to JSON string.

        Args:
            manifest: PITManifestData to serialize

        Returns:
            JSON string representation
        """
        def serialize_file_entry(entry: FileEntry) -> Dict[str, Any]:
            return {
                "path": entry.path,
                "checksum": entry.checksum,
                "size": entry.size,
                "timestamp": entry.timestamp,
            }

        manifest_dict = {
            "pit_id": manifest.pit_id,
            "timestamp": manifest.timestamp,
            "parent_pit_id": manifest.parent_pit_id,
            "added_files": [serialize_file_entry(f) for f in manifest.added_files],
            "deleted_files": manifest.deleted_files,
            "modified_files": [serialize_file_entry(f) for f in manifest.modified_files],
            "table_schema": manifest.table_schema,
            "table_properties": manifest.table_properties,
            "manifest_checksum": manifest.manifest_checksum,
        }

        return json.dumps(manifest_dict, indent=2, default=str)

    @staticmethod
    def deserialize_manifest(manifest_json: str) -> PITManifestData:
        """
        Deserialize manifest from JSON string.

        Args:
            manifest_json: JSON string representation

        Returns:
            PITManifestData

        Raises:
            ValueError: If manifest is invalid or checksum verification fails
        """
        manifest_dict = json.loads(manifest_json)

        # Reconstruct file entries
        added_files = [
            FileEntry(
                path=f["path"],
                checksum=f["checksum"],
                size=f["size"],
                timestamp=f["timestamp"],
            )
            for f in manifest_dict.get("added_files", [])
        ]

        modified_files = [
            FileEntry(
                path=f["path"],
                checksum=f["checksum"],
                size=f["size"],
                timestamp=f["timestamp"],
            )
            for f in manifest_dict.get("modified_files", [])
        ]

        manifest = PITManifestData(
            pit_id=manifest_dict["pit_id"],
            timestamp=manifest_dict["timestamp"],
            parent_pit_id=manifest_dict.get("parent_pit_id"),
            added_files=added_files,
            deleted_files=manifest_dict.get("deleted_files", []),
            modified_files=modified_files,
            table_schema=manifest_dict["table_schema"],
            table_properties=manifest_dict["table_properties"],
            manifest_checksum=manifest_dict.get("manifest_checksum"),
        )

        # Verify manifest checksum
        expected_checksum = PITManifest.compute_manifest_checksum(manifest)
        stored_checksum = manifest.manifest_checksum

        if stored_checksum and stored_checksum != expected_checksum:
            raise ValueError(
                f"Manifest checksum mismatch for PIT {manifest.pit_id}: "
                f"expected {expected_checksum}, got {stored_checksum}"
            )

        return manifest
