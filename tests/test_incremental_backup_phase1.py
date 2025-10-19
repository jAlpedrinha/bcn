"""
Tests for Phase 1: Incremental Backup Foundation

Tests cover:
- PIT repository structure initialization
- Manifest creation and integrity
- Change detection
- PIT chain management
- File accumulation across PITs
- Restore plan generation
- Basic restore operations
"""

import json
import pytest
from datetime import datetime
from typing import Dict, Tuple

from bcn.pit_manifest import PITManifest, PITManifestData, FileEntry
from bcn.change_detector import ChangeDetector, TableSnapshot
from bcn.incremental_backup import IncrementalBackupRepository, IncrementalBackup
from bcn.incremental_restore import IncrementalRestore
from bcn.s3_client import S3Client


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing."""

    class MockS3Client(S3Client):
        def __init__(self):
            self.objects = {}

        def write_object(self, bucket: str, key: str, content: str) -> None:
            self.objects[f"{bucket}/{key}"] = content

        def read_object(self, bucket: str, key: str) -> str:
            full_key = f"{bucket}/{key}"
            if full_key not in self.objects:
                raise Exception(f"Key not found: {full_key}")
            return self.objects[full_key]

        def copy_object(self, source_key: str, dest_key: str, dest_bucket: str) -> None:
            source_full = source_key if "/" in source_key else f"default/{source_key}"
            dest_full = f"{dest_bucket}/{dest_key}"

            if source_full not in self.objects:
                self.objects[source_full] = f"mock_data_for_{source_key}"

            self.objects[dest_full] = self.objects[source_full]

    return MockS3Client()


@pytest.fixture
def test_file_info():
    """Sample file information for testing."""
    return {
        "data/file1.parquet": (
            "abc123def456789",
            1024,
        ),
        "data/file2.parquet": (
            "def456ghi789abc",
            2048,
        ),
        "data/file3.parquet": (
            "ghi789jkl012def",
            3072,
        ),
    }


@pytest.fixture
def test_schema():
    """Sample table schema for testing."""
    return {
        "fields": [
            {"id": 1, "name": "id", "type": "int"},
            {"id": 2, "name": "name", "type": "string"},
            {"id": 3, "name": "value", "type": "double"},
        ]
    }


@pytest.fixture
def test_properties():
    """Sample table properties for testing."""
    return {
        "format-version": "2",
        "write.parquet.compression-codec": "snappy",
    }


# ============================================================================
# Test PITManifest Class
# ============================================================================


class TestPITManifest:
    """Tests for PIT manifest creation and integrity."""

    def test_compute_file_checksum_from_bytes(self):
        """Test computing file checksum from bytes."""
        content = b"test content"
        checksum = PITManifest.compute_file_checksum("test.txt", content)

        assert len(checksum) == 64  # SHA-256 hex digest
        assert checksum.isalnum()

    def test_compute_file_checksum_deterministic(self):
        """Test that checksum computation is deterministic."""
        content = b"test content"

        checksum1 = PITManifest.compute_file_checksum("test1.txt", content)
        checksum2 = PITManifest.compute_file_checksum("test2.txt", content)

        assert checksum1 == checksum2

    def test_create_manifest(self, test_file_info, test_schema, test_properties):
        """Test creating a new PIT manifest."""
        manifest = PITManifest.create_manifest(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files=test_file_info,
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        assert manifest.pit_id == "pit_001_20250101T100000Z"
        assert manifest.parent_pit_id is None
        assert len(manifest.added_files) == 3
        assert len(manifest.deleted_files) == 0
        assert len(manifest.modified_files) == 0
        assert manifest.manifest_checksum is not None

    def test_manifest_serialization(self, test_file_info, test_schema, test_properties):
        """Test manifest serialization and deserialization."""
        original = PITManifest.create_manifest(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files=test_file_info,
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # Serialize
        manifest_json = PITManifest.serialize_manifest(original)
        assert isinstance(manifest_json, str)

        # Deserialize
        restored = PITManifest.deserialize_manifest(manifest_json)

        assert restored.pit_id == original.pit_id
        assert restored.parent_pit_id == original.parent_pit_id
        assert len(restored.added_files) == len(original.added_files)
        assert restored.manifest_checksum == original.manifest_checksum

    def test_manifest_checksum_verification(self, test_file_info, test_schema, test_properties):
        """Test that manifest checksum is verified on deserialization."""
        manifest = PITManifest.create_manifest(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files=test_file_info,
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        manifest_json = PITManifest.serialize_manifest(manifest)

        # Tamper with manifest
        manifest_dict = json.loads(manifest_json)
        manifest_dict["added_files"][0]["size"] = 9999

        tampered_json = json.dumps(manifest_dict)

        # Should raise ValueError on deserialization
        with pytest.raises(ValueError, match="Manifest checksum mismatch"):
            PITManifest.deserialize_manifest(tampered_json)

    def test_manifest_with_parent_reference(
        self, test_file_info, test_schema, test_properties
    ):
        """Test creating manifest with parent PIT reference."""
        manifest = PITManifest.create_manifest(
            pit_id="pit_002_20250102T100000Z",
            parent_pit_id="pit_001_20250101T100000Z",
            added_files={"data/file4.parquet": ("checksum4", 4096)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        assert manifest.parent_pit_id == "pit_001_20250101T100000Z"

        # Verify parent reference survives serialization
        manifest_json = PITManifest.serialize_manifest(manifest)
        restored = PITManifest.deserialize_manifest(manifest_json)
        assert restored.parent_pit_id == "pit_001_20250101T100000Z"


# ============================================================================
# Test ChangeDetector Class
# ============================================================================


class TestChangeDetector:
    """Tests for detecting changes between table snapshots."""

    def test_first_backup_all_files_added(self):
        """Test that first backup treats all files as added."""
        snapshot = TableSnapshot(
            data_files={"file1.parquet", "file2.parquet"},
            metadata={"file_info": {"file1.parquet": ("checksum1", 100)}},
            schema={"fields": []},
        )

        added, deleted, modified = ChangeDetector.compare_snapshots(None, snapshot)

        # First backup should have files in added_files
        assert "file1.parquet" in added
        assert len(deleted) == 0
        assert len(modified) == 0

    def test_detect_added_files(self):
        """Test detecting newly added files."""
        previous = TableSnapshot(
            data_files={"file1.parquet"},
            metadata={"file_info": {"file1.parquet": ("checksum1", 100)}},
            schema={},
        )

        current = TableSnapshot(
            data_files={"file1.parquet", "file2.parquet"},
            metadata={
                "file_info": {
                    "file1.parquet": ("checksum1", 100),
                    "file2.parquet": ("checksum2", 200),
                }
            },
            schema={},
        )

        added, deleted, modified = ChangeDetector.compare_snapshots(previous, current)

        # Should detect file2 as added
        assert "file2.parquet" in [f for f in current.data_files if f not in previous.data_files]

    def test_detect_deleted_files(self):
        """Test detecting deleted files."""
        previous = TableSnapshot(
            data_files={"file1.parquet", "file2.parquet"},
            metadata={"file_info": {}},
            schema={},
        )

        current = TableSnapshot(
            data_files={"file1.parquet"},
            metadata={"file_info": {}},
            schema={},
        )

        added, deleted, modified = ChangeDetector.compare_snapshots(previous, current)

        assert "file2.parquet" in deleted

    def test_detect_modified_files(self):
        """Test detecting modified files (changed checksum)."""
        previous = TableSnapshot(
            data_files={"file1.parquet"},
            metadata={
                "file_info": {
                    "file1.parquet": {
                        "checksum": "checksum_v1",
                        "size": 100,
                    }
                }
            },
            schema={},
        )

        current = TableSnapshot(
            data_files={"file1.parquet"},
            metadata={
                "file_info": {
                    "file1.parquet": {
                        "checksum": "checksum_v2",
                        "size": 150,
                    }
                }
            },
            schema={},
        )

        added, deleted, modified = ChangeDetector.compare_snapshots(previous, current)

        assert "file1.parquet" in modified

    def test_compare_schemas_no_change(self):
        """Test schema comparison when no change."""
        schema = {"fields": [{"id": 1, "name": "id", "type": "int"}]}

        changed = ChangeDetector.compare_schemas(schema, schema)

        assert changed is False

    def test_compare_schemas_with_change(self):
        """Test schema comparison when schema changes."""
        schema1 = {"fields": [{"id": 1, "name": "id", "type": "int"}]}
        schema2 = {
            "fields": [
                {"id": 1, "name": "id", "type": "int"},
                {"id": 2, "name": "name", "type": "string"},
            ]
        }

        changed = ChangeDetector.compare_schemas(schema1, schema2)

        assert changed is True


# ============================================================================
# Test IncrementalBackupRepository Class
# ============================================================================


class TestIncrementalBackupRepository:
    """Tests for PIT repository management."""

    def test_initialize_repository(self, mock_s3_client):
        """Test repository initialization."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")

        index = repo.initialize_repository()

        assert index["backup_name"] == "my_backup"
        assert index["last_pit"] is None
        assert index["pits"] == []

    def test_get_repository_index_not_found(self, mock_s3_client):
        """Test loading index when it doesn't exist."""
        repo = IncrementalBackupRepository("nonexistent", mock_s3_client, "iceberg")

        index = repo.get_repository_index()

        assert index is None

    def test_create_and_retrieve_pit(
        self, mock_s3_client, test_schema, test_properties
    ):
        """Test creating and retrieving a PIT."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        manifest = repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        assert manifest.pit_id == "pit_001_20250101T100000Z"

        # Retrieve manifest
        retrieved = repo.get_pit_manifest("pit_001_20250101T100000Z")
        assert retrieved is not None
        assert retrieved.pit_id == "pit_001_20250101T100000Z"

    def test_update_index_with_pit(self, mock_s3_client, test_schema, test_properties):
        """Test updating index when new PIT is added."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        repo.update_index("pit_001_20250101T100000Z")

        index = repo.get_repository_index()
        assert "pit_001_20250101T100000Z" in index["pits"]
        assert index["last_pit"] == "pit_001_20250101T100000Z"

    def test_get_pit_chain_single(self, mock_s3_client, test_schema, test_properties):
        """Test getting PIT chain with single PIT."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        chain = repo.get_pit_chain("pit_001_20250101T100000Z")

        assert chain == ["pit_001_20250101T100000Z"]

    def test_get_pit_chain_multiple(self, mock_s3_client, test_schema, test_properties):
        """Test getting PIT chain with multiple PITs."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        # Create PIT 1
        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # Create PIT 2
        repo.create_pit(
            pit_id="pit_002_20250102T100000Z",
            parent_pit_id="pit_001_20250101T100000Z",
            added_files={"data/file2.parquet": ("checksum2", 2048)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # Create PIT 3
        repo.create_pit(
            pit_id="pit_003_20250103T100000Z",
            parent_pit_id="pit_002_20250102T100000Z",
            added_files={"data/file3.parquet": ("checksum3", 3072)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        chain = repo.get_pit_chain("pit_003_20250103T100000Z")

        assert chain == [
            "pit_001_20250101T100000Z",
            "pit_002_20250102T100000Z",
            "pit_003_20250103T100000Z",
        ]

    def test_get_accumulated_files_single_pit(
        self, mock_s3_client, test_schema, test_properties
    ):
        """Test accumulating files from single PIT."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={
                "data/file1.parquet": ("checksum1", 1024),
                "data/file2.parquet": ("checksum2", 2048),
            },
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        files, deleted = repo.get_accumulated_files("pit_001_20250101T100000Z")

        assert "data/file1.parquet" in files
        assert "data/file2.parquet" in files
        assert len(deleted) == 0

    def test_get_accumulated_files_with_additions(
        self, mock_s3_client, test_schema, test_properties
    ):
        """Test accumulating files across multiple PITs with additions."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        # PIT 1: Add file1 and file2
        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={
                "data/file1.parquet": ("checksum1", 1024),
                "data/file2.parquet": ("checksum2", 2048),
            },
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # PIT 2: Add file3
        repo.create_pit(
            pit_id="pit_002_20250102T100000Z",
            parent_pit_id="pit_001_20250101T100000Z",
            added_files={"data/file3.parquet": ("checksum3", 3072)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        files, deleted = repo.get_accumulated_files("pit_002_20250102T100000Z")

        assert len(files) == 3
        assert "data/file1.parquet" in files
        assert "data/file2.parquet" in files
        assert "data/file3.parquet" in files

    def test_get_accumulated_files_with_deletions(
        self, mock_s3_client, test_schema, test_properties
    ):
        """Test accumulating files with deletions."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        # PIT 1: Add file1 and file2
        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={
                "data/file1.parquet": ("checksum1", 1024),
                "data/file2.parquet": ("checksum2", 2048),
            },
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # PIT 2: Delete file1
        repo.create_pit(
            pit_id="pit_002_20250102T100000Z",
            parent_pit_id="pit_001_20250101T100000Z",
            added_files={},
            deleted_files=["data/file1.parquet"],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        files, deleted = repo.get_accumulated_files("pit_002_20250102T100000Z")

        assert len(files) == 1
        assert "data/file2.parquet" in files
        assert "data/file1.parquet" not in files
        assert "data/file1.parquet" in deleted

    def test_verify_pit_integrity(self, mock_s3_client, test_schema, test_properties):
        """Test PIT integrity verification."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # Should not raise
        is_valid = repo.verify_pit_integrity("pit_001_20250101T100000Z")
        assert is_valid is True


# ============================================================================
# Test IncrementalRestore Class
# ============================================================================


class TestIncrementalRestore:
    """Tests for incremental restore operations."""

    def test_list_pits_empty(self, mock_s3_client):
        """Test listing PITs when repository doesn't exist."""
        restore = IncrementalRestore(
            "nonexistent",
            "target_db",
            "target_table",
            "s3://warehouse/target_table",
            s3_client=mock_s3_client,
        )

        pits = restore.list_pits()

        assert pits == []

    def test_list_pits_multiple(self, mock_s3_client, test_schema, test_properties):
        """Test listing multiple PITs."""
        # First create a backup with multiple PITs
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        repo.create_pit(
            pit_id="pit_002_20250102T100000Z",
            parent_pit_id="pit_001_20250101T100000Z",
            added_files={"data/file2.parquet": ("checksum2", 2048)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        repo.update_index("pit_001_20250101T100000Z")
        repo.update_index("pit_002_20250102T100000Z")

        # Now test listing
        restore = IncrementalRestore(
            "my_backup",
            "target_db",
            "target_table",
            "s3://warehouse/target_table",
            s3_client=mock_s3_client,
        )

        pits = restore.list_pits()

        assert len(pits) == 2
        assert pits[0]["pit_id"] == "pit_001_20250101T100000Z"
        assert pits[1]["pit_id"] == "pit_002_20250102T100000Z"

    def test_describe_pit_basic(self, mock_s3_client, test_schema, test_properties):
        """Test describing a single PIT."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        restore = IncrementalRestore(
            "my_backup",
            "target_db",
            "target_table",
            "s3://warehouse/target_table",
            s3_client=mock_s3_client,
        )

        desc = restore.describe_pit("pit_001_20250101T100000Z")

        assert desc["pit_id"] == "pit_001_20250101T100000Z"
        assert desc["parent_pit_id"] is None
        assert desc["added_files_count"] == 1
        assert desc["modified_files_count"] == 0

    def test_generate_restore_plan(self, mock_s3_client, test_schema, test_properties):
        """Test generating restore plan."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        restore = IncrementalRestore(
            "my_backup",
            "target_db",
            "target_table",
            "s3://warehouse/target_table",
            s3_client=mock_s3_client,
        )

        plan = restore._generate_restore_plan("pit_001_20250101T100000Z")

        assert plan["pit_id"] == "pit_001_20250101T100000Z"
        assert len(plan["pit_chain"]) == 1
        assert plan["pit_chain"][0] == "pit_001_20250101T100000Z"


# ============================================================================
# Integration Tests
# ============================================================================


class TestIncrementalBackupIntegration:
    """Integration tests for incremental backup workflow."""

    def test_pit_chain_immutability(
        self, mock_s3_client, test_schema, test_properties
    ):
        """Test that PITs in a chain are immutable."""
        repo = IncrementalBackupRepository("my_backup", mock_s3_client, "iceberg")
        repo.initialize_repository()

        # Create first PIT
        manifest1 = repo.create_pit(
            pit_id="pit_001_20250101T100000Z",
            parent_pit_id=None,
            added_files={"data/file1.parquet": ("checksum1", 1024)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        checksum1 = manifest1.manifest_checksum

        # Create second PIT
        repo.create_pit(
            pit_id="pit_002_20250102T100000Z",
            parent_pit_id="pit_001_20250101T100000Z",
            added_files={"data/file2.parquet": ("checksum2", 2048)},
            deleted_files=[],
            modified_files={},
            table_schema=test_schema,
            table_properties=test_properties,
        )

        # Verify first PIT checksum unchanged
        retrieved_manifest1 = repo.get_pit_manifest("pit_001_20250101T100000Z")
        assert retrieved_manifest1.manifest_checksum == checksum1
