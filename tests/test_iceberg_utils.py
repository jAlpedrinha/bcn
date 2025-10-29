"""
Unit tests for iceberg_utils module
"""

import pytest

from bcn.iceberg_utils import PathAbstractor


class TestPathAbstractor:
    """Test PathAbstractor functionality"""

    def test_abstract_metadata_preserves_snapshot_ancestry(self):
        """
        Test that abstract_metadata_file preserves complete snapshot ancestry chain.

        This is critical for proper handling of position delete files and sequence
        number validation in restored tables.
        """
        # Create metadata with a snapshot ancestry chain
        # Snapshot 1 -> Snapshot 2 -> Snapshot 3 -> Snapshot 4 (current)
        metadata = {
            "format-version": 2,
            "table-uuid": "test-uuid",
            "location": "s3://bucket/warehouse/db/table",
            "current-snapshot-id": 4,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-1.avro",
                    "sequence-number": 1,
                },
                {
                    "snapshot-id": 2,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 2000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-2.avro",
                    "sequence-number": 2,
                },
                {
                    "snapshot-id": 3,
                    "parent-snapshot-id": 2,
                    "timestamp-ms": 3000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-3.avro",
                    "sequence-number": 3,
                },
                {
                    "snapshot-id": 4,
                    "parent-snapshot-id": 3,
                    "timestamp-ms": 4000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-4.avro",
                    "sequence-number": 4,
                },
            ],
            "snapshot-log": [
                {"timestamp-ms": 1000, "snapshot-id": 1},
                {"timestamp-ms": 2000, "snapshot-id": 2},
                {"timestamp-ms": 3000, "snapshot-id": 3},
                {"timestamp-ms": 4000, "snapshot-id": 4},
            ],
            "metadata-log": [],
        }

        table_location = "s3://bucket/warehouse/db/table"

        # Abstract the metadata
        abstracted = PathAbstractor.abstract_metadata_file(metadata, table_location)

        # Verify all snapshots in ancestry chain are preserved
        assert len(abstracted["snapshots"]) == 4, (
            f"Expected 4 snapshots in ancestry chain, got {len(abstracted['snapshots'])}"
        )

        # Verify snapshots are in chronological order (oldest first)
        snapshot_ids = [s["snapshot-id"] for s in abstracted["snapshots"]]
        assert snapshot_ids == [1, 2, 3, 4], (
            f"Snapshots should be in chronological order [1,2,3,4], got {snapshot_ids}"
        )

        # Verify parent relationships are preserved
        assert "parent-snapshot-id" not in abstracted["snapshots"][0], (
            "First snapshot should not have parent"
        )
        assert abstracted["snapshots"][1]["parent-snapshot-id"] == 1
        assert abstracted["snapshots"][2]["parent-snapshot-id"] == 2
        assert abstracted["snapshots"][3]["parent-snapshot-id"] == 3

        # Verify manifest-list paths are abstracted (relative)
        for snapshot in abstracted["snapshots"]:
            manifest_list = snapshot["manifest-list"]
            assert not manifest_list.startswith("s3://"), (
                f"Manifest list path should be abstracted: {manifest_list}"
            )
            assert manifest_list.startswith("metadata/"), (
                f"Manifest list path should be relative: {manifest_list}"
            )

        # Verify snapshot-log and metadata-log are cleared
        assert abstracted["snapshot-log"] == [], "Snapshot log should be cleared"
        assert abstracted["metadata-log"] == [], "Metadata log should be cleared"

        # Verify location is abstracted
        assert abstracted["location"] == "", "Location should be abstracted (empty)"

        # Verify current snapshot ID is preserved
        assert abstracted["current-snapshot-id"] == 4, (
            "Current snapshot ID should be preserved"
        )

        print("✓ Snapshot ancestry chain preserved correctly")

    def test_abstract_metadata_with_single_snapshot(self):
        """
        Test abstract_metadata_file with a table that has only one snapshot.
        """
        metadata = {
            "format-version": 2,
            "table-uuid": "test-uuid",
            "location": "s3://bucket/warehouse/db/table",
            "current-snapshot-id": 1,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-1.avro",
                    "sequence-number": 1,
                }
            ],
            "snapshot-log": [{"timestamp-ms": 1000, "snapshot-id": 1}],
            "metadata-log": [],
        }

        table_location = "s3://bucket/warehouse/db/table"
        abstracted = PathAbstractor.abstract_metadata_file(metadata, table_location)

        # Should preserve the single snapshot
        assert len(abstracted["snapshots"]) == 1
        assert abstracted["snapshots"][0]["snapshot-id"] == 1
        assert "parent-snapshot-id" not in abstracted["snapshots"][0]

        print("✓ Single snapshot preserved correctly")

    def test_abstract_metadata_with_branching_snapshots(self):
        """
        Test with a more complex scenario where there might be multiple branches.
        The function should only preserve the ancestry of the current snapshot.
        """
        # Snapshot tree:
        #   1 -> 2 -> 4 (current)
        #   1 -> 3 (branch, not in current ancestry)
        metadata = {
            "format-version": 2,
            "table-uuid": "test-uuid",
            "location": "s3://bucket/warehouse/db/table",
            "current-snapshot-id": 4,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-1.avro",
                    "sequence-number": 1,
                },
                {
                    "snapshot-id": 2,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 2000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-2.avro",
                    "sequence-number": 2,
                },
                {
                    "snapshot-id": 3,  # Branch from snapshot 1, not in current ancestry
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 2500,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-3.avro",
                    "sequence-number": 3,
                },
                {
                    "snapshot-id": 4,
                    "parent-snapshot-id": 2,
                    "timestamp-ms": 3000,
                    "manifest-list": "s3://bucket/warehouse/db/table/metadata/snap-4.avro",
                    "sequence-number": 4,
                },
            ],
            "snapshot-log": [],
            "metadata-log": [],
        }

        table_location = "s3://bucket/warehouse/db/table"
        abstracted = PathAbstractor.abstract_metadata_file(metadata, table_location)

        # Should only preserve snapshots 1, 2, 4 (current ancestry)
        # Snapshot 3 should be excluded as it's not in the current snapshot's ancestry
        snapshot_ids = [s["snapshot-id"] for s in abstracted["snapshots"]]
        assert snapshot_ids == [1, 2, 4], (
            f"Should only preserve current ancestry [1,2,4], got {snapshot_ids}"
        )

        print("✓ Branching snapshots handled correctly - only current ancestry preserved")

    def test_abstract_path(self):
        """Test path abstraction functionality"""
        # Test full S3 path
        full_path = "s3://bucket/warehouse/db/table/metadata/snap-1.avro"
        table_location = "s3://bucket/warehouse/db/table"

        abstracted = PathAbstractor.abstract_path(full_path, table_location)
        assert abstracted == "metadata/snap-1.avro", f"Expected relative path, got {abstracted}"

        # Test already relative path
        relative_path = "metadata/snap-1.avro"
        abstracted = PathAbstractor.abstract_path(relative_path, table_location)
        assert abstracted == "metadata/snap-1.avro"

        print("✓ Path abstraction works correctly")

    def test_restore_path(self):
        """Test path restoration functionality"""
        relative_path = "metadata/snap-1.avro"
        new_location = "s3://new-bucket/new-warehouse/db/table"

        restored = PathAbstractor.restore_path(relative_path, new_location)
        expected = "s3://new-bucket/new-warehouse/db/table/metadata/snap-1.avro"

        assert restored == expected, f"Expected {expected}, got {restored}"

        print("✓ Path restoration works correctly")
