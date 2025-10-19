"""
Tests for Phase 3: IntegrityChecker (Corruption Detection)

Tests cover:
- Full integrity checks
- Quick integrity checks
- Corruption detection
- Report generation
- Backup validation
"""

import pytest
from unittest.mock import Mock

from bcn.integrity_checker import IntegrityChecker, BackupValidator, CorruptionReport
from bcn.incremental_backup import IncrementalBackupRepository
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
            pass

    return MockS3Client()


# ============================================================================
# Test CorruptionReport
# ============================================================================


class TestCorruptionReport:
    """Tests for corruption report data structure."""

    def test_report_creation(self):
        """Test creating corruption report."""
        report = CorruptionReport(
            backup_name="test_backup",
            is_corrupted=False,
            corrupted_pits=[],
            missing_files=[],
            checksum_mismatches=[],
            total_files_checked=100,
            issues_found=0,
        )

        assert report.backup_name == "test_backup"
        assert report.is_corrupted is False
        assert report.issues_found == 0

    def test_report_with_issues(self):
        """Test report reflecting issues."""
        report = CorruptionReport(
            backup_name="test_backup",
            is_corrupted=True,
            corrupted_pits=["pit_001"],
            missing_files=[("pit_001", "data/file1.parquet")],
            checksum_mismatches=[],
            total_files_checked=100,
            issues_found=1,
        )

        assert report.is_corrupted is True
        assert len(report.corrupted_pits) == 1
        assert len(report.missing_files) == 1


# ============================================================================
# Test IntegrityChecker
# ============================================================================


class TestIntegrityChecker:
    """Tests for integrity checker."""

    def test_checker_initialization(self, mock_s3_client):
        """Test checker initialization."""
        checker = IntegrityChecker("my_backup", mock_s3_client)

        assert checker.backup_name == "my_backup"
        assert checker.repository is not None

    def test_verify_empty_backup(self, mock_s3_client):
        """Test verifying backup with no PITs."""
        checker = IntegrityChecker("empty_backup", mock_s3_client)

        report = checker.verify_quick()

        assert report.backup_name == "empty_backup"
        assert report.is_corrupted is False
        assert report.total_files_checked == 0

    def test_verify_full_creates_report(self, mock_s3_client):
        """Test full verify creates proper report."""
        checker = IntegrityChecker("my_backup", mock_s3_client)

        report = checker.verify_full()

        assert isinstance(report, CorruptionReport)
        assert report.backup_name == "my_backup"

    def test_verify_quick_creates_report(self, mock_s3_client):
        """Test quick verify creates proper report."""
        checker = IntegrityChecker("my_backup", mock_s3_client)

        report = checker.verify_quick()

        assert isinstance(report, CorruptionReport)
        assert report.backup_name == "my_backup"


# ============================================================================
# Test BackupValidator
# ============================================================================


class TestBackupValidator:
    """Tests for backup validation."""

    def test_validator_initialization(self, mock_s3_client):
        """Test validator initialization."""
        validator = BackupValidator("my_backup", mock_s3_client)

        assert validator.backup_name == "my_backup"
        assert validator.checker is not None

    def test_validate_nonexistent_pit(self, mock_s3_client):
        """Test validation fails for nonexistent PIT."""
        validator = BackupValidator("my_backup", mock_s3_client)

        result = validator.validate_after_backup("nonexistent_pit")

        assert result is False


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegrityCheckerIntegration:
    """Integration tests for integrity checker."""

    def test_verify_and_report_workflow(self, mock_s3_client):
        """Test verification and reporting workflow."""
        checker = IntegrityChecker("my_backup", mock_s3_client)

        # Verify
        report = checker.verify_quick()

        # Report should be valid
        assert report is not None
        assert hasattr(report, "is_corrupted")
        assert hasattr(report, "total_files_checked")

    def test_identify_corruption_on_clean_backup(self, mock_s3_client):
        """Test corruption identification on clean backup."""
        checker = IntegrityChecker("clean_backup", mock_s3_client)

        corrupted_pit = checker.identify_corruption_source()

        # Should return None for clean backup
        assert corrupted_pit is None


# ============================================================================
# Print Report Tests
# ============================================================================


class TestReportPrinting:
    """Tests for report printing."""

    def test_print_clean_report(self, capsys, mock_s3_client):
        """Test printing clean report."""
        report = CorruptionReport(
            backup_name="test_backup",
            is_corrupted=False,
            corrupted_pits=[],
            missing_files=[],
            checksum_mismatches=[],
            total_files_checked=100,
            issues_found=0,
        )

        checker = IntegrityChecker("test_backup", mock_s3_client)
        checker.print_report(report)

        captured = capsys.readouterr()
        assert "test_backup" in captured.out
        assert "OK" in captured.out

    def test_print_corrupted_report(self, capsys, mock_s3_client):
        """Test printing corrupted report."""
        report = CorruptionReport(
            backup_name="test_backup",
            is_corrupted=True,
            corrupted_pits=["pit_001"],
            missing_files=[("pit_001", "data/file1.parquet")],
            checksum_mismatches=[],
            total_files_checked=100,
            issues_found=1,
        )

        checker = IntegrityChecker("test_backup", mock_s3_client)
        checker.print_report(report, verbose=True)

        captured = capsys.readouterr()
        assert "CORRUPTED" in captured.out
        assert "pit_001" in captured.out
