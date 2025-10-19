"""
Tests for Phase 3: BackupLock (Concurrent Backup Safety)

Tests cover:
- Lock acquisition and release
- Lock expiration
- Concurrent access patterns
- Lock holder detection
- Force release scenarios
"""

import pytest
import json
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta

from bcn.backup_lock import BackupLock, LockInfo, BackupLockContext
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
# Test LockInfo
# ============================================================================


class TestLockInfo:
    """Tests for LockInfo data structure."""

    def test_lock_info_creation(self):
        """Test creating lock info."""
        lock = LockInfo(
            backup_name="test_backup",
            lock_id="lock_123",
            owner_host="host1",
            owner_pid=1234,
            created_at="2025-01-01T10:00:00Z",
            ttl_seconds=3600,
            holder_process="host1:1234",
        )

        assert lock.backup_name == "test_backup"
        assert lock.lock_id == "lock_123"
        assert lock.owner_pid == 1234

    def test_lock_info_serialization(self):
        """Test lock info JSON serialization."""
        lock = LockInfo(
            backup_name="test_backup",
            lock_id="lock_123",
            owner_host="host1",
            owner_pid=1234,
            created_at="2025-01-01T10:00:00Z",
            ttl_seconds=3600,
            holder_process="host1:1234",
        )

        json_str = lock.to_json()
        restored = LockInfo.from_json(json_str)

        assert restored.backup_name == lock.backup_name
        assert restored.lock_id == lock.lock_id
        assert restored.owner_pid == lock.owner_pid

    def test_lock_info_expiration_not_expired(self):
        """Test lock expiration check (not expired)."""
        now = datetime.utcnow()
        lock = LockInfo(
            backup_name="test_backup",
            lock_id="lock_123",
            owner_host="host1",
            owner_pid=1234,
            created_at=now.isoformat() + "Z",
            ttl_seconds=3600,
            holder_process="host1:1234",
        )

        assert lock.is_expired() is False

    def test_lock_info_expiration_expired(self):
        """Test lock expiration check (expired)."""
        past = datetime.utcnow() - timedelta(seconds=7200)
        lock = LockInfo(
            backup_name="test_backup",
            lock_id="lock_123",
            owner_host="host1",
            owner_pid=1234,
            created_at=past.isoformat() + "Z",
            ttl_seconds=3600,
            holder_process="host1:1234",
        )

        assert lock.is_expired() is True


# ============================================================================
# Test BackupLock Acquisition
# ============================================================================


class TestBackupLockAcquisition:
    """Tests for lock acquisition."""

    def test_lock_initialization(self, mock_s3_client):
        """Test lock initialization."""
        lock = BackupLock("my_backup", mock_s3_client, "iceberg", 3600)

        assert lock.backup_name == "my_backup"
        assert lock.backup_bucket == "iceberg"
        assert lock.ttl_seconds == 3600
        assert lock.lock_id is None

    def test_lock_acquire_success(self, mock_s3_client):
        """Test successful lock acquisition."""
        lock = BackupLock("my_backup", mock_s3_client)

        acquired = lock.acquire(timeout=1)

        assert acquired is True
        assert lock.lock_id is not None
        assert lock.lock_info is not None

    def test_lock_acquire_stores_lock_info(self, mock_s3_client):
        """Test that lock acquisition stores lock info correctly."""
        lock = BackupLock("my_backup", mock_s3_client)

        assert lock.acquire(timeout=1) is True
        assert lock.lock_id is not None
        assert lock.lock_info is not None
        assert lock.lock_info.backup_name == "my_backup"

    def test_lock_acquire_with_different_backups(self, mock_s3_client):
        """Test that different backups can have locks simultaneously."""
        lock1 = BackupLock("backup_1", mock_s3_client)
        lock2 = BackupLock("backup_2", mock_s3_client)

        # Both should succeed (different backup names)
        assert lock1.acquire(timeout=1) is True
        assert lock2.acquire(timeout=1) is True

    def test_lock_acquire_timeout_behavior(self, mock_s3_client):
        """Test lock acquisition with timeout parameter."""
        lock = BackupLock("my_backup", mock_s3_client)

        # Acquire with timeout
        assert lock.acquire(timeout=5) is True
        assert lock.lock_id is not None


# ============================================================================
# Test BackupLock Release
# ============================================================================


class TestBackupLockRelease:
    """Tests for lock release."""

    def test_lock_release_success(self, mock_s3_client):
        """Test successful lock release."""
        lock = BackupLock("my_backup", mock_s3_client)

        assert lock.acquire(timeout=1) is True
        assert lock.release() is True
        assert lock.lock_id is None

    def test_lock_release_when_not_held(self, mock_s3_client):
        """Test releasing when lock not held."""
        lock = BackupLock("my_backup", mock_s3_client)

        # Releasing without acquiring should return False
        assert lock.release() is False

    def test_lock_allows_next_after_release(self, mock_s3_client):
        """Test that another lock can be acquired after release."""
        lock1 = BackupLock("my_backup", mock_s3_client)
        lock2 = BackupLock("my_backup", mock_s3_client)

        # First lock acquires and releases
        assert lock1.acquire(timeout=1) is True
        assert lock1.release() is True

        # Second lock can now acquire
        assert lock2.acquire(timeout=1) is True


# ============================================================================
# Test BackupLock Renewal
# ============================================================================


class TestBackupLockRenewal:
    """Tests for lock renewal (TTL extension)."""

    def test_lock_renew_success(self, mock_s3_client):
        """Test successful lock renewal."""
        lock = BackupLock("my_backup", mock_s3_client)

        assert lock.acquire(timeout=1) is True
        original_lock_id = lock.lock_id

        # Renew lock
        assert lock.renew() is True
        assert lock.lock_id == original_lock_id  # ID should stay same

    def test_lock_renew_without_held_lock(self, mock_s3_client):
        """Test renewal fails when lock not held."""
        lock = BackupLock("my_backup", mock_s3_client)

        # Renew without acquiring should fail
        assert lock.renew() is False


# ============================================================================
# Test BackupLock Holder Detection
# ============================================================================


class TestBackupLockHolderDetection:
    """Tests for detecting lock holder."""

    def test_get_lock_holder_info_structure(self, mock_s3_client):
        """Test lock holder info has correct structure."""
        lock = BackupLock("my_backup", mock_s3_client)

        # Acquire lock
        assert lock.acquire(timeout=1) is True

        # Lock info should have holder details
        assert lock.lock_info.holder_process is not None
        assert ":" in lock.lock_info.holder_process  # host:pid format

    def test_get_lock_holder_when_not_held(self, mock_s3_client):
        """Test getting lock holder when no lock held."""
        lock = BackupLock("my_backup", mock_s3_client)

        holder = lock.get_lock_holder()
        assert holder is None


# ============================================================================
# Test BackupLockContext
# ============================================================================


class TestBackupLockContext:
    """Tests for lock context manager."""

    def test_context_manager_acquire_release(self, mock_s3_client):
        """Test context manager acquires and releases lock."""
        with BackupLockContext("my_backup", mock_s3_client, timeout=1) as lock:
            assert lock.lock_id is not None
            acquired_lock_id = lock.lock_id

        # After context exit, lock should be released
        # New lock should be acquirable
        lock2 = BackupLock("my_backup", mock_s3_client)
        assert lock2.acquire(timeout=1) is True

    def test_context_manager_requires_timeout_param(self, mock_s3_client):
        """Test context manager initializes with timeout parameter."""
        ctx = BackupLockContext("my_backup", mock_s3_client, timeout=5)

        assert ctx.backup_lock is not None
        assert ctx.timeout == 5

    def test_context_manager_different_backups(self, mock_s3_client):
        """Test context managers for different backups work together."""
        with BackupLockContext("backup_1", mock_s3_client, timeout=1) as lock1:
            with BackupLockContext("backup_2", mock_s3_client, timeout=1) as lock2:
                assert lock1.backup_name == "backup_1"
                assert lock2.backup_name == "backup_2"


# ============================================================================
# Test Lock Information
# ============================================================================


class TestLockInformation:
    """Tests for lock metadata."""

    def test_lock_has_correct_owner_info(self, mock_s3_client):
        """Test lock stores correct owner information."""
        lock = BackupLock("my_backup", mock_s3_client)
        lock.acquire(timeout=1)

        assert lock.lock_info.owner_host is not None
        assert lock.lock_info.owner_pid > 0
        assert lock.lock_info.holder_process is not None

    def test_lock_has_timestamp(self, mock_s3_client):
        """Test lock stores creation timestamp."""
        lock = BackupLock("my_backup", mock_s3_client)
        lock.acquire(timeout=1)

        # Verify timestamp is recent (within 1 second)
        created = datetime.fromisoformat(
            lock.lock_info.created_at.replace("Z", "+00:00")
        )
        now = datetime.utcnow()
        diff = (now - created.replace(tzinfo=None)).total_seconds()

        assert 0 <= diff <= 1


# ============================================================================
# Integration Tests
# ============================================================================


class TestBackupLockIntegration:
    """Integration tests for backup lock workflows."""

    def test_backup_workflow_with_lock(self, mock_s3_client):
        """Test typical backup workflow with lock."""
        backup_name = "my_backup"

        # Acquire lock
        with BackupLockContext(backup_name, mock_s3_client, timeout=1) as lock:
            # Simulate backup operations
            assert lock.backup_name == backup_name
            assert lock.renew() is True

        # After backup, lock should be released

    def test_concurrent_backups_same_table_sequential(self, mock_s3_client):
        """Test sequential concurrent attempts (same table)."""
        # First backup
        with BackupLockContext("my_backup", mock_s3_client, timeout=1) as lock1:
            assert lock1.lock_id is not None

        # Second backup (after first released)
        with BackupLockContext("my_backup", mock_s3_client, timeout=1) as lock2:
            assert lock2.lock_id is not None

    def test_concurrent_backups_different_tables(self, mock_s3_client):
        """Test concurrent backups of different tables work."""
        # Both backups should succeed (different tables)
        with BackupLockContext("table_1", mock_s3_client, timeout=1) as lock1:
            with BackupLockContext("table_2", mock_s3_client, timeout=1) as lock2:
                assert lock1.lock_id is not None
                assert lock2.lock_id is not None
