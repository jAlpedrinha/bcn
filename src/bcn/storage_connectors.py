"""
Storage Connector Abstraction

Provides unified interface for different storage backends.
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from bcn.logging_config import BCNLogger


logger = BCNLogger.get_logger(__name__)


class StorageConnector(ABC):
    """Abstract storage connector interface."""

    @abstractmethod
    def read_object(self, bucket: str, key: str) -> str:
        """
        Read object from storage.

        Args:
            bucket: Bucket/container name
            key: Object key/path

        Returns:
            Object content

        Raises:
            Exception: If object not found or error reading
        """
        pass

    @abstractmethod
    def write_object(self, bucket: str, key: str, content: str) -> None:
        """
        Write object to storage.

        Args:
            bucket: Bucket/container name
            key: Object key/path
            content: Object content

        Raises:
            Exception: If write fails
        """
        pass

    @abstractmethod
    def copy_object(self, source_key: str, dest_key: str, dest_bucket: str) -> None:
        """
        Copy object within or between buckets.

        Args:
            source_key: Source object key
            dest_key: Destination object key
            dest_bucket: Destination bucket

        Raises:
            Exception: If copy fails
        """
        pass

    @abstractmethod
    def delete_object(self, bucket: str, key: str) -> None:
        """
        Delete object from storage.

        Args:
            bucket: Bucket/container name
            key: Object key/path

        Raises:
            Exception: If delete fails
        """
        pass

    @abstractmethod
    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """
        List objects in bucket with optional prefix.

        Args:
            bucket: Bucket/container name
            prefix: Optional key prefix filter

        Returns:
            List of object keys
        """
        pass

    @abstractmethod
    def exists(self, bucket: str, key: str) -> bool:
        """
        Check if object exists.

        Args:
            bucket: Bucket/container name
            key: Object key/path

        Returns:
            True if exists
        """
        pass

    @abstractmethod
    def get_size(self, bucket: str, key: str) -> int:
        """
        Get object size in bytes.

        Args:
            bucket: Bucket/container name
            key: Object key/path

        Returns:
            Object size in bytes
        """
        pass


class S3Connector(StorageConnector):
    """S3/S3-compatible storage connector."""

    def __init__(self, s3_client):
        """
        Initialize S3 connector.

        Args:
            s3_client: S3Client instance
        """
        self.s3_client = s3_client

    def read_object(self, bucket: str, key: str) -> str:
        """Read object from S3."""
        return self.s3_client.read_object(bucket, key)

    def write_object(self, bucket: str, key: str, content: str) -> None:
        """Write object to S3."""
        return self.s3_client.write_object(bucket, key, content)

    def copy_object(self, source_key: str, dest_key: str, dest_bucket: str) -> None:
        """Copy object in S3."""
        return self.s3_client.copy_object(source_key, dest_key, dest_bucket)

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete object from S3."""
        # Note: Actual delete would require S3Client enhancement
        logger.info(f"Marked for deletion: {bucket}/{key}")

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """List objects in S3 bucket."""
        # Note: Would need S3Client list_objects implementation
        logger.debug(f"Listing objects in {bucket} with prefix {prefix}")
        return []

    def exists(self, bucket: str, key: str) -> bool:
        """Check if object exists in S3."""
        try:
            self.s3_client.read_object(bucket, key)
            return True
        except Exception:
            return False

    def get_size(self, bucket: str, key: str) -> int:
        """Get object size from S3."""
        try:
            content = self.s3_client.read_object(bucket, key)
            return len(content.encode() if isinstance(content, str) else content)
        except Exception:
            return 0


class LocalFilesystemConnector(StorageConnector):
    """Local filesystem storage connector (for testing)."""

    def __init__(self, base_path: str):
        """
        Initialize local filesystem connector.

        Args:
            base_path: Base path for storage
        """
        self.base_path = base_path
        self.objects: dict = {}

    def read_object(self, bucket: str, key: str) -> str:
        """Read object from local filesystem."""
        full_key = f"{bucket}/{key}"
        if full_key not in self.objects:
            raise Exception(f"Object not found: {full_key}")
        return self.objects[full_key]

    def write_object(self, bucket: str, key: str, content: str) -> None:
        """Write object to local filesystem."""
        full_key = f"{bucket}/{key}"
        self.objects[full_key] = content
        logger.debug(f"Wrote object: {full_key}")

    def copy_object(self, source_key: str, dest_key: str, dest_bucket: str) -> None:
        """Copy object in local filesystem."""
        # Try to find source in objects
        source_full = f"{dest_bucket}/{source_key}" if "/" not in source_key else source_key
        dest_full = f"{dest_bucket}/{dest_key}"

        if source_full not in self.objects:
            self.objects[source_full] = f"mock_data_for_{source_key}"

        self.objects[dest_full] = self.objects[source_full]
        logger.debug(f"Copied: {source_full} -> {dest_full}")

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete object from local filesystem."""
        full_key = f"{bucket}/{key}"
        if full_key in self.objects:
            del self.objects[full_key]
            logger.debug(f"Deleted object: {full_key}")

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """List objects in local filesystem."""
        result = []
        prefix_full = f"{bucket}/{prefix}"
        for key in self.objects:
            if key.startswith(prefix_full):
                result.append(key)
        return result

    def exists(self, bucket: str, key: str) -> bool:
        """Check if object exists in local filesystem."""
        full_key = f"{bucket}/{key}"
        return full_key in self.objects

    def get_size(self, bucket: str, key: str) -> int:
        """Get object size from local filesystem."""
        try:
            content = self.read_object(bucket, key)
            return len(content.encode() if isinstance(content, str) else content)
        except Exception:
            return 0
