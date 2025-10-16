"""
S3/MinIO client utilities for backup and restore operations
"""

from typing import Optional

import boto3
from botocore.exceptions import ClientError

from bcn.config import Config


class S3Client:
    """Client for interacting with S3/MinIO storage"""

    def __init__(self):
        """Initialize S3 client with configuration"""
        self.client = boto3.client("s3", **Config.get_s3_config())

    def copy_object(
        self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
    ) -> bool:
        """
        Copy an object from one S3 location to another

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key

        Returns:
            True if successful, False otherwise
        """
        try:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            self.client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
            return True
        except ClientError as e:
            print(f"Error copying {source_bucket}/{source_key} to {dest_bucket}/{dest_key}: {e}")
            return False

    def read_object(self, bucket: str, key: str) -> Optional[bytes]:
        """
        Read object content from S3

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            Object content as bytes, or None if error
        """
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            print(f"Error reading {bucket}/{key}: {e}")
            return None

    def write_object(self, bucket: str, key: str, content: bytes) -> bool:
        """
        Write content to S3 object

        Args:
            bucket: S3 bucket name
            key: Object key
            content: Content to write as bytes

        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=content)
            return True
        except ClientError as e:
            print(f"Error writing to {bucket}/{key}: {e}")
            return False

    def parse_s3_uri(self, uri: str) -> tuple:
        """
        Parse S3 URI into bucket and key

        Args:
            uri: S3 URI (e.g., s3://bucket/path/to/object or s3a://bucket/path/to/object)

        Returns:
            Tuple of (bucket, key)
        """
        # Normalize s3a:// and s3n:// schemes to s3://
        normalized_uri = uri
        if uri.startswith("s3a://") or uri.startswith("s3n://"):
            normalized_uri = "s3://" + uri[6:]

        if not normalized_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {uri}")

        parts = normalized_uri[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key
