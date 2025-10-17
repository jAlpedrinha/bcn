"""
Configuration for Iceberg Snapshot Backup/Restore
"""

import os


class Config:
    """Configuration settings for the backup/restore system"""

    # S3/MinIO Configuration
    # For AWS S3: Leave S3_ENDPOINT empty or unset
    # For MinIO: Set to "http://localhost:9000" or your MinIO endpoint
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")
    S3_REGION = os.getenv("S3_REGION", "us-east-1")

    # S3 Path Style Access
    # For MinIO: Use "true" (path-style: s3.example.com/bucket/key)
    # For AWS S3: Use "false" (virtual-hosted: bucket.s3.amazonaws.com/key)
    S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower() == "true"

    # Backup bucket configuration
    BACKUP_BUCKET = os.getenv("BACKUP_BUCKET", "iceberg")
    WAREHOUSE_BUCKET = os.getenv("WAREHOUSE_BUCKET", "warehouse")

    # Catalog Configuration
    # Catalog type: "hive" or "glue"
    CATALOG_TYPE = os.getenv("CATALOG_TYPE", "hive")
    # Catalog name in Spark (default: glue_catalog for Glue, hive_catalog for Hive)
    CATALOG_NAME = os.getenv("CATALOG_NAME", f"{CATALOG_TYPE}_catalog")

    # Hive Metastore Configuration (only used when CATALOG_TYPE=hive)
    HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")

    # Working directory for temporary files
    WORK_DIR = os.getenv("WORK_DIR", "/tmp/iceberg-snapshots")

    # Logging configuration
    LOG_LEVEL = os.getenv("BCN_LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("BCN_LOG_FILE", None)

    # Iceberg directory structure
    METADATA_DIR = "metadata"
    DATA_DIR = "data"

    @classmethod
    def get_s3_config(cls):
        """Get S3 configuration as a dictionary"""
        config = {
            "aws_access_key_id": cls.S3_ACCESS_KEY,
            "aws_secret_access_key": cls.S3_SECRET_KEY,
            "region_name": cls.S3_REGION,
        }

        # Only add endpoint_url if it's set (for MinIO)
        # AWS S3 should not have an endpoint_url
        if cls.S3_ENDPOINT and cls.S3_ENDPOINT.strip():
            config["endpoint_url"] = cls.S3_ENDPOINT

        return config
