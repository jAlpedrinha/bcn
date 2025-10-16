"""
Spark client for Iceberg table operations
"""

from typing import Dict, List, Optional

from bcn.config import Config


class SparkClient:
    """Client for interacting with Spark and Iceberg tables"""

    def __init__(self, app_name: str = "bcn"):
        """
        Initialize Spark client

        Args:
            app_name: Spark application name
        """
        self.app_name = app_name
        self._spark = None
        self.catalog_name = Config.CATALOG_NAME

    def get_spark_session(self):
        """Get or create Spark session with Iceberg configuration"""
        if self._spark is None:
            from pyspark.sql import SparkSession

            catalog_name = Config.CATALOG_NAME
            catalog_type = Config.CATALOG_TYPE

            builder = SparkSession.builder.appName(self.app_name)

            # Common Spark catalog configuration
            builder = (
                builder.config(
                    f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
                )
                .config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
                .config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    f"s3a://{Config.WAREHOUSE_BUCKET}/",
                )
                .config(
                    f"spark.sql.catalog.{catalog_name}.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
                .config("spark.sql.defaultCatalog", catalog_name)
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
            )

            # Catalog-specific configuration
            if catalog_type == "hive":
                # Hive metastore configuration
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.uri", Config.HIVE_METASTORE_URI
                )
            elif catalog_type == "glue":
                # AWS Glue configuration - uses AWS SDK defaults
                # Glue catalog uses AWS credentials from environment or IAM roles
                pass

            # S3 configuration for Iceberg catalog
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.s3.access-key-id", Config.S3_ACCESS_KEY
            ).config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", Config.S3_SECRET_KEY)

            # S3 endpoint and path-style access (only for MinIO/custom S3)
            if Config.S3_ENDPOINT and Config.S3_ENDPOINT.strip():
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.s3.endpoint", Config.S3_ENDPOINT
                )

            if Config.S3_PATH_STYLE_ACCESS:
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true"
                )

            # Hadoop S3A configuration (for direct S3 access)
            builder = (
                builder.config("spark.hadoop.fs.s3a.access.key", Config.S3_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", Config.S3_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            )

            # Hadoop S3A endpoint and path-style (only for MinIO/custom S3)
            if Config.S3_ENDPOINT and Config.S3_ENDPOINT.strip():
                builder = builder.config("spark.hadoop.fs.s3a.endpoint", Config.S3_ENDPOINT)

            if Config.S3_PATH_STYLE_ACCESS:
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")

            self._spark = builder.getOrCreate()

            # Set log level
            self._spark.sparkContext.setLogLevel("WARN")

        return self._spark

    def execute_sql(self, sql: str) -> Optional[List[Dict]]:
        """
        Execute SQL query and return results

        Args:
            sql: SQL query to execute

        Returns:
            List of rows as dictionaries, or None if error
        """
        try:
            spark = self.get_spark_session()
            df = spark.sql(sql)

            # If it's a SELECT query, return results
            if sql.strip().upper().startswith("SELECT"):
                rows = df.collect()
                return [row.asDict() for row in rows]
            else:
                # For DDL/DML, just execute
                return []

        except Exception as e:
            print(f"Error executing SQL: {e}")
            import traceback

            traceback.print_exc()
            return None

    def create_database(self, database: str) -> bool:
        """
        Create a database if it doesn't exist

        Args:
            database: Database name

        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{database}"
            self.execute_sql(sql)
            print(f"✓ Created database: {database}")
            return True
        except Exception as e:
            print(f"Error creating database {database}: {e}")
            return False

    def drop_table(self, database: str, table: str) -> bool:
        """
        Drop a table if it exists

        Args:
            database: Database name
            table: Table name

        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"DROP TABLE IF EXISTS {self.catalog_name}.{database}.{table} PURGE"
            self.execute_sql(sql)
            print(f"✓ Dropped table: {database}.{table}")
            return True
        except Exception as e:
            print(f"Error dropping table {database}.{table}: {e}")
            return False

    def create_table(self, database: str, table: str, schema: str, location: str) -> bool:
        """
        Create an Iceberg table

        Args:
            database: Database name
            table: Table name
            schema: Table schema (e.g., "id INT, name STRING")
            location: S3 location for the table

        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{database}.{table} (
                    {schema}
                )
                USING iceberg
                LOCATION '{location}'
            """
            self.execute_sql(sql)
            print(f"✓ Created table: {database}.{table}")
            return True
        except Exception as e:
            print(f"Error creating table {database}.{table}: {e}")
            return False

    def insert_data(self, database: str, table: str, values: List[tuple]) -> bool:
        """
        Insert data into a table

        Args:
            database: Database name
            table: Table name
            values: List of tuples representing rows to insert

        Returns:
            True if successful, False otherwise
        """
        try:
            spark = self.get_spark_session()

            # Get the table schema to extract column names
            table_ref = spark.table(f"{self.catalog_name}.{database}.{table}")
            column_names = table_ref.columns

            # Convert values to DataFrame with proper column names
            df = spark.createDataFrame(values, schema=column_names)

            # Write to table
            df.writeTo(f"{self.catalog_name}.{database}.{table}").append()

            print(f"✓ Inserted {len(values)} rows into {database}.{table}")
            return True
        except Exception as e:
            print(f"Error inserting data into {database}.{table}: {e}")
            return False

    def query_table(self, database: str, table: str) -> Optional[List[Dict]]:
        """
        Query all data from a table

        Args:
            database: Database name
            table: Table name

        Returns:
            List of rows as dictionaries
        """
        try:
            sql = f"SELECT * FROM {self.catalog_name}.{database}.{table} ORDER BY id"
            return self.execute_sql(sql)
        except Exception as e:
            print(f"Error querying table {database}.{table}: {e}")
            return None

    def get_row_count(self, database: str, table: str) -> Optional[int]:
        """
        Get row count from a table

        Args:
            database: Database name
            table: Table name

        Returns:
            Number of rows, or None if error
        """
        try:
            sql = f"SELECT COUNT(*) as count FROM {self.catalog_name}.{database}.{table}"
            result = self.execute_sql(sql)
            if result and len(result) > 0:
                return result[0]["count"]
            return None
        except Exception as e:
            print(f"Error getting row count from {database}.{table}: {e}")
            return None

    def list_tables(self, database: str) -> Optional[List[str]]:
        """
        List all tables in a database

        Args:
            database: Database name

        Returns:
            List of table names
        """
        try:
            sql = f"SHOW TABLES IN {self.catalog_name}.{database}"
            result = self.execute_sql(sql)
            if result:
                return [row["tableName"] for row in result]
            return []
        except Exception as e:
            print(f"Error listing tables in {database}: {e}")
            return None

    def cleanup_database(self, database: str) -> bool:
        """
        Drop all tables in a database

        Args:
            database: Database name

        Returns:
            True if successful, False otherwise
        """
        try:
            tables = self.list_tables(database)
            if tables:
                for table in tables:
                    self.drop_table(database, table)
            print(f"✓ Cleaned up database: {database}")
            return True
        except Exception as e:
            print(f"Error cleaning up database {database}: {e}")
            return False

    def get_table_metadata(self, database: str, table: str) -> Optional[Dict]:
        """
        Get table metadata using Iceberg metadata tables

        Args:
            database: Database name
            table: Table name

        Returns:
            Dictionary containing table metadata including location and metadata_location
        """
        try:
            spark = self.get_spark_session()

            metadata = {"database": database, "table": table}

            # Get table location using DESCRIBE FORMATTED
            desc_result = spark.sql(
                f"DESCRIBE FORMATTED {self.catalog_name}.{database}.{table}"
            ).collect()
            for row in desc_result:
                if row["col_name"] and row["col_name"].strip() == "Location":
                    metadata["location"] = row["data_type"].strip() if row["data_type"] else None
                    break

            # Get metadata file location from metadata_log_entries
            metadata_log_sql = f"""
                SELECT file
                FROM {self.catalog_name}.{database}.{table}.metadata_log_entries
                ORDER BY timestamp DESC
                LIMIT 1
            """
            metadata_log_result = spark.sql(metadata_log_sql).collect()

            if metadata_log_result and len(metadata_log_result) > 0:
                metadata["metadata_location"] = metadata_log_result[0]["file"]
            else:
                print(f"Warning: No metadata log entries found for {database}.{table}")

            return metadata

        except Exception as e:
            print(f"Error getting table metadata for {database}.{table}: {e}")
            import traceback

            traceback.print_exc()
            return None

    def create_iceberg_table_from_metadata(
        self, database: str, table: str, location: str, metadata_location: str
    ) -> bool:
        """
        Register an Iceberg table in the catalog pointing to existing metadata

        Args:
            database: Database name
            table: Table name
            location: Table S3 location (not used with register_table, but kept for API compatibility)
            metadata_location: Metadata file S3 location

        Returns:
            True if successful, False otherwise
        """
        try:
            spark = self.get_spark_session()

            # Drop the table if it exists to ensure a clean restore
            self.drop_table(database, table)

            # Use Spark CALL to register the table with its metadata location
            # This is the correct way to register an existing Iceberg table
            sql = f"""
                CALL {self.catalog_name}.system.register_table(
                    table => '{self.catalog_name}.{database}.{table}',
                    metadata_file => '{metadata_location}'
                )
            """

            spark.sql(sql)
            print(f"✓ Registered Iceberg table {database}.{table}")
            return True

        except Exception as e:
            print(f"Error registering table {database}.{table}: {e}")
            import traceback

            traceback.print_exc()
            return False

    def close(self):
        """Close Spark session"""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
