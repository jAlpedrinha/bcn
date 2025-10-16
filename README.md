# BCN - (B)ackup and Restore for Iceberg (C)atalogs for (N)extgen

A backup and restore solution for Apache Iceberg tables, similar to Amazon Redshift snapshots. This MVP allows you to create portable backups of Iceberg tables and restore them to different locations while maintaining full queryability.

## Features

- **Backup Iceberg Tables**: Create portable backups with abstracted paths
- **Restore to New Locations**: Restore backups to different table locations and names
- **Hive Metastore Support**: Works with Hive Metastore catalog
- **Path Abstraction**: Automatically handles path transformations for portability
- **Metadata Preservation**: Maintains Iceberg metadata and snapshot information
- **Data File Management**: Copies data files to new locations during restore

## Architecture

The system consists of two main operations:

### Backup Process
1. Retrieves table metadata from Hive Metastore
2. Downloads Iceberg metadata and manifest files
3. Abstracts paths (removes table location prefix)
4. Uploads abstracted metadata to backup bucket
5. Stores data file references (actual data remains in place)

### Restore Process
1. Downloads backup metadata
2. Restores paths with new table location
3. Copies data files from original to new location
4. Uploads restored metadata to new location
5. Registers new table in Hive Metastore

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Sufficient disk space for MinIO data

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts:
- MinIO (S3-compatible storage)
- PostgreSQL (Hive Metastore database)
- Hive Metastore
- HiveServer2
- Spark with Iceberg support

Wait for services to be healthy (2-3 minutes):
```bash
docker ps
```

### 2. Run End-to-End Test

The easiest way to verify everything works is to run the automated E2E test:

```bash
./test_runner.sh
```

This will:
1. Clean up the catalog
2. Create a test table with sample data
3. Create a backup
4. Restore to a new table
5. Validate that both tables have identical data

## Local Development Setup

To run tests and scripts **locally** (without Docker containers), use the setup script:

```bash
./setup_local_dev.sh
```

This will:
- ✓ Check for Java 11+ installation
- ✓ Install uv and Python dependencies
- ✓ Download Iceberg runtime JARs
- ✓ Create `.env.local` with environment variables
- ✓ Verify Docker infrastructure is running

After setup, load the environment and run tests locally:

```bash
# Load environment
source .env.local

# Run tests locally (no container needed!)
uv run pytest tests/ -v
```

**Requirements for local development:**
- Java 11 or higher (install with: `brew install openjdk@11`)
- Docker (for infrastructure services only)
- uv or Python 3.8+

## Manual Usage

### Install Dependencies

Using [uv](https://docs.astral.sh/uv/) (recommended):
```bash
# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies and create virtual environment
uv sync
```

Or using pip:
```bash
pip install -r requirements.txt
```

### Create a Backup

```bash
# If using uv
uv run python src/backup.py \
  --database default \
  --table my_table \
  --backup-name my_backup

# Or if using pip
cd src
python backup.py \
  --database default \
  --table my_table \
  --backup-name my_backup
```

### Restore a Backup

```bash
# If using uv
uv run python src/restore.py \
  --backup-name my_backup \
  --target-database default \
  --target-table my_table_restored \
  --target-location s3://warehouse/default/my_table_restored \
  --catalog-type hive

# Or if using pip
cd src
python restore.py \
  --backup-name my_backup \
  --target-database default \
  --target-table my_table_restored \
  --target-location s3://warehouse/default/my_table_restored \
  --catalog-type hive
```

## Project Structure

```
bcn/
 src/                      # Python source code
    backup.py            # Backup orchestrator
    restore.py           # Restore orchestrator
    test_e2e.py          # End-to-end test
    spark_client.py      # Spark/Iceberg client
    s3_client.py         # S3/MinIO client
    hive_client.py       # Hive Metastore client
    iceberg_utils.py     # Iceberg metadata utilities
    config.py            # Configuration
 specs/                    # Specifications
    1 - MVP.MD           # MVP specification
 study/                    # Study documents
 docker-compose.yml        # Infrastructure definition
 requirements.txt          # Python dependencies
 test_runner.sh           # E2E test runner (Spark container)
 run_e2e_test.sh          # E2E test runner (local)
```

## Configuration

Environment variables (all have defaults for local docker-compose setup):

- `S3_ENDPOINT`: S3 endpoint URL (default: http://localhost:9000)
- `S3_ACCESS_KEY`: S3 access key (default: admin)
- `S3_SECRET_KEY`: S3 secret key (default: password)
- `HIVE_METASTORE_URI`: Hive Metastore URI (default: thrift://localhost:9083)
- `BACKUP_BUCKET`: S3 bucket for backups (default: iceberg)
- `WAREHOUSE_BUCKET`: S3 bucket for warehouse (default: warehouse)

## How It Works

### Path Abstraction

The backup process abstracts paths to make backups portable:

**Original path:**
```
s3://warehouse/default/my_table/metadata/snap-123.avro
```

**Abstracted path:**
```
metadata/snap-123.avro
```

During restore, paths are reconstructed with the new table location:

**Restored path:**
```
s3://warehouse/default/my_table_restored/metadata/snap-123.avro
```

### Metadata Handling

The system handles three types of Iceberg files:

1. **Metadata JSON**: Main table metadata with schema and snapshot info
2. **Manifest Lists (Avro)**: Lists of manifest files for a snapshot
3. **Manifest Files (Avro)**: Lists of data files with statistics

All paths in these files are abstracted during backup and restored during restore.

## Testing

The project uses **pytest** for testing with a comprehensive test suite.

### Running Tests

**Recommended (runs in Spark container with all dependencies):**
```bash
./test_runner.sh
```

**For local development:**
```bash
# Using uv (recommended)
uv sync  # Install dependencies
uv run pytest tests/ -v

# Run specific test
uv run pytest tests/test_backup_restore.py::TestBackupRestore::test_complete_backup_restore_workflow -v

# Run only E2E tests
uv run pytest tests/ -m e2e -v

# Or using pip
pip install -r requirements.txt
pytest tests/ -v
```

### Test Suite

The test suite includes:

1. **E2E Tests** (`tests/test_backup_restore.py`):
   - Complete backup and restore workflow
   - Schema preservation validation
   - Multiple backup handling
   - Error handling for nonexistent tables/backups

2. **Fixtures** (`tests/conftest.py`):
   - Infrastructure health checks
   - Spark session management
   - Clean database provisioning
   - Sample data generation

3. **Infrastructure Management** (`tests/infrastructure.py`):
   - Docker container health monitoring
   - Automatic service startup
   - Wait-for-healthy logic

### What Gets Tested

- ✓ Catalog cleanup
- ✓ Table creation with sample data
- ✓ Backup creation
- ✓ Restore to new table
- ✓ Data integrity (row-by-row comparison)
- ✓ Schema preservation
- ✓ Error handling

### Manual Testing

You can also test manually using Spark SQL:

```bash
# Enter Spark container
docker exec -it spark-iceberg /bin/bash

# Start Spark SQL
spark-sql --conf spark.sql.catalog.hive_catalog=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.hive_catalog.type=hive \
          --conf spark.sql.catalog.hive_catalog.uri=thrift://hive-metastore:9083

# Create a table
CREATE TABLE hive_catalog.default.test_table (
  id INT,
  name STRING
) USING iceberg;

# Insert data
INSERT INTO hive_catalog.default.test_table VALUES (1, 'Alice'), (2, 'Bob');

# Query
SELECT * FROM hive_catalog.default.test_table;
```

## Limitations (MVP)

- Snapshot history is cleared (assumes no history)
- Only Hive Metastore catalog supported (Glue planned)
- Synchronous data copy (no parallel transfers)
- Manifest files stored as JSON for simplicity

## Future Enhancements

- Support for AWS Glue catalog
- Parallel data file copying
- Incremental backups
- Snapshot history preservation
- Compression for backup metadata
- Backup retention policies
- Restore validation and rollback

## Troubleshooting

### Services not starting

Check logs:
```bash
docker-compose logs hive-metastore
docker-compose logs minio
```

### Connection errors

Ensure all services are healthy:
```bash
docker ps
```

### Test failures

Check environment variables are set correctly and services are running.

## License

MIT

## Contributing

See CLAUDE.md for development guidelines.
