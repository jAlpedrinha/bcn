# 5 - Code Quality Improvements

## Overview
This spec addresses the remaining non-critical issues from the Code Review (docs/specs/3 - CodeReview.md), organized by priority. These improvements will enhance maintainability, readability, and robustness without changing core functionality.

## Problem Categories

### Significant Issues (High Priority)
- **Code Duplication**: Backup and restore share similar logic
- **Error Handling**: Inconsistent error handling patterns
- **Method Size**: Large methods need decomposition

### Minor Issues (Medium Priority)
- **Code Smells**: Nested try-except, magic strings, TODO comments
- **Validation**: Missing input validation
- **Documentation**: Incomplete docstrings

## Significant Issues

### Issue #4: Code Duplication in Manifest Processing
**Location**: backup.py:195-245, restore.py:236-283

**Problem**:
Both backup and restore have nearly identical logic for reading manifest files from S3:
```python
# Pattern repeated in both files
bucket, key = self.s3_client.parse_s3_uri(manifest_path)
content = self.s3_client.read_object(bucket, key)
if not content:
    continue
entries, schema = ManifestFileHandler.read_manifest_file(content)
```

**Solution**:
Create a shared method in `ManifestFileHandler`:

```python
# In iceberg_utils.py
class ManifestFileHandler:
    @staticmethod
    def read_manifest_from_s3(
        s3_client: 'S3Client',
        manifest_path: str,
        table_location: str
    ) -> Tuple[Optional[List[Dict]], Optional[any]]:
        """
        Read manifest file from S3

        Args:
            s3_client: S3 client instance
            manifest_path: Full or relative manifest path
            table_location: Table location for resolving relative paths

        Returns:
            Tuple of (entries, schema) or (None, None) on error
        """
        logger = BCNLogger.get_logger(__name__)

        # Convert relative paths to full S3 URIs
        if not manifest_path.startswith("s3://") and not manifest_path.startswith("s3a://"):
            full_path = f"{table_location}/{manifest_path}"
        else:
            full_path = manifest_path

        try:
            bucket, key = s3_client.parse_s3_uri(full_path)
            content = s3_client.read_object(bucket, key)
            if not content:
                logger.warning(f"Empty content for manifest: {manifest_path}")
                return None, None

            return ManifestFileHandler.read_manifest_file(content)
        except Exception as e:
            logger.error(f"Error reading manifest {manifest_path}: {e}")
            return None, None
```

**Impact**: Reduces ~50 lines of duplicated code, centralizes manifest reading logic.

### Issue #5: Code Duplication in S3 Path Handling
**Location**: backup.py:186-191, restore.py:220-223 (similar patterns throughout)

**Problem**:
Pattern for converting relative paths to full S3 URIs is repeated:
```python
if not path.startswith("s3://") and not path.startswith("s3a://"):
    full_path = f"{table_location}/{path}"
else:
    full_path = path
```

**Solution**:
Add helper method to `PathAbstractor`:

```python
# In iceberg_utils.py
class PathAbstractor:
    @staticmethod
    def resolve_path(path: str, base_location: str) -> str:
        """
        Resolve a potentially relative path to a full S3 URI

        Args:
            path: Path that may be relative or absolute
            base_location: Base location for resolving relative paths

        Returns:
            Full S3 URI
        """
        # Normalize s3a:// to s3://
        if path.startswith("s3a://"):
            return "s3://" + path[6:]

        # Already absolute
        if path.startswith("s3://"):
            return path

        # Relative path - combine with base
        base_location = base_location.rstrip("/")
        path = path.lstrip("/")
        return f"{base_location}/{path}"
```

**Impact**: Eliminates ~30 lines of duplicated logic, standardizes path resolution.

### Issue #7: Inconsistent Error Handling in Backup
**Location**: backup.py:87-130

**Problem**:
Some errors return False (critical), others just warn and continue (non-critical):
```python
if not self._download_backup_metadata():
    print("Error: Could not download backup metadata")
    return False  # Critical - stops execution

# But elsewhere:
except Exception as e:
    print(f"  Warning: Could not read manifest list: {e}")
    continue  # Non-critical - continues
```

**Solution**:
Make error handling policy explicit with custom exceptions:

```python
# In new file: src/bcn/exceptions.py
class BCNError(Exception):
    """Base exception for BCN operations"""
    pass

class BCNCriticalError(BCNError):
    """Critical error that should stop the operation"""
    pass

class BCNWarning(BCNError):
    """Non-critical error that can be logged and skipped"""
    pass
```

Then use consistently:
```python
def _collect_manifest_files(self, metadata: Dict, table_location: str) -> List[str]:
    manifest_files = []
    for snapshot in metadata.get("snapshots", []):
        try:
            # ... process manifest ...
            manifest_files.append(manifest_list_path)
        except Exception as e:
            # Non-critical: missing one manifest shouldn't fail entire backup
            raise BCNWarning(f"Could not process snapshot manifest: {e}")
    return manifest_files
```

**Impact**: Clear error handling policy, easier to understand what's critical vs. non-critical.

### Issue #8: No Error Context in S3 Operations
**Location**: s3_client.py:36-41, 55-59, 74-78

**Problem**:
S3 errors don't include operation context:
```python
except ClientError as e:
    print(f"Error reading {bucket}/{key}: {e}")
    return None
```

**Solution**:
Add context to error messages and re-raise with custom exceptions:

```python
def read_object(self, bucket: str, key: str) -> Optional[bytes]:
    logger = BCNLogger.get_logger(__name__)
    try:
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(
            f"S3 read failed: s3://{bucket}/{key}",
            extra={
                "error_code": error_code,
                "bucket": bucket,
                "key": key,
                "operation": "read_object"
            }
        )
        return None
```

**Impact**: Better error messages for debugging S3 issues.

### Issue #9: Silent Failures in Spark Operations
**Location**: spark_client.py:114-129

**Problem**:
SQL execution failures return None, losing error information:
```python
except Exception as e:
    print(f"Error executing SQL: {e}")
    return None
```

**Solution**:
Let critical errors propagate, catch only specific exceptions:

```python
def execute_sql(self, sql: str) -> Optional[List[Dict]]:
    logger = BCNLogger.get_logger(__name__)
    try:
        spark = self.get_spark_session()
        df = spark.sql(sql)

        if sql.strip().upper().startswith("SELECT"):
            rows = df.collect()
            return [row.asDict() for row in rows]
        else:
            return []
    except AnalysisException as e:
        # Expected for missing tables, etc.
        logger.warning(f"SQL analysis error: {e}")
        return None
    except Exception as e:
        # Unexpected errors should propagate
        logger.error(f"SQL execution failed: {sql[:100]}...")
        raise
```

**Impact**: Critical errors are visible, expected errors are handled gracefully.

### Issue #10: No Validation for Critical Inputs
**Location**: backup.py:26-41, restore.py:26-49

**Problem**:
No validation of constructor parameters:
```python
def __init__(self, database: str, table: str, backup_name: str):
    self.database = database
    self.table = table
    self.backup_name = backup_name
    # No validation!
```

**Solution**:
Add input validation:

```python
def __init__(self, database: str, table: str, backup_name: str):
    # Validate inputs
    if not database or not database.strip():
        raise ValueError("Database name cannot be empty")
    if not table or not table.strip():
        raise ValueError("Table name cannot be empty")
    if not backup_name or not backup_name.strip():
        raise ValueError("Backup name cannot be empty")

    # Check for invalid characters in backup_name
    if not re.match(r'^[a-zA-Z0-9_-]+$', backup_name):
        raise ValueError(
            "Backup name must contain only letters, numbers, hyphens, and underscores"
        )

    self.database = database.strip()
    self.table = table.strip()
    self.backup_name = backup_name.strip()
    # ...
```

**Impact**: Fail fast with clear error messages instead of mysterious failures later.

### Issue #12: Large Method - create_backup()
**Location**: backup.py:42-176 (135 lines)

**Problem**:
The `create_backup()` method does everything - retrieval, processing, uploading.

**Solution**:
Break into smaller focused methods:

```python
def create_backup(self) -> bool:
    """Create a backup of the Iceberg table"""
    logger = BCNLogger.get_logger(__name__)
    try:
        logger.info(f"Starting backup of {self.database}.{self.table}")

        # Step 1: Get metadata
        metadata = self._get_table_metadata()

        # Step 2: Process metadata and manifests
        backup_data = self._process_metadata(metadata)

        # Step 3: Upload to S3
        self._upload_backup(backup_data)

        # Step 4: Cleanup
        self._cleanup_work_dir()

        logger.info(f"Backup '{self.backup_name}' created successfully")
        return True

    except BCNCriticalError as e:
        logger.error(f"Backup failed: {e}")
        return False

def _get_table_metadata(self) -> Dict:
    """Get and validate table metadata from Spark catalog"""
    # Extract lines 52-78

def _process_metadata(self, metadata: Dict) -> Dict:
    """Process metadata, manifests, and collect file references"""
    # Extract lines 80-147

def _upload_backup(self, backup_data: Dict) -> None:
    """Upload backup files to S3"""
    # Extract lines 154-163
```

**Impact**: Each method has a single responsibility, easier to test and understand.

### Issue #13: Large Method - restore_backup()
**Location**: restore.py:51-218 (168 lines)

**Problem**:
Similar to create_backup(), does too much in one method.

**Solution**:
Similar decomposition:

```python
def restore_backup(self) -> bool:
    """Restore a backup to a new table"""
    logger = BCNLogger.get_logger(__name__)
    try:
        logger.info(f"Starting restore of '{self.backup_name}'")

        # Step 1: Download and prepare metadata
        backup_data = self._download_backup_data()

        # Step 2: Process manifests
        restored_manifests = self._restore_manifests(backup_data)

        # Step 3: Copy data files
        self._copy_data_files(backup_data)

        # Step 4: Upload metadata and register table
        self._finalize_restore(backup_data, restored_manifests)

        # Step 5: Cleanup
        self._cleanup_work_dir()

        logger.info(f"Restore completed successfully")
        return True

    except BCNCriticalError as e:
        logger.error(f"Restore failed: {e}")
        return False

def _download_backup_data(self) -> Dict:
    """Download backup metadata and validate"""
    # Extract lines 63-81

def _restore_manifests(self, backup_data: Dict) -> Dict:
    """Process and restore all manifest files"""
    # Extract lines 98-127
```

**Impact**: Easier to understand the restore flow, easier to test individual steps.

### Issue #14: Nested Try-Except Blocks
**Location**: backup.py:103-130, restore.py:176-193

**Problem**:
Try-except blocks nested 3 levels deep:
```python
for manifest_list_path in manifest_files:
    try:
        # ... outer operation ...
        for entry in entries:
            try:
                # ... inner operation ...
            except Exception as e:
                print(f"Warning: {e}")
    except Exception as e:
        print(f"Warning: {e}")
```

**Solution**:
Extract inner loop to separate method:

```python
def _process_manifest_list(self, manifest_list_path: str) -> List[str]:
    """Process a single manifest list and return individual manifest paths"""
    individual_manifests = []

    try:
        # Read manifest list
        entries, _ = ManifestFileHandler.read_manifest_from_s3(
            self.s3_client, manifest_list_path, self.table_location
        )

        # Process each entry
        for entry in entries:
            manifest_path = self._extract_manifest_path(entry)
            if manifest_path:
                individual_manifests.append(manifest_path)

    except Exception as e:
        logger.warning(f"Could not process manifest list {manifest_list_path}: {e}")

    return individual_manifests

# Then in the main method:
for manifest_list_path in manifest_files:
    manifests = self._process_manifest_list(manifest_list_path)
    individual_manifest_paths.extend(manifests)
```

**Impact**: Flatter structure, easier to test, clearer error handling.

## Minor Issues

### Issue #15: Magic Strings for S3 Prefixes
**Location**: Multiple files

**Problem**:
Hardcoded strings like `"metadata/"`, `"data/"`:
```python
metadata_path = f"metadata/{metadata_filename}"
```

**Solution**:
Add constants to Config:

```python
# In config.py
class Config:
    # ... existing config ...

    # Iceberg directory structure
    METADATA_DIR = "metadata"
    DATA_DIR = "data"

# Usage:
metadata_path = f"{Config.METADATA_DIR}/{metadata_filename}"
```

**Impact**: Centralized configuration, easier to change if needed.

### Issue #17: Incomplete Docstrings
**Location**: Throughout codebase

**Problem**:
Some methods lack complete docstrings:
```python
def _collect_manifest_files(self, metadata: Dict, table_location: str) -> List[str]:
    """Collect all manifest file paths from metadata"""
    # No Args/Returns documentation
```

**Solution**:
Complete all docstrings with Args, Returns, Raises:

```python
def _collect_manifest_files(self, metadata: Dict, table_location: str) -> List[str]:
    """
    Collect all manifest file paths from metadata

    Args:
        metadata: Parsed Iceberg metadata JSON
        table_location: Full S3 URI of the table location

    Returns:
        List of full S3 URIs for manifest list files

    Raises:
        BCNWarning: If individual manifest files cannot be read
    """
```

**Impact**: Better API documentation, clearer contracts.

### Issue #18: TODO Comments
**Location**: spark_client.py:217-218

**Problem**:
TODO comments indicate incomplete work:
```python
# Note: Not closing spark_client here as it may be shared with other processes
```

**Solution**:
Document the design decision properly:

```python
# Design decision: Spark session lifecycle is managed by the caller/fixture
# This allows session reuse across multiple operations in tests and CLI usage
# The caller is responsible for calling spark_client.close() when done
```

**Impact**: Clearer intent, not mistaken for incomplete work.

### Issue #19: Unused Imports
**Location**: Check with linter

**Problem**:
May have unused imports after refactoring.

**Solution**:
Run ruff and remove unused imports:
```bash
uv run ruff check --select F401 src/
```

**Impact**: Cleaner code, faster imports.

### Issue #20: Inconsistent String Formatting
**Location**: Throughout codebase

**Problem**:
Mix of f-strings, .format(), and % formatting:
```python
print(f"Table: {table}")
print("Table: {}".format(table))
```

**Solution**:
Standardize on f-strings everywhere (already mostly done):
```python
logger.info(f"Table: {table}")
```

**Impact**: Consistent style, more readable.

### Issue #21: No Type Hints for Internal Methods
**Location**: Private methods throughout

**Problem**:
Some private methods lack type hints:
```python
def _process_something(self, data):  # No type hints
```

**Solution**:
Add type hints to all methods:
```python
def _process_something(self, data: Dict[str, Any]) -> List[str]:
```

**Impact**: Better IDE support, catches type errors earlier.

### Issue #22: Hard-coded Sleep/Retry Logic Missing
**Location**: S3 and Spark operations

**Problem**:
No retry logic for transient failures.

**Solution**:
Add retry decorator for S3 operations:

```python
# In new file: src/bcn/retry.py
import time
from functools import wraps
from typing import Callable, Type, Tuple

def retry_on_error(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable:
    """
    Retry decorator with exponential backoff

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = BCNLogger.get_logger(func.__module__)
            attempt = 0
            current_delay = delay

            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts")
                        raise

                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{max_attempts}), "
                        f"retrying in {current_delay}s: {e}"
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper
    return decorator

# Usage in s3_client.py:
@retry_on_error(max_attempts=3, exceptions=(ClientError,))
def read_object(self, bucket: str, key: str) -> Optional[bytes]:
    # ... existing code ...
```

**Impact**: More resilient to transient network/service issues.

## Implementation Strategy

### Phase 1: Foundations (Depends on Spec 4)
- [ ] Create `src/bcn/exceptions.py` with custom exception classes
- [ ] Create `src/bcn/retry.py` with retry decorator
- [ ] Add constants to Config for magic strings

### Phase 2: Code Duplication (#4, #5)
- [ ] Add `resolve_path()` to PathAbstractor
- [ ] Add `read_manifest_from_s3()` to ManifestFileHandler
- [ ] Update backup.py to use shared methods
- [ ] Update restore.py to use shared methods
- [ ] Test and commit

### Phase 3: Error Handling (#7, #8, #9, #10)
- [ ] Add input validation to IcebergBackup.__init__()
- [ ] Add input validation to IcebergRestore.__init__()
- [ ] Update S3Client error handling with context
- [ ] Update SparkClient error handling to propagate critical errors
- [ ] Add retry decorator to S3 operations
- [ ] Test and commit

### Phase 4: Method Decomposition (#12, #13, #14)
- [ ] Break down create_backup() into smaller methods
- [ ] Break down restore_backup() into smaller methods
- [ ] Extract nested try-except blocks to separate methods
- [ ] Test and commit

### Phase 5: Polish (#15, #17, #18, #19, #20, #21, #22)
- [ ] Replace magic strings with Config constants
- [ ] Complete all docstrings
- [ ] Document design decisions (replace TODOs)
- [ ] Run ruff and remove unused imports
- [ ] Ensure consistent f-string usage
- [ ] Add type hints to all internal methods
- [ ] Test and commit

### Phase 6: Integration
- [ ] Run full test suite
- [ ] Update documentation
- [ ] Create summary of improvements

## Success Criteria

1. ✅ No code duplication in manifest processing
2. ✅ Consistent error handling with custom exceptions
3. ✅ All methods under 50 lines
4. ✅ No nested try-except blocks > 2 levels
5. ✅ All magic strings replaced with constants
6. ✅ All methods have complete docstrings
7. ✅ No unused imports
8. ✅ Consistent f-string usage
9. ✅ Type hints on all methods
10. ✅ Retry logic on S3 operations
11. ✅ All tests passing

## Dependencies

- **Spec 4** must be completed first (logging infrastructure)
- These improvements build on the logging foundation

## References

- Code Review: docs/specs/3 - CodeReview.md (Issues #4-22)
- Python exceptions: https://docs.python.org/3/tutorial/errors.html
- Effective Python: https://effectivepython.com/
