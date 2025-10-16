# 4 - Critical Issues Refactoring

## Overview
This spec addresses the two remaining critical issues from the Code Review (docs/specs/3 - CodeReview.md):
1. Print-and-return anti-pattern
2. Missing logging infrastructure

These issues are critical because they affect maintainability, debuggability, and production readiness of the BCN codebase.

## Problem Statement

### Issue #1: Print-and-return Anti-pattern
**Location**: Throughout backup.py, restore.py, spark_client.py, s3_client.py

**Current State**:
```python
def some_method(self) -> bool:
    print("Error: something went wrong")
    return False
```

**Problems**:
- Cannot suppress output in library usage
- Cannot redirect output to logs
- Cannot test output programmatically
- Mixes concerns (business logic + presentation)
- Poor separation of concerns

### Issue #2: No Logging Infrastructure
**Location**: Entire codebase

**Current State**:
- All output uses `print()` statements
- No log levels (DEBUG, INFO, WARNING, ERROR)
- No ability to configure logging
- No structured logging
- No log file support

**Problems**:
- Cannot filter messages by severity
- Cannot debug production issues
- Cannot integrate with monitoring systems
- Cannot control verbosity

## Solution Design

### 1. Logging Infrastructure Setup

#### 1.1 Create logging configuration module
**File**: `src/bcn/logging_config.py`

```python
"""
Logging configuration for BCN
"""
import logging
import sys
from typing import Optional

class BCNLogger:
    """Centralized logging setup for BCN"""

    _loggers = {}
    _configured = False

    @staticmethod
    def setup_logging(
        level: str = "INFO",
        log_file: Optional[str] = None,
        format_string: Optional[str] = None
    ):
        """
        Configure logging for BCN

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional file path for logs
            format_string: Custom format string
        """
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        # Configure root logger
        root_logger = logging.getLogger("bcn")
        root_logger.setLevel(getattr(logging, level.upper()))

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(console_handler)

        # File handler if specified
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(logging.Formatter(format_string))
            root_logger.addHandler(file_handler)

        BCNLogger._configured = True

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger for a module"""
        if not BCNLogger._configured:
            BCNLogger.setup_logging()

        if name not in BCNLogger._loggers:
            BCNLogger._loggers[name] = logging.getLogger(f"bcn.{name}")

        return BCNLogger._loggers[name]
```

#### 1.2 Add logging to Config
**File**: `src/bcn/config.py`

Add configuration options:
```python
# Logging configuration
LOG_LEVEL = os.getenv("BCN_LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("BCN_LOG_FILE", None)
```

### 2. Replace Print-and-return Pattern

#### 2.1 Pattern for Methods Returning Bool

**Before**:
```python
def create_backup(self) -> bool:
    try:
        print(f"Starting backup of {self.database}.{self.table}")
        # ... logic ...
        if not table_metadata:
            print(f"Error: Could not retrieve metadata")
            return False
        print("✓ Backup created successfully!")
        return True
    except Exception as e:
        print(f"Error during backup: {e}")
        return False
```

**After**:
```python
def create_backup(self) -> bool:
    logger = BCNLogger.get_logger(__name__)
    try:
        logger.info(f"Starting backup of {self.database}.{self.table}")
        # ... logic ...
        if not table_metadata:
            logger.error(f"Could not retrieve metadata for {self.database}.{self.table}")
            return False
        logger.info(f"Backup '{self.backup_name}' created successfully")
        return True
    except Exception as e:
        logger.error(f"Error during backup: {e}", exc_info=True)
        return False
```

#### 2.2 Pattern for Methods Returning Optional Data

**Before**:
```python
def read_object(self, bucket: str, key: str) -> Optional[bytes]:
    try:
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except ClientError as e:
        print(f"Error reading {bucket}/{key}: {e}")
        return None
```

**After**:
```python
def read_object(self, bucket: str, key: str) -> Optional[bytes]:
    logger = BCNLogger.get_logger(__name__)
    try:
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except ClientError as e:
        logger.error(f"Error reading s3://{bucket}/{key}: {e}")
        return None
```

#### 2.3 Progress Messages

For progress messages that should always be visible to CLI users:

**Before**:
```python
print(f"  Copied {copied}/{len(data_files)} files...")
```

**After**:
```python
logger.info(f"Copied {copied}/{len(data_files)} files")
```

### 3. Migration Strategy

#### Phase 1: Infrastructure Setup
1. Create `src/bcn/logging_config.py`
2. Update `src/bcn/config.py` with logging config
3. Add logger initialization to main entry points

#### Phase 2: Module-by-Module Migration
1. **s3_client.py** - Simple, few methods
2. **spark_client.py** - Moderate complexity
3. **iceberg_utils.py** - Minimal output
4. **backup.py** - Complex, many print statements
5. **restore.py** - Complex, many print statements

#### Phase 3: CLI Enhancement
1. Add `--log-level` argument to backup.py and restore.py CLIs
2. Add `--log-file` argument to backup.py and restore.py CLIs
3. Keep user-facing progress messages at INFO level
4. Move technical details to DEBUG level

### 4. Testing Strategy

#### 4.1 Unit Tests for Logging Config
**File**: `tests/test_logging.py`

```python
def test_logger_setup():
    """Test logger configuration"""
    BCNLogger.setup_logging(level="DEBUG")
    logger = BCNLogger.get_logger("test")
    assert logger.level == logging.DEBUG

def test_logger_output(caplog):
    """Test logger output capture"""
    logger = BCNLogger.get_logger("test")
    logger.info("Test message")
    assert "Test message" in caplog.text
```

#### 4.2 Update Existing Tests
- Tests should still pass with logging enabled
- Use pytest's `caplog` fixture to verify log messages where needed
- Ensure progress messages are logged at appropriate levels

### 5. Log Level Guidelines

**DEBUG**: Detailed technical information for debugging
- S3 URIs being parsed
- Avro schema details
- Intermediate calculation results

**INFO**: User-facing progress and status
- "Starting backup of db.table"
- "Copied 100/500 files"
- "✓ Backup created successfully"

**WARNING**: Recoverable issues or deprecated usage
- "Could not read manifest file, skipping"
- "Metadata log empty, using snapshot directly"

**ERROR**: Failures that cause operation failure
- "Could not retrieve metadata for table"
- "Failed to copy data files"
- "Error registering table in catalog"

## Implementation Checklist

### Phase 1: Infrastructure
- [ ] Create `src/bcn/logging_config.py` with `BCNLogger` class
- [ ] Add logging configuration to `src/bcn/config.py`
- [ ] Write unit tests for logging configuration
- [ ] Test logging configuration

### Phase 2: S3 Client Migration
- [ ] Replace all `print()` calls in `s3_client.py` with logger calls
- [ ] Add logger initialization at module level
- [ ] Test S3 client with logging
- [ ] Commit: "Migrate s3_client.py to logging infrastructure"

### Phase 3: Spark Client Migration
- [ ] Replace all `print()` calls in `spark_client.py` with logger calls
- [ ] Add logger initialization at module level
- [ ] Test Spark client with logging
- [ ] Commit: "Migrate spark_client.py to logging infrastructure"

### Phase 4: Iceberg Utils Migration
- [ ] Replace any `print()` calls in `iceberg_utils.py` with logger calls
- [ ] Add logger initialization at module level
- [ ] Test iceberg utils with logging
- [ ] Commit: "Migrate iceberg_utils.py to logging infrastructure"

### Phase 5: Backup Migration
- [ ] Replace all `print()` calls in `backup.py` with logger calls
- [ ] Add logger initialization at module level
- [ ] Add CLI arguments for `--log-level` and `--log-file`
- [ ] Initialize logging in `main()` with CLI arguments
- [ ] Test backup with different log levels
- [ ] Commit: "Migrate backup.py to logging infrastructure"

### Phase 6: Restore Migration
- [ ] Replace all `print()` calls in `restore.py` with logger calls
- [ ] Add logger initialization at module level
- [ ] Add CLI arguments for `--log-level` and `--log-file`
- [ ] Initialize logging in `main()` with CLI arguments
- [ ] Test restore with different log levels
- [ ] Commit: "Migrate restore.py to logging infrastructure"

### Phase 7: Integration Testing
- [ ] Run full test suite with logging enabled
- [ ] Test CLI with various log levels (DEBUG, INFO, WARNING, ERROR)
- [ ] Test log file output
- [ ] Update documentation with logging examples
- [ ] Commit: "Complete logging infrastructure migration"

## Success Criteria

1. ✅ No `print()` statements remain in src/bcn/*.py files
2. ✅ All output uses Python logging module
3. ✅ Log levels are used appropriately (DEBUG, INFO, WARNING, ERROR)
4. ✅ CLI tools accept `--log-level` and `--log-file` arguments
5. ✅ All existing tests pass
6. ✅ Log output can be captured and tested
7. ✅ User-facing messages remain clear and informative

## Non-Goals

- **Not** implementing structured logging (JSON logs) - can be added later
- **Not** implementing log rotation - can be added later
- **Not** implementing remote logging - can be added later
- **Not** changing the actual information being communicated - only the mechanism

## References

- Python logging documentation: https://docs.python.org/3/library/logging.html
- Python logging best practices: https://docs.python.org/3/howto/logging.html
- Code Review: docs/specs/3 - CodeReview.md (Issues #1, #2)
