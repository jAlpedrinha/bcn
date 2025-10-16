# 2 - Code Quality Improvements

## Overview

This document outlines identified code quality issues in the BCN codebase, specifically focusing on orphan code (unused functions, classes, and methods) and unused return values that should be cleaned up.

## Analysis Date

Analysis performed: October 16, 2025

## Findings

### 1. Completely Unused Classes

#### 1.1 IcebergMetadataParser Class

**Location**: `src/bcn/iceberg_utils.py:14-81`

**Status**: Completely unused - never imported or referenced anywhere in the codebase

**Contains**:
- `parse_metadata_file()` - line 18
- `extract_table_location()` - line 31
- `get_current_snapshot_id()` - line 44
- `get_snapshots()` - line 57
- `get_snapshot_log()` - line 70

**Impact**: ~67 lines of dead code

**Recommendation**: Remove the entire class

#### 1.2 HiveMetastoreClient Class

**Location**: `src/bcn/hive_client.py:10-229`

**Status**: Completely unused - entire class never imported or used

**Details**:
- The codebase uses `SparkClient` directly to interact with the Hive catalog
- Contains 7 methods including:
  - `get_table_metadata()` - line 48
  - `create_iceberg_table()` - line 87
  - `update_table_metadata_location()` - line 159
  - `_iceberg_type_to_hive_type()` - line 191
  - And others

**Impact**: ~220 lines of dead code

**Recommendation**: Remove the entire class unless there are future plans to support direct Hive Metastore interaction (without Spark)

### 2. Unused Methods in Active Classes

#### 2.1 PathAbstractor Class Methods

**Location**: `src/bcn/iceberg_utils.py`

**Unused Methods**:
- `abstract_snapshot_metadata()` - line 127
- `restore_snapshot_metadata()` - line 149

**Details**: These methods are defined but never called anywhere in the codebase

**Recommendation**: Remove both methods

#### 2.2 ManifestFileHandler Class Methods

**Location**: `src/bcn/iceberg_utils.py`

**Unused Methods**:

1. `_convert_str_to_bytes()` - line 271
   - Defined but never called
   - Note: The inverse function `_convert_bytes_to_str()` IS used

2. `read_manifest_list()` - line 214
   - Similar to `read_manifest_file()` but never used
   - Code uses `read_manifest_file()` instead

3. `abstract_manifest_paths()` - line 288
   - The non-Avro version
   - Replaced by `abstract_manifest_paths_avro()`
   - Still uses `_convert_bytes_to_str()` which adds overhead

**Recommendation**: Remove all three methods

#### 2.3 S3Client Class Methods

**Location**: `src/bcn/s3_client.py`

**Unused Methods**:

1. `download_file()` - line 22
   - Never used
   - Code uses `read_object()` instead for direct memory operations

2. `upload_file()` - line 42
   - Never used
   - Code uses `write_object()` instead for direct memory operations

3. `list_objects()` - line 61
   - Never used anywhere in the codebase

**Recommendation**: Remove all three methods (can be added back if file-based operations are needed in the future)

#### 2.4 IcebergBackup Class Methods

**Location**: `src/bcn/backup.py`

**Unused Methods**:
- `_process_manifest_file()` - line 198

**Details**: Defined but never called. Contains logic for downloading and abstracting manifest files that is implemented differently elsewhere.

**Recommendation**: Remove this method

### 3. Unused Return Values / Underutilized Data

#### 3.1 Return Value Not Used

**Location**: `src/bcn/restore.py:134-135`

```python
if not self._copy_data_files(data_files, original_location):
    print("Warning: Some data files could not be copied")
```

**Issue**: The boolean return value is checked but the code continues regardless of success/failure. The return value doesn't affect control flow.

**Recommendation**:
- Make the function raise an exception on failure

#### 3.2 Overcomplex Return Types

**Location**: `src/bcn/spark_client.py`

**Methods with underutilized return values**:

1. `execute_sql()` - line 103
   - Returns `Optional[List[Dict]]`
   - Callers often just check if it's not None without using the actual data

2. `get_table_metadata()` - line 310
   - Returns a dict with many fields
   - Only `location` and `metadata_location` are actually used from the return value

**Recommendation**: Consider simplifying return types or documenting which fields are actually used

### 4. Dead Code Paths

#### 4.1 Unreachable JSON Handling

**Location**: `src/bcn/backup.py:220-227`

```python
try:
    manifest_json = json.loads(content.decode("utf-8"))
    return {"type": "json", "content": manifest_json}
except Exception:
    print(f"  Warning: Could not parse manifest file {manifest_path}")
    return None
```

**Issue**: This code is inside `_process_manifest_file()` which is never called, making this entire code path dead.

**Recommendation**: Remove with the parent method

## Summary Statistics

| Category | Count | Lines of Code |
|----------|-------|---------------|
| Completely unused classes | 2 | ~287 |
| Unused methods in active classes | 11 | ~150 |
| Unused return value checks | 1 | - |
| Dead code paths | 1 | ~8 |
| **Total estimated orphan code** | - | **~445 lines (~20% of codebase)** |

## Prioritized Recommendations

### High Priority (Remove Immediately)

1. Remove `IcebergMetadataParser` class (src/bcn/iceberg_utils.py:14-81)
2. Remove `HiveMetastoreClient` class (src/bcn/hive_client.py:10-229)
3. Remove `_convert_str_to_bytes()` method (src/bcn/iceberg_utils.py:271)
4. Remove S3Client methods: `download_file()`, `upload_file()`, `list_objects()`
5. Remove `_process_manifest_file()` from IcebergBackup (src/bcn/backup.py:198)

### Medium Priority (Consider Removing)

1. Remove `abstract_snapshot_metadata()` and `restore_snapshot_metadata()` from PathAbstractor
2. Remove `read_manifest_list()` from ManifestFileHandler
3. Remove `abstract_manifest_paths()` (non-Avro version) from ManifestFileHandler

### Low Priority (Review and Optimize)

1. Review return value usage in `_copy_data_files()`
2. Simplify return types for methods where return values aren't fully utilized
3. Add documentation for which fields in complex return types are actually used

## Implementation Notes

- All removals should be done with proper testing to ensure no hidden dependencies
- Consider adding a linting tool (like `vulture` or `pylint`) to automatically detect unused code in the future
- After cleanup, re-run tests to ensure nothing breaks

## Next Steps

1. Review this document and approve changes
2. Create implementation tasks for each priority level
3. Execute removals starting with high priority items
4. Run full test suite after each removal
5. Update documentation if needed
