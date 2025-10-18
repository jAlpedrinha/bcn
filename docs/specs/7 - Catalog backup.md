# Catalog backups

We started the MVP focusing on a single table. We still want to be able to backup and restore a single table, but we should also have the option to backup and restore an entire catalog or database inside a catalog.

This requires our backup structure and scripts to be aware of catalog name, database name(s) and table name(s). This will also imply a rework on the backup structure that will now include a list of databases and tables which needs to be organized also at the level of the PIT.

## Backup Structure

The proposed repository structure organizes backups hierarchically with granularity metadata:

```
backup-repo/
├── manifest.json                      # Repository-level metadata (fixes granularity)
├── catalogs/
│   ├── catalog_name/
│   │   ├── catalog_metadata.json      # Catalog-level info and schemas
│   │   ├── databases/
│   │   │   ├── database_name/
│   │   │   │   ├── database_metadata.json
│   │   │   │   ├── table_name/
│   │   │   │   │   ├── snapshots/     # Full table snapshots
│   │   │   │   │   ├── deltas/        # Incremental changes
│   │   │   │   │   └── table_manifest.json
│   │   │   │   └── .deleted_tables.json  # Track deleted tables
│   │   │   └── .deleted_databases.json
│   │   └── pit_manifests/
│   │       └── pit_timestamp_001/
│   │           └── manifest.json      # What's included in this PIT
```

**Repository Manifest** (`manifest.json`):
```json
{
  "version": "1",
  "granularity": "CATALOG|DATABASE|TABLE",
  "scope": {
    "catalog": "prod",
    "database": null,
    "table": null
  },
  "created_at": "2025-10-17T...",
  "last_pit": "2025-10-17T..."
}
```

**PIT Manifest** (tracks what's included):
```json
{
  "pit_timestamp": "2025-10-17T...",
  "parent_pit": "2025-10-16T...",
  "included_objects": {
    "databases": ["db1", "db2"],
    "tables": {
      "db1": ["table1", "table2"],
      "db2": ["table3"]
    }
  },
  "deleted_in_source": {
    "tables": ["db1.old_table"],
    "databases": []
  },
  "unchanged_objects": {
    "tables": ["db1.table2"]  # Verified via checksum
  }
}
```

**Deleted Objects Tracking** (`.deleted_tables.json`):
```json
{
  "deleted_tables": [
    {
      "name": "old_table",
      "database": "db1",
      "deleted_at": "2025-10-16T...",
      "last_pit_included": "2025-10-15T..."
    }
  ]
}
```

## How to achieve it
 - Equip current CLI to accept catalog name as a parameter.
 - It could still receive database name and table name if needed.
 - Repository integrity must be maintained via granularity locking:
    - Once a backup repository is created, its granularity (CATALOG/DATABASE/TABLE) and scope are **fixed and immutable**.
    - Granularity is enforced at repository creation time and stored in `manifest.json`.
    - Incremental backups must maintain the same granularity and scope as the initial backup.
    - Example: If repo was created with granularity=CATALOG, all subsequent backups must be catalog-level.
    - **Enforcement logic:**
      - On first backup: Initialize `manifest.json` with granularity and scope
      - On subsequent backups: Validate incoming scope matches stored scope, reject if mismatch
    - Tables and databases **may be deleted** and that's expected—deletions are tracked in `.deleted_tables.json` and `deleted_databases.json`.
    - **Prevention of ambiguity:** Locking granularity prevents the confusion of "did someone delete this object or just not back it up this time?"
    - **Incremental backups benefit:** Once granularity is locked, users don't need to re-specify catalog/database/table names—the system uses the stored scope.
  - **Restore command is more flexible:** It can restore subsets with different granularity from what was backed up.
    - Example: A full catalog backup can be restored at database or table level to a different location or target.
  - Edge cases must be handled with explicit strategies:

### Edge Case Handling

| Edge Case | Proposed Solution |
|-----------|------------------|
| **Deleted database/table** | Record deletion in `.deleted_databases.json` / `.deleted_tables.json` with timestamp and source object ID. Include in PIT manifest's `deleted_in_source` field. Restore can optionally exclude deleted objects. |
| **Renamed table** | Without object ID tracking: Treat as deletion + creation (may require full backup context). **Recommended:** Implement object ID tracking (e.g., Iceberg table UUID) to detect renames by ID match. |
| **Unchanged object** | Store object in PIT manifest's `unchanged_objects` field with checksum/hash reference to previous backup. Avoids ambiguity with deletion. Include object metadata for verification. |
| **Schema evolution** | Track `schema_version` in table metadata and PIT manifest. Store all schema versions. During restore, validate compatibility and apply transformations if needed. |
| **Table format changes** | Store Iceberg format version, properties, and spec version in table metadata. Validate compatibility during restore. |
| **Concurrent backups** | Implement PIT locking mechanism. New backup cannot proceed if previous PIT is incomplete (check timestamp and `.in_progress` marker). |
| **Orphaned/unreferenced data** | Implement garbage collection metadata tracking which objects belong to which PITs. Allow cleanup of data unreferenced by active PITs. |
| **Large table optimization** | Support partial backups for table-level granularity (optional early optimization). Store backup coverage info in metadata. |

## Tooling

### CLI Commands

**Backup command** enhancements:
- Accept optional `--catalog`, `--database`, `--table` parameters
- On first backup: Infer granularity from parameters and lock it
- On subsequent backups: Validate scope matches and proceed with incremental logic
- Output: PIT timestamp and summary of what was backed up

**Describe command** enhancements:
- Display repository granularity and fixed scope
- Show all PITs with timestamps
- Show object counts per PIT (databases, tables, snapshots)
- Indicate presence of deleted objects

**List command** additions:
- `bcn list --repo <path> --pit <timestamp>` → list all databases and tables in a specific PIT
- `bcn list --repo <path> --pit <timestamp> --database <db>` → list tables in a database at a specific PIT
- Include object status (ACTIVE, DELETED, UNCHANGED) for each object

**Restore command** enhancements:
- Accept `--pit <timestamp>` to restore from a specific PIT
- Support `--database`, `--table` filters even when repo granularity is broader
- Support `--target-catalog`, `--target-database` to restore to different location
- Include `--include-deleted` flag to optionally restore deleted objects


## Testing

### Comprehensive Test Suite

#### 1. Single Table Backups & Restores
- `test_backup_single_table_first_time` - Initial single table backup creates manifest
- `test_incremental_backup_single_table` - Changes detected and backed up
- `test_incremental_backup_no_changes` - Unchanged objects tracked correctly
- `test_restore_single_table` - Restore to original location
- `test_restore_single_table_to_different_location` - Restore to alternate catalog/database
- `test_backup_single_table_idempotent` - Same backup twice produces same result

#### 2. Database-Level Backups & Restores
- `test_backup_entire_database` - All tables in database backed up
- `test_incremental_database_with_new_tables` - New tables detected
- `test_incremental_database_with_deleted_tables` - Deletions tracked
- `test_incremental_database_with_renamed_tables` - Renamed tables detected (with object ID)
- `test_restore_partial_database_subset` - Restore subset of tables
- `test_restore_database_to_different_catalog` - Restore with scope change

#### 3. Catalog-Level Backups & Restores
- `test_backup_entire_catalog` - All databases and tables in catalog
- `test_incremental_catalog_backup` - Subsequent catalog backups
- `test_incremental_catalog_with_new_database` - New database appears
- `test_incremental_catalog_with_deleted_database` - Database deleted
- `test_restore_full_catalog` - Complete catalog restore
- `test_restore_catalog_subset_to_new_location` - Selective database/table restore

#### 4. Granularity & Repository Integrity
- `test_repository_granularity_is_locked` - Cannot change granularity after creation
- `test_reject_broader_scope_on_existing_repo` - Cannot expand scope
- `test_reject_narrower_scope_on_existing_repo` - Cannot narrow scope
- `test_incremental_same_granularity_accepted` - Same scope allowed
- `test_manifest_integrity_across_pits` - Manifest consistency
- `test_concurrent_backup_prevention` - PIT locking works

#### 5. Edge Cases
- `test_deleted_table_tracking` - Deletions properly recorded
- `test_deleted_database_tracking` - Database deletions tracked
- `test_renamed_table_detection_with_object_id` - Renames detected via UUID
- `test_unnamed_table_without_object_id_treated_as_delete_create` - Fallback behavior
- `test_unchanged_object_in_manifest` - Unchanged objects marked
- `test_schema_evolution_tracking` - Schema changes tracked
- `test_table_format_change_detection` - Format version tracked
- `test_orphaned_data_detection` - Orphaned data identified
- `test_large_table_partial_backup` - Large table handling

#### 6. Restore Flexibility
- `test_restore_subset_from_full_catalog_backup` - Granular restore from broad backup
- `test_restore_subset_from_full_database_backup` - Table-level restore from database backup
- `test_restore_with_object_filters` - Include/exclude filters work
- `test_restore_with_deleted_objects_included` - Deleted objects can be restored
- `test_restore_with_deleted_objects_excluded` - Deleted objects skipped by default
- `test_restore_schema_validation_and_migration` - Schema compatibility checked

#### 7. Multi-Stack Support
**Local Stack:**
- `test_local_minio_backup_single_table` - S3-compatible storage works
- `test_local_minio_incremental_backup` - Incremental with MinIO
- `test_local_hive_metadata_restore` - Hive catalog restore

**AWS Stack:**
- `test_aws_s3_backup_single_table` - AWS S3 storage
- `test_aws_s3_incremental_backup` - Incremental with AWS S3
- `test_aws_glue_metadata_handling` - AWS Glue catalog integration
- `test_aws_glue_vs_hive_compatibility` - Cross-stack compatibility

**Cross-Stack:**
- `test_restore_from_local_to_aws` - Local backup → AWS restore

#### 8. Tooling & CLI
- `test_describe_shows_granularity` - Describe displays granularity
- `test_describe_shows_all_pits` - All PITs listed with metadata
- `test_list_databases_for_pit` - List command filters by PIT
- `test_list_tables_for_database_pit` - Table listing per database
- `test_list_shows_object_status` - Status indicators work
- `test_restore_with_pit_selection` - Specific PIT selection
- `test_restore_with_database_filter` - Database filtering
- `test_restore_to_different_location` - Target location parameters

#### 9. Performance & Stress Tests
- `test_incremental_backup_performance_large_catalog` - Performance with large data
- `test_restore_performance_large_dataset` - Restore speed acceptable
- `test_concurrent_read_during_backup` - No locking conflicts
- `test_pit_manifest_query_performance` - Manifest queries fast

#### 10. Error Handling & Validation
- `test_reject_backup_narrower_than_previous` - Scope validation
- `test_reject_backup_broader_than_previous` - Scope validation
- `test_invalid_catalog_name` - Input validation
- `test_invalid_database_name` - Input validation
- `test_corrupted_pit_manifest_recovery` - Recovery handling
- `test_missing_referenced_data_error` - Missing data detection
- `test_insufficient_storage_error` - Storage errors handled

### Test Data Strategy
- Fixtures for pre-built catalogs with various table counts
- Schemas with evolution history
- Scenarios with deleted/renamed/unchanged objects
- Parameterized tests for local and AWS stacks