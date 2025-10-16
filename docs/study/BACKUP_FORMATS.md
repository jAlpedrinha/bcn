# Database Backup Formats: State of the Art for Iceberg Tables

## Your Use Case: Iceberg Database Backup

**Requirements:**
- Backup entire database (multiple Iceberg tables)
- Incremental backups (only changed data)
- Cost-efficient storage
- Restore to same or different database
- Preserve table metadata + data files

---

## State of the Art: Database Backup Formats

### 1. **Traditional RDBMS Approaches**

#### PostgreSQL: pg_dump / pg_basebackup
```bash
# Logical backup (SQL dump)
pg_dump -Fc database > backup.dump  # Custom format, compressed

# Physical backup (file-level)
pg_basebackup -D /backup -Ft -z    # Tarball, compressed
```
**Format:** Custom binary or tar.gz
**Incremental:** WAL (Write-Ahead Logs)

#### MySQL: mysqldump / Percona XtraBackup
```bash
# Logical backup
mysqldump --single-transaction db > backup.sql

# Physical backup with incremental support
xtrabackup --backup --incremental-basedir=/backup/base
```
**Format:** SQL text or custom binary
**Incremental:** Binary logs or custom incremental format

### 2. **Modern Data Lake Formats**

None of the traditional formats work well for your use case because:
- ❌ Iceberg already stores data in optimized formats (Parquet)
- ❌ Re-compressing Parquet into tar.gz is wasteful
- ❌ Tar requires unpacking entire archive to access files
- ❌ No deduplication of unchanged files

---

## Better Approaches for Iceberg

### Option 1: **Object Store Snapshots (Recommended)**

Don't create a single file. Keep the native S3 structure.

```
s3://backups/
  └── database-backup-2025-01-15/
      ├── backup-manifest.json          # Your metadata
      ├── test_table/
      │   ├── metadata/
      │   │   ├── v1.metadata.json
      │   │   ├── snap-*.avro
      │   │   └── manifest-*.avro
      │   └── data/
      │       └── *.parquet
      └── test_table_2/
          └── ...
```

**Incremental Strategy:**
```
s3://backups/
  ├── full-2025-01-15/                  # Full backup
  │   └── [all files]
  ├── incremental-2025-01-16/           # Only changes since full
  │   ├── backup-manifest.json          # References full backup
  │   ├── test_table/
  │   │   ├── metadata/
  │   │   │   └── v2.metadata.json     # New snapshot only
  │   │   └── data/
  │   │       └── new-file.parquet     # New data only
  │   └── deleted-files.txt             # Track deletions
  └── incremental-2025-01-17/
      └── ...
```

**Advantages:**
- ✅ No re-compression of already-compressed Parquet
- ✅ Efficient incremental (only copy changed files)
- ✅ Fast restore (direct S3 copy)
- ✅ Parallel restore
- ✅ Can use S3 Object Lock for immutability

**This is what most cloud data platforms do!**

---

### Option 2: **PAX (Parquet Archive Exchange) - Emerging Standard**

A new format being developed for shipping Parquet datasets:

```python
# Conceptual (not yet standardized)
pax_archive = {
    "version": "1.0",
    "tables": [
        {
            "name": "test_table",
            "metadata": {...},
            "data_files": ["ref://data/00000.parquet"],
            "metadata_files": ["ref://metadata/v1.metadata.json"]
        }
    ],
    "files": [
        {"path": "data/00000.parquet", "offset": 0, "size": 1024},
        {"path": "metadata/v1.metadata.json", "offset": 1024, "size": 256}
    ]
}
```

**Status:** Concept stage, not production-ready
**Use case:** Shipping datasets between systems

---

### Option 3: **Restic-style Backup (Content-Addressable Storage)**

Used by modern backup tools (Restic, Borg, Duplicity):

```
s3://backups/
  ├── snapshots/
  │   ├── snapshot-2025-01-15.json      # Points to chunks
  │   └── snapshot-2025-01-16.json
  ├── index/
  │   └── file-hash → chunk-id mapping
  └── data/
      ├── chunk-abc123                   # Deduplicated chunks
      ├── chunk-def456
      └── chunk-ghi789
```

**How it works:**
1. Split files into chunks (4-8MB)
2. Hash each chunk (SHA256)
3. Store unique chunks only (deduplication)
4. Snapshots reference chunks

**Advantages:**
- ✅ Automatic deduplication
- ✅ True incremental backups
- ✅ Space-efficient
- ✅ Compression per chunk

**Disadvantages:**
- ❌ Complex to implement
- ❌ Requires rebuilding files on restore
- ❌ Parquet files are already compressed (limited benefit)

**Tools:**
- [Restic](https://restic.net/) - Go-based, S3 support
- [BorgBackup](https://www.borgbackup.org/) - Python, local/SSH
- [Duplicity](http://duplicity.nongnu.org/) - Python, cloud support

---

### Option 4: **Iceberg Metadata-Only Snapshots**

**Key insight:** Iceberg is already immutable and versioned!

```json
// backup-manifest.json
{
  "backup_id": "2025-01-15T10:00:00Z",
  "backup_type": "full",
  "tables": [
    {
      "name": "test_table",
      "metadata_location": "s3://warehouse/test_table/metadata/00001-*.json",
      "snapshot_id": 5307506118292527157,
      "files": [
        "s3://warehouse/test_table/data/00000-*.parquet",
        "s3://warehouse/test_table/data/00001-*.parquet"
      ]
    }
  ]
}
```

**For incremental:**
```json
{
  "backup_id": "2025-01-16T10:00:00Z",
  "backup_type": "incremental",
  "base_backup": "2025-01-15T10:00:00Z",
  "tables": [
    {
      "name": "test_table",
      "metadata_location": "s3://warehouse/test_table/metadata/00002-*.json",
      "snapshot_id": 9876543210987654321,
      "new_files": [
        "s3://warehouse/test_table/data/00002-*.parquet"
      ],
      "removed_files": []
    }
  ]
}
```

**Advantages:**
- ✅ Minimal metadata storage
- ✅ Leverage S3 versioning
- ✅ No data duplication
- ✅ Fast "backup" (just save manifest)

**Disadvantages:**
- ❌ Depends on source S3 bucket availability
- ❌ Not true "backup" (no copy to different location)

---

### Option 5: **Apache Hudi Timeline / Delta Lake Log Compaction**

Both Hudi and Delta Lake use a similar approach:

**Delta Lake:**
```
_delta_log/
  ├── 00000000000000000000.json         # Transaction 0
  ├── 00000000000000000001.json         # Transaction 1
  └── 00000000000000000010.checkpoint.parquet  # Compacted state
```

**Iceberg Equivalent:**
- Use `snapshots[]` array in metadata.json
- Each snapshot is a "checkpoint"
- Backup = copy metadata.json + referenced files

---

## Recommended Solution for Your Use Case

### **Hybrid Approach: Metadata Manifest + S3 Structure**

```
s3://backups/
  ├── manifests/
  │   ├── backup-2025-01-15-full.json
  │   ├── backup-2025-01-16-incr.json
  │   └── backup-2025-01-17-incr.json
  └── data/
      ├── test_table/
      │   ├── metadata/
      │   │   ├── 00001-*.json         # Full backup
      │   │   ├── 00002-*.json         # Incremental 1
      │   │   └── snap-*.avro
      │   └── data/
      │       ├── 00000-*.parquet      # Full backup
      │       └── 00001-*.parquet      # Incremental 1
      └── test_table_2/
          └── ...
```

### Manifest Format (JSON)

```json
{
  "backup_metadata": {
    "backup_id": "backup-2025-01-15-full",
    "backup_type": "full",
    "timestamp": "2025-01-15T10:00:00Z",
    "database": "default",
    "retention_days": 90,
    "compression": "none"  // Parquet is already compressed
  },
  "tables": {
    "test_table": {
      "location": "s3://backups/data/test_table",
      "original_location": "s3://warehouse/test_table",
      "metadata_location": "s3://backups/data/test_table/metadata/00001-*.json",
      "snapshot_id": 5307506118292527157,
      "schema_version": 0,
      "files": {
        "metadata": [
          {
            "path": "metadata/00001-*.json",
            "size": 2048,
            "checksum": "sha256:abc123..."
          },
          {
            "path": "metadata/snap-*.avro",
            "size": 4096,
            "checksum": "sha256:def456..."
          }
        ],
        "data": [
          {
            "path": "data/00000-*.parquet",
            "size": 935,
            "checksum": "sha256:ghi789...",
            "row_count": 1
          },
          {
            "path": "data/00001-*.parquet",
            "size": 921,
            "checksum": "sha256:jkl012...",
            "row_count": 1
          }
        ]
      }
    },
    "test_table_2": {
      "location": "s3://backups/data/test_table_2",
      ...
    }
  },
  "statistics": {
    "total_tables": 2,
    "total_files": 6,
    "total_size_bytes": 10240,
    "total_rows": 2
  }
}
```

### Incremental Manifest

```json
{
  "backup_metadata": {
    "backup_id": "backup-2025-01-16-incr",
    "backup_type": "incremental",
    "base_backup_id": "backup-2025-01-15-full",
    "timestamp": "2025-01-16T10:00:00Z"
  },
  "tables": {
    "test_table": {
      "changed": true,
      "new_snapshot_id": 9876543210987654321,
      "new_files": {
        "metadata": [
          {
            "path": "metadata/00002-*.json",
            "size": 2100,
            "checksum": "sha256:new123..."
          }
        ],
        "data": [
          {
            "path": "data/00002-*.parquet",
            "size": 950,
            "checksum": "sha256:new456...",
            "row_count": 1
          }
        ]
      },
      "deleted_files": []  // Track deletions for restore
    },
    "test_table_2": {
      "changed": false
    }
  }
}
```

---

## Comparison Table

| Format | Dedup | Incremental | Compression | Random Access | Cloud-Native | Complexity |
|--------|-------|-------------|-------------|---------------|--------------|------------|
| **tar.gz** | ❌ | ❌ | ✅ | ❌ | ❌ | Low |
| **tar + manifest** | ❌ | ✅ | ✅ | ❌ | ❌ | Low |
| **S3 native + manifest** | Manual | ✅ | N/A | ✅ | ✅ | Medium |
| **Restic/Borg** | ✅ | ✅ | ✅ | ❌ | ✅ | High |
| **Metadata-only** | ✅ | ✅ | N/A | ✅ | ✅ | Low |

---

## Why NOT tar.gz?

1. **Parquet is already compressed** (Snappy/ZSTD)
   - Re-compressing gains little (<5%)
   - Actually slower (CPU cost)

2. **Random access impossible**
   - Must extract entire archive to restore one table
   - Can't inspect backup without extraction

3. **Incremental backups difficult**
   - Need to track which files changed
   - Tar doesn't support deduplication

4. **Poor cloud performance**
   - S3 optimized for many small objects, not huge files
   - Parallel upload/download not possible with single tar

---

## Industry Examples

### **Databricks Delta Lake Backups**
- Keep native Delta format
- Use Delta Log for incremental tracking
- Store manifests separately
- Leverage S3 versioning

### **Snowflake Time Travel**
- Store only metadata changes
- Data files are immutable and deduplicated
- Snapshots are lightweight

### **Apache Iceberg Native**
- Already has snapshot isolation
- Metadata files track lineage
- No need for external backup format

### **AWS Backup for S3**
- Uses S3 Inventory + manifests
- Continuous backup (no tar/zip)
- Incremental by nature

---

## Recommendation

**Use S3 native structure with JSON manifests:**

1. ✅ No compression needed (Parquet already compressed)
2. ✅ True incremental (copy only changed files)
3. ✅ Fast random access (restore single table)
4. ✅ Parallel operations (S3 optimized)
5. ✅ Simple to implement
6. ✅ Cloud-native
7. ✅ Easy to inspect/verify

**Implementation:**
```python
# Backup
def backup_database():
    manifest = {
        "backup_id": timestamp,
        "tables": {}
    }

    for table in get_all_tables():
        # Get current snapshot
        metadata = read_metadata(table)

        # Copy files to backup location
        for file in get_table_files(metadata):
            if not exists_in_backup(file):
                copy_to_backup(file)
                manifest["tables"][table]["new_files"].append(file)

    save_manifest(manifest)

# Restore
def restore_database(backup_id):
    manifest = load_manifest(backup_id)

    # If incremental, load base backups first
    if manifest["backup_type"] == "incremental":
        restore_database(manifest["base_backup_id"])

    # Copy files and register tables
    for table, info in manifest["tables"].items():
        copy_files(info["files"])
        register_table(table, info["metadata_location"])
```

**Cost savings:**
- Incremental: Only copy new files (~10-20% of full backup)
- Deduplication: Via file-level comparison (checksums)
- Compression: Already in Parquet (no double compression)

This is the modern, cloud-native approach used by all major data platforms!
