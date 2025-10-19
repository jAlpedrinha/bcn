# Incremental Backups

The goal of this feature is to enable the backup and restore procedures to be aware of previous backups and maintain a chain of incremental changes. This enables Point-in-Time (PIT) restore without duplicating data.

## Goals & Benefits

- **Enable PIT restore**: Restore tables to any previous backup point in time
- **Efficient storage**: Avoid full table copies by storing only incremental changes
- **Faster backups**: Subsequent backups only capture changed files, not entire tables
- **No data duplication**: Files unchanged since previous backup are referenced, not re-copied
- **Space optimization**: Repository size grows with changes, not table size

## Core Concepts

### Repository Structure
A backup repository is a directory (local or cloud storage) containing all backup data organized by Point in Time. Instead of single full backups, the repository maintains a **chain of incremental backups** where each PIT references its parent.

```
backup-repo/
├── pits/
│   ├── pit_001_2025-01-01T10:00Z/
│   │   ├── manifest.json              # Immutable snapshot of what changed
│   │   ├── data_files/                # New/modified data files
│   │   └── metadata_files/            # New/modified metadata
│   ├── pit_002_2025-01-02T10:00Z/
│   │   ├── manifest.json              # References parent PIT
│   │   ├── data_files/
│   │   └── metadata_files/
│   └── pit_003_2025-01-03T10:00Z/
│       ├── manifest.json
│       ├── data_files/
│       └── metadata_files/
└── index.json                         # Master index of all PITs
```

### Manifest Files Strategy
**Decision: Manifest files are versioned per PIT (never altered)**

Each PIT stores an **immutable manifest** that documents:
- Parent PIT reference (enables chain reconstruction)
- Data files added in this PIT
- Data files removed from previous PIT (deletions)
- Metadata file changes (schemas, configurations)
- Checksum/hash of each file for integrity verification

**Rationale:**
- Immutability prevents accidental overwrites
- Enables auditing of exactly what changed between PITs
- Simplifies corruption detection
- Makes incremental restore deterministic

### What Constitutes "Incremental Change"

An incremental backup captures:
1. **New data files** - Files not present in previous PIT
2. **Modified data files** - Files with different content (detected via checksum or modification time)
3. **Deleted data files** - Files present before, absent now (recorded in manifest)
4. **Schema/metadata changes** - New table version, schema evolution
5. **Manifest changes** - For Iceberg tables, new manifest entries reflecting updates/deletes

## How to Achieve It

### Backup Procedure

1. **Initialize repository** (first backup only):
   - Create backup repository structure
   - Generate `index.json` with repository metadata
   - Create first PIT directory

2. **Detect changes** (all backups):
   - Compare current table state with previous PIT
   - Build list of added/modified/deleted data files
   - Compare metadata (schemas, properties)
   - Generate manifest for current PIT

3. **Store incremental data**:
   - Copy only new/modified files to current PIT `data_files/` directory
   - Do NOT re-copy unchanged files (reference parent instead in manifest)
   - Store metadata changes in `metadata_files/`

4. **Create PIT manifest**:
   - Record parent PIT reference
   - List all data files (with checksums)
   - List deleted files from previous PIT
   - Store table schema/properties snapshot
   - Timestamp the PIT

5. **Update index.json**:
   - Add new PIT entry
   - Update `last_pit` pointer

### Restore Procedure

1. **Restore Planning Phase**:
   - Load target PIT manifest
   - Trace parent chain back to root PIT (or to most recent full snapshot)
   - Accumulate all data files needed across the chain:
     - Start with root PIT files
     - Add files from each subsequent PIT up to target
     - Remove any files marked as deleted in the chain
   - Verify all referenced files exist and checksums match
   - Generate restore plan (which physical files to copy where)

2. **Restore Execution Phase**:
   - Create temporary working directory
   - Copy all accumulated data files in order
   - Apply metadata (schema, properties) from target PIT
   - Create table in target catalog/database with target name
   - Validate data integrity (row counts, checksums if applicable)
   - Cleanup temporary files

3. **Cleanup Phase**:
   - Remove files not part of the target PIT chain
   - Result is a clean table with only data from target PIT

### Data Integrity & Consistency

**Atomicity:**
- Each PIT is immutable once created
- Backup fails if any file write fails (no partial PITs)
- Restore uses immutable plan, cannot be interrupted mid-way (or atomic checkpoint)

**Verification:**
- Store checksum (SHA-256 or similar) of each data file
- Verify checksums during restore for all files copied
- Detect corrupted/missing files before committing restore

**Garbage Collection (Future):**
- Track which files are referenced by which PITs
- Safe to delete data files if no active PIT references them
- Implement retention policies (e.g., keep last 30 days of PITs)
- Archived PITs can be offloaded to cheaper storage

## Additional Tooling

### Describe/List Commands

We will need an interface to interact with backups to know what PITs are available and their characteristics:

**Simple output** (`bcn describe --backup-name my_backup`):
- PIT ID
- Timestamp (creation date)
- Parent PIT reference
- Total data volume

**Verbose output** (`bcn describe --verbose --backup-name my_backup`):
- All simple output fields
- Number of data files in this PIT
- Number of deleted files
- Schema version
- Changed metadata fields
- Storage breakdown (data files size, metadata size)
- Child PITs (which newer backups reference this one)

### CLI Unification

We already have separate `backup` and `restore` scripts. We should unify all functionality into a single CLI with subcommands:

```bash
# Backup commands
bcn backup \
  --catalog prod \
  --database default \
  --table my_table \
  --backup-name my_backup

bcn backup \
  --catalog prod \
  --database default \
  --table my_table \
  --backup-name my_backup \
  --incremental  # Adds to existing repository (infers from --backup-name)

# Restore commands
bcn restore \
  --backup-name my_backup \
  --target-catalog prod \
  --target-database default \
  --target-table my_table_restored \
  --pit pit_001_2025-01-01T10:00Z  # Optional: restore from specific PIT

bcn restore \
  --backup-name my_backup \
  --target-catalog prod \
  --target-database default \
  --target-table my_table_restored \
  --target-location s3://warehouse/default/my_table_restored

# Describe commands
bcn describe --backup-name my_backup

bcn describe --verbose --backup-name my_backup

# List PITs in backup
bcn list --backup-name my_backup

# List files in specific PIT
bcn list --backup-name my_backup --pit pit_001_2025-01-01T10:00Z
```

## Implementation Strategy

### Phase 1: Foundation (Single Table)
- Implement repository structure with PIT chain
- Build backup detection (what changed)
- Create immutable manifest format
- Implement basic restore from any PIT
- Add checksum verification

### Phase 2: CLI & Tooling
- Unify backup/restore into single CLI
- Implement describe/list commands
- Add interactive PIT selection for restore

### Phase 3: Advanced Features
- Garbage collection with retention policies
- Concurrent backup locking
- Backup corruption detection and repair
- Cross-storage backup migration

## Testing Strategy

### 1. PIT Restore Test
Validates that different PITs contain correct data at different points in time:

**Steps:**
1. Create table with initial schema
2. Insert batch 1 of data → Create backup (PIT 1)
3. Insert batch 2 of data → Create backup (PIT 2)
4. Insert batch 3 of data → Create backup (PIT 3)
5. Restore from PIT 1 to table_v1 → Validate only batch 1 exists
6. Restore from PIT 2 to table_v2 → Validate batches 1+2 exist
7. Restore from PIT 3 to table_v3 → Validate batches 1+2+3 exist

**Success criteria:**
- Each restored table has correct row counts
- Data matches exact values from each PIT
- Timestamps/order preserved

### 2. Schema Evolution Test
Validates schema changes are tracked and restored correctly:

**Steps:**
1. Create table with schema v1 (columns: id, name) → Backup (PIT 1)
2. Add column "email" → Insert data with new column → Backup (PIT 2)
3. Rename column "name" to "full_name" → Backup (PIT 3)
4. Restore from PIT 1 → Validate schema is (id, name), no email
5. Restore from PIT 2 → Validate schema is (id, name, email)
6. Restore from PIT 3 → Validate schema is (id, full_name, email)

**Success criteria:**
- Each restored table has correct schema
- Data is correctly mapped to schema
- No errors on schema mismatch

### 3. Data Deletion & Updates Test
Validates Iceberg's manifest-based deletion/update tracking:

**Steps:**
1. Create table, insert 100 rows → Backup (PIT 1)
2. Delete 10 rows (where id > 90) → Backup (PIT 2)
3. Update 20 rows (set status = 'updated' where id <= 20) → Backup (PIT 3)
4. Restore from PIT 1 → Validate all 100 rows exist, all status='original'
5. Restore from PIT 2 → Validate 90 rows exist, all status='original'
6. Restore from PIT 3 → Validate 90 rows exist, 20 have status='updated'

**Iceberg Specifics:**
- Iceberg tracks deletions via manifests with "deleted_files" field
- Updates are recorded as new data files + deletion of old files in manifest
- Both deletions and updates result in manifest changes, some data file changes
- Restore must reconstruct correct state by:
  - Including data files that haven't been deleted
  - Excluding data files marked as deleted in target PIT manifest
  - Applying any filtering/masking from manifest delete entries

**Success criteria:**
- Row counts match expectations at each PIT
- Data values correct at each PIT
- Manifests properly reflect delete/update operations

### 4. Incremental Efficiency Test
Validates storage efficiency (files not re-copied):

**Steps:**
1. Create 1GB table → Backup (PIT 1) → Record file list
2. Add 100MB new data → Backup (PIT 2) → Record file list
3. Verify PIT 2 only contains new 100MB files (not full 1.1GB)
4. Restore from PIT 2 → Validate 1.1GB of data copied (pulling from both PITs)

**Success criteria:**
- PIT 2 storage ≈ 100MB (not 1.1GB)
- Restore reconstructs full 1.1GB correctly

### 5. Manifest Integrity Test
Validates manifests are immutable and accurate:

**Steps:**
1. Create backup (PIT 1), record manifest.json checksum
2. Attempt to modify PIT 1 manifest → Should fail
3. Create backup (PIT 2), verify PIT 1 manifest checksum unchanged
4. Restore from PIT 2, verify all file checksums match manifest

**Success criteria:**
- Immutability enforced
- Manifest checksums provide integrity proof

### 6. Multi-Stack Test
Validates same incremental logic works on local and AWS:

**Local Stack (MinIO + Hive):**
- Run all tests above against MinIO
- Verify PIT chain works with S3-compatible storage
- Verify Hive metadata catalog updates correctly

**AWS Stack (S3 + Glue):**
- Run all tests above against AWS S3
- Verify PIT chain works with actual S3 storage
- Verify Glue metadata catalog updates correctly

**Success criteria:**
- All PIT restore tests pass on both stacks
- Storage efficiency maintained on both
- No stack-specific bugs in incremental logic
