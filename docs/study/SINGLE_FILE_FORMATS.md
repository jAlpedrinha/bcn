# Single-File Archive Formats for Iceberg Backups

## Your Requirements Revisited

1. **Easy data movement between environments** (e.g., S3 → on-prem, S3 → different S3)
2. **Optional: Random access to internal files** (extract single table without unpacking all)
3. **No need for compression** (Parquet already compressed)

---

## Option 1: Plain TAR (No Compression)

### Format
```bash
tar -cf backup.tar warehouse/
```

**Structure:**
```
backup.tar
├── test_table/
│   ├── metadata/
│   │   ├── v1.metadata.json
│   │   └── snap-*.avro
│   └── data/
│       └── *.parquet
└── test_table_2/
    └── ...
```

### Advantages
✅ **Single file** - Easy to move/copy/download
✅ **No compression overhead** - Just packaging
✅ **Deterministic** - Tar is a well-defined format
✅ **Universal** - Works everywhere
✅ **Incremental tar** - `tar --append` or `tar --update`
✅ **Simple tooling** - Standard Unix tool

### Disadvantages
❌ **No random access** - Must extract entire archive (or use `tar -xf backup.tar specific/file`)
❌ **No deduplication** - Full copy every time
❌ **Single point of failure** - Corrupted tar = lost backup
❌ **Sequential writes** - Can't parallelize creation
❌ **Size limit** - Tar has practical limits (~8GB for old format, unlimited for GNU tar)

### Random Access in TAR?
**Possible but limited:**
```bash
# Extract single file without unpacking everything
tar -xf backup.tar test_table/metadata/v1.metadata.json

# List contents without extracting
tar -tf backup.tar
```

**However:**
- Still needs to scan entire archive to find file
- Slow for large archives (GB+)
- Not truly "random access"

---

## Option 2: ZIP Archive

### Format
```bash
zip -r -0 backup.zip warehouse/  # -0 = no compression
```

### Advantages
✅ **True random access** - Central directory at end
✅ **Single file** - Easy to move
✅ **Partial extraction** - Extract single file efficiently
✅ **Universal** - Works everywhere
✅ **No compression** - Use `-0` flag

### Disadvantages
❌ **4GB file size limit** (ZIP64 extends to 16EB, but not universal)
❌ **No incremental** - Must recreate entire archive
❌ **Memory overhead** - Central directory
❌ **Corruption risk** - Central directory at end

### Random Access
```bash
# List contents instantly (reads central directory)
unzip -l backup.zip

# Extract single file efficiently
unzip backup.zip test_table/metadata/v1.metadata.json

# Even works on S3 with range requests!
aws s3 cp s3://backups/backup.zip - --range bytes=1000-2000
```

**This is actually promising for your use case!**

---

## Option 3: SquashFS (Read-Only Compressed Filesystem)

### Format
```bash
mksquashfs warehouse/ backup.sqfs -noappend
```

### Advantages
✅ **True filesystem** - Mount and browse
✅ **Random access** - Like a real filesystem
✅ **Efficient** - Block-level compression
✅ **Read-only** - Immutable by design

### Disadvantages
❌ **Linux-specific** - Requires FUSE on macOS/Windows
❌ **Compression** - Even with `-noInodeCompression`, still compresses
❌ **Not portable** - Harder to work with

---

## Option 4: Apache Arrow Flight Archive (AFAR)

### Concept
Apache Arrow has discussed archive formats, but nothing standardized yet.

**Status:** Not production-ready

---

## Option 5: SQLite Database

### Interesting approach:
```sql
CREATE TABLE files (
    path TEXT PRIMARY KEY,
    content BLOB,
    size INTEGER,
    checksum TEXT
);

INSERT INTO files VALUES
    ('test_table/metadata/v1.metadata.json', <blob>, 2048, 'sha256:...');
```

### Advantages
✅ **Single file** - Easy to move
✅ **True random access** - SQL queries
✅ **Efficient** - Indexed lookups
✅ **ACID** - Transactional integrity
✅ **Universal** - SQLite everywhere

### Disadvantages
❌ **Not designed for large blobs** - Parquet files are big
❌ **Performance** - Not optimized for this use case
❌ **Overhead** - SQLite page structure

---

## Option 6: Custom Format (What Backup Tools Use)

### Restic Repository Format

```
repo/
├── config
├── keys/
│   └── <key-id>
├── snapshots/
│   ├── <snapshot-1>
│   └── <snapshot-2>
├── index/
│   ├── <index-1>
│   └── <index-2>
└── data/
    ├── 00/
    │   ├── 00abc123...  # Content-addressed chunks
    │   └── 00def456...
    └── 01/
        └── ...
```

**Each "pack file" is a custom format:**
- Header with chunk index
- Multiple chunks in one file
- Random access via index

**But this is complex!**

---

## Option 7: S3 + Manifest (Your "Solve it when needed" approach)

### The Pragmatic Solution

**Normal operation:**
```
s3://warehouse/
├── test_table/
│   ├── metadata/
│   └── data/
└── test_table_2/
    └── ...

s3://backups/
├── manifests/
│   └── backup-2025-01-15.json  # Lists all files
└── (no data yet)
```

**When you need to move environments:**
```bash
# Option A: S3 sync (if both are S3)
aws s3 sync s3://warehouse/ s3://other-region-warehouse/

# Option B: Create tar on-demand
aws s3 sync s3://warehouse/ ./local-copy/
tar -cf backup.tar local-copy/
# Transfer tar file
# Extract in target environment

# Option C: Use manifest to copy only needed files
cat backup-manifest.json | jq -r '.tables[].files[].path' | \
  xargs -I {} aws s3 cp s3://warehouse/{} ./local-copy/{}
```

**Advantages:**
✅ **Lazy approach** - Only create archive when needed
✅ **Flexible** - Choose format based on destination
✅ **No duplication** - Don't store both formats
✅ **Fast daily backups** - Just update manifest

---

## Recommended Approach: ZIP with Store Mode

### Why ZIP?

1. **Random access** - You can extract single files efficiently
2. **S3 range requests** - Works with cloud storage
3. **No compression** - Use `-0` (store mode)
4. **Universal** - Every platform has zip/unzip
5. **Single file** - Easy to transfer

### Example

```bash
# Create backup (no compression)
cd /path/to/warehouse
zip -r -0 backup-2025-01-15.zip test_table/ test_table_2/

# Upload to S3
aws s3 cp backup-2025-01-15.zip s3://backups/

# Later: Extract single table without downloading entire zip
# (requires local zip, but reads only needed bytes)
unzip backup-2025-01-15.zip "test_table/*" -d restore/

# Or: Download entire zip and extract
aws s3 cp s3://backups/backup-2025-01-15.zip .
unzip backup-2025-01-15.zip
```

### S3 Range Requests with ZIP

ZIP format has **central directory at the end**, so you can:

```python
import boto3
import io
import zipfile

s3 = boto3.client('s3')

# 1. Get file size
response = s3.head_object(Bucket='backups', Key='backup.zip')
file_size = response['ContentLength']

# 2. Read central directory (last ~64KB usually)
response = s3.get_object(
    Bucket='backups',
    Key='backup.zip',
    Range=f'bytes={file_size-65536}-{file_size}'
)
end_bytes = response['Body'].read()

# 3. Parse central directory to find file offset
# (using zipfile library)

# 4. Read specific file using range request
response = s3.get_object(
    Bucket='backups',
    Key='backup.zip',
    Range=f'bytes={offset}-{offset+size}'
)
file_content = response['Body'].read()
```

**This actually works!** Tools like `rclone` do this.

---

## Incremental Backups with ZIP

### Challenge
ZIP doesn't support true incremental like tar's `--append`.

### Solution: Multiple ZIPs
```
s3://backups/
├── backup-2025-01-15-full.zip
├── backup-2025-01-16-incr.zip     # Only new/changed files
├── backup-2025-01-17-incr.zip
└── manifest.json                   # Links them together
```

**manifest.json:**
```json
{
  "full_backup": "backup-2025-01-15-full.zip",
  "incrementals": [
    "backup-2025-01-16-incr.zip",
    "backup-2025-01-17-incr.zip"
  ],
  "restore_order": ["full", "incr1", "incr2"]
}
```

**Restore:**
```bash
# Extract in order (incrementals overwrite older files)
unzip backup-2025-01-15-full.zip
unzip -o backup-2025-01-16-incr.zip  # -o = overwrite
unzip -o backup-2025-01-17-incr.zip
```

---

## Comparison Matrix

| Format | Single File | Random Access | S3 Range | Incremental | Compression | Tooling |
|--------|-------------|---------------|----------|-------------|-------------|---------|
| **tar** | ✅ | ⚠️ Slow | ❌ | ⚠️ Append | Optional | Universal |
| **tar.gz** | ✅ | ❌ | ❌ | ❌ | ✅ | Universal |
| **zip** | ✅ | ✅ Fast | ✅ | ⚠️ Multi-file | Optional | Universal |
| **SquashFS** | ✅ | ✅ | ❌ | ❌ | ✅ | Linux |
| **SQLite** | ✅ | ✅ | ⚠️ | ✅ | ❌ | Universal |
| **S3 native** | ❌ | ✅ | ✅ | ✅ | N/A | Cloud |

---

## Final Recommendation

### For Your Use Case:

**Primary strategy: S3 native + manifest**
- Fast daily backups
- No duplication
- Incremental by nature

**When you need to move environments:**

**Option A: Direct sync (S3 to S3)**
```bash
aws s3 sync s3://source/warehouse/ s3://dest/warehouse/
```

**Option B: ZIP for portability (S3 to on-prem)**
```bash
# Create on-demand
aws s3 sync s3://warehouse/ ./temp/
zip -r -0 backup.zip temp/
# Transfer zip file
# Extract in target
```

**Option C: Incremental ZIP backups**
```bash
# Daily: Create small zip with changes only
zip -r -0 backup-2025-01-15.zip test_table/metadata/00002-*.json test_table/data/00002-*.parquet

# Track with manifest.json
```

### Why This Works

1. **Don't pre-optimize** - Create archives only when moving data
2. **S3-native is fastest** - For daily backups
3. **ZIP is most portable** - For cross-environment transfers
4. **Manifest tracks everything** - Know what's in each backup

### Implementation

```python
class BackupManager:
    def daily_backup(self):
        """Fast incremental backup using S3 native"""
        manifest = self.create_manifest()
        self.save_manifest(manifest)
        # Files already in S3, just track them

    def create_portable_backup(self, format='zip'):
        """On-demand: create single file for transfer"""
        if format == 'zip':
            self.create_zip_from_manifest()
        elif format == 'tar':
            self.create_tar_from_manifest()

    def restore_from_s3(self, target_location):
        """Fast restore using S3 sync"""
        aws s3 sync source target_location

    def restore_from_archive(self, archive_file):
        """Restore from ZIP/TAR"""
        if archive_file.endswith('.zip'):
            unzip(archive_file)
        elif archive_file.endswith('.tar'):
            untar(archive_file)
```

---

## Answer to Your Questions

### Q: Would a simple .tar file be good?
**A:** Yes, for portability! But:
- Use **uncompressed tar** (no .gz)
- Create **on-demand** when moving environments
- Not ideal for daily backups (use S3-native)

### Q: Is there a format that allows fetching just internal files from S3?
**A:** Yes! **ZIP with store mode (-0)**
- Central directory enables random access
- S3 range requests work
- Can extract single file without downloading all
- Still single file for easy transfer

### Q: Maybe solve data movement only when needed?
**A:** **This is the best approach!**
- Daily backups: S3-native + manifest (fast, cheap)
- Moving environments: Create ZIP/TAR on-demand
- Best of both worlds

---

## Proposed Hybrid Solution

```
Daily Backups:
└── S3-native structure + manifest.json

When Moving Environments:
├── Same region/cloud: aws s3 sync
├── Different cloud: Create ZIP on-demand
└── On-prem: Create ZIP, transfer, extract

When Restoring Single Table:
├── From S3: Direct copy
└── From ZIP: Extract specific path
```

**This gives you:**
- ✅ Fast daily backups
- ✅ Easy data movement when needed
- ✅ Random access when needed
- ✅ Cost-efficient storage
- ✅ Flexible restore options
