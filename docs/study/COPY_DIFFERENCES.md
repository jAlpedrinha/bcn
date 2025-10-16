# File-Based Table Copy: Hive vs Glue Differences

## TL;DR: Almost No Difference!

When copying Iceberg tables by **only manipulating files**, the process is **99% identical** between Hive and Glue.

---

## The Copy Process (Identical for Both)

### Step 1-3: File Operations (SAME)
```bash
# 1. Copy data files
s3://warehouse/test_table/data/*.parquet
  → s3://warehouse/test_table_2/data/*.parquet

# 2. Copy and modify manifest file (update paths)
beec321a-ac49-45cd-b591-46b7bcfe32f8-m0.avro
  → s3://warehouse/test_table_2/metadata/beec321a-*.avro

# 3. Copy and modify manifest list (update paths)
snap-5307506118292527157-1-*.avro
  → s3://warehouse/test_table_2/metadata/snap-5307506118292527157-1-*.avro

# 4. Copy and modify metadata.json (update paths)
00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json
  → s3://warehouse/test_table_2/metadata/00001-*.metadata.json
```

All path replacements: `test_table` → `test_table_2`

---

## Step 4: Registration (The ONLY Difference)

This is where you tell the catalog: **"There's a new table at this location, use this metadata file"**

### Hive Metastore

**PostgreSQL database stores:**
```sql
-- TABLE_PARAMS table
INSERT INTO "TABLE_PARAMS" VALUES
  (tbl_id, 'metadata_location', 's3://warehouse/test_table_2/metadata/00001-*.json');
```

**How to register:**

**Option A: Let Spark/Iceberg do it (Recommended)**
```sql
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3://warehouse/test_table_2';
```
→ Automatically finds the metadata.json and registers in Hive

**Option B: Direct database manipulation (Not recommended)**
```sql
-- Manual INSERT into Hive's PostgreSQL tables
INSERT INTO "TBLS" ...
INSERT INTO "TABLE_PARAMS" VALUES (..., 'metadata_location', 's3://...');
```

---

### AWS Glue

**AWS Glue API stores:**
```json
{
  "Table": {
    "Name": "test_table_2",
    "Parameters": {
      "metadata_location": "s3://warehouse/test_table_2/metadata/00001-*.json"
    }
  }
}
```

**How to register:**

**Option A: Let Spark/Iceberg do it (Recommended)**
```sql
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3://warehouse/test_table_2';
```
→ Automatically finds the metadata.json and registers in Glue

**Option B: Direct Glue API call**
```python
import boto3
glue = boto3.client('glue')

glue.create_table(
    DatabaseName='default',
    TableInput={
        'Name': 'test_table_2',
        'StorageDescriptor': {
            'Location': 's3://warehouse/test_table_2'
        },
        'Parameters': {
            'metadata_location': 's3://warehouse/test_table_2/metadata/00001-*.json',
            'table_type': 'ICEBERG'
        }
    }
)
```

**Option C: AWS CLI**
```bash
aws glue create-table \
  --database-name default \
  --table-input file://table-definition.json
```

---

## Pointing to a Specific Snapshot

### Understanding the Chain

```
Catalog (Hive/Glue)
    ↓ (metadata_location parameter)
metadata.json
    ↓ (current-snapshot-id: 5307506118292527157)
snapshots[] array
    ↓ (manifest-list path)
Manifest List
    ↓ (manifest_path)
Manifest File
    ↓ (file_path)
Data Files
```

### To Point to a Different Snapshot

**You ONLY modify the metadata.json file:**

```json
{
  "current-snapshot-id": 5307506118292527157,  // ← Change this
  "snapshots": [
    {
      "snapshot-id": 5307506118292527157,
      "manifest-list": "s3://warehouse/test_table_2/metadata/snap-5307506118292527157-*.avro"
    },
    {
      "snapshot-id": 1234567890123456789,      // ← Different snapshot
      "manifest-list": "s3://warehouse/test_table_2/metadata/snap-1234567890123456789-*.avro"
    }
  ]
}
```

**Then both Hive and Glue work the same way:**
1. Read `metadata_location` from catalog
2. Open that metadata.json
3. Look at `current-snapshot-id`
4. Find matching snapshot in `snapshots[]` array
5. Follow the chain to data files

**No difference between Hive and Glue here!**

---

## Summary Table

| Step | Hive | Glue | Notes |
|------|------|------|-------|
| Copy data files | Same | Same | No modification needed |
| Modify manifest file | Same | Same | Update `file_path` in Avro |
| Modify manifest list | Same | Same | Update `manifest_path` in Avro |
| Modify metadata.json | Same | Same | Update paths, optionally change `current-snapshot-id` |
| **Register in catalog** | SQL or PostgreSQL | SQL or boto3/CLI | **Only difference** |

---

## Key Insight

**The catalog (Hive or Glue) only stores ONE thing:**
```
table_name → metadata_location (path to metadata.json)
```

Everything else (which snapshot, which files, schema, etc.) is in the **metadata.json file**.

So for file-based copying:
1. ✅ Copy and modify files (same process)
2. ✅ Point catalog to new metadata.json (different API, same concept)
3. ✅ To use a different snapshot, edit metadata.json's `current-snapshot-id`

**No functional difference for your use case!**
