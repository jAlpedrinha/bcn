# How Hive Metastore Tracks Iceberg Tables

## Key Discovery: The metadata_location Parameter

Hive Metastore stores Iceberg table information in **TWO** places:

### 1. **SDS Table** (Storage Descriptor)
```sql
SELECT s."LOCATION"
FROM "TBLS" t
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
WHERE t."TBL_NAME" = 'test_table';
```
Result: `s3a://warehouse/test_table`

This is the **base table location** (the directory containing metadata/ and data/)

### 2. **TABLE_PARAMS Table** (Most Important!)
```sql
SELECT tp."PARAM_KEY", tp."PARAM_VALUE"
FROM "TBLS" t
JOIN "TABLE_PARAMS" tp ON t."TBL_ID" = tp."TBL_ID"
WHERE t."TBL_NAME" = 'test_table';
```

**Key parameters:**
```
PARAM_KEY                  | PARAM_VALUE
---------------------------+----------------------------------------------------
metadata_location          | s3a://warehouse/test_table/metadata/00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json
previous_metadata_location | s3a://warehouse/test_table/metadata/00000-775e1fc3-f157-40d6-8c8f-e45387928af2.metadata.json
current-snapshot-id        | 5307506118292527157
table_type                 | ICEBERG
uuid                       | aaa46a26-d351-4538-8180-6e9b883a92d1
```

---

## How Iceberg Finds the Current Snapshot

```
Hive Query
    ↓
Hive Metastore (PostgreSQL)
    ↓
TABLE_PARAMS.metadata_location
    ↓
metadata.json file (e.g., 00001-*.metadata.json)
    ↓
current-snapshot-id: 5307506118292527157
    ↓
snapshots[] array → find matching snapshot
    ↓
manifest-list: "s3a://warehouse/test_table/metadata/snap-*.avro"
    ↓
Manifest List (Avro) → points to manifest files
    ↓
Manifest File (Avro) → lists data files
    ↓
Data Files (Parquet)
```

---

## Creating a New Table: test_table_2

To register a copied Iceberg table in Hive, you have **2 options**:

### Option 1: Use Spark SQL (Simplest)

```sql
-- This will automatically register with Hive Metastore
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3a://warehouse/test_table_2';
```

When you run this, Spark will:
1. Look for `metadata/version-hint.text` or `metadata/v*.metadata.json`
2. Read the latest metadata.json
3. Register the table in Hive Metastore with correct `metadata_location`

### Option 2: Direct Hive Metastore SQL (Manual)

You would need to insert into 3 tables:

```sql
-- 1. Insert into TBLS
INSERT INTO "TBLS"
  ("TBL_ID", "CREATE_TIME", "DB_ID", "LAST_ACCESS_TIME",
   "OWNER", "SD_ID", "TBL_NAME", "TBL_TYPE", "RETENTION")
VALUES
  (nextval('tbls_seq'), extract(epoch from now()),
   (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'default'),
   extract(epoch from now()), 'spark',
   (INSERT INTO "SDS" ... RETURNING "SD_ID"),
   'test_table_2', 'EXTERNAL_TABLE', 0);

-- 2. Insert into SDS (Storage Descriptor)
INSERT INTO "SDS"
  ("SD_ID", "LOCATION", ...)
VALUES
  (nextval('sds_seq'), 's3a://warehouse/test_table_2', ...);

-- 3. Insert into TABLE_PARAMS (MOST IMPORTANT!)
INSERT INTO "TABLE_PARAMS"
  ("TBL_ID", "PARAM_KEY", "PARAM_VALUE")
VALUES
  (last_tbl_id, 'metadata_location',
   's3a://warehouse/test_table_2/metadata/00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json'),
  (last_tbl_id, 'table_type', 'ICEBERG'),
  (last_tbl_id, 'uuid', 'aaa46a26-d351-4538-8180-6e9b883a92d1'),
  (last_tbl_id, 'current-snapshot-id', '5307506118292527157'),
  ...
```

**⚠️ This is complex and error-prone!**

---

## Recommended Copy Strategy

### Step 1: Copy All Files
```bash
# Copy data files
cp -r s3://warehouse/test_table/data/* s3://warehouse/test_table_2/data/

# Copy metadata files (after modifying paths)
cp modified-manifest-m0.avro s3://warehouse/test_table_2/metadata/
cp modified-manifest-list.avro s3://warehouse/test_table_2/metadata/
cp modified-metadata.json s3://warehouse/test_table_2/metadata/00001-*.metadata.json
```

### Step 2: Create Entry Point

**Option A: Create version-hint.text**
```bash
echo "1" > s3://warehouse/test_table_2/metadata/version-hint.text
```

**Option B: Create v1.metadata.json symlink**
```bash
cp 00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json \
   s3://warehouse/test_table_2/metadata/v1.metadata.json
```

### Step 3: Register with Hive

```sql
-- This discovers the table from the location
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3a://warehouse/test_table_2';
```

OR if the table metadata already exists, just register it:

```sql
-- Register existing Iceberg table
CALL iceberg.system.register_table(
  table => 'default.test_table_2',
  metadata_file => 's3a://warehouse/test_table_2/metadata/00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json'
);
```

---

## What metadata_location Does

The `metadata_location` parameter is the **single source of truth** that:

1. Points to the specific metadata.json file to use
2. That metadata.json contains:
   - `current-snapshot-id` - which snapshot to read
   - `snapshots[]` - array with all snapshots
   - Each snapshot points to its manifest-list
   - Manifest list → manifests → data files

**Critical**: Every time you write to an Iceberg table, a NEW metadata.json is created, and Hive updates the `metadata_location` parameter to point to it.

---

## Summary

To copy `test_table` → `test_table_2`:

1. ✅ Copy all files (data + modified metadata)
2. ✅ Ensure metadata.json is the entry point
3. ✅ Register with: `CREATE TABLE ... LOCATION` or `register_table()`
4. ✅ Hive will set `metadata_location` parameter automatically

**You do NOT need to**:
- Manually insert into Hive tables
- Track snapshot IDs yourself
- Just point Hive to the table location, and Iceberg handles the rest!
