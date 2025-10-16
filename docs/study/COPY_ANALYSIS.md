# Iceberg Table Copy Analysis: test_table → test_table_2

## Files That Need Path Updates

### 1. **metadata.json** (Latest version)
Location: `s3a://warehouse/test_table/metadata/00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json`

**Changes needed:**
```json
{
  "location": "s3a://warehouse//test_table",
  // ❌ CHANGE TO: "s3a://warehouse//test_table_2"

  "snapshots": [{
    "manifest-list": "s3a://warehouse/test_table/metadata/snap-5307506118292527157-1-beec321a-ac49-45cd-b591-46b7bcfe32f8.avro",
    // ❌ CHANGE TO: "s3a://warehouse/test_table_2/metadata/snap-5307506118292527157-1-beec321a-ac49-45cd-b591-46b7bcfe32f8.avro"
  }],

  "metadata-log": [{
    "metadata-file": "s3a://warehouse/test_table/metadata/00000-775e1fc3-f157-40d6-8c8f-e45387928af2.metadata.json"
    // ❌ CHANGE TO: "s3a://warehouse/test_table_2/metadata/00000-775e1fc3-f157-40d6-8c8f-e45387928af2.metadata.json"
  }]
}
```

### 2. **Manifest List** (snap-*.avro)
Location: `s3a://warehouse/test_table/metadata/snap-5307506118292527157-1-beec321a-ac49-45cd-b591-46b7bcfe32f8.avro`

**Current content:**
```json
{
  "manifest_path": "s3a://warehouse/test_table/metadata/beec321a-ac49-45cd-b591-46b7bcfe32f8-m0.avro",
  // ❌ CHANGE TO: "s3a://warehouse/test_table_2/metadata/beec321a-ac49-45cd-b591-46b7bcfe32f8-m0.avro"
}
```

### 3. **Manifest File** (*-m0.avro)
Location: `s3a://warehouse/test_table/metadata/beec321a-ac49-45cd-b591-46b7bcfe32f8-m0.avro`

**Current content (2 records):**
```json
// Record 1:
{
  "data_file": {
    "file_path": "s3a://warehouse/test_table/data/00000-0-92295d80-bdae-46ab-8504-0705a7021838-00001.parquet",
    // ❌ CHANGE TO: "s3a://warehouse/test_table_2/data/00000-0-92295d80-bdae-46ab-8504-0705a7021838-00001.parquet"
  }
}

// Record 2:
{
  "data_file": {
    "file_path": "s3a://warehouse/test_table/data/00001-1-92295d80-bdae-46ab-8504-0705a7021838-00001.parquet",
    // ❌ CHANGE TO: "s3a://warehouse/test_table_2/data/00001-1-92295d80-bdae-46ab-8504-0705a7021838-00001.parquet"
  }
}
```

### 4. **Data Files** (Parquet files)
Location:
- `s3a://warehouse/test_table/data/00000-0-92295d80-bdae-46ab-8504-0705a7021838-00001.parquet`
- `s3a://warehouse/test_table/data/00001-1-92295d80-bdae-46ab-8504-0705a7021838-00001.parquet`

**Changes needed:**
- ✅ **NO CHANGES** to the actual Parquet file contents
- Just copy them to new location: `s3a://warehouse/test_table_2/data/`

---

## Summary of Path Replacements

| File Type | Field Name | Old Path Pattern | New Path Pattern |
|-----------|-----------|-----------------|------------------|
| metadata.json | `location` | `s3a://warehouse//test_table` | `s3a://warehouse//test_table_2` |
| metadata.json | `snapshots[].manifest-list` | `s3a://warehouse/test_table/metadata/*.avro` | `s3a://warehouse/test_table_2/metadata/*.avro` |
| metadata.json | `metadata-log[].metadata-file` | `s3a://warehouse/test_table/metadata/*.json` | `s3a://warehouse/test_table_2/metadata/*.json` |
| Manifest List | `manifest_path` | `s3a://warehouse/test_table/metadata/*-m0.avro` | `s3a://warehouse/test_table_2/metadata/*-m0.avro` |
| Manifest File | `data_file.file_path` | `s3a://warehouse/test_table/data/*.parquet` | `s3a://warehouse/test_table_2/data/*.parquet` |

---

## Copy Strategy

### Step 1: Copy Data Files (No modification needed)
```bash
# Copy all parquet files
s3://warehouse/test_table/data/*.parquet
  → s3://warehouse/test_table_2/data/*.parquet
```

### Step 2: Copy and Modify Metadata Files

#### 2a. Modify Manifest File (Avro)
- Read: `beec321a-ac49-45cd-b591-46b7bcfe32f8-m0.avro`
- Update: All `data_file.file_path` fields (replace `test_table` → `test_table_2`)
- Write: `s3://warehouse/test_table_2/metadata/beec321a-ac49-45cd-b591-46b7bcfe32f8-m0.avro`

#### 2b. Modify Manifest List (Avro)
- Read: `snap-5307506118292527157-1-beec321a-ac49-45cd-b591-46b7bcfe32f8.avro`
- Update: `manifest_path` field (replace `test_table` → `test_table_2`)
- Write: `s3://warehouse/test_table_2/metadata/snap-5307506118292527157-1-beec321a-ac49-45cd-b591-46b7bcfe32f8.avro`

#### 2c. Modify metadata.json (JSON)
- Read: `00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json`
- Update:
  - `location`
  - `snapshots[].manifest-list`
  - `metadata-log[].metadata-file`
- Write: `s3://warehouse/test_table_2/metadata/00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json`

#### 2d. Copy previous metadata.json (for metadata-log)
- Read: `00000-775e1fc3-f157-40d6-8c8f-e45387928af2.metadata.json`
- Update: Same fields as 2c
- Write: `s3://warehouse/test_table_2/metadata/00000-775e1fc3-f157-40d6-8c8f-e45387928af2.metadata.json`

### Step 3: Create version-hint.text
```bash
echo "1" > s3://warehouse/test_table_2/metadata/version-hint.text
```

### Step 4: Create v1.metadata.json symlink/pointer
```bash
# Point to latest metadata
cp 00001-a8115fba-fabc-4277-a1c4-0c42b0347192.metadata.json \
   s3://warehouse/test_table_2/metadata/v1.metadata.json
```

---

## Important Notes

1. **UUIDs and IDs**: Do NOT change:
   - `table-uuid`
   - `snapshot-id`
   - File names (they contain UUIDs)

2. **Only Paths Change**: Replace `test_table` → `test_table_2` in path strings only

3. **Avro Modification**: You'll need to:
   - Parse Avro with schema
   - Modify string fields
   - Re-serialize with same schema

4. **Atomicity**: After copying all files, register the new table by:
   - Creating `s3://warehouse/test_table_2/metadata/v1.metadata.json`
   - Or registering in Hive Metastore pointing to the new location

5. **Testing**: After copy, verify with:
   ```sql
   SELECT * FROM iceberg.default.test_table_2;
   ```
