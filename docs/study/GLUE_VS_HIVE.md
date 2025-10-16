# AWS Glue vs Hive Metastore for Iceberg Tables

## Architecture Differences

### Hive Metastore (What You're Using Now)
```
Application (Spark/Trino)
    ↓
Thrift Protocol (port 9083)
    ↓
Hive Metastore Service (Java process)
    ↓
PostgreSQL Database (stores metadata)
    ↓
Table Parameters:
  - metadata_location: s3://warehouse/test_table/metadata/00001-*.json
  - current-snapshot-id: 5307506118292527157
```

### AWS Glue Catalog
```
Application (Spark/Athena/EMR)
    ↓
AWS API (HTTPS REST calls)
    ↓
Glue Data Catalog Service (Managed AWS service)
    ↓
Internal AWS Database (managed by AWS)
    ↓
Table Properties:
  - metadata_location: s3://warehouse/test_table/metadata/00001-*.json
  - table_type: ICEBERG
```

---

## Key Differences

### 1. **Storage & Management**

| Aspect | Hive Metastore | AWS Glue |
|--------|---------------|----------|
| **Database** | Self-managed PostgreSQL/MySQL | AWS-managed (hidden) |
| **Server** | You run HMS Java process | AWS runs it for you |
| **Scaling** | Manual (vertical/horizontal) | Automatic, serverless |
| **High Availability** | You configure it | Built-in by AWS |
| **Backups** | You manage | AWS manages |

### 2. **Connection Configuration**

**Hive Metastore:**
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/") \
    .getOrCreate()
```

**AWS Glue:**
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()
```

### 3. **Authentication & Permissions**

**Hive Metastore:**
- Network access to HMS (port 9083)
- S3 credentials (access key/secret)
- No fine-grained permissions on metadata

**AWS Glue:**
- AWS IAM roles/credentials
- Fine-grained permissions via IAM policies:
  ```json
  {
    "Effect": "Allow",
    "Action": [
      "glue:GetTable",
      "glue:GetDatabase",
      "glue:CreateTable",
      "glue:UpdateTable"
    ],
    "Resource": "arn:aws:glue:region:account:table/database/table_name"
  }
  ```
- S3 permissions via IAM

### 4. **Metadata Storage Format**

**BOTH store the same critical parameter:**
```
metadata_location = s3://warehouse/test_table/metadata/00001-*.metadata.json
```

**Hive Metastore (PostgreSQL):**
```sql
-- TABLE_PARAMS table
TBL_ID | PARAM_KEY           | PARAM_VALUE
-------+---------------------+-------------------------------------------
123    | metadata_location   | s3://warehouse/test_table/metadata/00001-*.json
123    | table_type          | ICEBERG
123    | current-snapshot-id | 5307506118292527157
```

**AWS Glue (via AWS API):**
```json
{
  "Table": {
    "Name": "test_table",
    "DatabaseName": "default",
    "StorageDescriptor": {
      "Location": "s3://warehouse/test_table"
    },
    "Parameters": {
      "metadata_location": "s3://warehouse/test_table/metadata/00001-*.json",
      "table_type": "ICEBERG",
      "current-snapshot-id": "5307506118292527157"
    }
  }
}
```

---

## Copying Tables: Hive vs Glue

### With Hive Metastore (Current Setup)

**Register copied table:**
```sql
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3://warehouse/test_table_2';
```

OR programmatically:
```python
from pyspark.sql import SparkSession

spark.sql("""
  CREATE TABLE iceberg.default.test_table_2
  LOCATION 's3://warehouse/test_table_2'
""")
```

### With AWS Glue

**Option 1: Using Spark (Recommended)**
```sql
-- Same as Hive!
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3://warehouse/test_table_2';
```

**Option 2: AWS Glue API (Python boto3)**
```python
import boto3

glue = boto3.client('glue')

# Get original table metadata
response = glue.get_table(
    DatabaseName='default',
    Name='test_table'
)

# Create new table with updated location
glue.create_table(
    DatabaseName='default',
    TableInput={
        'Name': 'test_table_2',
        'StorageDescriptor': {
            'Location': 's3://warehouse/test_table_2',
            'InputFormat': 'org.apache.hadoop.mapred.FileInputFormat',
            'OutputFormat': 'org.apache.hadoop.mapred.FileOutputFormat',
        },
        'Parameters': {
            'metadata_location': 's3://warehouse/test_table_2/metadata/00001-*.json',
            'table_type': 'ICEBERG',
            'EXTERNAL': 'TRUE'
        }
    }
)
```

**Option 3: AWS CLI**
```bash
aws glue create-table \
  --database-name default \
  --table-input '{
    "Name": "test_table_2",
    "StorageDescriptor": {
      "Location": "s3://warehouse/test_table_2"
    },
    "Parameters": {
      "metadata_location": "s3://warehouse/test_table_2/metadata/00001-*.json",
      "table_type": "ICEBERG"
    }
  }'
```

---

## Advantages & Disadvantages

### Hive Metastore

**Advantages:**
- ✅ Open source, portable
- ✅ Works anywhere (on-prem, any cloud)
- ✅ Full control over infrastructure
- ✅ No AWS dependency
- ✅ Direct database access for debugging
- ✅ Lower cost (just compute + storage)

**Disadvantages:**
- ❌ You manage infrastructure
- ❌ You handle scaling
- ❌ You handle backups/HA
- ❌ Single point of failure if not configured properly
- ❌ Manual upgrades

### AWS Glue

**Advantages:**
- ✅ Fully managed (no infrastructure)
- ✅ Automatic scaling
- ✅ High availability built-in
- ✅ Integrated with AWS services (Athena, EMR, Redshift Spectrum)
- ✅ Fine-grained IAM permissions
- ✅ No server maintenance
- ✅ CloudWatch integration for monitoring
- ✅ Data Lake Formation integration

**Disadvantages:**
- ❌ AWS vendor lock-in
- ❌ Cost per API call (though minimal)
- ❌ Cannot access underlying database directly
- ❌ Limited to AWS region
- ❌ Debugging requires AWS console/APIs

---

## Migration: Hive → Glue

If you wanted to migrate your current setup to Glue:

### Step 1: Export Hive Tables
```python
import boto3

glue = boto3.client('glue')

# For each table in Hive
tables = spark.sql("SHOW TABLES IN default").collect()

for table in tables:
    table_name = table.tableName

    # Get metadata_location from Hive
    metadata_location = spark.sql(f"""
        DESCRIBE EXTENDED iceberg.default.{table_name}
    """).filter("col_name = 'metadata_location'").collect()[0].data_type

    # Register in Glue
    glue.create_table(
        DatabaseName='default',
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Location': f's3://warehouse/{table_name}'
            },
            'Parameters': {
                'metadata_location': metadata_location,
                'table_type': 'ICEBERG'
            }
        }
    )
```

### Step 2: Update Spark Configuration
```python
# OLD (Hive)
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")

# NEW (Glue)
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
```

### Step 3: No Data Movement Needed!
The Iceberg metadata files and data files remain in S3. Only the catalog changes.

---

## Cost Comparison

### Hive Metastore
```
Costs:
- EC2 instance for HMS: $50-200/month
- RDS PostgreSQL: $50-500/month
- Total: $100-700/month (depending on size)
```

### AWS Glue
```
Costs:
- $0 for storage (just the tables, not your data)
- $1 per million requests over 1M free tier/month
- Typical usage: <$10/month for most workloads
```

**For most workloads, Glue is significantly cheaper.**

---

## Your Copy Operation with Glue

**The core process is THE SAME:**

1. ✅ Copy data files: `s3://warehouse/test_table/data/` → `s3://warehouse/test_table_2/data/`
2. ✅ Modify and copy metadata files (manifest, manifest-list, metadata.json)
3. ✅ Update all paths from `test_table` → `test_table_2`

**Registration differs only in the API:**

**Hive:**
```sql
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3://warehouse/test_table_2';
```

**Glue (via Spark - same SQL!):**
```sql
CREATE TABLE iceberg.default.test_table_2
LOCATION 's3://warehouse/test_table_2';
```

**Glue (via AWS API - alternative):**
```python
glue.create_table(
    DatabaseName='default',
    TableInput={
        'Name': 'test_table_2',
        'Parameters': {
            'metadata_location': 's3://warehouse/test_table_2/metadata/00001-*.json'
        }
    }
)
```

---

## Recommendation

For your use case (copying Iceberg tables by manipulating files):

**Use Hive if:**
- You need on-premises deployment
- You want full control
- You're building a multi-cloud solution
- You have existing Hive infrastructure

**Use Glue if:**
- You're on AWS
- You want managed infrastructure
- You integrate with Athena/EMR
- You want to minimize operational overhead
- Cost is a concern

**The good news:** Your file-based copy approach works **identically** with both! The only difference is the registration step at the end.
