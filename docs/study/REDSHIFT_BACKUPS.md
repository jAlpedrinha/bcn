# Amazon Redshift Backup and Restore Capabilities

## Overview

Amazon Redshift provides comprehensive backup and restore capabilities for both provisioned clusters and serverless namespaces. These features enable data protection, disaster recovery, and point-in-time recovery for data warehouses. Backups are stored internally in Amazon S3 using encrypted SSL connections, ensuring data security and durability.

---

## Backup Types

### 1. Provisioned Clusters

Amazon Redshift provisioned clusters support two types of snapshots:

#### Automated Snapshots
- **Frequency**: Taken automatically every 8 hours OR after every 5 GB per node of data changes, whichever comes first
- **Type**: Incremental (only changed data since the previous snapshot)
- **Default Retention**: 1 day (24 hours)
- **Configurable Retention**: 1-35 days for RA3 node types
- **Deletion**: Cannot be manually deleted; automatically removed after retention period expires
- **Disabling**: Can be disabled by setting retention period to zero

#### Manual Snapshots
- **Creation**: User-initiated at any time
- **Default Retention**: Indefinite
- **Configurable Retention**: 1-3653 days (up to ~10 years)
- **Deletion**: Must be manually deleted by users
- **Quota**: Subject to AWS account quota per region

### 2. Redshift Serverless

Redshift Serverless uses a different backup mechanism:

#### Recovery Points (Automatic)
- **Frequency**: Automatically created every 30 minutes OR after every 5 GB of data changes per node, whichever comes first
- **Retention**: Fixed at 24 hours (cannot be changed)
- **Type**: Incremental
- **Purpose**: Quick, short-term recovery
- **Control**: Users cannot create custom schedules for recovery points
- **Conversion**: Can be converted to snapshots for longer-term retention

#### Snapshots (Manual)
- **Creation**: User-initiated
- **Default Retention**: Indefinite
- **Configurable Retention**: 1-3653 days
- **Sharing**: Can be shared with other AWS accounts for cross-account access
- **Conversion**: Recovery points can be converted to snapshots

---

## Snapshot Scheduling

### Custom Scheduling (Provisioned Clusters)
- **Syntax**: Modified cron syntax
- **Minimum Frequency**: Once per hour
- **Maximum Frequency**: Once per day
- **Configuration Options**:
  - Specific time intervals
  - Days of the week
  - Specific times of day
- **Limitation**: Overlapping schedules within 1-hour window are not allowed

### Automated Scheduling (Serverless)
- Recovery points are automatically created on a fixed schedule
- Users cannot customize the schedule for recovery points
- Manual snapshots can be created at any time

---

## Restore Capabilities

### 1. Full Cluster/Namespace Restore

#### Process
- Creates a new cluster/namespace immediately
- Data is streamed on-demand during restoration
- New cluster becomes available before all data is fully loaded
- Background data loading continues while the cluster is queryable

#### Flexibility
- Can restore snapshots from provisioned clusters to serverless namespaces
- Can restore snapshots from serverless namespaces to provisioned clusters
- Cross-configuration restore is supported

### 2. Table-Level Restore

#### Capabilities
- Restore a single table from a snapshot or recovery point
- Granular recovery without affecting entire database
- Restores table to its state at backup time

#### Requirements
- Must specify source and target:
  - Database name
  - Schema name
  - Table name
- Restored table cannot have the same name as an existing table

#### Preserved Attributes
- Column definitions
- Table attributes
- Row-level security settings (if enabled)

#### Limitations
- Only **one table** can be restored at a time
- **Foreign keys** and dependencies are **NOT transferred**
- **Views** and **permissions** are **NOT transferred**
- Follows serializable isolation rules

#### Ownership
- If source table owner exists and has sufficient permissions → becomes the new table owner
- Otherwise → admin user becomes the table owner

---

## Cross-Region Snapshot Copy (Disaster Recovery)

### Overview
Amazon Redshift can automatically copy snapshots to another AWS Region for disaster recovery purposes.

### Key Features
- **Continuous Replication**: Automatically, continuously, and incrementally backs up clusters to two AWS Regions
- **Automatic Copy**: All new manual and automated snapshots are copied to the specified region
- **Disaster Recovery**: Enables cluster restoration from recent data if primary region becomes unavailable

### Configuration
- Must enable cross-region copy for each data warehouse (both serverless and provisioned)
- Configure destination region
- Configure retention period for copied snapshots in destination region

### Limitations
- **Single Destination**: Can configure cluster to copy snapshots to only **ONE AWS Region at a time**
- **Copy Duration**: Cross-region snapshot copy can take hours to complete depending on:
  - AWS regions involved
  - Amount of data to be copied
- **Availability Risk**: If most recent snapshot copy is still in progress during a disaster, may need to restore from an older, completed snapshot

### Recovery Time/Recovery Point Objectives (RTO/RPO)
- **RTO**: Less than 60 minutes (using latest completed snapshot copy)
- **RPO**: Depends on automated snapshot frequency
  - Default: ~8 hours or 5 GB per node
  - Best achievable: 1 hour (minimum supported frequency for custom schedules)

---

## AWS Backup Integration

### Overview
AWS Backup provides centralized backup management across multiple AWS services, including Redshift. As of April 2025, AWS Backup supports both Amazon Redshift provisioned clusters and Redshift Serverless.

### Capabilities
- **Scope**: Backs up entire Redshift clusters (not individual tables)
- **Backup Methods**:
  - On-demand backups
  - Scheduled backups through backup plans
- **Centralized Management**: Manage backups across all AWS accounts using AWS Organizations integration
- **Immutable Backups**: Protects data warehouses with immutable backups and separate access policies

### Configuration
- Must opt-in to protecting Amazon Redshift clusters in AWS Backup console
- Can select "All Amazon Redshift clusters" or specific clusters
- Available via console, API, or CLI

### Restore Options
- **Full Cluster Restore**: Can restore entire cluster
- **No Table-Level Restore**: Cannot restore individual tables through AWS Backup

### Important Considerations
- If using AWS Backup for Redshift, **cannot continue managing manual snapshot settings in Redshift console**
- Backup management becomes centralized through AWS Backup service

### Programmatic Operations
- Start backup job
- Describe backup job
- Get recovery point metadata
- List recovery points
- List recovery point tags

---

## Storage and Security

### Storage Location
- All snapshots stored internally in **Amazon S3**
- Managed by AWS (users do not directly access S3 buckets)

### Security
- **Encryption in Transit**: Snapshots transferred using encrypted **SSL connections**
- **Encryption at Rest**: Inherits encryption settings from the source cluster
- **Access Control**: Managed through IAM policies and Redshift permissions

### Snapshot Sharing
- Manual snapshots can be shared with other AWS accounts
- Shared snapshots allow recipients to:
  - Access data within the snapshot
  - Run queries
  - Restore to their own clusters/namespaces

---

## Retention Policies

| Backup Type | Default Retention | Configurable Retention | Auto-Deletion |
|-------------|------------------|------------------------|---------------|
| Automated Snapshots (Provisioned) | 1 day | 1-35 days (RA3 nodes) | Yes |
| Manual Snapshots (Provisioned) | Indefinite | 1-3653 days | No (manual deletion required) |
| Recovery Points (Serverless) | 24 hours | Fixed (cannot change) | Yes |
| Manual Snapshots (Serverless) | Indefinite | 1-3653 days | No (manual deletion required) |
| Cross-Region Copied Snapshots | Configurable | Per destination region policy | Based on retention setting |

---

## Snapshot Exclusions

### Table-Level Exclusions
- Individual tables can be excluded from snapshots using the `BACKUP NO` parameter
- Useful for temporary or staging tables that don't require backup

### Limitations
- Cannot exclude specific schemas or databases (only individual tables)

---

## Key Limitations and Constraints

### Scheduling
- Minimum snapshot frequency: Once per hour
- Maximum snapshot frequency: Once per day
- Cannot have overlapping schedules within 1-hour window

### Recovery Points (Serverless)
- Fixed 24-hour retention (cannot be extended directly)
- Must convert to snapshot for longer retention

### Cross-Region Copy
- Can only copy to one destination region at a time
- Copy operations can take hours for large datasets
- In-progress snapshot copies may not be available for immediate restore

### Table-Level Restore
- Only one table at a time
- No foreign keys, views, or permissions transferred
- Cannot restore table with same name as existing table

### AWS Backup Integration
- Cannot backup individual tables (cluster-level only)
- Snapshot management moves entirely to AWS Backup
- Cannot mix AWS Backup management with native Redshift snapshot management

### Restore Behavior
- New cluster/namespace is created (cannot restore in-place)
- Data becomes available incrementally during restore
- Full query capability available before complete data load

---

## Use Cases and Best Practices

### Automated Snapshots
- Use for daily operations and short-term recovery
- Configure retention based on compliance requirements
- Monitor snapshot schedule to ensure proper frequency

### Manual Snapshots
- Create before major changes (schema migrations, bulk deletes, etc.)
- Use for long-term archival (compliance, auditing)
- Tag snapshots with meaningful metadata for easy identification

### Cross-Region Copy
- Essential for disaster recovery strategy
- Choose destination region based on:
  - Geographic distance from primary
  - Compliance requirements
  - Network latency considerations
- Monitor copy completion status regularly
- Test restore procedures in secondary region

### Table-Level Restore
- Use for accidental data deletion/corruption of specific tables
- Faster than full cluster restore for single-table issues
- Rename original table if needed before restore
- Manually recreate foreign keys and permissions after restore

### Recovery Points (Serverless)
- Leverage for quick rollback (within 24 hours)
- Convert important recovery points to snapshots for longer retention
- Monitor conversion process to ensure critical points are preserved

---

## Summary

Amazon Redshift provides a comprehensive backup and restore solution suitable for various data protection and disaster recovery scenarios:

- **Automated Protection**: Incremental snapshots/recovery points every 30 minutes to 8 hours
- **Flexible Retention**: From 24 hours to indefinite retention based on requirements
- **Granular Recovery**: Both full cluster and individual table restore options
- **Disaster Recovery**: Cross-region replication with RTO < 60 minutes
- **Centralized Management**: AWS Backup integration for multi-service backup orchestration
- **Security**: Encrypted storage and transmission with IAM-based access control

These capabilities make Redshift suitable for enterprise data warehousing with strict data protection, compliance, and availability requirements.
