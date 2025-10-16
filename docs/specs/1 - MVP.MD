# Minimal viable product 

The goal of this project is to enable backup and restores similar to how redshift works

The MVP consists of being able to copy one table to another location in a way that it is still queryable after.


## How it works

First step is to create the sanpshot. This will for now consist of copying the iceberg files of a specific table into an s3 bucket dedicated for backups.

Second step is to restore that table into a different table (different name and s3 location).

## First step

An iceberg table consists of the metadata and the data files. Data files don't need to be changed.
Metadata files contain snapshots and manifests. Snapshots are JSON files which contain references to table location, manifest files and previous snapshot files.
For now we can ignore references to snapshot files and we will assume that the table has no history (empty snapshot log)

To create a backup we will start the backup script that takes as parameters the table name and the name of the backup we're creating. 
It will download the iceberg table files and update them as required locally.
References to manifest files and previous snapshots paths must be abstracted (they contain the table location + file path) into containg only the relative path
Manifest files are a bit trickier as they are avro files that contain references to either other manifest files or data files. Those paths also need to be abstracted
After this is updated we upload the files to the the backup s3 bucket in a folder with the name of the backup

## Second step
The restore script should accept a backup identifier, a target table name and S3 location and catalog type.

First thing is to register the new table on the catalog (hive or glue).
Then we will download the backup and update all the paths that were abstracted to include the new table location.

After this we will copy the data from the backup onto the new target table location.
Last step is to update the new table we created to point to the latest snapshot of the backup.

## Testing

Before starting we shoud clean up the entire catalog.
After the cleanup we should create a new table my_table and some rows 
Once the table is created we should create a backup called iceberg_backup
After this we should restore my_table into my_table_2 from the iceberg_backup
We should validate that my_table_2 is queryable and contains the same data as my_table

