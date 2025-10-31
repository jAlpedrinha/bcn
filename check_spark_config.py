#!/usr/bin/env python3
"""
Diagnostic script to check Spark configuration for Glue catalog
"""
from bcn.config import Config
from bcn.spark_client import SparkClient

print("=" * 80)
print("CHECKING ENVIRONMENT CONFIGURATION")
print("=" * 80)
print(f"CATALOG_TYPE: {Config.CATALOG_TYPE}")
print(f"CATALOG_NAME: {Config.CATALOG_NAME}")
print(f"WAREHOUSE_BUCKET: {Config.WAREHOUSE_BUCKET}")
print(f"S3_ENDPOINT: {Config.S3_ENDPOINT}")
print(f"S3_PATH_STYLE_ACCESS: {Config.S3_PATH_STYLE_ACCESS}")
print()

print("=" * 80)
print("CREATING SPARK SESSION")
print("=" * 80)
client = SparkClient(app_name="config-check")
spark = client.get_spark_session()

print("Spark session created successfully!")
print()

print("=" * 80)
print("SPARK CATALOG CONFIGURATION")
print("=" * 80)
catalog_name = Config.CATALOG_NAME

# Get all Spark configurations related to the catalog
all_conf = spark.sparkContext.getConf().getAll()
catalog_configs = [(k, v) for k, v in all_conf if catalog_name in k]

for key, value in sorted(catalog_configs):
    print(f"{key} = {value}")

print()
print("=" * 80)
print("Checking for 'type' property...")
print("=" * 80)
type_key = f"spark.sql.catalog.{catalog_name}.type"
type_value = dict(all_conf).get(type_key, "NOT SET")
print(f"{type_key} = {type_value}")

if type_value == "glue":
    print()
    print("❌ ERROR: catalog type is set to 'glue' which is invalid!")
    print("   This should either be NOT SET (for Glue with catalog-impl)")
    print("   or 'hive' (for Hive metastore)")
elif type_value == "NOT SET":
    print("✓ Correct: type is not set (using catalog-impl for Glue)")
else:
    print(f"✓ Type is set to: {type_value}")

client.close()
print()
print("Done!")
