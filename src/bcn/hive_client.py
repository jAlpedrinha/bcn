"""
Hive Metastore client for retrieving Iceberg table metadata
"""
from typing import Dict, Optional

from bcn.config import Config


class HiveMetastoreClient:
    """Client for interacting with Hive Metastore"""

    def __init__(self, metastore_uri: Optional[str] = None):
        """
        Initialize Hive Metastore client

        Args:
            metastore_uri: Thrift URI for Hive Metastore
        """
        self.metastore_uri = metastore_uri or Config.HIVE_METASTORE_URI
        self._client = None

    def _get_client(self):
        """Get or create Hive Metastore client connection"""
        if self._client is None:
            from hive_metastore import ThriftHiveMetastore
            from thrift.protocol import TBinaryProtocol
            from thrift.transport import TSocket, TTransport

            # Parse the metastore URI (thrift://host:port)
            uri = self.metastore_uri.replace('thrift://', '')
            if ':' in uri:
                host, port = uri.split(':')
                port = int(port)
            else:
                host = uri
                port = 9083

            # Create connection
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self._client = ThriftHiveMetastore.Client(protocol)
            transport.open()

        return self._client

    def get_table_metadata(self, database: str, table: str) -> Optional[Dict]:
        """
        Get table metadata from Hive Metastore

        Args:
            database: Database name
            table: Table name

        Returns:
            Dictionary containing table metadata, or None if not found
        """
        try:
            client = self._get_client()
            table_obj = client.get_table(database, table)

            # Extract Iceberg-specific metadata
            metadata = {
                'database': database,
                'table': table,
                'location': table_obj.sd.location,
                'parameters': table_obj.parameters,
                'table_type': table_obj.tableType
            }

            # Get the metadata location (for Iceberg tables, this is in parameters)
            if 'metadata_location' in table_obj.parameters:
                metadata['metadata_location'] = table_obj.parameters['metadata_location']
            elif 'current-snapshot-id' in table_obj.parameters:
                # This is an Iceberg table - metadata location should be present
                print(f"Warning: Iceberg table {database}.{table} found but metadata_location not in parameters")

            return metadata

        except Exception as e:
            print(f"Error retrieving table metadata for {database}.{table}: {e}")
            return None

    def create_iceberg_table(self, database: str, table: str, location: str,
                            metadata_location: str, schema_dict: Dict) -> bool:
        """
        Create a new Iceberg table in Hive Metastore

        Args:
            database: Database name
            table: Table name
            location: Table location (S3 path)
            metadata_location: Metadata file location (S3 path)
            schema_dict: Schema dictionary from Iceberg metadata

        Returns:
            True if successful, False otherwise
        """
        try:
            from hive_metastore.ttypes import FieldSchema, SerDeInfo, StorageDescriptor, Table

            client = self._get_client()

            # Build columns from schema
            columns = []
            if 'fields' in schema_dict:
                for field in schema_dict['fields']:
                    columns.append(FieldSchema(
                        name=field.get('name', 'unknown'),
                        type=self._iceberg_type_to_hive_type(field.get('type', 'string')),
                        comment=field.get('doc', '')
                    ))

            # Create storage descriptor
            sd = StorageDescriptor(
                cols=columns,
                location=location,
                inputFormat='org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
                outputFormat='org.apache.iceberg.mr.hive.HiveIcebergOutputFormat',
                serdeInfo=SerDeInfo(
                    serializationLib='org.apache.iceberg.mr.hive.HiveIcebergSerDe'
                )
            )

            # Create table parameters for Iceberg
            parameters = {
                'EXTERNAL': 'TRUE',
                'table_type': 'ICEBERG',
                'metadata_location': metadata_location,
                'engine.hive.enabled': 'true'
            }

            # Create table object
            table_obj = Table(
                tableName=table,
                dbName=database,
                owner='iceberg',
                tableType='EXTERNAL_TABLE',
                sd=sd,
                parameters=parameters
            )

            # Create the table
            client.create_table(table_obj)
            print(f"✓ Created Iceberg table {database}.{table}")
            return True

        except Exception as e:
            print(f"Error creating table {database}.{table}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def update_table_metadata_location(self, database: str, table: str,
                                      metadata_location: str) -> bool:
        """
        Update the metadata_location parameter for an existing table

        Args:
            database: Database name
            table: Table name
            metadata_location: New metadata file location

        Returns:
            True if successful, False otherwise
        """
        try:
            client = self._get_client()

            # Get existing table
            table_obj = client.get_table(database, table)

            # Update metadata location
            table_obj.parameters['metadata_location'] = metadata_location

            # Update the table
            client.alter_table(database, table, table_obj)
            print(f"✓ Updated metadata location for {database}.{table}")
            return True

        except Exception as e:
            print(f"Error updating table {database}.{table}: {e}")
            return False

    def _iceberg_type_to_hive_type(self, iceberg_type) -> str:
        """
        Convert Iceberg type to Hive type

        Args:
            iceberg_type: Iceberg type (can be string or dict)

        Returns:
            Hive type string
        """
        if isinstance(iceberg_type, str):
            type_map = {
                'boolean': 'boolean',
                'int': 'int',
                'long': 'bigint',
                'float': 'float',
                'double': 'double',
                'date': 'date',
                'timestamp': 'timestamp',
                'timestamptz': 'timestamp',
                'string': 'string',
                'binary': 'binary'
            }
            return type_map.get(iceberg_type, 'string')
        elif isinstance(iceberg_type, dict):
            if 'type' in iceberg_type:
                return self._iceberg_type_to_hive_type(iceberg_type['type'])
        return 'string'

    def close(self):
        """Close the Hive Metastore connection"""
        if self._client is not None:
            try:
                self._client._iprot.trans.close()
            except Exception as e:
                print(f"Error closing Hive Metastore connection: {e}")
            finally:
                self._client = None
