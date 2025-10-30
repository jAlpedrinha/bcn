"""
Utility for rewriting position delete Parquet files to update data file path references.

Position delete files in Iceberg contain a special 'file_path' column that references
the data files to delete from. When restoring a table to a new location, these paths
need to be updated to match the new data file locations.
"""

import io
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from bcn.logging_config import BCNLogger

logger = BCNLogger.get_logger(__name__)


class DeleteFileRewriter:
    """Handles rewriting position delete files with updated data file paths"""

    @staticmethod
    def rewrite_delete_file_paths(
        delete_file_content: bytes, old_location: str, new_location: str
    ) -> Optional[bytes]:
        """
        Rewrite a position delete Parquet file to update data file path references.

        Position delete files contain a 'file_path' column (field ID 2147483546) that
        references data files. When restoring to a new location, these paths must be
        updated from the old table location to the new location.

        Args:
            delete_file_content: Binary content of the delete Parquet file
            old_location: Original table location (e.g., "s3://bucket/db/table")
            new_location: New table location (e.g., "s3://bucket/db/table_copy")

        Returns:
            Rewritten Parquet file content as bytes, or None if rewrite fails

        Example:
            Original delete file has:
                file_path: s3://bucket/db/table/data/abc.parquet
                pos: 123
            After rewrite:
                file_path: s3://bucket/db/table_copy/data/abc.parquet
                pos: 123
        """
        try:
            # Read the Parquet file from bytes
            reader = pq.ParquetFile(io.BytesIO(delete_file_content))
            table = reader.read()

            # Check if this is actually a delete file (has file_path column)
            if "file_path" not in table.column_names:
                logger.warning("Delete file missing 'file_path' column, skipping rewrite")
                return delete_file_content

            # Get the file_path column
            file_path_column = table.column("file_path")

            # Convert to Python strings, update paths, convert back
            updated_paths = []
            for path in file_path_column.to_pylist():
                if path and isinstance(path, str):
                    # Replace old location with new location
                    if path.startswith(old_location):
                        new_path = new_location + path[len(old_location) :]
                        updated_paths.append(new_path)
                        logger.debug(f"Rewrote path: {path} -> {new_path}")
                    else:
                        logger.warning(
                            f"Path doesn't start with old location: {path} "
                            f"(expected prefix: {old_location})"
                        )
                        updated_paths.append(path)
                else:
                    updated_paths.append(path)

            # Create new table with updated file_path column
            new_file_path_array = pa.array(updated_paths, type=file_path_column.type)

            # Find the index of the file_path column
            file_path_idx = table.column_names.index("file_path")

            # Rebuild the table with updated column
            new_columns = []
            new_column_names = []
            for i, (name, column) in enumerate(zip(table.column_names, table.columns)):
                new_column_names.append(name)
                if i == file_path_idx:
                    new_columns.append(new_file_path_array)
                else:
                    new_columns.append(column)

            new_table = pa.Table.from_arrays(new_columns, names=new_column_names)

            # Write back to Parquet bytes
            output = io.BytesIO()
            pq.write_table(
                new_table,
                output,
                compression="ZSTD",  # Match Iceberg default
                write_statistics=True,  # Maintain statistics for bounds
            )

            rewritten_content = output.getvalue()
            logger.debug(
                f"Rewrote delete file: {len(delete_file_content)} bytes -> "
                f"{len(rewritten_content)} bytes, updated {len(updated_paths)} paths"
            )

            return rewritten_content

        except Exception as e:
            logger.error(f"Failed to rewrite delete file: {e}", exc_info=True)
            return None
