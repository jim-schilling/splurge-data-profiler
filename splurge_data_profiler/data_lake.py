from pathlib import Path
from typing import Union, List
from os import PathLike

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, insert
from sqlalchemy.exc import SQLAlchemyError
from splurge_tools.dsv_helper import DsvHelper
from splurge_tools.streaming_tabular_data_model import StreamingTabularDataModel

from splurge_data_profiler.source import DsvSource, DbSource


class DataLake:

    def __init__(
            self,
            *,
            db_source: DbSource
    ) -> None:
        self._db_source = db_source
        self._column_names = [column.name for column in db_source.columns]
        self._db_url = db_source.db_url
        self._db_schema = db_source.db_schema
        self._db_table = db_source.db_table

    @property
    def db_source(self) -> DbSource:
        """Get the database source."""
        return self._db_source

    @property
    def column_names(self) -> List[str]:
        """Get the list of column names."""
        return self._column_names

    @property
    def db_url(self) -> str:
        """Get the database URL."""
        return self._db_url

    @property
    def db_schema(self) -> str:
        """Get the database schema."""
        return self._db_schema

    @property
    def db_table(self) -> str:
        """Get the database table name."""
        return self._db_table


class DataLakeFactory:

    @classmethod
    def _stream_dsv_to_sqlite(
            cls,
            dsv_source: DsvSource,
            *,
            db_source: DbSource,
            batch_size: int = 1000
    ) -> None:
        """
        Stream DSV data into a SQLite table using StreamingTabularDataModel.
        
        This private static method reads data from a DSV file using streaming
        to minimize memory usage and inserts the data into a SQLite table
        as defined in the DbSource object.
        
        Args:
            dsv_source: The DSV source containing file and parsing configuration
            db_source: The database source defining the target SQLite table
            batch_size: Number of rows to insert in each batch
            
        Raises:
            RuntimeError: If streaming or database insertion fails
        """
        try:
            # Create database engine
            engine = create_engine(db_source.db_url)
            
            # Create streaming parser using DsvHelper.parse_stream()
            raw_stream = DsvHelper.parse_stream(
                dsv_source.file_path,
                delimiter=dsv_source.delimiter,
                bookend=dsv_source.bookend,
                bookend_strip=dsv_source.bookend_strip,
                skip_header_rows=dsv_source.skip_header_rows,
                skip_footer_rows=dsv_source.skip_footer_rows
            )
            
            # Create streaming tabular data model
            streaming_model = StreamingTabularDataModel(
                raw_stream,
                header_rows=dsv_source.header_rows,
                skip_empty_rows=dsv_source.skip_empty_rows,
                chunk_size=batch_size
            )
            
            # Get column names from the streaming model
            column_names = streaming_model.column_names
            
            # Validate that column names match the database schema
            db_column_names = [col.name for col in db_source.columns]
            
            if column_names != db_column_names:
                raise ValueError(
                    f"Column mismatch: DSV columns {column_names} "
                    f"do not match database columns {db_column_names}"
                )
            
            # Prepare batch insertion
            batch_data = []
            
            # Stream through the data rows
            for row_data in streaming_model.iter_rows():
                # Skip the header row (first row) since it contains column names
                if row_data == column_names:
                    continue
                # Ensure all columns are present, treat missing and empty string as None
                row_dict = {
                    col: (val if val not in (None, "") else None)
                    for col in column_names
                    for val in [row_data.get(col, None)]
                }
                batch_data.append(row_dict)
                
                # Insert batch when it reaches the batch size
                if len(batch_data) >= batch_size:
                    cls._insert_batch(
                        engine=engine,
                        table_name=db_source.db_table,
                        batch_data=batch_data
                    )
                    batch_data = []
            
            # Insert any remaining data in the final batch
            if batch_data:
                cls._insert_batch(
                    engine=engine,
                    table_name=db_source.db_table,
                    batch_data=batch_data
                )

            engine.dispose()
            
        except SQLAlchemyError as exc:
            raise RuntimeError(f"Database insertion failed: {exc}")
        except Exception as exc:
            raise RuntimeError(f"Streaming DSV to SQLite failed: {exc}")

    @staticmethod
    def _insert_batch(
            engine,
            *,
            table_name: str,
            batch_data: List[dict]
    ) -> None:
        """
        Insert a batch of data into the specified table.
        
        Args:
            engine: SQLAlchemy engine instance
            table_name: Name of the table to insert into
            batch_data: List of dictionaries representing rows to insert
            
        Raises:
            SQLAlchemyError: If insertion fails
        """
        with engine.connect() as connection:
            # Get the table object from the database
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            
            # Create insert statement using the table object
            insert_stmt = insert(table)
            
            # Execute batch insert
            result = connection.execute(insert_stmt, batch_data)
            connection.commit()

    @classmethod
    def from_dsv_source(
        cls,
        dsv_source: DsvSource,
        *,
        data_lake_path: Union[str, PathLike]  
    ) -> DataLake:
        """
        Create a DataLake from a DSV source by generating a SQLite table.
        
        Args:
            dsv_source: The DSV source containing file and column information
            data_lake_path: Directory path where the SQLite database will be created
            
        Returns:
            DataLake instance with the created database source
            
        Raises:
            RuntimeError: If database creation fails
        """
        # Convert paths to Path objects
        data_lake_path = Path(data_lake_path)
        dsv_file_path = Path(dsv_source.file_path)
        
        # Create the database filename using the DSV filename without extension
        db_filename = f"{dsv_file_path.stem}.sqlite"
        db_path = data_lake_path / db_filename

        # Ensure the data lake directory exists
        data_lake_path.mkdir(parents=True, exist_ok=True)
        
        # Create SQLite database URL
        db_url = f"sqlite:///{db_path}"
        
        try:
            # Create engine and metadata
            engine = create_engine(db_url)
            metadata = MetaData()
            
            # Create table columns from DSV source columns
            table_columns = []
            for column in dsv_source.columns:               
                sa_column = SAColumn(
                    column.name,
                    String,
                    nullable=True
                )
                table_columns.append(sa_column)
            
            # Create the table
            table_name = dsv_file_path.stem
            table = Table(table_name, metadata, *table_columns)
            
            # Create the table in the database
            metadata.create_all(engine)
            engine.dispose()
            
            # Create DbSource for the new table
            db_source = DbSource(
                db_url=db_url,
                db_schema=None,  # SQLite doesn't use schemas
                db_table=table_name
            )
            
            # Stream the DSV data into the SQLite table
            cls._stream_dsv_to_sqlite(
                dsv_source=dsv_source,
                db_source=db_source
            )
            
            # Create and return DataLake
            return DataLake(db_source=db_source)
            
        except SQLAlchemyError as exc:
            raise RuntimeError(f"Failed to create SQLite table from DSV source: {exc}")
        except Exception as exc:
            raise RuntimeError(f"Unexpected error creating SQLite table: {exc}")


    
    
    
    
    