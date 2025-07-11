import copy
from typing import List, Optional, Any, Dict
from dataclasses import dataclass

from sqlalchemy import (
    create_engine, text, inspect, MetaData, Table, Column as SAColumn, 
    String as SAString, Integer, Float, Boolean, Date, DateTime, Time
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from splurge_tools.type_helper import profile_values, DataType as ToolsDataType, String

from splurge_data_profiler.data_lake import DataLake
from splurge_data_profiler.source import Column, DataType

class Profiler:

    @dataclass(frozen=True)
    class _SampleRule:
        threshold: float
        factor: float

    _SAMPLE_RULES = [
        _SampleRule(5_000, 1.0),
        _SampleRule(10_000, 0.80),
        _SampleRule(25_000, 0.60),
        _SampleRule(100_000, 0.40),
        _SampleRule(500_000, 0.20),
        _SampleRule(float('inf'), 0.10),
    ]

    @classmethod
    def calculate_adaptive_sample_size(
        cls,
        *,
        total_rows: int
    ) -> int:
        """
        Calculate adaptive sample size based on total dataset size using class-level rules.
        The rules are defined in the _SAMPLE_RULES class variable.
        
        Args:
            total_rows: Total number of rows in the dataset
            
        Returns:
            Calculated sample size
        """
        for rule in cls._SAMPLE_RULES:
            if total_rows < rule.threshold:
                return int(total_rows * rule.factor)
        return int(total_rows)  # fallback, should never hit

    def __init__(
            self,
            *,
            data_lake: DataLake
    ) -> None:
        if data_lake is None:
            raise ValueError("data_lake cannot be None")
        self._data_lake = data_lake
        # Create a private copy of the DbSource columns
        self._profiled_columns = copy.deepcopy(data_lake.db_source.columns)

    def profile(
            self,
            *,
            sample_size: Optional[int] = None
    ) -> None:
        """
        Profile the data lake by analyzing each column's data types.
        
        Uses SQLAlchemy to connect to the database and samples data from each column
        to determine the inferred data types using the profile_values function.
        Updates a private copy of the columns with profiling results.
        
        Args:
            sample_size: Number of rows to sample for profiling. If None, uses adaptive sampling
                        based on dataset size (default: None for adaptive sampling)
            
        Raises:
            RuntimeError: If database connection or profiling fails
        """
        try:
            # Create database engine
            engine = create_engine(self._data_lake.db_url)
            
            # Calculate adaptive sample size if not provided
            if sample_size is None:
                # Get total row count
                with engine.connect() as connection:
                    table_name = self._data_lake.db_table
                    if self._data_lake.db_schema:
                        table_name = f"{self._data_lake.db_schema}.{table_name}"
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    total_rows = result.fetchone()[0]
                
                sample_size = self.calculate_adaptive_sample_size(total_rows=total_rows)
            
            # Profile each column
            for column in self._profiled_columns:
                self._profile_column(
                    engine=engine,
                    column=column,
                    sample_size=sample_size
                )
            
            engine.dispose()
            
        except SQLAlchemyError as exc:
            raise RuntimeError(f"Database profiling failed: {exc}")
        except (ValueError, TypeError, AttributeError) as exc:
            raise RuntimeError(f"Profiling failed: {exc}")
        except Exception as exc:
            raise RuntimeError(f"Unexpected error during profiling: {exc}")

    def _profile_column(
            self,
            engine: Engine,
            column: Column,
            *,
            sample_size: int
    ) -> None:
        """
        Profile a single column by sampling its data and analyzing the values.
        
        Args:
            engine: SQLAlchemy engine instance
            column: Column object to profile (from private copy)
            sample_size: Number of rows to sample
            
        Raises:
            SQLAlchemyError: If database query fails
        """
        # Build the query to sample data from the column
        # Use random sampling to get diverse data for profiling
        table_name = self._data_lake.db_table
        if self._data_lake.db_schema:
            table_name = f"{self._data_lake.db_schema}.{table_name}"
        
        # Use ORDER BY RANDOM() for SQLite or RAND() for other databases
        if 'sqlite' in self._data_lake.db_url:
            order_clause = "ORDER BY RANDOM()"
        else:
            order_clause = "ORDER BY RAND()"
        
        query = f"""
            SELECT {column.name}
            FROM {table_name}
            WHERE {column.name} IS NOT NULL
            {order_clause}
            LIMIT {sample_size}
        """
        
        # Execute query and collect values
        with engine.connect() as connection:
            result = connection.execute(text(query))
            values = [row[0] for row in result.fetchall()]
        
        # Profile the values using the profile_values function
        if values:
            profiling_result = profile_values(values)
            
            # Map the profiling result to our DataType enum
            inferred_type = self._map_tools_datatype_to_source_datatype(profiling_result)
            if inferred_type is not None:
                column.inferred_type = inferred_type

    def _map_tools_datatype_to_source_datatype(
            self,
            tools_datatype: ToolsDataType
    ) -> Optional[DataType]:
        """
        Map splurge_tools.type_helper.DataType to splurge_data_profiler.source.DataType.
        
        Args:
            tools_datatype: DataType from splurge_tools.type_helper
            
        Returns:
            Mapped DataType from splurge_data_profiler.source, or None if no mapping
        """
        mapping = {
            ToolsDataType.STRING: DataType.TEXT,
            ToolsDataType.INTEGER: DataType.INTEGER,
            ToolsDataType.FLOAT: DataType.FLOAT,
            ToolsDataType.BOOLEAN: DataType.BOOLEAN,
            ToolsDataType.DATE: DataType.DATE,
            ToolsDataType.TIME: DataType.TIME,
            ToolsDataType.DATETIME: DataType.DATETIME,
            ToolsDataType.MIXED: DataType.TEXT,  # Mixed types default to TEXT
            ToolsDataType.EMPTY: DataType.TEXT,  # Empty values default to TEXT
            ToolsDataType.NONE: DataType.TEXT,   # None values default to TEXT
        }
        
        return mapping.get(tools_datatype)

    @property
    def profiled_columns(self) -> List[Column]:
        """Get the profiled columns with updated inferred types."""
        return self._profiled_columns.copy()
    
    @property
    def data_lake(self) -> DataLake:
        """Get the data lake instance."""
        return self._data_lake

    def create_inferred_table(
            self,
            *,
            table_name_suffix: str = "_inferred"
    ) -> str:
        """
        Create a new table with original text columns and cast columns based on inferred types.
        
        Creates a table named <original_table_name>_inferred with:
        - Original columns as VARCHAR (preserving original text values)
        - Cast columns named <original_column_name>_cast with inferred types
        - Uses splurge_tools.type_helper.String casting methods for population
        
        Args:
            table_name_suffix: Suffix to append to original table name (default: "_inferred")
            
        Returns:
            Name of the created table
            
        Raises:
            RuntimeError: If profiling has not been performed or if table creation fails
        """       
        
        try:
            # Create database engine
            engine = create_engine(self._data_lake.db_url)
            
            # Enable WAL mode for SQLite
            with engine.connect() as connection:
                if 'sqlite' in self._data_lake.db_url:
                    connection.execute(text("PRAGMA journal_mode=WAL;"))
            
            # Generate new table name
            new_table_name = f"{self._data_lake.db_table}{table_name_suffix}"
            
            # Drop the inferred table if it exists (for SQLite and other DBs)
            with engine.connect() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {new_table_name}"))

            # Create metadata and table
            metadata = MetaData()
            table_columns = []
            
            # Add original text columns (VARCHAR)
            for column in self._profiled_columns:
                # Original column (VARCHAR)
                original_col = SAColumn(
                    column.name,
                    SAString,
                    nullable=True
                )
                table_columns.append(original_col)
                
                # Cast column based on inferred type
                cast_col_name = f"{column.name}_cast"
                cast_col_type = self._get_sqlalchemy_type_for_datatype(column.inferred_type)
                cast_col = SAColumn(
                    cast_col_name,
                    cast_col_type,
                    nullable=True
                )
                table_columns.append(cast_col)
            
            # Create the table
            new_table = Table(new_table_name, metadata, *table_columns)
            
            # Create table in database
            metadata.create_all(engine)
            
            # Populate the table with data
            self._populate_inferred_table(
                engine=engine,
                new_table=new_table
            )
            
            engine.dispose()
            return new_table_name
            
        except SQLAlchemyError as exc:
            raise RuntimeError(f"Failed to create inferred table: {exc}")
        except (ValueError, TypeError, AttributeError) as exc:
            raise RuntimeError(f"Error creating inferred table: {exc}")
        except Exception as exc:
            raise RuntimeError(f"Unexpected error creating inferred table: {exc}")

    def _get_sqlalchemy_type_for_datatype(
            self,
            datatype: DataType
    ) -> Any:
        """
        Map DataType enum to SQLAlchemy type.
        
        Args:
            datatype: DataType from splurge_data_profiler.source
            
        Returns:
            SQLAlchemy type class
        """
        # Only TEXT should be VARCHAR (MIXED, EMPTY, NONE are already mapped to TEXT)
        if datatype == DataType.TEXT:
            return SAString
        
        mapping = {
            DataType.INTEGER: Integer,
            DataType.FLOAT: Float,
            DataType.BOOLEAN: Boolean,
            DataType.DATE: Date,
            DataType.TIME: Time,
            DataType.DATETIME: DateTime,
        }
        return mapping.get(datatype, SAString)

    def _populate_inferred_table(
            self,
            engine: Engine,
            new_table: Table,
            *,
            batch_size: int = 1000
    ) -> None:
        """
        Populate the inferred table with data from the original table.
        
        Args:
            engine: SQLAlchemy engine instance
            new_table: The new table object
            batch_size: Number of rows to process in each batch
            
        Raises:
            SQLAlchemyError: If data population fails
        """
        # Handle schema prefix if needed
        original_table_name = self._data_lake.db_table
        if self._data_lake.db_schema:
            original_table_name = f"{self._data_lake.db_schema}.{original_table_name}"
        
        # Build column list for SELECT
        original_columns = [col.name for col in self._profiled_columns]
        select_columns = ", ".join(original_columns)
        
        # Query to get all data from original table
        query = f"SELECT {select_columns} FROM {original_table_name}"
        
        batch_data = []
        
        with engine.connect() as connection:
            # Enable WAL mode for SQLite
            if 'sqlite' in self._data_lake.db_url:
                connection.execute(text("PRAGMA journal_mode=WAL;"))
            result = connection.execute(text(query))
            
            for row in result:
                # Create row data for new table
                row_data = {}
                
                # Add original text values
                for i, column in enumerate(self._profiled_columns):
                    original_value = row[i]
                    row_data[column.name] = original_value
                    
                    # Add cast value
                    cast_col_name = f"{column.name}_cast"
                    cast_value = self._cast_value(
                        value=original_value,
                        target_type=column.inferred_type
                    )
                    row_data[cast_col_name] = cast_value
                
                batch_data.append(row_data)
                
                # Insert batch when it reaches the batch size
                if len(batch_data) >= batch_size:
                    self._insert_batch_to_table(
                        engine=engine,
                        table=new_table,
                        batch_data=batch_data
                    )
                    batch_data = []
            
            # Insert any remaining data in the final batch
            if batch_data:
                self._insert_batch_to_table(
                    engine=engine,
                    table=new_table,
                    batch_data=batch_data
                )

    def _cast_value(
            self,
            value: Any,
            *,
            target_type: DataType
    ) -> Any:
        """
        Cast a value to the target type using splurge_tools.type_helper.String methods.
        
        Args:
            value: The value to cast
            target_type: The target data type
            
        Returns:
            Cast value or None if casting fails
        """
        if value is None:
            return None
        
        # Convert to string for processing
        str_value = str(value).strip()
        
        if not str_value:
            return None
        
        try:
            # Use String class methods for casting (they are class methods)
            if target_type == DataType.INTEGER:
                return String.to_int(str_value)
            elif target_type == DataType.FLOAT:
                return String.to_float(str_value)
            elif target_type == DataType.BOOLEAN:
                # Custom boolean parsing to ensure we get proper Python booleans
                str_value_lower = str_value.lower().strip()
                if str_value_lower in ['true', '1', 'yes', 'y', 'on']:
                    return True
                elif str_value_lower in ['false', '0', 'no', 'n', 'off']:
                    return False
                else:
                    # Try the String.to_bool method as fallback
                    try:
                        bool_result = String.to_bool(str_value)
                        return bool(bool_result)
                    except (ValueError, TypeError):
                        return None
            elif target_type == DataType.DATE:
                return String.to_date(str_value)
            elif target_type == DataType.TIME:
                return String.to_time(str_value)
            elif target_type == DataType.DATETIME:
                return String.to_datetime(str_value)
            else:
                # For TEXT, MIXED, EMPTY, NONE - return original string value
                return str_value
                
        except (ValueError, TypeError):
            # If casting fails, return None for non-TEXT types
            if target_type == DataType.TEXT:
                return str_value
            else:
                return None

    def _insert_batch_to_table(
            self,
            engine: Engine,
            table: Table,
            *,
            batch_data: List[Dict[str, Any]]
    ) -> None:
        """
        Insert a batch of data into the specified table.
        
        Args:
            engine: SQLAlchemy engine instance
            table: The table to insert into
            batch_data: List of dictionaries representing rows to insert
            
        Raises:
            SQLAlchemyError: If insertion fails
        """
        with engine.connect() as connection:
            from sqlalchemy import insert
            insert_stmt = insert(table)
            result = connection.execute(insert_stmt, batch_data)
            connection.commit()
    
    def __str__(self) -> str:
        return f"Profiler(data_lake={self._data_lake}, profiled_columns={len(self._profiled_columns)})"
    
    def __repr__(self) -> str:
        return f"Profiler(data_lake={self._data_lake}, profiled_columns={self._profiled_columns})"
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Profiler):
            return False
        return (
            self._data_lake == other._data_lake and
            self._profiled_columns == other._profiled_columns
        )
