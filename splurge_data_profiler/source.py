from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from splurge_tools.dsv_helper import DsvHelper
from splurge_tools.tabular_data_model import TabularDataModel
from splurge_tools.text_file_helper import TextFileHelper


class DataType(Enum):
    """Enumeration of supported data types for column profiling."""
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    FLOAT = "FLOAT"
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    TIME = "TIME"


class Column:
    """Represents a column with its metadata and profiling information."""

    def __init__(
            self,
            name: str,
            *,
            inferred_type: DataType = DataType.TEXT,
            is_nullable: bool = True
    ) -> None:
        """
        Initialize a Column instance.

        Args:
            name: The name of the column
            inferred_type: The inferred data type from profiling
            is_nullable: Whether the column can contain null values
        """
        self._name = name
        self._raw_type = DataType.TEXT
        self._inferred_type = inferred_type
        self._is_nullable = is_nullable

    @property
    def name(self) -> str:
        """Get the column name."""
        return self._name
    
    @property
    def inferred_type(self) -> DataType:
        """Get the inferred data type from profiling."""
        return self._inferred_type
    
    @inferred_type.setter
    def inferred_type(self, value: DataType) -> None:
        """Set the inferred data type from profiling."""
        self._inferred_type = value
    
    @property
    def raw_type(self) -> DataType:
        """Get the raw data type from the source."""
        return self._raw_type
    
    @property
    def is_nullable(self) -> bool:
        """Get whether the column can contain null values."""
        return self._is_nullable
    
    def __str__(self) -> str:
        return f"{self._name} ({self._inferred_type})"
    
    def __repr__(self) -> str:
        return (
            f"Column(name={self._name}, inferred_type={self._inferred_type}, "
            f"raw_type={self._raw_type}, is_nullable={self._is_nullable})"
        )
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Column):
            return NotImplemented
        return (
            self._name == other._name and
            self._inferred_type == other._inferred_type and
            self._raw_type == other._raw_type and
            self._is_nullable == other._is_nullable
        )

    def __hash__(self) -> int:
        return hash((self._name, self._inferred_type, self._raw_type, self._is_nullable))


class Source(ABC):
    """Abstract base class for data sources."""

    def __init__(
            self,
            *,
            columns: Optional[List[Column]] = None
    ) -> None:
        """
        Initialize a Source instance.

        Args:
            columns: List of column definitions
        """
        self._columns = columns or []

    @property
    def columns(self) -> List[Column]:
        """Get the list of column definitions."""
        return self._columns.copy()
    
    def __str__(self) -> str:
        return f"Source(columns={self._columns})"
    
    def __repr__(self) -> str:
        return f"Source(columns={self._columns})"
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Source):
            return False
        return self._columns == other._columns
    
    def __iter__(self) -> Iterator[Column]:
        return iter(self._columns)
    
    def __len__(self) -> int:
        return len(self._columns)
    
    def __getitem__(self, index: int) -> Column:
        return self._columns[index]


class DsvSource(Source):
    """Source for delimiter-separated value files (CSV, TSV, etc.)."""

    def __init__(
            self,
            file_path: Union[str, Path],
            *,
            delimiter: str = ",",
            strip: bool = True,
            bookend: str = '"',
            bookend_strip: bool = True,
            encoding: str = "utf-8",
            skip_header_rows: int = 0,
            skip_footer_rows: int = 0,
            header_rows: int = 1,
            skip_empty_rows: bool = True
    ) -> None:
        """
        Initialize a DsvSource instance.

        Args:
            file_path: Path to the DSV file
            delimiter: Character used to separate values
            strip: Whether to strip whitespace from values
            bookend: Character used to quote values
            bookend_strip: Whether to strip bookend characters
            encoding: File encoding
            skip_header_rows: Number of header rows to skip
            skip_footer_rows: Number of footer rows to skip
            header_rows: Number of header rows
            skip_empty_rows: Whether to skip empty rows
        """
        self._file_path = file_path
        self._delimiter = delimiter
        self._bookend = bookend
        self._bookend_strip = bookend_strip
        self._encoding = encoding
        self._skip_header_rows = skip_header_rows
        self._skip_footer_rows = skip_footer_rows
        self._header_rows = header_rows
        self._strip = strip
        self._skip_empty_rows = skip_empty_rows
        # Call parent first to initialize _strip and other attributes
        super().__init__(columns=self._initialize())

    @property
    def file_path(self) -> Union[str, Path]:
        """Get the file path."""
        return self._file_path
    
    @property
    def delimiter(self) -> str:
        """Get the delimiter character."""
        return self._delimiter
    
    @property
    def bookend(self) -> str:
        """Get the bookend character."""
        return self._bookend
    
    @property
    def bookend_strip(self) -> bool:
        """Get whether to strip bookend characters."""
        return self._bookend_strip
    
    @property
    def encoding(self) -> str:
        """Get the file encoding."""
        return self._encoding
    
    @property
    def skip_header_rows(self) -> int:
        """Get the number of header rows to skip."""
        return self._skip_header_rows
    
    @property
    def skip_footer_rows(self) -> int:
        """Get the number of footer rows to skip."""
        return self._skip_footer_rows
    
    @property
    def header_rows(self) -> int:
        """Get the number of header rows."""
        return self._header_rows
    
    @property
    def strip(self) -> bool:
        """Get whether to strip whitespace from values."""
        return self._strip
    
    @property
    def skip_empty_rows(self) -> bool:
        """Get whether to skip empty rows."""
        return self._skip_empty_rows
    
    def _initialize(self) -> List[Column]:
        """
        Initialize the header columns from the file.
        """
        try:
            raw_header_model = TextFileHelper.preview(
                self._file_path,
                max_lines=self._header_rows,               
                strip=self._strip,                
                encoding=self._encoding,
                skip_header_rows=self._skip_header_rows
            )

            raw_data_model = DsvHelper.parses(
                raw_header_model,
                delimiter=self._delimiter,
                bookend=self._bookend,
                bookend_strip=self._bookend_strip
            )
            
            data_model = TabularDataModel(
                raw_data_model,
                header_rows=self._header_rows,
                skip_empty_rows=self._skip_empty_rows
            )
            columns = [Column(name=col_name) for col_name in data_model.column_names]
            return columns
        except Exception as exc:
            raise RuntimeError(f"Failed to initialize columns from file: {exc}")
    
    
    def __str__(self) -> str:
        return (
            f"DsvSource(file_path={self._file_path}, delimiter={self._delimiter}, "
            f"bookend={self._bookend}, bookend_strip={self._bookend_strip}, "
            f"encoding={self._encoding}, skip_header_rows={self._skip_header_rows}, "
            f"skip_footer_rows={self._skip_footer_rows}, header_rows={self._header_rows}, "
            f"skip_empty_rows={self._skip_empty_rows}, columns={self._columns})"
        )
    
    def __repr__(self) -> str:
        return (
            f"DsvSource(file_path={self._file_path}, delimiter={self._delimiter}, "
            f"bookend={self._bookend}, bookend_strip={self._bookend_strip}, "
            f"encoding={self._encoding}, skip_header_rows={self._skip_header_rows}, "
            f"skip_footer_rows={self._skip_footer_rows}, header_rows={self._header_rows}, "
            f"skip_empty_rows={self._skip_empty_rows}, columns={self._columns})"
        )
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DsvSource):
            return False
        return (
            self._file_path == other._file_path and
            self._delimiter == other._delimiter and
            self._bookend == other._bookend and
            self._bookend_strip == other._bookend_strip and
            self._encoding == other._encoding and
            self._skip_header_rows == other._skip_header_rows and
            self._skip_footer_rows == other._skip_footer_rows and
            self._header_rows == other._header_rows and
            self._strip == other._strip and
            self._skip_empty_rows == other._skip_empty_rows and
            self._columns == other._columns
        )
    


class DbSource(Source):
    """Source for database tables."""

    def __init__(
            self,
            *,
            db_url: str,
            db_schema: str,
            db_table: str
    ) -> None:
        """
        Initialize a DbSource instance.

        Args:
            db_url: Database connection URL
            db_schema: Database schema name
            db_table: Database table name
        """
        self._db_url = db_url
        self._db_schema = db_schema
        self._db_table = db_table
        super().__init__(columns=self._initialize())
    
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

    def _initialize(self) -> List[Column]:
        """
        Connect to the database and initialize column definitions.

        Connects to the database using SQLAlchemy, reflects the table schema, and creates
        Column objects for each column in the specified table. Validates that all columns
        are text-like datatypes. Only sets raw_type; inferred_type is not set until profiling.

        Returns:
            List of Column objects representing the table schema

        Raises:
            ValueError: If any column is not a text-like datatype
            RuntimeError: If database connection or schema reflection fails
        """
        from sqlalchemy import create_engine, inspect, MetaData, Table
        from sqlalchemy.exc import SQLAlchemyError

        try:
            engine = create_engine(self._db_url)
            metadata = MetaData()
            table = Table(
                self._db_table,
                metadata,
                autoload_with=engine,
                schema=self._db_schema
            )
            inspector = inspect(engine)
            columns_info = inspector.get_columns(self._db_table, schema=self._db_schema)

            columns = []
            for column_info in columns_info:
                column_name = column_info['name']
                column_type = column_info['type']
                is_nullable = column_info.get('nullable', True)

                # Check if column is text-like
                type_string = str(column_type).lower()
                text_types = ['varchar', 'text', 'char', 'string', 'nvarchar', 'ntext']
                is_text_like = any(text_type in type_string for text_type in text_types)

                if not is_text_like:
                    raise ValueError(f"Column '{column_name}' is not a text-like datatype. Found: {column_type}")

                # Create column with raw_type set to TEXT
                column = Column(
                    name=column_name,
                    is_nullable=is_nullable
                )
                column._raw_type = DataType.TEXT
                columns.append(column)

            return columns
        
        except SQLAlchemyError as exc:
            raise RuntimeError(f"Failed to initialize columns from database: {exc}")

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DbSource):
            return False
        return (
            self._db_url == other._db_url and
            self._db_schema == other._db_schema and
            self._db_table == other._db_table and
            self._columns == other._columns
        )

