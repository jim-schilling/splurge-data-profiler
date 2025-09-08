import tempfile
from pathlib import Path

from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


class TestProfilerComprehensive:
    """Test comprehensive profiling functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures before each test method."""
        # Create temporary directory for data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

        # Create temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        self.csv_path = Path(self.temp_path)

        # Generate comprehensive test data (reduced from 15000 to 1000 rows)
        self._generate_comprehensive_csv()

        # Create DsvSource and DataLake
        self.dsv_source = DsvSource(self.csv_path, delimiter="|", bookend='"')
        self.data_lake = DataLakeFactory.from_dsv_source(dsv_source=self.dsv_source, data_lake_path=self.data_lake_path)

        # Create a fresh Profiler instance for each test
        self.profiler = Profiler(data_lake=self.data_lake)

    def _generate_comprehensive_csv(self) -> None:
        """Generate a comprehensive CSV file with all data types and 1000 rows."""
    # ...existing code...
    pass
