from pathlib import Path
import pytest

from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


class TestProfilerComprehensive:
    """Test comprehensive profiling functionality."""


@pytest.fixture(autouse=True)
def _profiler_setup(tmp_path: Path, request):
    """Autouse fixture to provide pytest tmp_path-based resources for profiler tests.

    Sets attributes on the test instance so existing test methods can use
    `self.csv_path`, `self.data_lake_path`, `self.dsv_source`, and `self.profiler`.
    """
    if not hasattr(request, "instance") or request.instance is None:
        return

    if request.instance.__class__.__name__ != "TestProfilerComprehensive":
        return

    inst = request.instance
    # Create pytest-managed directories and files
    inst.data_lake_path = tmp_path / "data_lake"
    inst.data_lake_path.mkdir(parents=True, exist_ok=True)

    inst.csv_path = tmp_path / "comprehensive.csv"
    # Write a modest representative CSV for integration tests
    inst.csv_path.write_text("id,name,value\n0,Item0,0.0\n1,Item1,1.5\n", encoding="utf-8")

    # Create DsvSource and DataLake
    inst.dsv_source = DsvSource(inst.csv_path, delimiter="|", bookend='"')
    inst.data_lake = DataLakeFactory.from_dsv_source(dsv_source=inst.dsv_source, data_lake_path=inst.data_lake_path)
    inst.profiler = Profiler(data_lake=inst.data_lake)

    yield
