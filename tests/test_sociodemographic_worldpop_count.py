"""Tests for process functions in sociodemographic/worldpop_count.py."""
from pathlib import Path
from unittest.mock import patch, MagicMock

from freezegun import freeze_time
import pandas as pd

from dart_pipeline.process import process_worldpopcount
from dart_pipeline.constants import MIN_FLOAT


@freeze_time('2025-02-06')
@patch('rasterio.open')
@patch('dart_pipeline.process.source_path')
def test_process_worldpopcountdata(
    mock_source_path, mock_rasterio_open
):
    # Set up mock return values
    mock_source_path.return_value = Path('/mock/path')

    mock_raster = MagicMock()
    mock_raster.count = 1
    mock_raster.read.return_value = [[1, 2, MIN_FLOAT], [4, 5, 6]]
    mock_raster.transform = 'mock_transform'
    mock_rasterio_open.return_value = mock_raster

    # Call the function with test inputs
    iso3 = 'VNM'
    year = 2020
    rt = 'ppp'
    result, filename = process_worldpopcount(iso3, year, rt)

    # Assertions
    expected = 'VNM_sociodemographic_worldpop-count_2020_2025-02-06.csv'
    assert filename == expected
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1  # Single row in the output DataFrame

    # Verify the DataFrame contents
    expected_population = 1 + 2 + 4 + 5 + 6  # Excludes MIN_FLOAT
    assert result.loc[0, 'iso3'] == iso3
    assert result.loc[0, 'admin_level_0'] == 'Vietnam'
    assert result.loc[0, 'year'] == year
    assert result.loc[0, 'metric'] == 'population'
    assert result.loc[0, 'unit'] == 'unitless'
    assert result.loc[0, 'value'] == expected_population
    assert result.loc[0, 'resolution'] == 'people per pixel'
    assert str(result.loc[0, 'creation_date']) == '2025-02-06'
