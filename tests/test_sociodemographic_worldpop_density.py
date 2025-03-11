from datetime import date
from pathlib import Path

from freezegun import freeze_time
from unittest.mock import patch, MagicMock
import pandas as pd

from dart_pipeline.constants import MIN_FLOAT
from dart_pipeline.process import process_worldpopdensity


@freeze_time('2025-02-06')
@patch('rasterio.open')
@patch('dart_pipeline.process.DEFAULT_SOURCES_ROOT')
@patch('dart_pipeline.process.BASE_DIR')
@patch('dart_pipeline.process.source_path')
@patch('dart_pipeline.process.get_country_name')
def test_process_worldpopdensity(
    mock_get_country_name, mock_source_path, mock_base_dir,
    mock_default_sources_root, mock_rasterio_open
):
    # Set up mock return values
    mock_get_country_name.return_value = 'Vietnam'
    mock_source_path.return_value = Path('/mock/path')
    mock_base_dir.return_value = 'mock/dir'
    mock_default_sources_root.return_value = 'sources'

    mock_raster = MagicMock()
    mock_raster.count = 1
    mock_raster.read.return_value = [[1, 2, MIN_FLOAT], [4, 5, 6]]
    mock_raster.transform = 'mock_transform'
    mock_rasterio_open.return_value = mock_raster

    # Call the function with test inputs
    iso3 = 'VNM'
    year = '2020'
    rt = 'ppp'
    result, filename = process_worldpopdensity(iso3, year, rt)

    # Assertions
    expected = 'VNM_sociodemographic_worldpop-density_2020_2025-02-06.csv'
    assert filename == expected
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1  # Single row in the output DataFrame

    # Verify the DataFrame contents
    assert result.loc[0, 'GID_0'] == iso3
    assert result.loc[0, 'COUNTRY'] == 'Vietnam'
    assert result.loc[0, 'year'] == 2020
    assert result.loc[0, 'metric'] == 'Population Density'
    assert result.loc[0, 'unit'] == ''
    assert result.loc[0, 'value'] == 4
    assert result.loc[0, 'creation_date'] == date.today()
