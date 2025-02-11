from io import BytesIO

from unittest.mock import patch, MagicMock, mock_open

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from dart_pipeline.process import process_gadm_era5reanalysis


@patch('pathlib.Path.iterdir')
@patch('netCDF4.Dataset')
@patch('geopandas.read_file')
def test_process_gadm_era5reanalysis(
    mock_read_file, mock_nc_dataset, mock_iterdir
):
    # Mock the NetCDF dataset
    mock_nc = MagicMock()
    mock_variable = MagicMock()
    mock_variable.__getitem__.return_value = np.random.rand(3, 3)
    mock_variable.long_name = 'Temperature'
    mock_variable.units = 'K'
    mock_nc.variables = {
        'temp': mock_variable,
        'latitude': np.array([10, 20, 30]),
        'longitude': np.array([100, 110, 120]),
    }
    mock_nc_dataset.return_value = mock_nc

    # Mock the shapefile with a single geometry and admin information
    mock_geometry = Polygon([(100, 10), (100, 20), (110, 20), (110, 10)])
    mock_gdf = gpd.GeoDataFrame({
        'COUNTRY': ['Mockland'],
        'NAME_1': ['MockRegion'],
        'NAME_2': [None],
        'NAME_3': [None],
        'geometry': [mock_geometry]
    })
    mock_read_file.return_value = mock_gdf

    # Other parameters
    dataset = 'mock-dataset'
    partial_date = '2023-01'
    iso3 = 'MCK'
    admin_level = '1'
    plots = False

    # Mock folder and its iteration
    mock_file = MagicMock()
    mock_file.name = f'{dataset}_2023-01.nc'
    mock_iterdir.return_value = [mock_file]

    # Run the function
    df, filename = process_gadm_era5reanalysis(
        dataset, iso3, admin_level, partial_date, plots
    )

    # Validate the returned DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert 'admin_level_0' in df.columns
    assert 'admin_level_1' in df.columns
    # Ensure temperature column was added
    assert 'value' in df.columns
    assert df['admin_level_0'].iloc[0] == 'Mockland'
    assert df['year'].iloc[0] == 2023
    assert df['month'].iloc[0] == 1

    # Check that filename was created correctly
    assert filename == 'era5-reanalysis.csv'

    # Ensure function fails gracefully with an invalid date
    with pytest.raises(ValueError):
        process_gadm_era5reanalysis(
            dataset, 'MCK', '1', 'invalid-date', False
        )

    # Ensure plots can be generated if requested
    partial_date = '2023-01'
    iso3 = 'MCK'
    admin_level = '0'

    with patch('matplotlib.pyplot.savefig') as mock_savefig:
        process_gadm_era5reanalysis(
            dataset, iso3, admin_level, partial_date, plots=True
        )
        # Verify that plotting occurred
        mock_savefig.assert_called()
