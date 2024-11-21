"""Tests for process functions in process.py."""
from unittest.mock import patch, MagicMock

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import pytest

from dart_pipeline.process import \
    process_gadm_chirps_rainfall, \
    process_gadm_worldpopcount, \
    process_chirps_rainfall, \
    process_terraclimate


@patch('geopandas.read_file')
@patch("dart_pipeline.process.get_chirps_rainfall_data_path")
@patch("dart_pipeline.process.get_shapefile")
@patch("rasterio.open")
@patch("dart_pipeline.process.output_path")
@patch("matplotlib.pyplot.savefig")
def test_process_gadm_chirps_rainfall(
    mock_savefig,
    mock_output_path,
    mock_raster_open,
    mock_get_shapefile,
    mock_get_chirps_rainfall_data_path,
    mock_read_file
):
    iso3 = 'VNM'
    admin_level = '3'
    partial_date = '2023-05'
    plots = False

    # Mock the file paths
    mock_get_chirps_rainfall_data_path.return_value = 'mocked_file.tif'
    mock_get_shapefile.return_value = 'mocked_shapefile.shp'
    mock_output_path.return_value = 'mocked_output_path'

    # Mock rasterio dataset and set up a test array for rainfall data
    mock_dataset = MagicMock()
    mock_dataset.bounds = MagicMock()
    mock_dataset.bounds.left = -180
    mock_dataset.bounds.bottom = -90
    mock_dataset.bounds.right = 180
    mock_dataset.bounds.top = 90
    mock_raster_open.return_value.__enter__.return_value = mock_dataset

    # Mock GeoDataFrame and region data
    mock_gdf = MagicMock(spec=gpd.GeoDataFrame)
    region_geometry = Polygon([(-180, -90), (0, -90), (0, 0), (-180, 0)])
    mock_region = MagicMock()
    mock_region.geometry = region_geometry
    mock_region.__getitem__.side_effect = lambda key: {
        'COUNTRY': 'Vietnam',
        'NAME_1': 'Hanoi',
        'NAME_2': 'District 1',
        'NAME_3': 'Ward 1'
    }[key]
    mock_gdf.iterrows.return_value = [(0, mock_region)]
    mock_gdf.to_crs.return_value = mock_gdf
    mock_read_file.return_value = mock_gdf

    # Call the function
    output, filename = process_gadm_chirps_rainfall(
        iso3, admin_level, partial_date, plots=plots
    )

    # Verify the output DataFrame
    expected_data = {
        'admin_level_0': ['Vietnam'],
        'admin_level_1': ['Hanoi'],
        'admin_level_2': ['District 1'],
        'admin_level_3': ['Ward 1'],
        'year': [2023],
        'month': [5],
        'day': [None],
        'rainfall': [0]
    }
    expected_df = pd.DataFrame(expected_data).astype(object)
    pd.testing.assert_frame_equal(output, expected_df)

    # Verify the CSV filename
    assert filename == 'VNM.csv'

    # Verify the interaction with GeoDataFrame and rasterio
    mock_gdf.to_crs.assert_called_once()

    # Verify the correct handling of bounds and plot creation
    min_lon, min_lat, max_lon, max_lat = region_geometry.bounds


@patch('os.listdir')
@patch('geopandas.gpd.read_file')
@patch('dart_pipeline.process.get_shapefile')
@patch('dart_pipeline.util.source_path')
@patch("rasterio.open")
def test_process_gadm_worldpopcount(
    mock_rasterio_open, mock_source_path, mock_get_shapefile, mock_read_file,
    mock_listdir
):
    # Test case 1: Process valid data
    mock_read_file.return_value = MagicMock()
    mock_rasterio_open.return_value = MagicMock(
        read=lambda x: [[1, 1], [1, 1]]
    )
    # Run the function with valid data
    output, csv_filename = process_gadm_worldpopcount('VNM', '2020', '2')
    # Assertions for valid data processing
    assert isinstance(output, pd.DataFrame), 'Output should be a DataFrame'
    msg = 'Expected column missing in output'
    assert 'admin_level_0' in output.columns, msg
    assert 'metric' in output.columns, 'Expected column missing in output'
    msg = 'CSV filename does not match expected value'
    assert csv_filename == 'VNM.csv', msg

    # Test case 2: Invalid date with day included
    with pytest.raises(ValueError, match='Provide only a year in YYYY format'):
        process_gadm_worldpopcount('VNM', '2020-01-01', admin_level='0')

    # Test case 3: Invalid date with month included
    with pytest.raises(ValueError, match='Provide only a year in YYYY format'):
        process_gadm_worldpopcount('VNM', '2020-01', admin_level='0')

    # Test case 4: Missing raster file, falling back to previous year
    # Simulate missing file for the given year but available fallback file
    mock_listdir.return_value = ['VNM_ppp_v2b_2019_UNadj.tif']
    mock_rasterio_open.side_effect = [
        rasterio.errors.RasterioIOError, MagicMock()
    ]
    # Call the function
    output, csv_filename = process_gadm_worldpopcount('VNM', '2020', '0')
    # Check that fallback file was used and output generated
    msg = 'Output should be a DataFrame even with fallback file'
    assert isinstance(output, pd.DataFrame), msg


@patch('dart_pipeline.process.output_path')
@patch('rasterio.open')
@patch('dart_pipeline.process.get_chirps_rainfall_data_path')
def test_process_chirps_rainfall(
    mock_data_path, mock_raster_open, mock_output_path
):
    partial_date = '2023-05'
    plots = False

    mock_data_path.return_value = 'mocked_file.tif'
    mock_output_path.return_value = 'mocked_output_path'

    # Mock rasterio dataset and set up a test array for rainfall data
    mock_dataset = MagicMock()
    mock_data = np.array(
        [[0, 1, 2], [3, 4, -3.4028234663852886e38], [np.nan, -9999, 5]]
    )
    mock_dataset.read.return_value = mock_data
    mock_raster_open.return_value.__enter__.return_value = mock_dataset

    # Call the function
    output, filename = process_chirps_rainfall(partial_date, plots=plots)

    # Verify the output DataFrame
    expected_data = {
        'year': [2023],
        'month': [5],
        'day': [None],
        'rainfall': [0.0]
    }
    expected_df = pd.DataFrame(expected_data).astype(object)
    pd.testing.assert_frame_equal(output, expected_df)

    # Verify file output name
    assert filename == 'chirps-rainfall.csv'


@patch('netCDF4.Dataset')
@patch('geopandas.read_file')
@patch('dart_pipeline.process.source_path')
def test_process_terraclimate(
    mock_source_path, mock_read_file, mock_nc_dataset
):
    # Mock the path to the raw data
    mock_source_path.return_value = 'mocked/path/to/netcdf/file.nc'
    # Mock the NetCDF dataset
    mock_nc = MagicMock()
    mock_nc.variables = {
        'lat': np.array([10, 20]),
        'lon': np.array([100, 110]),
        # Jan, Feb, Mar in days since 1900-01-01
        'time': [0, 31, 59],
        'aet': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'def': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'PDSI': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'pet': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'ppt': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'q': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'soil': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'srad': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'swe': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'tmax': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'tmin': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'vap': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'vpd': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        ),
        'ws': MagicMock(
            __getitem__=MagicMock(
                return_value=np.array([[[0.5, 0.6], [0.7, 0.8]]])
            ),
            scale_factor=1.0,
            add_offset=0.0,
            _FillValue=-9999.0,
            standard_name='temperature',
            description='Temperature',
            units='C'
        )
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

    # Expected partial date, iso3, and admin level
    partial_date = '2023-01'
    iso3 = 'MCK'
    admin_level = '1'

    # Run the function
    output, filename = process_terraclimate(partial_date, iso3, admin_level)

    # Check that source_path was called with correct arguments
    mock_source_path.assert_called_with(
        'meteorological/terraclimate', 'TerraClimate_ws_2023.nc'
    )

    # Validate the returned DataFrame structure
    assert isinstance(output, pd.DataFrame)
    assert 'admin_level_0' in output.columns
    assert 'admin_level_1' in output.columns
    # Ensure temperature column was added
    assert 'temperature' in output.columns
    assert output['admin_level_0'].iloc[0] == 'Mockland'
    assert output['year'].iloc[0] == 1900
    assert output['month'].iloc[0] == 1

    # Check that filename was created correctly
    assert filename == 'MCK.csv'

    # Ensure function fails gracefully with an invalid date
    with pytest.raises(ValueError):
        process_terraclimate('invalid-date', 'MCK', '1')

    # Ensure plots can be generated if requested
    partial_date = '2023-01'
    iso3 = 'MCK'
    admin_level = '1'

    with patch('matplotlib.pyplot.savefig') as mock_savefig:
        process_terraclimate(partial_date, iso3, admin_level, plots=True)
        # Verify that plotting occurred
        mock_savefig.assert_called()
