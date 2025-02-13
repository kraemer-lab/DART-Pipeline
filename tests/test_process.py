"""Tests for process functions in process.py."""
from io import BytesIO

from unittest.mock import patch, MagicMock, mock_open

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from dart_pipeline.process import \
    process_rwi, \
    process_dengueperu, \
    process_gadm_aphroditeprecipitation, \
    process_gadm_chirps_rainfall, \
    process_aphrodite_precipitation_data, \
    process_chirps_rainfall, \
    process_terraclimate

# Smallest single-precision floating-point number
MIN_FLOAT = -3.4028234663852886e38


@patch('pandas.DataFrame.parallel_apply')
@patch('pandas.read_csv')
@patch('dart_pipeline.util.source_path')
@patch('geopandas.read_file')
@patch('dart_pipeline.process.get_shapefile')
def test_process_rwi(
    mock_get_shapefile, mock_read_file, mock_source_path, mock_read_csv,
    mock_parallel_apply
):
    # Test Case 1: Valid data processing
    # Mocking shapefile
    mock_get_shapefile.return_value = 'mock_path_to_shapefile'
    mock_read_file.return_value = pd.DataFrame({
        'GID_2': ['ID1', 'ID2'],
        'geometry': [MagicMock(), MagicMock()],
        'COUNTRY': ['Country', 'Country'],
        'NAME_1': ['Region1', 'Region2'],
        'NAME_2': ['Subregion1', 'Subregion2']
    })
    mock_source_path.return_value = 'mock_path_to_csv'
    # Mock RWI data with latitude, longitude, and rwi columns
    mock_read_csv.return_value = pd.DataFrame({
        'latitude': [10.5, 20.5],
        'longitude': [105.5, 110.5],
        'rwi': [1.2, 0.5]
    })
    # Mock parallel_apply()
    mock_parallel_apply.return_value = None

    # Run the function
    rwi_df, csv_filename = process_rwi('VNM', '2', plots=False)

    # Assertions for valid data processing
    assert isinstance(rwi_df, pd.DataFrame), 'Output should be a DataFrame'
    assert 'admin_level_0' in rwi_df.columns, 'Expected column missing'
    assert 'value' in rwi_df.columns, 'Expected column missing in output'
    assert csv_filename == 'VNM.csv', 'CSV filename does not match expected'

    # Test Case 2: Missing RWI CSV file
    # Simulate missing CSV file
    mock_read_csv.side_effect = FileNotFoundError('RWI CSV file not found')
    with pytest.raises(FileNotFoundError):
        process_rwi('VNM', '2')
    # Reset the side effect to avoid affecting the next test case
    mock_read_csv.side_effect = None

    # Test Case 3: Check for plotting
    with patch('dart_pipeline.process.plot_gadm_macro_heatmap') as mock_plot:
        # Run with plotting enabled
        rwi_df, csv_filename = process_rwi('VNM', '2', plots=True)
        # Assert plot function was called
        mock_plot.assert_called_once()


@pytest.fixture
def mock_source_path():
    with patch('dart_pipeline.util.source_path') as mock_path:
        mock_path.return_value = '/mock/source/path'
        yield mock_path


@pytest.fixture
def mock_output_path():
    with patch('dart_pipeline.util.output_path') as mock_path:
        mock_path.return_value = '/mock/output/path'
        yield mock_path


@pytest.fixture
def mock_plot_timeseries():
    with patch('dart_pipeline.plots.plot_timeseries') as mock_plot:
        yield mock_plot


@pytest.fixture
def mock_read_excel():
    with patch('pandas.read_excel') as mock_read:
        mock_read.return_value = pd.DataFrame({
            'ano': [2023, 2023],
            'semana': [1, 2],
            'tipo_dx': ['C', 'P'],
            'n': [10, 20],
        })
        yield mock_read


@pytest.fixture
def mock_os_walk():
    with patch('os.walk') as mock_walk:
        mock_walk.return_value = [(
            '/mock/source/path', ['subdir'],
            ['casos_dengue_nacional.xlsx', 'casos_dengue_region1.xlsx']
        )]
        yield mock_walk


@pytest.mark.parametrize(
    'admin_level, expected_admin_level_1, expected_plot_calls, should_raise',
    [
        ('0', '', 1, False),  # Admin level 0
        ('1', 'Region1', 1, False),  # Admin level 1
        ('2', None, 0, True),  # Invalid admin level
    ]
)
def test_process_dengueperu(
    admin_level, expected_admin_level_1, expected_plot_calls, should_raise,
    mock_source_path, mock_output_path, mock_plot_timeseries,
    mock_read_excel, mock_os_walk
):
    if should_raise:
        match = f'Invalid admin level: {admin_level}'
        with pytest.raises(ValueError, match=match):
            process_dengueperu(admin_level=admin_level)
    else:
        master, output_filename = process_dengueperu(
            admin_level=admin_level, plots=True
        )

        # Validate the output DataFrame
        assert isinstance(master, pd.DataFrame)
        assert master['admin_level_0'].iloc[0] == 'Peru'
        assert master['admin_level_1'].iloc[0] == expected_admin_level_1
        assert master['metric'].tolist() == [
            'Confirmed Dengue Cases', 'Probable Dengue Cases'
        ]
        assert output_filename == 'dengue_peru.csv'

        # Check the mock calls
        mock_read_excel.assert_called()


class MockFile(BytesIO):
    """A mock file object that adds a fileno method."""
    def fileno(self):
        return 1


def test_process_gadm_aphroditeprecipitation():
    iso3 = 'VNM'
    admin_level = '0'
    partial_date = '2023-07'
    resolution = ['025deg']
    plots = False

    with patch('dart_pipeline.process.PartialDate') as mock_partial_date, \
         patch('dart_pipeline.util.get_shapefile') as mock_get_shapefile, \
         patch('geopandas.read_file') as mock_read_file, \
         patch('dart_pipeline.util.source_path') as mock_source_path, \
         patch('dart_pipeline.util.output_path') as mock_output_path, \
         patch('builtins.open') as mock_open, \
         patch('numpy.fromfile') as mock_fromfile:

        # Mock PartialDate to return a specific year when accessed
        mock_partial_date.from_string.return_value = MagicMock(
            year=2023, month=7, day=None, scope='year'
        )

        # Mock shapefile loading
        mock_get_shapefile.return_value = 'mock_shapefile_path'

        # Create a mock GeoDataFrame row with a geometry attribute
        mock_row = MagicMock()
        mock_row.geometry = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # Mock Polygon
        mock_row.COUNTRY = 'Vietnam'
        mock_row.NAME_1 = 'Mock Province'
        mock_row.NAME_2 = 'Mock District'
        mock_row.NAME_3 = 'Mock Sub-district'

        # Mock geopandas dataframe and its iterrows method
        mock_gdf = MagicMock()
        mock_gdf.iterrows.return_value = iter([(0, mock_row)])  # Return the mock row
        mock_read_file.return_value = mock_gdf

        # Mock source_path and output_path
        mock_source_path.return_value = MagicMock()
        mock_output_path.return_value = MagicMock()

        # Mock np.fromfile() to return a fake array
        nx, ny = 360, 280
        recl = nx * ny

        # Create a fake array with the correct number of values
        fake_array = np.ones(recl, dtype='float32')

        # Mock np.fromfile() to return the fake array when called
        mock_fromfile.return_value = fake_array

        # Create a mock file object
        mock_file = MockFile()
        mock_open.return_value.__enter__.return_value = mock_file

        # Call the function
        output, csv_path = process_gadm_aphroditeprecipitation(
            iso3, admin_level, partial_date, resolution, plots
        )

        # Assertions
        assert isinstance(output, pd.DataFrame)
        assert 'iso3' in output.columns
        assert 'value' in output.columns
        assert output['iso3'].iloc[0] == iso3
        assert output['value'].sum() >= 0  # Ensure non-negative precipitation values
        assert csv_path == 'aphrodite-daily-precip.csv'


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


def test_process_aphrodite_precipitation_data():
    # Minimal mocking for `np.fromfile` and file operations
    nx, ny, _ = 360, 280, 365
    # Mock precipitation data
    mock_prcp = np.full((ny, nx), 10.0, dtype='float32')
    # Mock station count data
    mock_rstn = np.ones((ny, nx), dtype='float32')

    def mock_fromfile(file, dtype, count):
        if dtype == 'float32' and count == nx * ny:
            return mock_prcp.flatten() \
                if 'prcp' in file.name else mock_rstn.flatten()
        raise ValueError(
            f'Unexpected call to np.fromfile with {file}, {dtype}, {count}'
        )

    # Mock file opening
    mocked_open = mock_open()
    with patch('builtins.open', mocked_open), \
            patch('numpy.fromfile', mock_fromfile):
        # Call the function
        year = 2023
        output, csv_name = process_aphrodite_precipitation_data(
            year=year, resolution=['025deg'], plots=False
        )

        # Assert the output is a DataFrame
        assert isinstance(output, pd.DataFrame)
        assert len(output) > 0  # Ensure some data is processed
        assert 'year' in output.columns
        assert 'value' in output.columns

        # Check key output values
        assert (output['year'] == year).all()
        assert csv_name == 'aphrodite-daily-precip.csv'


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
