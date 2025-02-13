"""Test the processing of APHRODITE precipitation (V1901) data."""
from io import BytesIO
from unittest.mock import patch, MagicMock

from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd

from dart_pipeline.geospatial.aphroditeprecipitation import \
    process_gadm_aphroditeprecipitation


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

    with patch('dart_pipeline.util.get_shapefile') as mock_get_shapefile, \
         patch('geopandas.read_file') as mock_read_file, \
         patch('dart_pipeline.util.source_path') as mock_source_path, \
         patch('dart_pipeline.util.output_path') as mock_output_path, \
         patch('builtins.open') as mock_open, \
         patch('numpy.fromfile') as mock_fromfile:

        # Mock shapefile loading
        mock_get_shapefile.return_value = 'mock_shapefile_path'

        # Create geopandas dataframe
        data = {
            'COUNTRY': 'Vietnam',
            'NAME_1': [10, 20, 30],
            'NAME_2': 'Mock District',
            'NAME_3': 'Mock Sub-district',
            'geometry': Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        }
        gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
        mock_read_file.return_value = gdf

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
        assert output['value'].sum() == 0
        assert csv_path == 'aphrodite-daily-precip.csv'
