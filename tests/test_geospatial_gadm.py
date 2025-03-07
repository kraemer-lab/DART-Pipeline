"""Tests for process functions in geospatial/gadm.py."""
from pathlib import Path
from unittest.mock import patch

from freezegun import freeze_time
from shapely.geometry import Polygon
import geopandas as gpd

from dart_pipeline.constants import BASE_DIR
from dart_pipeline.process import process_gadm


@freeze_time('2024-02-17')
@patch('geopandas.read_file')
@patch('matplotlib.pyplot.savefig')
@patch('matplotlib.pyplot.close')
def test_process_gadm(mock_close, mock_savefig, mock_read_file):
    # Create a mock GeoDataFrame
    data = {
        'COUNTRY': ['Vietnam'],
        'NAME_1': ['Hanoi'],
        'NAME_2': ['Ba Dinh'],
        'NAME_3': ['Phuc Xa'],
        'geometry': [
            Polygon(
                [(105.8, 21.0), (105.9, 21.0), (105.9, 21.1), (105.8, 21.1)]
            )
        ]  # Hanoi region
    }
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    mock_read_file.return_value = gdf

    # Run the function
    df, output_filename = process_gadm('VNM', '2', plots=True)

    # Assertions
    expected_filename = 'VNM_geospatial_gadm_2024-02-17.csv'
    assert output_filename == expected_filename

    # Check data frame contents
    assert df.shape[0] == 1  # Should have one row
    assert df.loc[0, 'iso3'] == 'VNM'
    assert df.loc[0, 'admin_level_0'] == 'Vietnam'
    assert df.loc[0, 'admin_level_1'] == 'Hanoi'
    assert df.loc[0, 'admin_level_2'] == 'Ba Dinh'
    assert df.loc[0, 'admin_level_3'] == ''  # Admin Level 3 not included

    # Check calculated area
    expected_area_km2 = gdf.geometry.area.iloc[0] / 1e6
    assert abs(df.loc[0, 'value'] - expected_area_km2) < 200

    # Ensure mocks were called
    path = Path(BASE_DIR, 'data/sources/geospatial/gadm/VNM/gadm41_VNM_2.shp')
    mock_read_file.assert_called_once_with(path)
    mock_savefig.assert_called_once()
