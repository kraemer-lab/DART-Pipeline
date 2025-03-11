"""Tests for functions in population-weighted/relative-wealth-index.py."""
from unittest import mock

from freezegun import freeze_time
import geopandas as gpd
import pandas as pd

from dart_pipeline.meta.relative_wealth_index import \
    process_gadm_popdensity_rwi


@freeze_time('2025-03-05')
@mock.patch('geopandas.read_file')
@mock.patch('pandas.read_csv')
def test_process_gadm_popdensity_rwi(mock_pd_read, mock_gpd_read):
    # Mock GADM shapefile
    mock_gpd_read.return_value = gpd.GeoDataFrame(
        pd.DataFrame({
            'GID_0': ['MCK'],
            'COUNTRY': ['Mockland'],
            'GID_1': ['GID1'],
            'NAME_1': ['NAME1'],
            'GID_2': ['GID2'],
            'NAME_2': ['NAME2'],
        }),
        geometry=gpd.points_from_xy([106.0], [10.0]),
        crs='EPSG:4326'
    )

    # Mock CSV imports
    mock_pd_read.side_effect = [
        # Mock RWI data
        pd.DataFrame({
            'latitude': [10.0],
            'longitude': [106.0],
            'rwi': [0.5]
        }),
        # Mock population data
        pd.DataFrame({
            'latitude': [10.0],
            'longitude': [106.0],
            'quadkey': ['1234'],
            'pop_2023': [1000]
        })
    ]

    df, filename = process_gadm_popdensity_rwi('VNM', '2023', '2', plots=False)

    fn = 'VNM_population-weighted_relative-wealth-index_2023_2025-03-05.csv'
    assert filename == fn
    assert isinstance(df, pd.DataFrame)
    assert 'ISO3' in df.columns
    assert 'value' in df.columns
    assert not df.empty
