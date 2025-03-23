"""Tests for functions in population-weighted/relative-wealth-index.py."""

from unittest import mock

import geopandas as gpd
import pandas as pd
import shapely.geometry

from dart_pipeline.metrics.meta_relative_wealth_index import (
    get_admin_region,
    process_gadm_rwi,
    process_rwi_point_estimate,
    process_gadm_popdensity_rwi,
)


def test_get_admin_region():
    polygons = {
        "region_1": shapely.geometry.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
        "region_2": shapely.geometry.Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
    }
    assert get_admin_region(0.5, 0.5, polygons) == "region_1"
    assert get_admin_region(1.5, 1.5, polygons) == "region_2"
    assert get_admin_region(3, 3, polygons) == "null"


@mock.patch("dart_pipeline.metrics.meta_relative_wealth_index.get_path")
@mock.patch("dart_pipeline.metrics.meta_relative_wealth_index.get_country_name")
@mock.patch("dart_pipeline.metrics.meta_relative_wealth_index.plot_gadm_macro_heatmap")
def test_process_gadm_rwi(mock_plot, mock_get_country, mock_get_path):
    mock_get_path.return_value = "mock_rwi.csv"
    mock_get_country.return_value = "Mockland"

    # Mock GADM shapefile
    gdf = gpd.GeoDataFrame(
        {
            "GID_1": ["region_1", "region_2"],
            "geometry": [
                shapely.geometry.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
                shapely.geometry.Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
            ],
            "COUNTRY": ["Mockland", "Mockland"],
            "NAME_1": ["Region 1", "Region 2"],
        }
    ).set_crs("EPSG:4326")

    # Mock RWI data
    rwi = pd.DataFrame(
        {
            "latitude": [0.5, 1.5, 2.5],
            "longitude": [0.5, 1.5, 2.5],
            "rwi": [0.2, -0.3, 0.5],
        }
    )

    with (
        mock.patch("geopandas.read_file", return_value=gdf),
        mock.patch("pandas.read_csv", return_value=rwi),
        mock.patch(
            "pandas.DataFrame.parallel_apply",
            side_effect=lambda func, axis: rwi.apply(func, axis=axis),
        ),
    ):
        output_rwi = process_gadm_rwi("VNM", 1, plots=False)

    assert "iso3" in output_rwi.columns
    assert "value" in output_rwi.columns
    assert "unit" in output_rwi.columns
    assert output_rwi.iloc[0]["admin_level_1"] == "Region 1"
    assert output_rwi.iloc[0]["value"] == 0.2
    assert not mock_plot.called


@mock.patch("dart_pipeline.metrics.meta_relative_wealth_index.get_path")
@mock.patch("dart_pipeline.metrics.meta_relative_wealth_index.get_country_name")
def test_process_rwi(mock_get_country, mock_get_path):
    mock_get_path.return_value = "mock_rwi.csv"
    mock_get_country.return_value = "Mockland"

    # Mock RWI data
    rwi = pd.DataFrame(
        {
            "latitude": [0.5, 1.5, 2.5],
            "longitude": [0.5, 1.5, 2.5],
            "rwi": [0.2, -0.3, 0.5],
        }
    )

    with mock.patch("pandas.read_csv", return_value=rwi):
        output_value = process_rwi_point_estimate("VNM")

    assert output_value == 0.4 / 3


@mock.patch("geopandas.read_file")
@mock.patch("pandas.read_csv")
def test_process_gadm_popdensity_rwi(mock_pd_read, mock_gpd_read):
    # Mock GADM shapefile
    mock_gpd_read.return_value = gpd.GeoDataFrame(
        pd.DataFrame(
            {
                "GID_0": ["GID0"],
                "COUNTRY": ["Mockland"],
                "NAME_1": ["NAME1"],
                "NAME_2": ["NAME2"],
                "GID_2": ["GID2"],
            }
        ),
        geometry=gpd.points_from_xy([106.0], [10.0]),
        crs="EPSG:4326",
    )

    # Mock CSV imports
    mock_pd_read.side_effect = [
        # Mock RWI data
        pd.DataFrame({"latitude": [10.0], "longitude": [106.0], "rwi": [0.5]}),
        # Mock population data
        pd.DataFrame(
            {
                "latitude": [10.0],
                "longitude": [106.0],
                "quadkey": ["1234"],
                "pop_2023": [1000],
            }
        ),
    ]

    df = process_gadm_popdensity_rwi("VNM", "2023", "2", plots=False)

    assert isinstance(df, pd.DataFrame)
    assert "iso3" in df.columns
    assert "value" in df.columns
    assert not df.empty
