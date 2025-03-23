"""Tests for functions in population-weighted/relative-wealth-index.py."""

from unittest import mock

import geopandas as gpd
import pandas as pd
import shapely.geometry

from dart_pipeline.geospatial.relative_wealth_index import (
    get_admin_region,
    process_gadm_rwi,
)


def test_get_admin_region():
    polygons = {
        "region_1": shapely.geometry.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
        "region_2": shapely.geometry.Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
    }
    assert get_admin_region(0.5, 0.5, polygons) == "region_1"
    assert get_admin_region(1.5, 1.5, polygons) == "region_2"
    assert get_admin_region(3, 3, polygons) == "null"


@mock.patch("dart_pipeline.geospatial.relative_wealth_index.get_path")
@mock.patch("dart_pipeline.geospatial.relative_wealth_index.get_country_name")
@mock.patch("dart_pipeline.geospatial.relative_wealth_index.plot_gadm_macro_heatmap")
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
