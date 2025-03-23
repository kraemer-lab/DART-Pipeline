"""Tests for functions in population-weighted/relative-wealth-index.py."""

from unittest import mock
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely.geometry
import requests_mock

from dart_pipeline.types import URLCollection
from dart_pipeline.metrics.meta_relative_wealth_index import (
    get_admin_region,
    process_gadm_rwi,
    process_rwi_point_estimate,
    process_gadm_popdensity_rwi,
    fetch_relative_wealth_index,
    meta_pop_density_data,
)


def test_meta_pop_density_data():
    web_snapshot = Path(
        "tests/webarchive/vietnam-high-resolution-population-density-maps-demographic-estimates.html"
    )
    url = "https://data.humdata.org/dataset/vietnam-high-resolution-population-density-maps-demographic-estimates"
    dataset = "/dataset/191b04c5-3dc7-4c2a-8e00-9c0bdfdfbf9d/resource"
    with requests_mock.Mocker() as m:
        m.get(
            url,
            text=web_snapshot.read_text(),
        )
        assert meta_pop_density_data("VNM") == URLCollection(
            "https://data.humdata.org",
            files=[
                f"{dataset}/b60dab07-0a27-47f3-894d-02e9fbee6473/download/vnm_children_under_five_2020_csv.zip",
                f"{dataset}/a2ca47a3-b7d7-4a39-a605-161d4790c6f9/download/vnm_children_under_five_2020_geotiff.zip",
                f"{dataset}/77edfcc3-d037-4233-9b16-7ebb684b4752/download/vnm_elderly_60_plus_2020_csv.zip",
                f"{dataset}/61b6a396-ffcc-4710-a453-174914822164/download/vnm_elderly_60_plus_2020_geotiff.zip",
                f"{dataset}/e0d42fbb-2436-4f3e-afd1-83d1ac6314b2/download/vnm_men_2020_csv.zip",
                f"{dataset}/76990cd9-a2dc-44e6-809c-492698b090c0/download/vnm_men_2020_geotiff.zip",
                f"{dataset}/0bd525fa-3f63-447b-9afa-3d5075657ce4/download/vnm_women_2020_csv.zip",
                f"{dataset}/5bd99468-ea44-4a50-b64b-54343b182842/download/vnm_women_2020_geotiff.zip",
                f"{dataset}/5727eb4e-ce2a-4250-a9a0-2603d889ff02/download/vnm_women_of_reproductive_age_15_49_2020_csv.zip",
                f"{dataset}/855e27d7-d191-440d-936a-ecedfded238d/download/vnm_women_of_reproductive_age_15_49_2020_geotiff.zip",
                f"{dataset}/aa6dd9dd-eecd-4f3d-b6b2-fa0f0b5faa70/download/vnm_youth_15_24_2020_csv.zip",
                f"{dataset}/e36dce9f-ea1d-4933-8a37-dc5838df11f0/download/vnm_youth_15_24_2020_geotiff.zip",
                f"{dataset}/fade8620-0935-4d26-b0c6-15515dd4bf8b/download/vnm_general_2020_geotiff.zip",
                f"{dataset}/0fbf4055-7091-4041-a7ea-25f057debd7c/download/vnm_general_2020_csv.zip",
            ],
            relative_path="VNM",
        )


def test_relative_wealth_index():
    web_snapshot = Path("tests/webarchive/relative-wealth-index.html")
    base_url = "https://data.humdata.org/dataset/relative-wealth-index"
    with requests_mock.Mocker() as m:
        m.get(
            base_url,
            text=web_snapshot.read_text(),
        )
        assert fetch_relative_wealth_index("VNM") == URLCollection(
            "https://data.humdata.org",
            [
                "/dataset/76f2a2ea-ba50-40f5-b79c-db95d668b843/resource/06d29bc0-5a4c-4be0-be1a-c546a9be540c/download/vnm_relative_wealth_index.csv"
            ],
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
        # mock.patch(
        #     "pandas.DataFrame.parallel_apply",
        #     side_effect=lambda func, axis: rwi.apply(func, axis=axis),
        # ),
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
