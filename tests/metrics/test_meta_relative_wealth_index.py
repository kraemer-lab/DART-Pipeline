"""Tests for functions in population-weighted/relative-wealth-index.py."""

import os
from unittest import mock
from pathlib import Path

import xarray as xr
import shapely.geometry
import requests_mock
from geoglue.region import Country
from geoglue.types import Bbox

from dart_pipeline.types import URLCollection
from dart_pipeline.metrics.meta_relative_wealth_index import (
    get_admin_region,
    fetch_relative_wealth_index,
    meta_pop_density_data,
    process_popdensity_rwi,
)

VNM = Country(
    "VNM",
    "https://gadm.org",
    Bbox(minx=102, miny=8, maxx=110, maxy=24),
    "VNM",
    "+07:00",
    {
        1: Path("data/VNM/geoboundaries/geoBoundaries-VNM-ADM1.shp"),
        2: Path("data/VNM/geoboundaries/geoBoundaries-VNM-ADM2.shp"),
    },
    "shapeID",
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
        assert meta_pop_density_data(VNM) == URLCollection(
            "https://data.humdata.org",
            files=[
                f"{dataset}/0fbf4055-7091-4041-a7ea-25f057debd7c/download/vnm_general_2020_csv.zip",
            ],
        )


def test_fetch_relative_wealth_index():
    web_snapshot = Path("tests/webarchive/relative-wealth-index.html")
    base_url = "https://data.humdata.org/dataset/relative-wealth-index"
    with requests_mock.Mocker() as m:
        m.get(
            base_url,
            text=web_snapshot.read_text(),
        )
        assert fetch_relative_wealth_index(VNM) == URLCollection(
            "https://data.humdata.org",
            [
                "/dataset/76f2a2ea-ba50-40f5-b79c-db95d668b843/resource/06d29bc0-5a4c-4be0-be1a-c546a9be540c/download/vnm_relative_wealth_index.csv"
            ],
            unpack=False,
        )


def test_get_admin_region():
    polygons = {
        "region_1": shapely.geometry.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
        "region_2": shapely.geometry.Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
    }
    assert get_admin_region(0.5, 0.5, polygons) == "region_1"
    assert get_admin_region(1.5, 1.5, polygons) == "region_2"
    assert get_admin_region(3, 3, polygons) == "null"


@mock.patch.dict(os.environ, {"DART_PIPELINE_DATA_HOME": "data"})
def test_process_gadm_popdensity_rwi():
    da = process_popdensity_rwi(VNM.admin(2))
    assert isinstance(da, xr.DataArray)
    assert "DART_region" in da.attrs
    assert da.attrs["units"] == "1"
    assert da.attrs["long_name"] == "Relative wealth index"
