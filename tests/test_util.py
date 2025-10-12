"""Tests for utility functions."""

from pathlib import Path

from geoglue.region import BaseCountry
from geoglue.types import Bbox
import pytest
import requests_mock
import pandas as pd

from dart_pipeline.util import (
    detect_region_col,
    download_file,
    determine_netcdf_filename,
    days_in_year,
    get_country_name,
    use_range,
)

VNM = BaseCountry("VNM", "https://gadm.org", Bbox(102, 8, 110, 24), "VNM")


def test_region_col():
    df = pd.DataFrame(data=[], columns=["shapeID"])  # type: ignore
    assert detect_region_col(df) == "shapeID"
    df = pd.DataFrame(data=[], columns=["GID_1", "GID_2"])  # type: ignore
    assert detect_region_col(df) == "GID_2"


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"region": VNM}, "VNM-era5.a_b.nc"),
        ({"region": VNM, "date": "2020-2023"}, "VNM-2020-2023-era5.a_b.nc"),
        (
            {"region": VNM, "date": "2020", "param1": "hello", "param2": "there"},
            "VNM-2020-era5.a_b.hello.there.nc",
        ),
    ],
)
def test_determine_netcdf_filename(kwargs, expected):
    assert determine_netcdf_filename("era5.a_b", **kwargs) == expected


def test_download_file():
    url = "http://example.com/a.csv"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "delete_this.csv"
    with requests_mock.Mocker() as m:
        m.get("http://example.com/a.csv", text="colA,colB\n1,2")
        download_file(url, path)
        assert path.read_text() == "colA,colB\n1,2"
        path.unlink()


def test_download_file_unzip():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path)
        # download_file should unpack zip files by default
        assert (source_path / "hello.txt").read_text() == "Hello there\n"
        assert (source_path / "world.txt").read_text() == "Hello world\n"
        path.unlink()
        (source_path / "hello.txt").unlink()
        (source_path / "world.txt").unlink()


def test_download_file_without_unzip():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path, unpack=False)
        assert not (source_path / "hello.txt").exists()
        assert path.exists()
        path.unlink()


def test_download_file_unzip_create_folder():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path, unpack=True, unpack_create_folder=True)
        assert (source_path / "file" / "hello.txt").read_text() == "Hello there\n"
        assert (source_path / "file" / "world.txt").read_text() == "Hello world\n"
        (source_path / "file" / "hello.txt").unlink()
        (source_path / "file" / "world.txt").unlink()


@pytest.mark.parametrize("year,days", [(2024, 366), (2023, 365)])
def test_days_in_year(year, days):
    assert days_in_year(year) == days


@pytest.mark.parametrize("iso3,name", [("BOL", "Bolivia"), ("IND", "India")])
def test_get_country_name(iso3, name):
    assert get_country_name(iso3) == name


def test_use_range():
    with pytest.raises(ValueError):
        use_range(20, 5, 15, "something beyond range")


@pytest.fixture
def new_dataframe():
    """Fixture to create a sample data frame."""
    return pd.DataFrame(
        {
            "iso3": ["VNM"],
            "admin_level_0": ["Vietnam"],
            "admin_level_1": ["An Giang"],
            "admin_level_2": ["An Phú"],
            "admin_level_3": ["Khánh An"],
            "year": [""],
            "month": [""],
            "day": [""],
            "week": [""],
            "metric": ["Example"],
            "value": [0.998],
            "unit": [""],
            "resolution": [""],
            "creation_date": [""],
        }
    )


@pytest.fixture
def old_dataframe():
    """Fixture to create a mock existing data frame."""
    return pd.DataFrame(
        {
            "iso3": ["VNM"],
            "admin_level_0": ["Vietnam"],
            "admin_level_1": ["An Giang"],
            "admin_level_2": ["An Phú"],
            "admin_level_3": ["Khánh An"],
            "year": [""],
            "month": [""],
            "day": [""],
            "week": [""],
            "metric": ["Example"],
            "value": [0.002],
            "unit": [""],
            "resolution": [""],
            "creation_date": [""],
        }
    )
