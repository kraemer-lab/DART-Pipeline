import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from dart_pipeline.util import source_path
from dart_pipeline.metrics.era5.fetch_era5 import (
    era5_variables,
    era5_request,
    era5_extract_hourly_data,
    era5_fetch_hourly,
    ERA5HourlyPath,
)

# fmt: off
MONTHS =  ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
TIMES = [
    "00:00", "01:00", "02:00",
    "03:00", "04:00", "05:00",
    "06:00", "07:00", "08:00",
    "09:00", "10:00", "11:00",
    "12:00", "13:00", "14:00",
    "15:00", "16:00", "17:00",
    "18:00", "19:00", "20:00",
    "21:00", "22:00", "23:00"
]
DAYS =  [
    "01", "02", "03", "04", "05", "06", "07",
    "08", "09", "10", "11", "12", "13", "14",
    "15", "16", "17", "18", "19", "20", "21",
    "22", "23", "24", "25", "26", "27", "28",
    "29", "30", "31"
]
# fmt: on


@pytest.fixture(scope="module")
def variables():
    return era5_variables()


@pytest.fixture(scope="module")
def cdsapi_request(variables):
    return {
        "product_type": ["reanalysis"],
        "variable": variables,
        "year": ["2020"],
        "month": MONTHS,
        "day": DAYS,
        "time": TIMES,
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": [2, 103, 1, 105],
    }


def test_era5_variables(variables):
    assert variables == [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "2m_dewpoint_temperature",
        "2m_temperature",
        "evaporation",
        "surface_pressure",
        "surface_solar_radiation_downwards",
        "total_precipitation",
    ]


@pytest.mark.parametrize(
    "iso3,extents", [("VNM", [24, 102, 8, 110]), ("SGP", [2, 103, 1, 105])]
)
def test_era5_request(iso3, extents, variables):
    request = era5_request(iso3, 2020)
    assert request == {
        "product_type": ["reanalysis"],
        "variable": variables,
        "year": ["2020"],
        "month": MONTHS,
        "day": DAYS,
        "time": TIMES,
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": extents,
    }


def test_era5_extract_hourly_data():
    with tempfile.TemporaryDirectory() as folder:
        assert era5_extract_hourly_data(
            Path("tests/data/SGP-2020-era5.zip"), Path(folder)
        ) == ERA5HourlyPath(
            instant=Path(folder) / "SGP-2020-era5.instant.nc",
            accum=Path(folder) / "SGP-2020-era5.accum.nc",
        )


@patch("dart_pipeline.metrics.era5.fetch_era5.source_path")
@patch("cdsapi.Client", autospec=True)
def test_era5_fetch_hourly_file_exists(mock_client, mock_source_path):
    test_data = Path("tests/data")
    mock_source_path.return_value = test_data
    assert era5_fetch_hourly("SGP", 2020) == ERA5HourlyPath(
        instant=test_data / "SGP-2020-era5.instant.nc",
        accum=test_data / "SGP-2020-era5.accum.nc",
    )
    # file already exists, so no need to call cdsapi.Client().retrieve
    mock_client().retrieve.assert_not_called()


@patch("cdsapi.Client", autospec=True)
def test_era5_fetch_hourly(mock_client, variables):
    era5_fetch_hourly("SGP", 2020)
    mock_client().retrieve.assert_called_once_with(
        "reanalysis-era5-single-levels",
        {
            "product_type": ["reanalysis"],
            "variable": variables,
            "year": ["2020"],
            "month": MONTHS,
            "day": DAYS,
            "time": TIMES,
            "data_format": "netcdf",
            "download_format": "unarchived",
            "area": [2, 103, 1, 105],
        },
        source_path("meteorological/era5-reanalysis", "SGP-2020-era5.zip"),
    )
