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
    MONTHS,
    DAYS,
    TIMES,
    ERA5HourlyPath,
)


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
