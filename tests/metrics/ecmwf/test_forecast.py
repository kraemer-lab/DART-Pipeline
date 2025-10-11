"Tests for dart_pipeline.metrics.ecmwf forecast code"

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import freezegun

from geoglue.region import BaseCountry
from geoglue.types import Bbox

from dart_pipeline.metrics.ecmwf import get_forecast_open_data

FRA = BaseCountry(
    "FRA", "https://gadm.org", Bbox(minx=-6, miny=41, maxx=10, maxy=52), "FRA"
)


@freezegun.freeze_time("2025-01-05")
@patch("dart_pipeline.metrics.ecmwf.forecast_path")
@patch("geoglue.region.gadm")
@patch("dart_pipeline.metrics.ecmwf.forecast_grib_to_netcdf")
@patch("ecmwf.opendata.Client")
@patch("dart_pipeline.paths.get_path")
def test_successful_forecast_download(
    mock_get_path,
    mock_client_class,
    mock_forecast_grib,
    mock_gadm,
    mock_forecast_path,
):
    # Setup test values
    date = "2025-01-03"
    instant_ds = MagicMock()
    accum_ds = MagicMock()

    mock_forecast_path.return_value.exists.return_value = False

    mock_gadm = MagicMock()
    mock_gadm.return_value = {
        "name": "FRA-1",
        "path": "/path/to/FRA.shp",
        "pk": "GID_1",
        "tz": "+01:00",
        "url": "https://gadm.org",
        "bounds": Bbox(maxy=50.0, miny=40.0, minx=-5.0, maxx=10.0),
    }

    mock_forecast_grib.return_value = (instant_ds, accum_ds)

    sources_path = Path("/mock/sources")
    mock_get_path.return_value = sources_path

    result = get_forecast_open_data(FRA, date)

    # Check result is a list of 2 files
    assert len(result) == 2
    assert result[0].name == "FRA-2025-01-03-ecmwf.forecast.instant.nc"
    assert result[1].name == "FRA-2025-01-03-ecmwf.forecast.accum.nc"

    # Check data saving was called
    instant_ds.to_netcdf.assert_called_once()
    accum_ds.to_netcdf.assert_called_once()

    # Check retrieval was triggered
    mock_client_class.return_value.retrieve.assert_called_once()


@pytest.mark.parametrize("invalid_hour", [-1, 5, 24])
def test_invalid_start_hour(invalid_hour):
    with pytest.raises(ValueError, match="start_hour must be one of"):
        get_forecast_open_data(FRA, "2025-01-05", start_hour=invalid_hour)


@pytest.mark.parametrize("invalid_step", [0, 5, 7, 10])
def test_invalid_step_hours(invalid_step):
    with pytest.raises(ValueError, match="must be a multiple of 6"):
        get_forecast_open_data(FRA, "2025-01-05", step_hours=invalid_step)


@freezegun.freeze_time("2025-01-05")
def test_future_date():
    future_date = "2025-01-06"
    with pytest.raises(ValueError, match="Can't fetch a forecast from the future"):
        get_forecast_open_data(FRA, future_date)


@freezegun.freeze_time("2025-01-05")
def test_too_old_date():
    old_date = "2024-12-31"  # 5 days in the past
    with pytest.raises(
        ValueError, match="Can't fetch a forecast more than 4 days in the past"
    ):
        get_forecast_open_data(FRA, old_date)
