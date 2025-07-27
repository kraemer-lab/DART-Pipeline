import pytest
import numpy as np
import xarray as xr

from dart_pipeline.types import InvalidCounts
from dart_pipeline.metrics import (
    MetricInfo,
    get_cell_methods,
    subset_cfattrs,
    get_name_cfattrs,
    get_invalid_counts,
)


def test_get_invalid_counts():
    # Create a test DataArray
    da = xr.DataArray(
        data=[-10, 0, 5, 15, 25, np.nan, 10],
        dims="x",
        attrs={"valid_min": 0, "valid_max": 20},
    )

    result = get_invalid_counts(da)
    assert result == InvalidCounts(below_min=1, above_max=1, nan_count=1, total=7)


@pytest.mark.parametrize(
    "agg, dim_name, expected",
    [
        ("weekly_sum", "time", "time: sum (interval: 1 week)"),
        ("weekly_sum", "forecast", "forecast: sum (interval: 1 week)"),
        ("daily_mean", "time", "time: mean (interval: 1 day)"),
        ("daily_max", "valid_time", "valid_time: max (interval: 1 day)"),
    ],
)
def test_get_cell_methods_valid(agg, dim_name, expected):
    assert get_cell_methods(agg, dim_name) == expected


@pytest.mark.parametrize(
    "agg, expected_message",
    [
        (
            "weekly_mean",
            "Weekly aggregation for instantaneous variables is only done at the 'collate' step",
        ),
        ("hourly_sum", "Unsupported aggregation agg='hourly_sum'"),
    ],
)
def test_get_cell_methods_invalid(agg, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        get_cell_methods(agg)


def test_subset_cfattrs():
    info: MetricInfo = {
        "long_name": "Wind speed",
        "depends": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
        "valid_min": 0,
        "valid_max": 110,
        "standard_name": "wind_speed",
        "units": "m s-1",
        "part_of": "era5",
        "short_name": "ws",
    }
    assert subset_cfattrs(info) == {
        "long_name": "Wind speed",
        "valid_min": 0,
        "valid_max": 110,
        "standard_name": "wind_speed",
        "units": "m s-1",
    }


@pytest.mark.parametrize(
    "metric,exp_name,exp_cfattrs",
    [
        (
            "era5.2m_temperature.daily_max",
            "mx2t",
            {
                "long_name": "2 meters air temperature",
                "valid_min": 175,
                "valid_max": 335,
                "standard_name": "air_temperature",
                "cell_methods": "time: max (interval: 1 day)",
                "units": "K",
            },
        ),
        (
            "era5.total_precipitation.daily_sum",
            "tp",
            {
                "long_name": "Total precipitation",
                "valid_min": 0,
                "standard_name": "",
                "units": "m",
                "cell_methods": "time: sum (interval: 1 day)",
            },
        ),
        (
            "era5.total_precipitation.weekly_sum",
            "tp",
            {
                "long_name": "Total precipitation",
                "valid_min": 0,
                "standard_name": "",
                "units": "m",
                "cell_methods": "time: sum (interval: 1 week)",
            },
        ),
    ],
)
def test_get_metrics_temp(metric, exp_name, exp_cfattrs):
    name, cfattrs = get_name_cfattrs(metric)
    assert name == exp_name
    assert cfattrs == exp_cfattrs
