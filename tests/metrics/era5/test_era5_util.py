"""ERA5 metrics utility tests"""

from datetime import date
from pathlib import Path

import pytest
import numpy as np
import numpy.testing as npt

from dart_pipeline.metrics.era5.util import (
    temperature_daily_dataset,
    precipitation_weekly_dataset,
    balance_weekly_dataarray,
    get_date_range_for_years,
)

params = {"iso3": "SGP", "ystart": 2020, "yend": 2020, "data_path": Path("tests/data")}


def test_temperature_daily_dataset():
    ds = temperature_daily_dataset(**params)
    assert {"t2m", "mn2t24", "mx2t24"} < set(ds.variables)
    assert ds.valid_time.min() == np.datetime64("2020-01-01")
    assert ds.valid_time.max() == np.datetime64("2020-12-31")
    assert (ds.mn2t24 < ds.t2m).all()
    assert (ds.mx2t24 > ds.t2m).all()


def test_precipitation_weekly_dataset():
    ds = precipitation_weekly_dataset(**params)
    print(ds)
    # Check that both extrema are Mondays
    assert ds.valid_time.min() == np.datetime64("2020-01-06")
    assert ds.valid_time.max() == np.datetime64("2020-12-28")
    # check that dataset is weekly
    assert ds.valid_time[1] == np.datetime64("2020-01-13")


def test_balance_weekly_dataarray():
    da = balance_weekly_dataarray(**params)
    # Check balance extrema
    npt.assert_approx_equal(da.min().item(), 0.001613094)
    npt.assert_approx_equal(da.max().item(), 0.22384445)


@pytest.mark.parametrize(
    "window,expected", [(0, date(2019, 1, 1)), (40, date(2018, 11, 22))]
)
def test_get_date_range_for_years_non_aligned_weeks(window, expected):
    assert get_date_range_for_years(2019, 2020, window=window, align_weeks=False) == (
        expected,
        date(2020, 12, 31),
    )


@pytest.mark.parametrize(
    "window,expected", [(0, date(2019, 1, 7)), (42, date(2018, 11, 26))]
)
def test_get_date_range_for_years_aligned_weeks(window, expected):
    assert get_date_range_for_years(2019, 2020, window=window, align_weeks=True) == (
        expected,
        date(2021, 1, 3),
    )
