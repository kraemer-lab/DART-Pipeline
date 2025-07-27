from dart_pipeline.metrics.era5.core_weekly import get_cfattrs

import pytest

hb_bc = {
    "long_name": "Weekly hydrological balance (bias corrected)",
    "units": "m",
    "cell_methods": "time: sum (interval: 7 days)",
}
tp = {
    "long_name": "Weekly total precipitation",
    "units": "m",
    "cell_methods": "time: sum (interval: 7 days)",
    "valid_min": 0,
}
mx2t24 = {
    "long_name": "Weekly mean of daily maximum 2 meters air temperature",
    "units": "K",
    "cell_methods": "time: maximum within days (interval: 1 day) time: mean over days (interval: 7 days)",
    "valid_min": 175,
    "valid_max": 335,
    "standard_name": "air_temperature",
}
mnq24 = {
    "long_name": "Weekly mean of daily minimum specific humidity",
    "units": "g kg-1",
    "cell_methods": "time: minimum within days (interval: 1 day) time: mean over days (interval: 7 days)",
    "valid_min": 0,
    "standard_name": "specific_humidity",
}
r = {
    "long_name": "Weekly relative humidity",
    "units": "percent",
    "cell_methods": "time: mean (interval: 7 days)",
    "valid_min": 0,
    "valid_max": 100,
    "standard_name": "relative_humidity",
}


@pytest.mark.parametrize(
    "var,expected",
    [("hb_bc", hb_bc), ("tp", tp), ("mx2t24", mx2t24), ("mnq24", mnq24), ("r", r)],
)
def test_get_cfattrs(var, expected):
    assert get_cfattrs(var) == expected
