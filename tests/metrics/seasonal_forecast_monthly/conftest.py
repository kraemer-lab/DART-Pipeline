"""Common fixtures for seasonal_forecast_monthly"""

import pytest
import pandas as pd
import numpy as np
import xarray as xr


@pytest.fixture(scope="session")
def sample_monthly_forecast():
    times = pd.date_range("2024-01-01", periods=4, freq="MS")  # first of each month
    # step coordinates (float, allow NaN)
    # fmt: off
    steps = np.array([
        29., 30., 31.,
        60., 61.,
        91., 92.,
        121., 122.,
        152., 153.,
        182., 183., 184.
    ])
    # fmt: on
    steps = steps * np.timedelta64(1, "D")
    # Make a data array where for each time only one step is "selected" (others NaN)

    # Here steps is an array of 14 floats, but at each timestep (start of each month)
    # only *one* of the steps is selected, and that will always correspond to an
    # integer lead_month 1..6. So out of the 4x14 array (time, step) exactly 4x6 will be filled
    # Goal is to get this down to a non-sparse matrix

    data_single = np.full((len(times), len(steps)), np.nan)
    filled_coordinates = [
        [2, 3, 5, 7, 9, 11],
        [0, 3, 5, 7, 9, 11],
        [2, 4, 6, 8, 10, 13],
        [1, 4, 5, 8, 10, 12],
    ]
    u = 1
    for i, z in enumerate(filled_coordinates):
        for k in z:
            data_single[i, k] = u
            u += 1
    da = xr.DataArray(data_single, coords={"time": times, "step": steps})
    spatial = xr.DataArray(
        np.array([[0.5, 1], [1.5, 3]]),
        dims=("latitude", "longitude"),
        coords={"latitude": np.array([-1, 1]), "longitude": np.array([-2, 2])},
    )
    return da * spatial  # broadcast to include spatial dims
