import xarray as xr
import numpy as np


def collapse_step_to_month(da: xr.DataArray, time_coord: str = "time") -> xr.DataArray:
    """Given a DataArray with (time, step) variables

    For cdsapi seasonal forecast monthly data, data is presented as a (time,
    step) coordinate where time = YYYY-MM-01 (monthly timesteps), but step is
    presented as timesteps in days with type timedelta64[ns]. This results in a
    sparse representation as the step coordinate is all possible values e.g. 29,
    30, 31, 60, 61, 62, 91, 92, 93 while only one of them is selected at each
    timestep so for time=2024-01-01, step=31, 60, 91 is non-nan.

    This function converts (time, step) to a (time, month) dense representation
    with month going from 1..6. As the forecast data always includes a fixed
    number of steps, this dense operation is valid.
    """
    if not {time_coord, "latitude", "longitude", "step"} <= set(da.coords):
        raise ValueError(
            "Invalid DataArray passed, must have (time, step) coordinates and spatial (latitude, longitude) coordinates"
        )

    # TODO: calculate valid_time = time + step
    # TODO: check all values where da != NaN correspond to valid_time YYYY-MM-01

    da = da.transpose("time", "latitude", "longitude", "step")
    arr = da.values  # shape (T, Y, X, S)
    T, Y, X, S = arr.shape
    arr_flat = arr.reshape(T * Y * X, S)

    # collapse non-NaNs per spatial–time point
    dense_flat = np.vstack([row[~np.isnan(row)] for row in arr_flat])

    # infer number of valid steps (assume constant)
    m = dense_flat.shape[1]
    dense = dense_flat.reshape(T, Y, X, m)

    # build new DataArray with 'month' axis
    return xr.DataArray(
        dense,
        dims=("time", "latitude", "longitude", "month"),
        coords={
            time_coord: da[time_coord],
            "latitude": da["latitude"],
            "longitude": da["longitude"],
            "month": np.arange(1, m + 1),
        },
        name=f"{da.name}_dense" if da.name else None,
    )
