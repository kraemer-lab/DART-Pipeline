"Derived metrics for era5"

import xarray as xr
import metpy.calc as mp
from metpy.units import units

DERIVED_METRICS_CALL_TABLE = {}


def derived_metric(name: str | None = None):
    def decorator(func):
        DERIVED_METRICS_CALL_TABLE[name or func.__name__] = func
        return func

    return decorator


@derived_metric()
def specific_humidity(ds: xr.Dataset) -> xr.DataArray:
    return (
        mp.specific_humidity_from_dewpoint(ds.sp * units.pascal, ds.d2m * units.kelvin)
        * 1000
        * units("g/kg")
    )


@derived_metric()
def relative_humidity(ds: xr.Dataset) -> xr.DataArray:
    return (
        mp.relative_humidity_from_dewpoint(ds.t2m * units.kelvin, ds.d2m * units.kelvin)
        * 100
        * units.percent
    )


@derived_metric()
def wind_speed(ds: xr.Dataset) -> xr.DataArray:
    return mp.wind_speed(
        ds.u10 * units.meters / units.seconds, ds.v10 * units.meters / units.seconds
    )


@derived_metric()
def hydrological_balance(ds: xr.Dataset, bias_corrected=False) -> xr.DataArray:
    tp_col = "tp_corrected" if bias_corrected else "tp"
    return ds[tp_col] + ds.e


def compute_derived_metric(metric: str, ds: xr.Dataset, **kwargs):
    DERIVED_METRICS_CALL_TABLE[metric](ds, **kwargs)
