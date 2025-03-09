"Derived metrics for era5"

import xarray as xr
import metpy.calc as mp
from metpy.units import units


def specific_humidity(ds: xr.Dataset) -> xr.DataArray:
    return (
        mp.specific_humidity_from_dewpoint(ds.sp * units.pascal, ds.d2m * units.kelvin)
        * 1000
        * units("g/kg")
    )


def relative_humidity(ds: xr.Dataset) -> xr.DataArray:
    return (
        mp.relative_humidity_from_dewpoint(ds.t2m * units.kelvin, ds.d2m * units.kelvin)
        * 100
        * units.percent
    )


def wind_speed(ds: xr.Dataset) -> xr.DataArray:
    return mp.wind_speed(
        ds.u10 * units.meters / units.seconds, ds.v10 * units.meters / units.seconds
    )


def hydrological_balance(ds: xr.Dataset, bias_corrected=False) -> xr.DataArray:
    tp_col = "tp_corrected" if bias_corrected else "tp"
    return ds[tp_col] + ds.ev
