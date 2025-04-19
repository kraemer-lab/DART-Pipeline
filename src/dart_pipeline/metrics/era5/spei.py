"""
Standardised precipitation evapotranspiration index (SPEI)
"""

import xarray as xr
from geoglue.util import find_unique_time_coord

from ...metrics import register_process

from .util import fit_gamma_distribution, balance_weekly_dataarray


@register_process("era5.spei.gamma")
def gamma_spei(
    iso3: str,
    ystart: int,
    yend: int,
    window: int = 6,
) -> xr.Dataset:
    balance_hist = balance_weekly_dataarray(iso3, ystart, yend)
    tdim = find_unique_time_coord(balance_hist)
    ds = fit_gamma_distribution(balance_hist, window=window, dimension=tdim)
    ds.attrs["DART_history"] = f"gamma_spei({iso3!r}, {ystart=}, {yend=}, {window=})"
    ds.attrs["ISO3"] = iso3
    ds.attrs["metric"] = "era5.spei.gamma"
    return ds
