"""
Standardised precipitation evapotranspiration index (SPEI)
"""

import re
import logging
import xarray as xr
import pandas as pd

from geoglue.country import Country
from geoglue.resample import resampled_dataset
from geoglue.util import find_unique_time_coord, set_lonlat_attrs
from geoglue.zonal_stats import DatasetZonalStatistics

from ...paths import get_path
from ...util import iso3_admin_unpack
from ...metrics import register_process, find_metric

from .util import (
    fit_gamma_distribution,
    gamma_func,
    norminv,
)
from . import get_population

from .util import (
    balance_weekly_dataarray,
    parse_year_range,
    assert_data_available_for_weekly_reduce,
)


@register_process("era5.spei.gamma")
def gamma_spei(
    iso3: str,
    date: str,
    window: int = 6,
) -> xr.Dataset:
    """Calculates gamma parameter for SPEI for a date range

    Parameters
    ----------
    iso3
        Country ISO3 code
    date
        Specify year range here for which to calculate gamma distribution
        e.g. 2000-2020
    window
        Length of the time window used to measure SPEI in weeks (default=6 weeks)
    """
    ystart, yend = parse_year_range(date, warn_duration_less_than_years=15)
    assert_data_available_for_weekly_reduce(iso3, ystart, yend)
    balance_hist = balance_weekly_dataarray(iso3, ystart, yend)
    tdim = find_unique_time_coord(balance_hist)
    ds = fit_gamma_distribution(balance_hist, window=window, dimension=tdim)
    ds.attrs["DART_history"] = f"gamma_spei({iso3!r}, {ystart=}, {yend=}, {window=})"
    ds.attrs["ISO3"] = iso3
    ds.attrs["metric"] = "era5.spei.gamma"
    return ds


@register_process("era5.spei")
def process_spei(iso3: str, date: str) -> pd.DataFrame:
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)

    gamma_params: xr.Dataset = find_metric("era5.spei.gamma", iso3)
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])
    logging.info("Using era5.spei.gamma window=%d", window)

    ds = balance_weekly_dataarray(iso3, year, year, window=window)
    ds_ma = (
        ds.rolling(valid_time=window, center=False)
        .mean(dim="valid_time")
        .dropna(dim="valid_time")
    )
    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)
    norm_spei = xr.apply_ufunc(norminv, gamma)
    spei = xr.Dataset({"spei": norm_spei})
    set_lonlat_attrs(spei)

    # resample to weights
    spei_path = get_path(
        "scratch", iso3, "era5", f"{iso3}-{year}-era5.spei.weekly_sum.nc"
    )
    spei.to_netcdf(spei_path)

    with resampled_dataset(
        "remapdis", spei_path, get_population(iso3, year)
    ) as resampled_ds:
        ds = DatasetZonalStatistics(
            resampled_ds,
            Country(iso3).admin(admin),
            weights=get_population(iso3, year),
        )
        df = ds.zonal_stats(
            "spei",
            operation="area_weighted_sum",
            const_cols={
                "ISO3": iso3,
                "metric": "era5.spei.weekly_sum",
                "unit": "unitless",
            },
        )
        df.attrs["admin"] = admin
        return df
