"""
Standardised precipitation index (SPI)
"""

import re
import logging

import xarray as xr
import pandas as pd
import scipy.stats

from geoglue.country import Country
from geoglue.resample import resample
from geoglue.util import set_lonlat_attrs
from geoglue.zonal_stats import DatasetZonalStatistics

from ...paths import get_path
from ...util import iso3_admin_unpack
from ...metrics import register_process, find_metric

from .util import fit_gamma_distribution, precipitation_weekly_dataset
from . import get_dataset_pool, get_population


@register_process("era5.spi.gamma")
def gamma_spi(iso3: str, date: str, window: int = 6) -> xr.Dataset:
    """Calculates gamma parameter for SPI for a date range"""
    try:
        ystart, yend = date.split("-")
        ystart = int(ystart)
        yend = int(yend)
    except ValueError:
        raise ValueError(
            "For era5.spi.gamma, specify date as a year range, e.g. 2000-2020"
        )
        raise
    if ystart >= yend:
        raise ValueError("For era5.spi.gamma, year end must be greater than year start")
    if yend - ystart < 20:
        raise ValueError(
            "For era5.spi.gamma, historical dataset must span at least 20 years"
        )
    ref = precipitation_weekly_dataset(iso3, ystart, yend, window - 1)
    tdim = [d for d in list(ref.coords) if d.endswith("time")]
    if len(tdim) > 1:
        raise ValueError(
            f"More than one time dimension detected in reference dataset {tdim=}"
        )
    if len(tdim) == 0:
        raise ValueError("No time dimension detected in reference dataset")
    ds = fit_gamma_distribution(ref.tp, window=window, dimension=tdim[0])
    ds.attrs["DART_history"] = f"gamma_spi({iso3!r}, {ystart=}, {yend=}, {window=})"
    ds.attrs["ISO3"] = iso3
    ds.attrs["metric"] = "era5.spi.gamma"
    return ds


@register_process("era5.spi")
def process_spi(iso3: str, date: str) -> pd.DataFrame:
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)
    pool = get_dataset_pool(iso3)

    gamma_params: xr.Dataset = find_metric("era5.spi.gamma", iso3)
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])
    logging.info("using era5.spi.gamma window=%d", window)
    ds = pool.weekly_reduce(year, "accum", window=window - 1)
    ds_ma = (
        ds.rolling(valid_time=window, center=False)
        .mean(dim="valid_time")
        .dropna(dim="valid_time")
    )

    def gamma_func(data, a, scale):
        return scipy.stats.gamma.cdf(data, a=a, scale=scale)

    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)

    # standarized precipitation index (inverse of CDF)
    def norminv(data):
        return scipy.stats.norm.ppf(data, loc=0, scale=1)

    norm_spi = xr.apply_ufunc(norminv, gamma)
    spi = norm_spi.rename({"tp": "spi"}).drop_vars(["e", "ssrd"])
    set_lonlat_attrs(spi)

    # resample to weights
    spi_path = get_path(
        "scratch", iso3, "era5", f"{iso3}-{year}-era5.spi.weekly_sum.nc"
    )
    spi.to_netcdf(spi_path)

    spi_resampled_path = resample(
        "remapdis",
        spi_path,
        get_population(iso3, year),
        skip_exists=False,
    )
    resampled_ds = xr.open_dataset(spi_resampled_path)

    ds = DatasetZonalStatistics(
        resampled_ds,
        Country(iso3).admin(admin),
        weights=get_population(iso3, year),
    )
    df = ds.zonal_stats(
        "spi",
        operation="area_weighted_sum",
        const_cols={"ISO3": iso3, "metric": "era5.spi.weekly_sum", "unit": "unitless"},
    )
    df.attrs["admin"] = admin
    return df
