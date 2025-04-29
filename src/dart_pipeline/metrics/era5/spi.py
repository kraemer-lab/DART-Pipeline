"""
Standardised precipitation index (SPI)
"""

import re
import logging

import xarray as xr
import pandas as pd

from geoglue.country import Country
from geoglue.resample import resampled_dataset
from geoglue.util import set_lonlat_attrs
from geoglue.zonal_stats import DatasetZonalStatistics

from ...paths import get_path
from ...util import iso3_admin_unpack
from ...metrics import register_process, find_metric

from .util import (
    fit_gamma_distribution,
    parse_year_range,
    precipitation_weekly_dataset,
    assert_data_available_for_weekly_reduce,
    gamma_func,
    norminv,
)
from . import get_dataset_pool, get_population


@register_process("era5.spi.gamma")
def gamma_spi(iso3: str, date: str, window: int = 6) -> xr.Dataset:
    """Calculates gamma parameter for SPI for a date range

    Parameters
    ----------
    iso3
        Country ISO3 code
    date
        Specify year range here for which to calculate gamma distribution
        e.g. 2000-2020
    window
        Length of the time window used to measure SPI in weeks (default=6 weeks)
    """
    ystart, yend = parse_year_range(date, warn_duration_less_than_years=15)
    assert_data_available_for_weekly_reduce(iso3, ystart, yend)
    ref = precipitation_weekly_dataset(iso3, ystart, yend)
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

    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)
    norm_spi = xr.apply_ufunc(norminv, gamma)
    spi = norm_spi.rename({"tp": "spi"}).drop_vars(["e", "ssrd"])
    set_lonlat_attrs(spi)

    # resample to weights
    spi_path = get_path(
        "scratch", iso3, "era5", f"{iso3}-{year}-era5.spi.weekly_sum.nc"
    )
    spi.to_netcdf(spi_path)

    with resampled_dataset(
        "remapdis", spi_path, get_population(iso3, year)
    ) as resampled_ds:
        ds = DatasetZonalStatistics(
            resampled_ds,
            Country(iso3).admin(admin),
            weights=get_population(iso3, year),
        )
        df = ds.zonal_stats(
            "spi",
            operation="area_weighted_sum",
            const_cols={
                "ISO3": iso3,
                "metric": "era5.spi.weekly_sum",
                "unit": "unitless",
            },
        )
        df.attrs["admin"] = admin
        return df
