"""
Standardised precipitation index (SPI)
"""

import re
import logging

import xarray as xr

from geoglue.region import gadm
from geoglue.resample import resampled_dataset
from geoglue.util import set_lonlat_attrs

from ...paths import get_path
from ...util import iso3_admin_unpack
from ...metrics import register_process, get_gamma_params, zonal_stats_xarray
from ...metrics.worldpop import get_worldpop

from .util import (
    fit_gamma_distribution,
    parse_year_range,
    precipitation_weekly_dataset,
    corrected_precipitation_weekly_dataset,
    assert_data_available_for_weekly_reduce,
    gamma_func,
    norminv,
)
from . import get_dataset_pool

logger = logging.getLogger(__name__)

# 99% of values should be in this range
# This is to avoid -inf propagating as NaN in zonal statistics
# This clip is used by other Python packages such as climate_indices:
# https://github.com/monocongo/climate_indices/blob/master/src/climate_indices/indices.py
MIN_SPI = -3.09
MAX_SPI = 3.09


def gamma_spi(
    iso3: str, date: str, window: int = 6, bias_correct: bool = False
) -> xr.Dataset:
    """Calculates gamma parameter for SPI for a date range

    Parameters
    ----------
    iso3 : str
        Country ISO3 code
    date : str
        Specify year range here for which to calculate gamma distribution
        e.g. 2000-2020
    window : int
        Length of the time window used to measure SPI in weeks (default=6 weeks)
    bias_correct : bool
        Whether to use bias corrected total precipitation (default=False)

    Returns
    -------
    xr.Dataset
        Dataset with ``alpha`` and ``beta`` gamma parameters
    """
    ystart, yend = parse_year_range(date, warn_duration_less_than_years=15)
    assert_data_available_for_weekly_reduce(iso3, ystart, yend)
    precipitation_weekly_func = (
        corrected_precipitation_weekly_dataset
        if bias_correct
        else precipitation_weekly_dataset
    )
    ref = precipitation_weekly_func(iso3, ystart, yend, window=window)
    tdim = [d for d in list(ref.coords) if d.endswith("time")]
    if len(tdim) > 1:
        raise ValueError(
            f"More than one time dimension detected in reference dataset {tdim=}"
        )
    if len(tdim) == 0:
        raise ValueError("No time dimension detected in reference dataset")
    tp_col = "tp_bc" if bias_correct else "tp"
    ds = fit_gamma_distribution(ref[tp_col], window=window, dimension=tdim[0])
    ds.attrs["DART_history"] = (
        f"gamma_spi({iso3!r}, {ystart=}, {yend=}, {window=}, {bias_correct=})"
    )
    ds.attrs["ISO3"] = iso3
    ds.attrs["metric"] = (
        "era5.spi_corrected.gamma" if bias_correct else "era5.spi.gamma"
    )
    return ds


@register_process("era5.spi.gamma", multiple_years=True)
def gamma_spi_uncorrected(iso3: str, date: str, window: int = 6) -> xr.Dataset:
    "Calculates gamma parameter for SPI for a date range"
    return gamma_spi(iso3, date, window, bias_correct=False)


@register_process("era5.spi_corrected.gamma", multiple_years=True)
def gamma_spi_corrected(iso3: str, date: str, window: int = 6) -> xr.Dataset:
    "Calculates gamma parameter for SPI with corrected precipitation for a date range"
    return gamma_spi(iso3, date, window, bias_correct=True)


@register_process("era5.spi")
def process_spi(iso3: str, date: str) -> xr.DataArray:
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)
    pool = get_dataset_pool(iso3)

    gamma_params = get_gamma_params(iso3, "spi")
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])
    logger.info("Using era5.spi.gamma window=%d", window)
    ds = pool.weekly_reduce(year, "accum", window=window - 1)
    ds_ma = (
        ds.rolling(valid_time=window, center=False)
        .mean(dim="valid_time")
        .dropna(dim="valid_time")
    )

    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)
    norm_spi = xr.apply_ufunc(norminv, gamma)
    spi = norm_spi.rename({"tp": "spi"}).drop_vars(["e", "ssrd"])
    spi["spi"] = spi.spi.clip(MIN_SPI, MAX_SPI)
    set_lonlat_attrs(spi)

    # resample to weights
    spi_path = get_path(
        "scratch", iso3, "era5", f"{iso3}-{year}-era5.spi.weekly_sum.nc"
    )
    spi.to_netcdf(spi_path)

    population = get_worldpop(iso3, year)
    with resampled_dataset("remapdis", spi_path, population) as resampled_ds:
        return zonal_stats_xarray(
            "era5.spi.weekly_sum",
            resampled_ds.spi,
            gadm(iso3, admin),
            operation="area_weighted_sum",
            weights=population,
        )


@register_process("era5.spi_corrected")
def process_spi_corrected(iso3: str, date: str) -> xr.DataArray:
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)

    gamma_params = get_gamma_params(iso3, "spi_corrected")
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])

    logger.info("Using era5.spi_corrected.gamma window=%d", window)

    ds = corrected_precipitation_weekly_dataset(iso3, year, year, window=window)
    ds_ma = (
        ds.rolling(valid_time=window, center=False)
        .mean(dim="valid_time")
        .dropna(dim="valid_time")
    )

    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)
    norm_spi_corrected = xr.apply_ufunc(norminv, gamma)
    spi_corrected = norm_spi_corrected.rename({"tp_bc": "spi_bc"})
    spi_corrected["spi_bc"] = spi_corrected.spi_bc.clip(MIN_SPI, MAX_SPI)
    set_lonlat_attrs(spi_corrected)

    # resample to weights
    spi_corrected_path = get_path(
        "scratch", iso3, "era5", f"{iso3}-{year}-era5.spi_corrected.weekly_sum.nc"
    )
    spi_corrected.to_netcdf(spi_corrected_path)

    population = get_worldpop(iso3, year)
    with resampled_dataset("remapdis", spi_corrected_path, population) as resampled_ds:
        return zonal_stats_xarray(
            "era5.spi_corrected.weekly_sum",
            resampled_ds.spi_bc,
            gadm(iso3, admin),
            operation="area_weighted_sum",
            weights=population,
        )
