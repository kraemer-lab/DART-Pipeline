"""
Standardised precipitation index (SPI)
"""

import re
import logging

from geoglue import AdministrativeLevel
import xarray as xr

from geoglue.region import ZonedBaseRegion
from geoglue.resample import resampled_dataset
from geoglue.util import set_lonlat_attrs

from ...paths import get_path
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
    region: ZonedBaseRegion, date: str, window: int = 6, bias_correct: bool = False
) -> xr.Dataset:
    """Calculates gamma parameter for SPI for a date range

    Parameters
    ----------
    region : ZonedBaseRegion
        Region over which to obtain SPI
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
    assert_data_available_for_weekly_reduce(region, ystart, yend)
    precipitation_weekly_func = (
        corrected_precipitation_weekly_dataset
        if bias_correct
        else precipitation_weekly_dataset
    )
    ref = precipitation_weekly_func(region, ystart, yend, window=window)
    tdim = [d for d in list(ref.coords) if str(d).endswith("time")]
    if len(tdim) > 1:
        raise ValueError(
            f"More than one time dimension detected in reference dataset {tdim=}"
        )
    if len(tdim) == 0:
        raise ValueError("No time dimension detected in reference dataset")
    tp_col = "tp_bc" if bias_correct else "tp"
    ds = fit_gamma_distribution(ref[tp_col], window=window, dimension=tdim[0])
    ds.attrs["DART_history"] = (
        f"gamma_spi({region.name!r}, {ystart=}, {yend=}, {window=}, {bias_correct=})"
    )
    ds.attrs["DART_region"] = str(region)
    ds.attrs["metric"] = (
        "era5.spi_corrected.gamma" if bias_correct else "era5.spi.gamma"
    )
    return ds


@register_process("era5.spi.gamma", multiple_years=True)
def gamma_spi_uncorrected(
    region: ZonedBaseRegion, date: str, window: int = 6
) -> xr.Dataset:
    "Calculates gamma parameter for SPI for a date range"
    return gamma_spi(region, date, window, bias_correct=False)


@register_process("era5.spi_corrected.gamma", multiple_years=True)
def gamma_spi_corrected(
    region: ZonedBaseRegion, date: str, window: int = 6
) -> xr.Dataset:
    "Calculates gamma parameter for SPI with corrected precipitation for a date range"
    return gamma_spi(region, date, window, bias_correct=True)


@register_process("era5.spi")
def process_spi(region: AdministrativeLevel, date: str) -> xr.DataArray:
    year = int(date)
    pool = get_dataset_pool(region)

    gamma_params = get_gamma_params(region, "spi")
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
        "scratch", region.name, "era5", f"{region.name}-{year}-era5.spi.weekly_sum.nc"
    )
    spi.to_netcdf(spi_path)

    population = get_worldpop(region, year)
    with resampled_dataset("remapdis", spi_path, population) as resampled_ds:
        return zonal_stats_xarray(
            "era5.spi.weekly_sum",
            resampled_ds.spi,
            region,
            operation="area_weighted_sum",
            weights=population,
        )


@register_process("era5.spi_corrected")
def process_spi_corrected(region: AdministrativeLevel, date: str) -> xr.DataArray:
    year = int(date)

    gamma_params = get_gamma_params(region, "spi_corrected")
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])

    logger.info("Using era5.spi_corrected.gamma window=%d", window)

    ds = corrected_precipitation_weekly_dataset(region, year, year, window=window)
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
        "scratch",
        region.name,
        "era5",
        f"{region.name}-{year}-era5.spi_corrected.weekly_sum.nc",
    )
    spi_corrected.to_netcdf(spi_corrected_path)

    population = get_worldpop(region, year)
    with resampled_dataset("remapdis", spi_corrected_path, population) as resampled_ds:
        return zonal_stats_xarray(
            "era5.spi_corrected.weekly_sum",
            resampled_ds.spi_bc,
            region,
            operation="area_weighted_sum",
            weights=population,
        )
