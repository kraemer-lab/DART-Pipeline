"""
Standardised precipitation evapotranspiration index (SPEI)
"""

import re
import logging
import xarray as xr

from geoglue.region import ZonedBaseRegion, AdministrativeLevel
from geoglue.resample import resampled_dataset
from geoglue.util import find_unique_time_coord, set_lonlat_attrs

from ...paths import get_path
from ...metrics import register_process, get_gamma_params, zonal_stats_xarray
from ...metrics.worldpop import get_worldpop

from .util import (
    fit_gamma_distribution,
    gamma_func,
    norminv,
)

from .util import (
    balance_weekly_dataarray,
    parse_year_range,
    assert_data_available_for_weekly_reduce,
)

logger = logging.getLogger(__name__)

# 99% of values should be in this range
# This is to avoid -inf propagating as NaN in zonal statistics
# This clip is used by other Python packages such as climate_indices:
# https://github.com/monocongo/climate_indices/blob/master/src/climate_indices/indices.py
MIN_SPEI = -3.09
MAX_SPEI = 3.09


def gamma_spei(
    region: ZonedBaseRegion,
    date: str,
    window: int = 6,
    bias_correct: bool = False,
) -> xr.Dataset:
    """Calculates gamma parameter for SPEI for a date range

    Parameters
    ----------
    region : Region
        Gamma parameters for SPEI
    date : str
        Specify year range here for which to calculate gamma distribution
        e.g. 2000-2020
    window : int
        Length of the time window used to measure SPEI in weeks (default=6 weeks)
    bias_correct : bool
        Whether to use bias corrected precipitation when determining gamma params
        (default=False)

    Returns
    -------
    xr.Dataset
        Dataset with ``alpha`` and ``beta`` gamma parameters
    """
    ystart, yend = parse_year_range(date, warn_duration_less_than_years=15)
    assert_data_available_for_weekly_reduce(region, ystart, yend)
    balance_hist = balance_weekly_dataarray(
        region, ystart, yend, bias_correct=bias_correct
    )
    tdim = find_unique_time_coord(balance_hist)
    ds = fit_gamma_distribution(balance_hist, window=window, dimension=tdim)
    ds.attrs["DART_history"] = (
        f"gamma_spei({region.name!r}, {ystart=}, {yend=}, {window=}, {bias_correct=})"
    )
    ds.attrs["DART_region"] = str(region)
    ds.attrs["metric"] = (
        "era5.spei_corrected.gamma" if bias_correct else "era5.spei.gamma"
    )
    return ds


def process_spei(
    region: AdministrativeLevel, date: str, bias_correct: bool = False
) -> xr.DataArray:
    """Determines SPEI for a particular year

    Parameters
    ----------
    region : Region
        Region for which to process SPEI
    date : str
        Year for which to determine SPEI
    bias_correct : bool
        Whether to use bias corrected precipitation when determining gamma params
        (default=False)

    Returns
    -------
    pd.DataFrame
        Zonal aggregated SPEI
    """
    year = int(date)
    index = "spei_corrected" if bias_correct else "spei"
    gamma_params = get_gamma_params(region, index)
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])
    logger.info("Using era5.spei.gamma window=%d", window)

    ds = balance_weekly_dataarray(
        region, year, year, window=window, bias_correct=bias_correct
    )
    ds_ma = (
        ds.rolling(valid_time=window, center=False)
        .mean(dim="valid_time")
        .dropna(dim="valid_time")
    )
    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)
    norm_spei = xr.apply_ufunc(norminv, gamma)
    spei_name = "spei_bc" if bias_correct else "spei"
    spei = xr.Dataset({spei_name: norm_spei.clip(MIN_SPEI, MAX_SPEI)})
    set_lonlat_attrs(spei)

    # resample to weights
    spei_name = "spei_corrected" if bias_correct else "spei"
    spei_path = get_path(
        "scratch",
        region.name,
        "era5",
        f"{region.name}-{year}-era5.{spei_name}.weekly_sum.nc",
    )
    spei.to_netcdf(spei_path)

    population = get_worldpop(region, year)
    with resampled_dataset("remapdis", spei_path, population) as resampled_ds:
        return zonal_stats_xarray(
            f"era5.{spei_name}.weekly_sum",
            resampled_ds.spei_bc if bias_correct else resampled_ds.spei,
            region,
            operation="area_weighted_sum",
            weights=population,
        )


@register_process("era5.spei.gamma", multiple_years=True)
def gamma_spei_uncorrected(
    region: ZonedBaseRegion, date: str, window: int = 6
) -> xr.Dataset:
    "Fit gamma parameters for SPEI with uncorrected precipitation"
    return gamma_spei(region, date, window, bias_correct=False)


@register_process("era5.spei_corrected.gamma", multiple_years=True)
def gamma_spei_corrected(
    region: ZonedBaseRegion, date: str, window: int = 6
) -> xr.Dataset:
    "Fit gamma parameters for SPEI with corrected precipitation"
    return gamma_spei(region, date, window, bias_correct=True)


@register_process("era5.spei")
def process_spei_uncorrected(region: AdministrativeLevel, date: str) -> xr.DataArray:
    "Processes SPEI with uncorrected precipitation"
    return process_spei(region, date, bias_correct=False)


@register_process("era5.spei_corrected")
def process_spei_corrected(region: AdministrativeLevel, date: str) -> xr.DataArray:
    "Processes SPEI with uncorrected precipitation"
    return process_spei(region, date, bias_correct=True)
