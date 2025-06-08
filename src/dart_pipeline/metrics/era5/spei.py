"""
Standardised precipitation evapotranspiration index (SPEI)
"""

import re
import logging
import xarray as xr
import pandas as pd

from geoglue.region import gadm, get_worldpop_1km
from geoglue.resample import resampled_dataset
from geoglue.util import find_unique_time_coord, set_lonlat_attrs

from ...paths import get_path
from ...util import iso3_admin_unpack
from ...metrics import register_process, find_metric, zonal_stats

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


def gamma_spei(
    iso3: str,
    date: str,
    window: int = 6,
    bias_correct: bool = False,
) -> xr.Dataset:
    """Calculates gamma parameter for SPEI for a date range

    Parameters
    ----------
    iso3 : str
        Country ISO3 code
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
    assert_data_available_for_weekly_reduce(iso3, ystart, yend)
    balance_hist = balance_weekly_dataarray(
        iso3, ystart, yend, bias_correct=bias_correct
    )
    tdim = find_unique_time_coord(balance_hist)
    ds = fit_gamma_distribution(balance_hist, window=window, dimension=tdim)
    ds.attrs["DART_history"] = (
        f"gamma_spei({iso3!r}, {ystart=}, {yend=}, {window=}, {bias_correct=})"
    )
    ds.attrs["ISO3"] = iso3
    ds.attrs["metric"] = (
        "era5.spei_corrected.gamma" if bias_correct else "era5.spei.gamma"
    )
    return ds


def process_spei(iso3: str, date: str, bias_correct: bool = False) -> pd.DataFrame:
    """Determines SPEI for a particular year

    Parameters
    ----------
    iso3 : str
        ISO3 code of the country
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
    iso3, admin = iso3_admin_unpack(iso3)

    if bias_correct:
        gamma_params: xr.Dataset = find_metric("era5.spei.gamma", iso3)
    else:
        gamma_params: xr.Dataset = find_metric("era5.spei_corrected.gamma", iso3)
    re_matches = re.match(r".*window=(\d+)", gamma_params.attrs["DART_history"])
    if re_matches is None:
        raise ValueError("No window option found in gamma parameters file")
    window = int(re_matches.groups()[0])
    logger.info("Using era5.spei.gamma window=%d", window)

    ds = balance_weekly_dataarray(
        iso3, year, year, window=window, bias_correct=bias_correct
    )
    ds_ma = (
        ds.rolling(valid_time=window, center=False)
        .mean(dim="valid_time")
        .dropna(dim="valid_time")
    )
    gamma = xr.apply_ufunc(gamma_func, ds_ma, gamma_params.alpha, gamma_params.beta)
    norm_spei = xr.apply_ufunc(norminv, gamma)
    spei_name = "spei_bc" if bias_correct else "spei"
    spei = xr.Dataset({spei_name: norm_spei})
    set_lonlat_attrs(spei)

    # resample to weights
    spei_name = "spei_corrected" if bias_correct else "spei"
    spei_path = get_path(
        "scratch", iso3, "era5", f"{iso3}-{year}-era5.{spei_name}.weekly_sum.nc"
    )
    spei.to_netcdf(spei_path)

    population = get_worldpop_1km(iso3, year)
    with resampled_dataset("remapdis", spei_path, population) as resampled_ds:
        return zonal_stats(
            f"era5.{spei_name}.weekly_sum",
            resampled_ds.spei,
            gadm(iso3, admin),
            operation="area_weighted_sum",
            weights=population,
        )


@register_process("era5.spei.gamma", multiple_years=True)
def gamma_spei_uncorrected(iso3: str, date: str, window: int = 6) -> xr.Dataset:
    "Fit gamma parameters for SPEI with uncorrected precipitation"
    return gamma_spei(iso3, date, window, bias_correct=False)


@register_process("era5.spei_corrected.gamma", multiple_years=True)
def gamma_spei_corrected(iso3: str, date: str, window: int = 6) -> xr.Dataset:
    "Fit gamma parameters for SPEI with corrected precipitation"
    return gamma_spei(iso3, date, window, bias_correct=True)


@register_process("era5.spei")
def process_spei_uncorrected(iso3: str, date: str) -> pd.DataFrame:
    "Processes SPEI with uncorrected precipitation"
    return process_spei(iso3, date, bias_correct=False)


@register_process("era5.spei_corrected")
def process_spei_corrected(iso3: str, date: str) -> pd.DataFrame:
    "Processes SPEI with uncorrected precipitation"
    return process_spei(iso3, date, bias_correct=True)
