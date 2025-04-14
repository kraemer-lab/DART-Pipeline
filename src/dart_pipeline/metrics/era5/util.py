"""
Methods to calculate standardised precipitation index and
standardised precipitation-evaporation index
"""

import functools
from typing import Literal

import scipy.stats
import numpy as np
import xarray as xr
from geoglue.util import find_unique_time_coord

from . import get_dataset_pool


def precipitation_weekly_dataset(
    iso3: str, ystart: int, yend: int, window: int
) -> xr.Dataset:
    """
    Returns weekly dataset of precipitation for a iso3 code for a
    closed, inclusive range of years.

    `window` specifies an integer number of offset weeks that are included
    before the beginning of the dataset. This is to allow rolling means to have
    non-NA values when using windowed means.
    """
    pool = get_dataset_pool(iso3)
    avail_years = set(pool.years)
    required_years = set(
        range(ystart - 1, yend + 1)
    )  # one extra year required for windowed data
    if not required_years <= avail_years:
        raise ValueError(
            f"Required years not available in DatasetPool for {iso3!r}:\n"
            f"\t{required_years - avail_years}\n"
            "\tUse `dart-pipeline get era5 VNM <year>` to download these"
        )
    ds = pool.weekly_reduce(ystart, "accum")
    variables = set(ds.variables) - set(ds.coords)
    # drop variables other than 'tp'
    to_drop = variables - {"tp"}
    ds = ds.drop_vars(to_drop)
    tdim = find_unique_time_coord(ds)
    for year in range(ystart + 1, yend + 1):
        ds = xr.concat(
            [ds, pool.weekly_reduce(year, "accum").drop_vars(to_drop)], dim=tdim
        )
    return ds


def fit_gamma_distribution(ds: xr.Dataset, window: int, dimension: str) -> xr.Dataset:
    ds_ma = ds.rolling(time=window, center=False).mean()
    # Nat log of moving averages
    ds_In = np.log(ds_ma)
    ds_In = ds_In.where(np.isinf(ds_In) == False)  # noqa: E712 comparison with False
    ds_mu = ds_ma.mean(dimension)

    # Overall mean of moving averages
    ds_mu = ds_ma.mean(dimension)

    # Summation of Natural log of moving averages
    ds_sum = ds_In.sum(dimension)

    # Computing essentials for gamma distribution
    n = ds_In[window - 1 :, :, :].count(dimension)  # size of dataset

    A = np.log(ds_mu) - (ds_sum / n)
    alpha = (1 / (4 * A)) * (1 + (1 + ((4 * A) / 3)) ** 0.5)
    beta = ds_mu / alpha
    return xr.Dataset({"alpha": alpha, "beta": beta})


def standardized_precipitation(
    var: Literal["spi", "spie", "bc_spi", "bc_spie"],
    ds: xr.Dataset,
    ds_ref: xr.Dataset,
    window: int,
    dimension: str,
) -> xr.DataArray:
    """
    The goal of this function is to measure SPI or SPIE by introducing
    precipitation/ precipitation - potential evapotranspiration data. In order
    to measure SPI, first we need to measure the parameters of a gamma
    distribution using historical data, and then adjust the precipitation data
    to that distribution.

    Parameters
    ----------
    ds
       Data we want to adjust. Depending upon the choice of ``var``, certain
       columns must be present. If var="SPI", ds and ds_ref must contain tp
       (total_precipitation) if var="SPIE ds and ds_ref must contain the
       product of precipitation - potential evapotranspiration
    ds_ref
       The historical dataset that we will use to obtain the gamma parameters
    window
       The time window length used to measure SPI.
    dimension
       The dimension over which to compute the rolling mean and gamma parameters
    var
       The variable name to be adjusted ('tp' for precipitation,
       'tp_corrected' for corrected precipitation) 'balance' for measuring
       SPIE

    ds and ds_ref must contain the same variable (tp for measuring SPI and
    precipitation - potential evapotranspiration if for SPIE)

    Returns
    -------
    The standardized precipitation index (SPI) or SPIE for the given variable.
    """
    tp = "tp" if not var.startswith("bc_") else "bc_tp"
    match var.removeprefix("bc_"):
        case "spi":
            params = fit_gamma_distribution(
                ds_ref[tp], window=window, dimension=dimension
            )
        case "spie":
            params = fit_gamma_distribution(ds_ref, window=window, dimension=dimension)
    ds_ma = ds.rolling(time=window, center=False).mean(dim=dimension)

    def gamma_func(data, a, scale):
        return scipy.stats.gamma.cdf(data, a=a, scale=scale)

    gamma = xr.apply_ufunc(gamma_func, ds_ma, params.alpha, params.beta)  # type: ignore
    # standardized precipitation index (inverse of CDF)
    norminv = functools.partial(scipy.stats.norm.ppf, loc=0, scale=1)
    norm_spi = xr.apply_ufunc(norminv, gamma)

    match var.removeprefix("bc_"):
        case "spi":
            return norm_spi[tp].rename(var)
        case "spie":
            return norm_spi.rename(var)
