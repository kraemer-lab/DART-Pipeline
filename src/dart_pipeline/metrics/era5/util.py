"""
Methods to calculate standardised precipitation index and
standardised precipitation-evaporation index
"""

import functools
import warnings
from typing import Literal
from pathlib import Path

import xclim
import scipy.stats
import numpy as np
import xarray as xr
from geoglue.country import Country
from geoglue.util import find_unique_time_coord
from geoglue.cds import CdsDataset

from . import get_dataset_pool


def gamma_func(data, a, scale):
    return scipy.stats.gamma.cdf(data, a=a, scale=scale)


def norminv(data):
    return scipy.stats.norm.ppf(data, loc=0, scale=1)


def assert_data_available_for_weekly_reduce(iso3: str, ystart: int, yend: int) -> None:
    "Asserts that sufficient data is available for weekly_reduce() call"
    timezone_offset = Country(iso3).timezone_offset
    negative_longitude = "-" in timezone_offset

    # Always get one year around requested range as weeks may overlap contiguous years
    ystart -= 1
    yend += 1
    if negative_longitude:
        yend += 1  # time shifting requires data from succeeding year
    else:
        ystart -= 1  # time shifting requires data from preceding year
    pool = get_dataset_pool(iso3)
    if missing := set(range(ystart, yend + 1)) - set(pool.years):
        raise FileNotFoundError(f"""Missing data for {iso3} for years: {missing}
For methods requiring weekly aggregations, we require a year before and
after the end of the requested period as weeks do not overlap 1:1 with years.
Timezone offsets may require an extra year on either end, depending upon
whether the timezone is negative (extra year required at end of period), or
positive (extra year required at beginning of period).""")


def parse_year_range(date: str, warn_duration_less_than_years: int) -> tuple[int, int]:
    try:
        ystart, yend = date.split("-")
        ystart = int(ystart)
        yend = int(yend)
    except ValueError:
        raise ValueError("Specify date as a year range, e.g. 2000-2020")
    if ystart >= yend:
        raise ValueError(f"Year end {yend} must be greater than year start {ystart}")
    if yend - ystart < warn_duration_less_than_years:
        warnings.warn(
            f"Using dataset spanning <{warn_duration_less_than_years} years for parameter estimation is not recommended"
        )
    return ystart, yend


def temperature_stat_daily(cds: CdsDataset) -> xr.Dataset:
    "Daily statistics for temperature from CdsDataset"
    return xr.Dataset(
        {
            "t2m": cds.instant.t2m.resample(valid_time="D").mean("valid_time"),
            "mn2t24": cds.instant.t2m.resample(valid_time="D").min("valid_time"),
            "mx2t24": cds.instant.t2m.resample(valid_time="D").max("valid_time"),
        },
    )


def temperature_daily_dataset(
    iso3: str, ystart: int, yend: int, data_path: Path | None = None
) -> xr.Dataset:
    """
    Returns weekly dataset of temperature for a iso3 code for a
    closed, inclusive range of years.

    The returned dataset has the following variables:
    - t2m: weekly mean of the daily mean temperature
    - mx2t24: weekly mean of the daily maximum temperature
    - mn2t24: weekly mean of the daily minimum temperature
    """
    pool = get_dataset_pool(iso3, data_path)
    avail_years = set(pool.years)
    required_years = set(range(ystart, yend + 1))
    if not required_years <= avail_years:
        raise ValueError(
            f"Required years not available in DatasetPool for {iso3!r}:\n"
            f"\t{required_years - avail_years}\n"
            "\tUse `dart-pipeline get era5 VNM <year>` to download these"
        )

    cds0 = pool[ystart]
    ds = temperature_stat_daily(cds0)
    for year in range(ystart + 1, yend + 1):
        cdsy = pool[year]
        ds_y = temperature_stat_daily(cdsy)
        ds = xr.concat([ds, ds_y], dim="valid_time")
    return ds


def precipitation_weekly_dataset(
    iso3: str, ystart: int, yend: int, data_path: Path | None = None
) -> xr.Dataset:
    """
    Returns weekly dataset of precipitation for a iso3 code for a
    closed, inclusive range of years.

    The returned dataset has the following variables:
    - tp: weekly sum of the total daily precipitation
    """
    pool = get_dataset_pool(iso3, data_path)
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
    to_drop = variables - {"tp"}  # drop variables other than 'tp'
    ds = ds.drop_vars(to_drop)
    tdim = find_unique_time_coord(ds)
    for year in range(ystart + 1, yend + 1):
        ds = xr.concat(
            [ds, pool.weekly_reduce(year, "accum").drop_vars(to_drop)], dim=tdim
        )
    return ds


def fit_gamma_distribution(
    ds: xr.Dataset | xr.DataArray, window: int, dimension: str
) -> xr.Dataset:
    ds_ma = ds.rolling({dimension: window}, center=False).mean().dropna(dimension)
    # Nat log of moving averages
    ds_In = np.log(ds_ma)
    ds_In = ds_In.where(np.isinf(ds_In) == False)  # noqa: E712 comparison with False
    ds_mu = ds_ma.mean(dimension)

    # Overall mean of moving averages
    ds_mu = ds_ma.mean(dimension)

    # Summation of Natural log of moving averages
    ds_sum = ds_In.sum(dimension)

    # size of the dataset independently of the location of time position
    n = len(ds_In[dimension]) - (window - 1)

    A = np.log(ds_mu) - (ds_sum / n)
    alpha = (1 / (4 * A)) * (1 + (1 + ((4 * A) / 3)) ** 0.5)
    beta = ds_mu / alpha
    return xr.Dataset({"alpha": alpha, "beta": beta})


def balance_weekly_dataarray(
    iso3: str, ystart: int, yend: int, data_path: Path | None = None
) -> xr.DataArray:
    """
    Returns weekly dataset of potential evapotranspiration for a iso3 code for
    a closed, inclusive range of years.

    The returned dataset has the following variables:
    * balance: difference between total precipitation and potential evapotranspiration
    """
    temp = temperature_daily_dataset(iso3, ystart, yend, data_path).rename(
        {"valid_time": "time"}
    )
    pevt = (
        xclim.indicators.atmos.potential_evapotranspiration(
            tasmin=temp.mn2t24, tasmax=temp.mx2t24, tas=temp.t2m
        )
        # closed and label MUST be identical with that obtained from
        # precipitation_weekly_dataset, which in turn must be aligned
        # with geoglue.cds.weekly_reduce
        .resample(time="W-MON", closed="left", label="left")
        .sum()
    )

    # Resample precipitation to weekly sum
    ds_precip = precipitation_weekly_dataset(iso3, ystart, yend, data_path).rename(
        {"valid_time": "time"}
    )
    # TODO: check alignment of datasets
    balance = (ds_precip.tp - pevt).rename("balance")
    return balance.rename({"time": "valid_time"})


def standardized_precipitation(
    var: Literal["spi", "spei", "spi_corrected", "spei_corrected"],
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
       (total_precipitation) if var="SPEI" ds and ds_ref must contain the
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
       SPEI

    ds and ds_ref must contain the same variable (tp for measuring SPI and
    precipitation - potential evapotranspiration if for SPEI)

    Returns
    -------
    The standardized precipitation index (SPI) or SPEI for the given variable.
    """
    tp = "tp" if not var.endswith("_corrected") else "tp_corrected"
    match var.removeprefix("bc_"):
        case "spi":
            params = fit_gamma_distribution(
                ds_ref[tp], window=window, dimension=dimension
            )
        case "spei":
            params = fit_gamma_distribution(ds_ref, window=window, dimension=dimension)
    ds_ma = ds.rolling(time=window, center=False).mean(dim=dimension)

    def gamma_func(data, a, scale):
        return scipy.stats.gamma.cdf(data, a=a, scale=scale)

    gamma = xr.apply_ufunc(gamma_func, ds_ma, params.alpha, params.beta)  # type: ignore
    # standardized precipitation index (inverse of CDF)
    norminv = functools.partial(scipy.stats.norm.ppf, loc=0, scale=1)
    norm_spi = xr.apply_ufunc(norminv, gamma)

    match var.removesuffix("_corrected"):
        case "spi":
            return norm_spi[tp].rename(var)
        case "spei":
            return norm_spi.rename(var)
