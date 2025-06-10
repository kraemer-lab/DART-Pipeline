"""
Common methods to calculate standardised precipitation index and
standardised precipitation-evaporation index
"""

import logging
import functools
import datetime
import warnings
from typing import Literal
from pathlib import Path

import xclim
import scipy.stats
import numpy as np
import xarray as xr
from geoglue.util import find_unique_time_coord
from geoglue.region import gadm
from geoglue.cds import (
    CdsDataset,
    ReanalysisSingleLevels,
    DatasetPool,
)
from geoglue.util import get_first_monday

from ...paths import get_path
from .list_metrics import VARIABLES

logger = logging.getLogger(__name__)


def get_dataset_pool(iso3: str, data_path: Path | None = None) -> DatasetPool:
    return ReanalysisSingleLevels(
        gadm(iso3, 1), VARIABLES, path=data_path or get_path("sources", iso3, "era5")
    ).get_dataset_pool()


def gamma_func(data, a, scale):
    return scipy.stats.gamma.cdf(data, a=a, scale=scale)


def norminv(data):
    return scipy.stats.norm.ppf(data, loc=0, scale=1)


def tp_corrected_path(iso3: str, year: int) -> Path:
    return get_path(
        "sources", iso3, "era5", f"{iso3}-{year}-era5.accum.tp_corrected.nc"
    )


def missing_tp_corrected_files(iso3, years: set[int]) -> list[Path]:
    out = []
    for year in years:
        if not (path := tp_corrected_path(iso3, year)).exists():
            out.append(path)
    return out


def get_tp_corrected(
    iso3: str, year: int, shift_hours: int, dim: str = "valid_time"
) -> xr.DataArray:
    if shift_hours < -12 or shift_hours > 12:
        raise ValueError(
            f"shift_hours should be an int between -12 and 12, got {shift_hours=}"
        )

    # accum variables should have 1 subtracted from timeshift
    # https://confluence.ecmwf.int/display/CKB/ERA5+family+post-processed+daily+statistics+documentation
    shift = shift_hours - 1
    if shift == 0:
        return xr.open_dataarray(tp_corrected_path(iso3, year))
    if shift > 0:
        da1 = xr.open_dataarray(tp_corrected_path(iso3, year - 1))
        da2 = xr.open_dataarray(tp_corrected_path(iso3, year))
        da1 = da1.isel(**{dim: slice(-shift, None)})  # type: ignore
        da = xr.concat([da1, da2], dim=dim)
        da = da.isel(**{dim: slice(None, -shift)})  # type: ignore
    else:
        da1 = xr.open_dataarray(tp_corrected_path(iso3, year))
        da2 = xr.open_dataarray(tp_corrected_path(iso3, year + 1))
        da2 = da2.isel(**{dim: slice(None, abs(shift))})  # type: ignore
        da = xr.concat([da1, da2], dim=dim)
        da = da.isel(**{dim: slice(abs(shift), None)})  # type: ignore
    return da


def add_bias_corrected_tp(
    accum: xr.Dataset, iso3: str, year: int, shift_hours: int
) -> xr.Dataset:
    try:
        tp_corrected = get_tp_corrected(iso3, year, shift_hours)
    except FileNotFoundError:
        logger.info(f"No tp_corrected file found for {iso3}-{year} {shift_hours=}")
        return accum
    accum["tp_bc"] = tp_corrected
    return accum


def assert_data_available_for_weekly_reduce(
    iso3: str, ystart: int, yend: int, data_path: Path | None = None
) -> None:
    "Asserts that sufficient data is available for weekly_reduce() call"
    region = gadm(iso3, 1, data_path=data_path)
    negative_longitude = "-" in region.tz

    # Always get one year around requested range as weeks may overlap contiguous years
    ystart -= 1
    yend += 1
    if negative_longitude:
        yend += 1  # time shifting requires data from succeeding year
    else:
        ystart -= 1  # time shifting requires data from preceding year
    pool = get_dataset_pool(iso3, data_path)
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


def get_date_range_for_years(
    ystart: int, yend: int, window: int = 0, align_weeks: bool = False
) -> tuple[datetime.date, datetime.date]:
    """Gets date range in years corresponding to a time window

    Parameters
    ----------
    ystart
        First year to include
    yend
        Last year to include
    window
        Window of days to include at the beginning of the time period. This is to
        ensure that rolling means with a positive window do not return NA values
        at the start of the period
    align_weeks
        Whether to align start and end dates with Mondays and Sundays (last day
        of week), respectively. Off by default. If set to True, `window` *must*
        be a multiple of 7.

    Returns
    -------
    Tuple of start and end dates
    """
    if align_weeks and window % 7 != 0:
        raise ValueError("When align_weeks=True, window must be a multiple of 7")
    if not align_weeks:
        return datetime.date(ystart, 1, 1) - datetime.timedelta(
            days=window
        ), datetime.date(yend, 12, 31)
    return get_first_monday(ystart) - datetime.timedelta(days=window), get_first_monday(
        yend + 1
    ) - datetime.timedelta(days=1)


def temperature_daily_dataset(
    iso3: str,
    ystart: int,
    yend: int,
    window: int = 0,
    align_weeks: bool = False,
    data_path: Path | None = None,
) -> xr.Dataset:
    """
    Returns daily dataset of temperature for a iso3 code for a closed,
    inclusive range of years, aligned with weeks beginning on Monday, such that
    resampling with W-MON returns values representing full weeks.

    The returned dataset has the following variables:
    - t2m: weekly mean of the daily mean temperature
    - mx2t24: weekly mean of the daily maximum temperature
    - mn2t24: weekly mean of the daily minimum temperature
    """
    pool = get_dataset_pool(iso3, data_path)
    assert_data_available_for_weekly_reduce(iso3, ystart, yend, data_path)
    cds0 = pool[ystart - 1]
    ds = temperature_stat_daily(cds0)
    for year in range(ystart, yend + 1):
        cdsy = pool[year]
        ds_y = temperature_stat_daily(cdsy)
        ds = xr.concat([ds, ds_y], dim="valid_time")
    start_date, end_date = get_date_range_for_years(ystart, yend, window, align_weeks)
    return ds.sel(valid_time=slice(start_date.isoformat(), end_date.isoformat()))


def precipitation_weekly_dataset(
    iso3: str, ystart: int, yend: int, window: int = 1, data_path: Path | None = None
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
    # Create the initial dataset with a window to enable rolling means
    # to return a non-NA value for the first week of the year
    ds = pool.weekly_reduce(ystart, "accum", window=window - 1)
    variables = set(ds.variables) - set(ds.coords)
    to_drop = variables - {"tp"}  # drop variables other than 'tp'
    ds = ds.drop_vars(to_drop)
    tdim = find_unique_time_coord(ds)
    for year in range(ystart + 1, yend + 1):
        ds = xr.concat(
            [ds, pool.weekly_reduce(year, "accum").drop_vars(to_drop)], dim=tdim
        )
    return ds


def corrected_precipitation_weekly_dataset(
    iso3: str, ystart: int, yend: int, window: int = 1, data_path: Path | None = None
) -> xr.Dataset:
    """
    Returns weekly dataset of bias corrected precipitation for a iso3 code for a
    closed, inclusive range of years.

    The returned dataset has the following variables:
    - tp_bc: weekly sum of the total daily precipitation
    """
    # We only need to construct the dataset pool to get the timeshift
    shift_hours = get_dataset_pool(iso3, data_path).shift_hours
    da = get_tp_corrected(iso3, ystart, shift_hours=shift_hours)

    # Create daily dataset from all years
    # We go up to yend + 1 to bring in some days from the succeeding year
    # for cases when Sundays are not 31 December (end of week aligns
    # with end of year)
    for y in range(ystart + 1, yend + 2):
        da_y = get_tp_corrected(iso3, y, shift_hours=shift_hours)
        da = xr.concat([da, da_y], dim="valid_time")

    # Crop to start timeseries on Mondays, with appropriate offset if window > 1
    start_date, end_date = get_date_range_for_years(
        ystart, yend, 7 * (window - 1), align_weeks=True
    )
    ds = xr.Dataset({"tp_bc": da}).sel(
        valid_time=slice(start_date.isoformat(), end_date.isoformat())
    )
    return ds.resample(valid_time="W-MON", closed="left", label="left").sum()


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
    iso3: str,
    ystart: int,
    yend: int,
    window: int = 1,
    data_path: Path | None = None,
    bias_correct: bool = False,
) -> xr.DataArray:
    """
    Returns weekly dataset of potential evapotranspiration for a iso3 code for
    a closed, inclusive range of years.

    The returned dataset has the following variables:
    * balance: difference between total precipitation and potential evapotranspiration
    """
    temp = temperature_daily_dataset(
        iso3,
        ystart,
        yend,
        window=7 * (window - 1),
        align_weeks=True,
        data_path=data_path,
    ).rename({"valid_time": "time"})
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
    precipitation_weekly_func = (
        corrected_precipitation_weekly_dataset
        if bias_correct
        else precipitation_weekly_dataset
    )
    ds_precip = precipitation_weekly_func(
        iso3, ystart, yend, window=window, data_path=data_path
    ).rename({"valid_time": "time"})
    assert ds_precip.time.min() == pevt.time.min()
    assert ds_precip.time.max() == pevt.time.max()
    tp_col = "tp_bc" if bias_correct else "tp"
    balance = (ds_precip[tp_col] - pevt).rename("balance")
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
       'tp_bc' for corrected precipitation) 'balance' for measuring
       SPEI

    ds and ds_ref must contain the same variable (tp for measuring SPI and
    precipitation - potential evapotranspiration if for SPEI)

    Returns
    -------
    The standardized precipitation index (SPI) or SPEI for the given variable.
    """
    tp = "tp" if not var.endswith("_corrected") else "tp_bc"
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
