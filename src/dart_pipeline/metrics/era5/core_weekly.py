"""Core ERA5 processing for weekly zonal aggregation"""

import logging
import functools
import datetime
from concurrent.futures import ProcessPoolExecutor

import xarray as xr
import pandas as pd

import geoglue.zonal_stats
from geoglue import AdministrativeLevel
from geoglue.util import get_first_monday
from geoglue.resample import resampled_dataset

from ...metrics import register_process, CFAttributes
from ...paths import get_path
from ...metrics.worldpop import get_worldpop

from .list_metrics import METRICS
from .util import get_dataset_pool, specific_humidity, relative_humidity

logger = logging.getLogger(__name__)


def get_cfattrs(var: str, time_col: str = "time") -> CFAttributes | None:
    accum_vars = ["tp", "hb", "e"]
    is_bias_corrected = var.endswith("_bc")
    var = var.removesuffix("_bc")
    agg = "sum" if var in accum_vars else "mean"
    if var.startswith("mx"):
        agg = "max"
        var = var.removeprefix("mx").removesuffix("24")
    if var.startswith("mn"):
        agg = "min"
        var = var.removeprefix("mn").removesuffix("24")
    if var == "2t":  # max/min temp of form mx2t24
        var = "t2m"

    # Find short name in variables array
    matching_vars = [v for v in METRICS if METRICS[v].get("short_name") == var]
    if not matching_vars:
        return None
    attrs = METRICS[matching_vars[0]]
    long_name = attrs["long_name"].lower()
    if is_bias_corrected:
        long_name = long_name + " (bias corrected)"
    if agg in ["mean", "sum"]:
        cell_methods = f"{time_col}: {agg} (interval: 7 days)"
        long_name = "Weekly " + long_name
    else:
        cell_methods = f"{time_col}: {agg}imum within days (interval: 1 day) {time_col}: mean over days (interval: 7 days)"
        long_name = f"Weekly mean of daily {agg}imum " + long_name
    out_attrs = {
        "long_name": long_name,
        "units": attrs["units"],
        "cell_methods": cell_methods,
    }
    for optional_attr in ["valid_min", "valid_max", "standard_name"]:
        if attrs.get(optional_attr) is not None:
            out_attrs[optional_attr] = attrs[optional_attr]

    return out_attrs


def get_weekly_tp_corrected(region: str, year: int) -> xr.DataArray:
    path = get_path(
        "sources", region, "era5", f"{region}-{year}-era5.accum.tp_corrected.nc"
    )
    # need next year path to get the last Sunday
    path_next_year = get_path(
        "sources", region, "era5", f"{region}-{year + 1}-era5.accum.tp_corrected.nc"
    )
    if path.exists() and path_next_year.exists():
        start_date = get_first_monday(year)
        end_date = get_first_monday(year + 1) - datetime.timedelta(days=1)
        da = xr.concat(
            [xr.open_dataarray(path), xr.open_dataarray(path_next_year)],
            dim="valid_time",
        )
        da = da.sel(
            valid_time=slice(start_date.isoformat(), end_date.isoformat())
        ).astype("float32")
        return (
            da.resample(valid_time="W-MON", closed="left", label="left")
            .sum()
            .rename("tp_bc")
        )
    else:
        raise FileNotFoundError(f"""Current year or next year tp_corrected file not available:
    {path}
    {path_next_year}""")


# TODO: add cfattrs
def zonal_stats(
    var: str, ds: xr.Dataset, region: geoglue.AdministrativeLevel
) -> xr.DataArray:
    geom = region.read()
    year = pd.to_datetime(ds.valid_time.min().values).year
    weights = get_worldpop(region, year)
    operation = (
        "area_weighted_sum"
        if var in ["tp", "tp_bc", "hb", "hb_bc"]
        else "mean(coverage_weight=area_spherical_km2)"
    )
    da = (
        geoglue.zonal_stats.zonal_stats_xarray(
            ds[var], geom, operation, weights, region_col=region.pk
        )
        .astype("float32")
        .rename({"date": "time"})
        .rename(var)
    )
    if var in ["r", "mxr24", "mnr24"]:
        da = da.clip(0, 100)
    da.attrs = get_cfattrs(var)
    return da


def weekly_mean(da: xr.DataArray) -> xr.DataArray:
    return (
        da.resample(valid_time="1D")
        .mean()
        .resample(valid_time="W-MON", closed="left", label="left")
        .mean()
    )


def weekly_mean_daily_max(da: xr.DataArray) -> xr.DataArray:
    return (
        da.resample(valid_time="1D")
        .max()
        .resample(valid_time="W-MON", closed="left", label="left")
        .mean()
    )


def weekly_mean_daily_min(da: xr.DataArray) -> xr.DataArray:
    return (
        da.resample(valid_time="1D")
        .min()
        .resample(valid_time="W-MON", closed="left", label="left")
        .mean()
    )


def prepare_weekly_data(region: AdministrativeLevel, year: int) -> xr.Dataset:
    accum_vars = ["e", "tp"]
    pool = get_dataset_pool(region)
    h = xr.concat(
        [
            pool[year].instant[["t2m", "d2m", "sp"]],
            pool[year + 1].instant[["t2m", "d2m", "sp"]],
        ],
        dim="valid_time",
    )
    tstart = get_first_monday(year).isoformat()
    tend = (get_first_monday(year + 1) - datetime.timedelta(days=1)).isoformat()
    h = h.sel(valid_time=slice(tstart, tend))

    t2m = weekly_mean(h.t2m)
    mx2t24 = weekly_mean_daily_max(h.t2m).rename("mx2t24")
    mn2t24 = weekly_mean_daily_min(h.t2m).rename("mn2t24")

    _q = specific_humidity(h)
    _r = relative_humidity(h)
    q = weekly_mean(_q).rename("q")
    mxq24 = weekly_mean_daily_max(_q).rename("mxq24")
    mnq24 = weekly_mean_daily_min(_q).rename("mnq24")
    r = weekly_mean(_r).rename("r")
    mxr24 = weekly_mean_daily_max(_r).rename("mxr24")
    mnr24 = weekly_mean_daily_min(_r).rename("mnr24")

    accum = pool.weekly_reduce(year, "accum")[accum_vars]
    ds = xr.merge([t2m, mx2t24, mn2t24, q, mxq24, mnq24, r, mxr24, mnr24, accum])
    try:
        tp_bc = get_weekly_tp_corrected(region.name, year)
        ds = xr.merge([ds, tp_bc])
    except FileNotFoundError:
        logger.warning(
            f"tp_corrected file not found, not adding tp_bc for ({region.name}, {year})"
        )

    # Calculate derived metrics
    if "tp_bc" in ds.data_vars:
        ds["hb_bc"] = ds.tp_bc + ds.e
    ds["hb"] = ds.tp + ds.e
    return ds.drop_vars(["e"])


@register_process("era5.core_weekly")
def era5_process_core_weekly(region: AdministrativeLevel, date: str) -> xr.Dataset:
    """Processes ERA5 data for a particular year, weekly timesteps

    Parameters
    ----------
    region : AdministrativeLevel
        Country ISO 3166-2 alpha-3 code
    date : str
        Year for which to process ERA5 data

    Returns
    -------
    List of generated or pre-existing data files in parquet format
    """
    year = int(date)
    logger.info(f"Processing {region.name}-{region.admin}-{year}-era5.core [weekly]")
    ds = prepare_weekly_data(region, year)
    weights = get_worldpop(region, year)
    fmt_region = " ".join([region.name, region.pk, region.tz])
    instant_vars = [
        "mn2t24",
        "t2m",
        "mx2t24",
        "q",
        "mxq24",
        "mnq24",
        "r",
        "mxr24",
        "mnr24",
    ]
    accum_vars = ["tp", "hb"]
    if "tp_bc" in ds.data_vars:
        accum_vars += ["tp_bc", "hb_bc"]
    logger.info("Resampling instant variables using CDO remapbil")
    with resampled_dataset("remapbil", ds[instant_vars], weights) as resampled_instant:
        logger.info("Starting zonal statistics for instant variables %s", instant_vars)
        with ProcessPoolExecutor() as executor:
            instant_stats = xr.merge(
                executor.map(
                    functools.partial(zonal_stats, ds=resampled_instant, region=region),
                    instant_vars,
                )
            )

    logger.info("Resampling accum variables using CDO remapdis")
    with resampled_dataset("remapdis", ds[accum_vars], weights) as resampled_accum:
        logger.info("Starting zonal statistics for accum variables: %s", accum_vars)
        with ProcessPoolExecutor() as executor:
            accum_stats = xr.merge(
                executor.map(
                    functools.partial(zonal_stats, ds=resampled_accum, region=region),
                    accum_vars,
                )
            )

    ds = xr.merge([instant_stats, accum_stats])
    ds.attrs = {}
    ds.attrs["DART_population"] = str(weights)
    ds.attrs["DART_region"] = fmt_region
    return ds
