import logging
from typing import Literal
from pathlib import Path
from functools import cache

import xarray as xr
from tqdm import tqdm

from geoglue import MemoryRaster, Country
from geoglue.cds import ReanalysisSingleLevels, CdsPath, CdsDataset
from geoglue.resample import resample
from geoglue.zonal_stats import DatasetZonalStatistics

from ...metrics import (
    register_metrics,
    register_fetch,
    register_process,
)
from ...util import iso3_admin_unpack
from ...paths import get_path

from .derived import compute_derived_metric
from .util import (
    get_dataset_pool,
    precipitation_weekly_dataset,
    temperature_daily_dataset,
    add_bias_corrected_tp,
)
from .list_metrics import (
    VARIABLE_MAPPINGS,
    METRICS,
    ACCUM_METRICS,
    INSTANT_METRICS,
    DERIVED_METRICS_SEPARATE_IMPL,
    VARIABLES,
)

logger = logging.getLogger(__name__)

STATS = ["min", "mean", "max", "sum"]

register_metrics(
    "era5",
    description="ERA5 reanalysis data",
    license_text="""Access to Copernicus Products is given for any purpose in so far
as it is lawful, whereas use may include, but is not limited to: reproduction;
distribution; communication to the public; adaptation, modification and
combination with other data and information; or any combination of the
foregoing.""",
    auth_url="https://cds.climate.copernicus.eu/how-to-api",
    metrics=METRICS,
)


@cache
def get_resampled_paths(iso3: str, year: int) -> dict[str, Path]:
    return {
        stat: get_path(
            "scratch", iso3, "era5", f"{iso3}-{year}-era5.daily_{stat}.resampled.nc"
        )
        for stat in ["mean", "min", "max", "sum"]
    }


def collect_variables_to_drop(kind: Literal["instant", "accum"]) -> list[str]:
    "Collect list of variables to drop for a particular variable type"
    ms = INSTANT_METRICS if kind == "instant" else ACCUM_METRICS
    collect = set()
    for m in ms:
        collect.update(METRICS[m].get("depends", []))
    vars_to_drop = sorted(collect - set(METRICS.keys()))
    return [VARIABLE_MAPPINGS.get(v, v) for v in vars_to_drop]


def metric_path(iso3: str, admin: int, year: int, metric: str, statistic: str) -> Path:
    assert statistic in STATS
    return get_path(
        "output",
        iso3,
        "era5",
        f"{iso3}-{admin}-{year}-era5.{metric}.daily_{statistic}.parquet",
    )


@cache
def get_population(iso3: str, year: int) -> MemoryRaster:
    return Country(iso3).population_raster(year)


def population_weighted_aggregation(
    metric: str,
    statistic: str,
    iso3: str,
    admin: int,
    year: int,
) -> Path:
    unit = METRICS[metric].get("unit")
    resampled_paths = get_resampled_paths(iso3, year)
    logger.info(
        f"Population weighted aggregation [{iso3}-{admin}] {year=} {metric=} {statistic=}"
    )
    country = Country(iso3)
    logger.info(
        "Making DatasetZonalStatistics(xr.open_dataset({resampled_paths[statistic]!r}, Country({iso3!r}).admin({admin}), weights=get_population({iso3!r}, year))"
    )
    ds = DatasetZonalStatistics(
        xr.open_dataset(resampled_paths[statistic]),
        country.admin(admin),
        weights=get_population(iso3, year),
    )
    operation = (
        "mean(coverage_weight=area_spherical_km2)"
        if metric not in ACCUM_METRICS
        else "area_weighted_sum"
    )
    variable = VARIABLE_MAPPINGS.get(metric, metric)
    logger.info(f"Performing zonal_stats({variable!r}, {operation=})")
    df = ds.zonal_stats(
        variable,
        operation,
        const_cols={"ISO3": iso3, "metric": f"era5.{metric}.{statistic}", "unit": unit},
    )
    # clamp relative_humidity to 100%
    if "relative_humidity" in metric:
        df["value"] = df.value.clip(0, 100)
    outfile = metric_path(iso3, admin, year, metric, statistic)
    df.to_parquet(outfile)
    logger.info(f"Output [{iso3}-{admin}] {year=} {metric=} {statistic=} -> {outfile}")
    return outfile


@register_fetch("era5")
def era5_fetch(iso3: str, date: str) -> CdsPath | None:
    iso3 = iso3.upper()
    year = int(date)
    data = ReanalysisSingleLevels(
        iso3, VARIABLES, path=get_path("sources", iso3, "era5")
    )
    return data.get(year)


@register_process("era5")
def era5_process(iso3: str, date: str, overwrite: bool = False) -> list[Path]:
    """Processes ERA5 data for a particular year

    Parameters
    ----------
    iso3
        Country ISO 3166-2 alpha-3 code
    date
        Year for which to process ERA5 data
    overwrite
        Whether to overwrite existing generated data, default
        is to skip generation if file exists (default=False)

    Returns
    -------
    List of generated or pre-existing data files in parquet format
    """
    logger.info("Processing era5")
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)
    paths = {
        stat: get_path("scratch", iso3, "era5", f"{iso3}-{year}-era5.daily_{stat}.nc")
        for stat in ["mean", "min", "max", "sum"]
    }
    # after cdo resampling
    resampled_paths = get_resampled_paths(iso3, year)

    iso3 = iso3.upper()
    pool = get_dataset_pool(iso3)
    ds = pool[year]

    # List of derived metrics that do not have another implementation (usually
    # requiring more parameters). In practice this includes all metrics that
    # can be calculated without using a reference dataset
    derived_metrics = [
        m
        for m in METRICS
        if METRICS[m].get("depends")
        and m not in DERIVED_METRICS_SEPARATE_IMPL
        and m != "hydrological_balance_corrected"
        # ^^^ handle separately as we will calculate this at the daily level
    ]
    for metric in derived_metrics:
        if metric in ACCUM_METRICS:
            ds.accum[metric] = compute_derived_metric(metric, ds.accum)
        else:
            ds.instant[metric] = compute_derived_metric(metric, ds.instant)

    ds = CdsDataset(
        instant=ds.instant.drop_vars(collect_variables_to_drop("instant")),
        accum=ds.accum.drop_vars(collect_variables_to_drop("accum")),
    )

    logger.info("Calculating daily statistics (mean, sum)")
    daily_agg = ds.daily()  # mean and sum
    daily_agg.instant.to_netcdf(paths["mean"])

    # Read in possible tp_corrected file here and add to accum dataset
    # If no tp_corrected file is found, add_bias_corrected_tp() returns
    # the daily accumulated dataset unaltered.
    # Note that tp_corrected for a year will require the corresponding files
    # for previous and succeeding years depending on shift_hours
    accum = add_bias_corrected_tp(
        daily_agg.accum, iso3, year, shift_hours=pool.shift_hours
    )
    is_bias_corrected: bool = "tp_corrected" in accum.variables
    if is_bias_corrected:
        accum["hydrological_balance_corrected"] = accum.tp_corrected + accum.e
    accum.to_netcdf(paths["sum"])

    # read in
    logger.info("Calculating daily statistics (min, max)")
    ds.daily_max().to_netcdf(paths["max"])
    ds.daily_min().to_netcdf(paths["min"])

    for stat in ("min", "max", "mean", "sum"):
        resampling = "remapdis" if stat == "sum" else "remapbil"
        logger.info(
            f"Resampling using CDO for {stat=} using {resampling=}: {paths[stat]} -> {resampled_paths[stat]}"
        )
        resample(
            resampling, paths[stat], get_population(iso3, year), resampled_paths[stat]
        )

    metric_statistic_combinations: list[tuple[str, str]] = [
        (metric, statistic)
        for metric in INSTANT_METRICS
        for statistic in ["min", "max", "mean"]
    ] + [(metric, "sum") for metric in ACCUM_METRICS]

    # remove metrics that are separately calculated and need reference datasets
    metric_statistic_combinations = [
        (m, s)
        for m, s in metric_statistic_combinations
        if m not in DERIVED_METRICS_SEPARATE_IMPL
    ]
    # remove corrected metrics if no bias correction available
    if not is_bias_corrected:
        metric_statistic_combinations = [
            (m, s)
            for m, s in metric_statistic_combinations
            if not m.endswith("_corrected")
        ]

    logger.info("Metric statistic combinations %r", metric_statistic_combinations)

    already_existing_metrics = [
        (m, s)
        for m, s in metric_statistic_combinations
        if metric_path(iso3, admin, year, m, s).exists()
    ]
    paths, generated_paths = [], []
    if not overwrite and already_existing_metrics:
        paths = [
            metric_path(iso3, admin, year, m, s) for m, s in already_existing_metrics
        ]
        metric_statistic_combinations = [
            (m, s)
            for m, s in metric_statistic_combinations
            if (m, s) not in already_existing_metrics
        ]
        logger.warning(
            f"Skipping calculations for existing metrics {iso3}-{admin} {year=} {already_existing_metrics!r}"
        )
    for metric, statistic in tqdm(
        metric_statistic_combinations, desc="Computing metrics"
    ):
        population_weighted_aggregation(metric, statistic, iso3, admin, year)

    return paths + generated_paths


@register_process("era5.prep_bias_correct")
def prep_bias_correct(iso3: str, date: str, profile: str) -> xr.Dataset:
    try:
        ystart, yend = date.split("-")
        ystart, yend = int(ystart), int(yend)
    except ValueError:
        raise ValueError("Date must be specified as a year range, e.g. 2000-2020")
    match profile:
        case "precipitation":
            return precipitation_weekly_dataset(iso3, ystart, yend)
        case "forecast":
            return temperature_daily_dataset(iso3, ystart, yend)
        case _:
            raise ValueError(f"Unknown prep_bias_correct {profile=}")
