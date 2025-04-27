import os
import logging
import multiprocessing
from typing import Literal
from pathlib import Path
from functools import cache, partial

import xarray as xr

from geoglue import MemoryRaster, Country
from geoglue.cds import ReanalysisSingleLevels, CdsPath, CdsDataset, DatasetPool
from geoglue.resample import resample
from geoglue.zonal_stats import DatasetZonalStatistics

from ...metrics import (
    register_metrics,
    register_fetch,
    register_process,
    MetricInfo,
)
from ...util import iso3_admin_unpack
from ...paths import get_path

from .derived import compute_derived_metric

ACCUM_METRICS = [
    "hydrological_balance",
    "total_precipitation",
    "spi",
    "spie",
    "bc_spi",
    "bc_spie",
    "bc_hydrological_balance",
    "bc_total_precipitation",
    "surface_solar_radiation_downwards",
]

VARIABLE_MAPPINGS = {
    "2m_temperature": "t2m",
    "surface_solar_radiation_downwards": "ssrd",
    "2m_dewpoint_temperature": "d2m",
    "surface_pressure": "sp",
    "evaporation": "e",
    "total_precipitation": "tp",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
}

depends_hydrological_balance = ["total_precipitation", "evaporation"]
METRICS: dict[str, MetricInfo] = {
    "2m_temperature": {
        "description": "2 meters air temperature",
        "unit": "K",
        "range": (-50, 50),
    },
    "surface_solar_radiation_downwards": {
        "description": "Accumulated solar radiation downwards",
        "unit": "J/m^2",
        "range": (0, 1e9),
    },
    "total_precipitation": {
        "description": "Total precipitation",
        "unit": "m",
        "range": (0, 1200),
    },
    "wind_speed": {
        "description": "Wind speed",
        "depends": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
        "range": (0, 110),
        "unit": "m/s",
    },
    "relative_humidity": {
        "description": "Relative humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "range": (0, 100),
        "unit": "percentage",
    },
    "specific_humidity": {
        "description": "Specific humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "range": (0, 30),
        "unit": "g/kg",
    },
    "hydrological_balance": {
        "description": "Hydrological balance",
        "depends": depends_hydrological_balance,
        "unit": "m",
    },
    "spi": {
        "description": "Standardised precipitation",
        "depends": ["total_precipitation"],
        "unit": "unitless",
    },
    # actually depends on potential_evapotranspiration which depends on 2m_temperature.daily_{min,mean,max}
    "spei": {
        "description": "Standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "unit": "unitless",
    },
    "bc_total_precipitation": {
        "description": "Bias-corrected total precipitation",
        "depends": ["total_precipitation"],
        "unit": "m",
    },
    "bc_spi": {
        "description": "Bias-corrected standardised precipitation",
        "depends": ["total_precipitation"],
        "unit": "unitless",
    },
    "bc_spei": {
        "description": "Bias-corrected standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "unit": "unitless",
    },
    "bc_hydrological_balance": {
        "description": "Bias-corrected hydrological balance",
        "depends": depends_hydrological_balance,
        "unit": "m",
    },
    "spi.gamma": {
        "description": "Fitted gamma distribution from historical data for SPI",
        "unit": "unitless",
        "depends": ["total_precipitation"],
    },
    "spei.gamma": {
        "description": "Fitted gamma distribution from historical data for SPEI",
        "unit": "unitless",
        "depends": ["2m_temperature", "total_precipitation"],
    },
}

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

STATS = ["min", "mean", "max", "sum"]
VARIABLES = sorted(set(sum([METRICS[m].get("depends", [m]) for m in METRICS], [])))

INSTANT_METRICS = [m for m in METRICS if m not in ACCUM_METRICS]
DERIVED_METRICS_SEPARATE_IMPL = ["spi", "spie"] + [
    m for m in ACCUM_METRICS if m.startswith("bc_")
]


def get_dataset_pool(iso3: str, data_path: Path | None = None) -> DatasetPool:
    return ReanalysisSingleLevels(
        iso3, VARIABLES, path=data_path or get_path("sources", iso3, "era5")
    ).get_dataset_pool()


@cache
def get_resampled_paths(iso3: str, year: int) -> dict[str, Path]:
    return {
        stat: get_path(
            "scratch", "era5", iso3, f"{iso3}-{year}-era5.daily_{stat}.resampled.nc"
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
        "era5",
        iso3,
        f"{iso3}-{admin}-{year}-era5.{metric}.daily_{statistic}.parquet",
    )


@cache
def get_population(iso3: str, year: int) -> MemoryRaster:
    return Country(iso3).population_raster(year)


def population_weighted_aggregation(
    metric_statistic: tuple[str, str],
    iso3: str,
    admin: int,
    year: int,
) -> Path:
    logger = multiprocessing.get_logger()
    metric, statistic = metric_statistic
    logger.info(f"Population aggregation for {metric=} with {statistic=}")
    unit = METRICS[metric].get("unit")
    resampled_paths = get_resampled_paths(iso3, year)
    country = Country(iso3)
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
    df = ds.zonal_stats(
        VARIABLE_MAPPINGS.get(metric, metric),
        operation,
        const_cols={"ISO3": iso3, "metric": f"era5.{metric}.{statistic}", "unit": unit},
    )
    outfile = metric_path(iso3, admin, year, metric, statistic)
    df.to_parquet(outfile)
    return outfile


@register_fetch("era5")
def era5_fetch(iso3: str, date: str) -> CdsPath | None:
    iso3 = iso3.upper()
    year = int(date)
    data = ReanalysisSingleLevels(
        iso3, VARIABLES, path=get_path("sources", "era5", iso3)
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
    logging.info("Processing era5")
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)
    paths = {
        stat: get_path("scratch", "era5", iso3, f"{iso3}-{year}-era5.daily_{stat}.nc")
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
        if METRICS[m].get("depends") and m not in DERIVED_METRICS_SEPARATE_IMPL
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

    logging.info("Calculating daily statistics (mean, sum)")
    daily_agg = ds.daily()  # mean and sum
    daily_agg.instant.to_netcdf(paths["mean"])
    daily_agg.accum.to_netcdf(paths["sum"])
    logging.info("Calculating daily statistics (min, max)")
    ds.daily_max().to_netcdf(paths["max"])
    ds.daily_min().to_netcdf(paths["min"])

    for stat in ("min", "max", "mean", "sum"):
        resampling = "remapdis" if stat == "sum" else "remapbil"
        logging.info(f"Resampling using CDO for {stat=} using {resampling=}")
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

    os.environ["TQDM_DISABLE"] = "1"
    print(metric_statistic_combinations)
    multiprocessing.log_to_stderr(logging.INFO)

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
        logging.warning(
            f"Skipping calculations for existing metrics {iso3}-{admin} {year=} {already_existing_metrics!r}"
        )
    if metric_statistic_combinations:
        with multiprocessing.Pool() as p:
            generated_paths = list(
                p.map(
                    partial(
                        population_weighted_aggregation,
                        iso3=iso3,
                        admin=admin,
                        year=year,
                    ),
                    metric_statistic_combinations,
                )
            )
    del os.environ["TQDM_DISABLE"]
    return paths + generated_paths
