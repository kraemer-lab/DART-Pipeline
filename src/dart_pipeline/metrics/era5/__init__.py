import multiprocessing
from typing import Literal
from pathlib import Path

import xarray as xr

from geoglue.country import Country
from geoglue.cds import ReanalysisSingleLevels, CdsPath
from geoglue.resample import resample
from geoglue.zonal_stats import DatasetZonalStatistics

from ...metrics import register_metrics, register_fetch, register_process, MetricInfo
from ...util import source_path, output_path, iso3_admin_unpack

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
        "units": "degree_Celsius",
        "range": (-50, 50),
    },
    "surface_solar_radiation_downwards": {
        "description": "Accumulated solar radiation downwards",
        "units": "J/m^2",
        "range": (0, 1e9),
    },
    "total_precipitation": {
        "description": "Total precipitation",
        "units": "m",
        "range": (0, 1200),
    },
    "wind_speed": {
        "description": "Wind speed",
        "depends": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
        "range": (0, 110),
    },
    "relative_humidity": {
        "description": "Relative humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "range": (0, 100),
    },
    "specific_humidity": {
        "description": "Specific humidity",
        "depends": ["2m_temperature", "2m_dewpoint_temperature", "surface_pressure"],
        "units": "g/kg",
        "range": (0, 30),
    },
    "hydrological_balance": {
        "description": "Hydrological balance",
        "depends": depends_hydrological_balance,
        "units": "m",
    },
    "spi": {
        "description": "Standardised precipitation",
        "depends": ["total_precipitation"],
        "units": "unitless",
    },
    # actually depends on potential_evapotranspiration which depends on 2m_temperature.daily_{min,mean,max}
    "spie": {
        "description": "Standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "units": "unitless",
    },
    "bc_total_precipitation": {
        "description": "Bias-corrected total precipitation",
        "depends": ["total_precipitation"],
        "units": "m",
    },
    "bc_spie": {
        "description": "Bias-corrected standardised precipitation-evaporation index",
        "depends": ["total_precipitation", "2m_temperature"],
        "units": "unitless",
    },
    "bc_hydrological_balance": {
        "description": "Bias-corrected hydrological balance",
        "depends": depends_hydrological_balance,
        "units": "m",
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
    requires_auth=True,
    auth_url="https://cds.climate.copernicus.eu/how-to-api",
    metrics=METRICS,
)

STATS = ["min", "mean", "max", "sum"]
VARIABLES = sum([METRICS[m].get("depends", [m]) for m in METRICS], [])
INSTANT_METRICS = [m for m in METRICS if m not in ACCUM_METRICS]


def collect_variables_to_drop(kind: Literal["instant", "accum"]) -> list[str]:
    "Collect list of variables to drop for a particular variable type"
    ms = INSTANT_METRICS if kind == "instant" else ACCUM_METRICS
    collect = set()
    for m in ms:
        collect.update(METRICS[m].get("depends", []))
    return sorted(collect - set(METRICS.keys()))


def metric_path(iso3: str, admin: int, year: int, metric: str, statistic: str) -> Path:
    assert statistic in STATS
    return output_path(
        f"{iso3}/era5",
        f"{iso3}-{admin}-{year}-era5.{metric}.daily_{statistic}.nc",
    )


@register_fetch("era5")
def era5_fetch(iso3: str, year: int) -> CdsPath | None:
    iso3 = iso3.upper()
    data = ReanalysisSingleLevels(iso3, VARIABLES, source_path(f"{iso3}/era5"))
    return data.get(year)


@register_process("era5")
def era5_process(iso3_admin: str, year: int) -> list[Path]:
    "Processes ERA5 data for a particular year"
    iso3, admin = iso3_admin_unpack(iso3_admin)
    paths = {
        stat: source_path(f"{iso3}/proc/era5", f"{iso3}-{year}-era5.daily_{stat}.nc")
        for stat in ["mean", "min", "max", "sum"]
    }
    # after cdo resampling
    resampled_paths = {
        stat: source_path(
            f"{iso3}/proc/era5", f"{iso3}-{year}-era5.daily_{stat}.resampled.nc"
        )
        for stat in ["mean", "min", "max", "sum"]
    }

    iso3 = iso3.upper()
    country = Country(iso3)
    pool = ReanalysisSingleLevels(
        iso3, VARIABLES, source_path(f"{iso3}/era5")
    ).get_dataset_pool()

    ds = pool[year]
    derived_metrics = [m for m in METRICS if METRICS[m].get("depends")]
    for metric in derived_metrics:
        if metric in ACCUM_METRICS:
            ds.accum[metric] = compute_derived_metric(metric, ds.accum)
        else:
            ds.instant[metric] = compute_derived_metric(metric, ds.instant)

    ds.instant = ds.instant.drop_vars(collect_variables_to_drop("instant"))
    ds.accum = ds.accum.drop_vars(collect_variables_to_drop("accum"))

    daily_agg = ds.daily()  # mean and sum
    daily_agg.instant.to_netcdf(paths["mean"])
    daily_agg.accum.to_netcdf(paths["sum"])

    ds.daily_max().to_netcdf(paths["max"])
    ds.daily_min().to_netcdf(paths["min"])

    population = country.population_raster(year)
    for stat in ("min", "max", "mean", "sum"):
        resampling = "remapdis" if stat == "sum" else "remapbil"
        resample(resampling, paths[stat], population, resampled_paths[stat])

    def population_weighted_aggregation(metric_statistic: tuple[str, str]) -> Path:
        metric, statistic = metric_statistic
        assert statistic in ["min", "max", "mean", "sum"], f"Invalid {statistic=}"
        ds = DatasetZonalStatistics(
            xr.open_dataset(resampled_paths[statistic]),
            country.admin(admin),
            weights=population,
        )
        operation = (
            "mean(coverage_weight=area_spherical_km2)"
            if metric not in ACCUM_METRICS
            else "area_weighted_sum"
        )
        df = ds.zonal_stats(
            VARIABLE_MAPPINGS.get(metric, metric),
            operation,
            const_cols={"ISO3": iso3, "metric": f"era5.{metric}.{statistic}"},
        )
        outfile = metric_path(iso3, admin, year, metric, statistic)
        df.to_parquet(outfile)
        return outfile

    metric_statistic_combinations: list[tuple[str, str]] = [
        (metric, statistic)
        for metric in INSTANT_METRICS
        for statistic in ["min", "max", "mean"]
    ] + [(metric, "sum") for metric in ACCUM_METRICS]

    with multiprocessing.Pool() as processing_pool:
        return list(
            processing_pool.map(
                population_weighted_aggregation, metric_statistic_combinations
            )
        )
