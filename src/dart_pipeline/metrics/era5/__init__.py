import logging
import functools
import multiprocessing
from typing import Literal
from pathlib import Path
from functools import cache

import xarray as xr

from geoglue.util import sha256
from geoglue.region import gadm
from geoglue.cds import ReanalysisSingleLevels, CdsPath, CdsDataset
from geoglue.resample import resample

from ...metrics import (
    register_metrics,
    register_fetch,
    register_process,
    zonal_stats_xarray,
    print_paths,
)
from ...metrics.worldpop import get_worldpop
from ...util import iso3_admin_unpack
from ...paths import get_path

from .derived import compute_derived_metric
from .util import (
    get_dataset_pool,
    precipitation_weekly_dataset,
    temperature_daily_dataset,
    add_bias_corrected_tp,
    parse_year_range,
    missing_tp_corrected_files,
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
        f"{iso3}-{admin}-{year}-era5.{metric}.daily_{statistic}.nc",
    )


def population_weighted_aggregation(
    metric: str,
    statistic: str,
    iso3: str,
    admin: int,
    year: int,
) -> Path:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(processName)s] %(levelname)s %(message)s",
    )
    resampled_paths = get_resampled_paths(iso3, year)
    message = f"zonal stats [{iso3}-{admin}, {year}] era5.{metric}.daily_{statistic}"

    logger = logging.getLogger(__name__)
    logger.info(f"START {message}")
    operation = (
        "mean(coverage_weight=area_spherical_km2)"
        if metric not in ACCUM_METRICS
        else "area_weighted_sum"
    )
    variable = VARIABLE_MAPPINGS.get(metric, metric)
    resampled_checksum = sha256(resampled_paths[statistic])
    ds = xr.open_dataset(resampled_paths[statistic])
    provenance = ds.attrs.get("provenance", "")
    da = zonal_stats_xarray(
        f"era5.{metric}.daily_{statistic}",
        ds[variable],
        gadm(iso3, admin),
        operation=operation,
        weights=get_worldpop(iso3, year),
    )
    # clamp relative_humidity to 100%
    if "relative_humidity" in metric:
        da = da.clip(0, 100)
    outfile = metric_path(iso3, admin, year, metric, statistic)
    assert outfile.suffix == ".nc"
    da.attrs["provenance"] = (
        f"zonal_stats.ds={resampled_checksum} {resampled_paths[statistic]}\n{provenance}"
    )
    da.to_netcdf(outfile)
    logger.info(f"  END {message} -> {outfile}")
    return outfile


def _pprint_ms(
    ms: dict[str, list[str]], existing_ms: dict[str, list[str]] | None = None
) -> str:
    "Pretty print metric statistic combinations"
    if existing_ms is None:
        return "\n\t" + "\n\t".join(
            sum(
                [
                    [f"[make] era5.{metric}.daily_{stat}" for metric in ms[stat]]
                    for stat in ms
                ],
                [],
            )
        )
    else:
        out = []
        for stat in ms:
            for metric in ms[stat]:
                if metric in existing_ms[stat]:
                    out.append(f"[skip] era5.{metric}.daily_{stat}")
                else:
                    out.append(f"[make] era5.{metric}.daily_{stat}")
        return "\n\t" + "\n\t".join(out)


@register_fetch("era5")
def era5_fetch(iso3: str, date: str) -> CdsPath | None:
    iso3 = iso3.upper()
    year = int(date)
    data = ReanalysisSingleLevels(
        gadm(iso3, 1), VARIABLES, path=get_path("sources", iso3, "era5")
    )
    return data.get(year)


@register_process("era5.core")
def era5_process_core(
    iso3: str, date: str, overwrite: bool = True, keep_resampled: bool = False
) -> list[Path]:
    """Processes ERA5 data for a particular year

    Parameters
    ----------
    iso3 : str
        Country ISO 3166-2 alpha-3 code
    date : str
        Year for which to process ERA5 data
    overwrite : bool
        Whether to overwrite existing generated data, default=True
    keep_resampled : bool
        Whether to keep files generated by CDO resample. These can be large files
        due to upsampling of source data. Default is False, and resampled files
        are deleted upon completion of processing.

    Returns
    -------
    List of generated or pre-existing data files in parquet format
    """
    year = int(date)
    iso3, admin = iso3_admin_unpack(iso3)
    logger.info(f"Processing {iso3}-{admin}-{year}-era5.core")
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
    is_bias_corrected: bool = "tp_bc" in accum.variables
    if is_bias_corrected:
        accum["hb_bc"] = accum.tp_bc + accum.e
    accum.to_netcdf(paths["sum"])

    # read in
    logger.info("Calculating daily statistics (min, max)")
    ds.daily_max().to_netcdf(paths["max"])
    ds.daily_min().to_netcdf(paths["min"])

    instant_metrics = [
        m for m in INSTANT_METRICS if m not in DERIVED_METRICS_SEPARATE_IMPL
    ]
    accum_metrics = [m for m in ACCUM_METRICS if m not in DERIVED_METRICS_SEPARATE_IMPL]
    if not is_bias_corrected:
        instant_metrics = [m for m in instant_metrics if not m.endswith("_corrected")]
        accum_metrics = [m for m in accum_metrics if not m.endswith("_corrected")]

    metric_statistic_combinations: dict[str, list[str]] = {
        s: instant_metrics for s in ["min", "max", "mean"]
    }
    metric_statistic_combinations["sum"] = accum_metrics

    already_existing_metrics: dict[str, list[str]] = {
        s: [
            m
            for m in metric_statistic_combinations[s]
            if metric_path(iso3, admin, year, m, s).exists()
        ]
        for s in metric_statistic_combinations
    }
    n_already_existing_metrics: int = sum(
        len(already_existing_metrics[s]) for s in already_existing_metrics
    )

    generated_paths = []
    if not overwrite and n_already_existing_metrics:
        generated_paths = sum(
            [
                [
                    metric_path(iso3, admin, year, m, s)
                    for m in already_existing_metrics[s]
                ]
                for s in already_existing_metrics
            ],
            [],
        )
        logger.info(
            "Metric statistic combinations: %s",
            _pprint_ms(metric_statistic_combinations, already_existing_metrics),
        )
        # filter to keep only metrics that need to be calculated
        metric_statistic_combinations = {
            s: [
                m
                for m in metric_statistic_combinations[s]
                if m not in already_existing_metrics[s]
            ]
            for s in metric_statistic_combinations
        }
    else:
        logger.info(
            "Metric statistic combinations: %s",
            _pprint_ms(metric_statistic_combinations),
        )

    for stat in metric_statistic_combinations:
        # skip if no metrics are requested to be generated for statistic
        metrics = metric_statistic_combinations[stat]
        if not metrics:
            continue
        logger.info("Computing zonal aggregation for statistic=%s", stat)
        resampling = "remapdis" if stat == "sum" else "remapbil"
        logger.info(
            f"Resampling using CDO for {stat=} using {resampling=}: {paths[stat]} -> {resampled_paths[stat]}"
        )
        resample(
            resampling, paths[stat], get_worldpop(iso3, year), resampled_paths[stat]
        )
        with multiprocessing.Pool() as p:
            new_paths = list(
                p.map(
                    functools.partial(
                        population_weighted_aggregation,
                        statistic=stat,
                        iso3=iso3,
                        admin=admin,
                        year=year,
                    ),
                    metrics,
                )
            )
        if not keep_resampled:
            resampled_paths[stat].unlink()
        generated_paths += new_paths

    return generated_paths


@register_process("era5.prep_bias_correct", multiple_years=True)
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


def run_task(task: str, overwrite: bool = True) -> Path:
    """Runs task definition

    VNM-2000-2020-era5.spi.gamma -- gamma parameters
    VNM-2-2000-era5.spi -- SPI
    """

    from .spi import process_spi, process_spi_corrected, gamma_spi
    from .spei import process_spei_uncorrected, process_spei_corrected, gamma_spei

    parts = task.split("-")
    ystart, admin = None, None
    ds = None
    metric = parts.pop()
    if not metric.startswith("era5"):
        raise ValueError("run_task() is only defined for era5 metrics")
    metric = metric.removeprefix("era5.")
    year = int(parts.pop())
    ystart_or_admin = int(parts.pop())
    if ystart_or_admin not in [1, 2, 3]:
        ystart = ystart_or_admin
    else:
        admin = ystart_or_admin
    iso3 = parts.pop()
    bias_correct = "corrected" in task
    metric_stub = metric.replace("_corrected", "")
    if "gamma" in metric:
        output = get_path(
            "output", iso3, "era5", f"{iso3}-{ystart}-{year}-era5.{metric}.nc"
        )
    else:
        output = get_path(
            "output", iso3, "era5", f"{iso3}-{admin}-{year}-era5.{metric}.nc"
        )
    if not overwrite and output.exists():
        return output
    match (metric_stub, bias_correct):
        case ("spi.gamma", _):
            ds = gamma_spi(iso3, f"{ystart}-{year}", bias_correct=bias_correct)
        case ("spei.gamma", _):
            ds = gamma_spei(iso3, f"{ystart}-{year}", bias_correct=bias_correct)
        case ("spi", False):
            ds = process_spi(f"{iso3}-{admin}", str(year))
        case ("spi", True):
            ds = process_spi_corrected(f"{iso3}-{admin}", str(year))
        case ("spei", False):
            ds = process_spei_uncorrected(f"{iso3}-{admin}", str(year))
        case ("spei", True):
            ds = process_spei_corrected(f"{iso3}-{admin}", str(year))
    if ds is None:
        raise RuntimeError(f"Task processing failed for {task}")
    ds.to_netcdf(output)
    return output


def run_tasks(
    task_group: str, tasks: list[str], show_paths: bool = True, overwrite: bool = True
) -> list[Path]:
    "Runs a list of tasks"

    logger.info("[%s]  starting tasks: %s", task_group, ", ".join(tasks))
    with multiprocessing.Pool() as pool:
        paths = list(pool.map(functools.partial(run_task, overwrite=overwrite), tasks))
    if show_paths:
        logger.info("[%s] processed tasks: %s", task_group, ", ".join(tasks))
    else:
        logger.info("[%s] processed %d tasks", task_group, len(tasks))
    return paths


@register_process("era5", multiple_years=True)
def process_era5(
    iso3: str, date: str, skip_correction: bool = False, keep=False
) -> list[Path]:
    """Overall era5 processor; runs core metrics and SPI and SPEI

    Parameters
    ----------
    iso3 : str
        Country code or region name
    date : str
        Range of years to calculate ERA5 metrics
    keep : bool
        Whether to keep (not overwrite) existing generated files, default=False
    skip_correction : bool
        Whether to skip calculation of corrected metrics

    Returns
    -------
    list[Path]
        List of generated files
    """
    overwrite = not keep
    if keep:
        logger.warning("""Keeping existing files, this may lead to incorrect or not updated data.
        For final release, always run without 'keep'""")
    ystart, yend = parse_year_range(date, warn_duration_less_than_years=15)
    iso3, admin = iso3_admin_unpack(iso3)

    # Get population data for year range
    logger.info(f"Retrieving population at 1km grid (Worldpop) from {ystart}-{yend}")
    for year in range(ystart, yend + 1):
        get_worldpop(iso3, year)
    pool = get_dataset_pool(iso3)
    required_years = set(range(ystart, yend + 1))
    present_years = set(pool.years)
    if not required_years < present_years:
        raise FileNotFoundError(
            f"""Requested year range {ystart}-{yend}, but files missing for years: {", ".join(map(str, required_years - present_years))}
    Use `uv run dart-pipeline get era5 {iso3} <year>` to download data for <year>"""
        )
    if not skip_correction and (
        missing_tp_corrected := missing_tp_corrected_files(iso3, required_years)
    ):
        raise FileNotFoundError(
            "Calculation for bias corrected metrics requested, but missing tp_corrected_files:"
            + print_paths(missing_tp_corrected)
            + """

        See https://dart-pipeline.readthedocs.io/en/latest/bias-correction-precipitation.html on how to generate these files
        Alternatively, pass 'skip_correction' to skip calculating bias-corrected metrics"""
        )
    paths = []

    gamma_tasks = [
        f"{iso3}-{ystart}-{yend}-era5.{index}.gamma" for index in ["spi", "spei"]
    ]
    if not skip_correction:
        gamma_tasks += [
            f"{iso3}-{ystart}-{yend}-era5.{index}_corrected.gamma"
            for index in ["spi", "spei"]
        ]

    # Run gamma parameter estimation first, required for SPI and SPEI index calculations later
    paths += run_tasks("GAMMA", gamma_tasks, overwrite=overwrite)

    index_tasks = [
        f"{iso3}-{admin}-{year}-era5.{index}"
        for year in range(ystart, yend + 1)
        for index in ["spi", "spei"]
    ]
    if not skip_correction:
        index_tasks += [
            f"{iso3}-{admin}-{year}-era5.{index}_corrected"
            for year in range(ystart, yend + 1)
            for index in ["spi", "spei"]
        ]

    paths += run_tasks("INDEX", index_tasks, overwrite=overwrite)

    # Run core metrics -- there is already parallelisation within each year, so
    # we don't parallelise processing further
    for year in range(ystart, yend + 1):
        paths += era5_process_core(f"{iso3}-{admin}", str(year), overwrite=overwrite)

    return paths
