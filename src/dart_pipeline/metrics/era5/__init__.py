import logging
import functools
import multiprocessing
from pathlib import Path
from typing import Literal

from geoglue.region import AdministrativeLevel, ZonedBaseRegion
import xarray as xr
import numpy as np

from geoglue.cds import ReanalysisSingleLevels, CdsPath
from tqdm import trange

from ...metrics import (
    register_metrics,
    register_fetch,
    register_process,
    print_paths,
)
from ...metrics.worldpop import get_worldpop
from ...util import get_region, msg
from ...paths import get_path

from .util import (
    get_dataset_pool,
    prompt_cdsapi_key,
    relative_humidity_from_arrays,
    parse_year_range,
    missing_tp_corrected_files,
)
from .list_metrics import (
    METRICS,
    VARIABLES,
)
from .collate import MetricCollection
from .core_weekly import era5_process_core_weekly
from .core_daily import era5_process_core_daily

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


@register_fetch("era5")
def era5_fetch(region: ZonedBaseRegion, date: str) -> CdsPath | None:
    year = int(date)
    prompt_cdsapi_key()
    data = ReanalysisSingleLevels(
        region, VARIABLES, path=get_path("sources", region.name, "era5")
    )
    return data.get(year)


@register_process("era5.prep_bias_correct", multiple_years=True)
def prep_bias_correct(region: ZonedBaseRegion, date: str) -> xr.Dataset:
    try:
        ystart, yend = date.split("-")
        ystart, yend = int(ystart), int(yend)
    except ValueError:
        raise ValueError("Date must be specified as a year range, e.g. 2000-2020")

    pool = get_dataset_pool(region)

    def _prep_year(year: int) -> xr.Dataset:
        cds = pool[year]
        print(cds.instant)
        t2m = (
            cds.instant.t2m.resample(valid_time="D")
            .mean()
            .rename({"valid_time": "time"})
        )

        # there is no d2m (dewpoint temperature), so we will create a random array
        diff = xr.DataArray(
            np.random.uniform(0, 10, size=t2m.shape), dims=t2m.dims, coords=t2m.coords
        )
        # and subtract this from t2m as dewpoint temperature is
        # always less than air temperature
        d2m = (t2m - diff).rename("d2m")
        tp = cds.accum.tp.rename({"valid_time": "time"}).resample(time="D").sum()
        r = relative_humidity_from_arrays(t2m, d2m).astype("float32")
        r.attrs.update(
            {
                "standard_name": "relative_humidity",
                "long_name": "Relative humidity",
                "units": "percent",
            }
        )
        return xr.Dataset({"t2m": t2m, "r": r, "tp": tp})

    ds = _prep_year(ystart)
    for y in range(ystart + 1, yend + 1):
        ds = xr.concat([ds, _prep_year(y)], dim="time")
    return ds


def run_task(task: str, overwrite: bool = True) -> Path:
    """Runs task definition

    VNM-2000-2020-era5.spi.gamma -- gamma parameters
    VNM-2-2000-era5.spi -- SPI
    """

    from .spi import process_spi, process_spi_corrected, gamma_spi
    from .spei import process_spei_uncorrected, process_spei_corrected, gamma_spei

    parts = task.split("-")
    ystart = None
    ds = None
    metric = parts.pop()
    if not metric.startswith("era5"):
        raise ValueError("run_task() is only defined for era5 metrics")
    metric = metric.removeprefix("era5.")
    year = int(parts.pop())
    ystart_or_adm = int(parts.pop())
    adm = None
    if ystart_or_adm not in [1, 2, 3]:
        ystart = ystart_or_adm
    else:
        adm = ystart_or_adm
    _region_arg = parts.pop()
    region = get_region(_region_arg) if isinstance(_region_arg, str) else _region_arg
    if adm:  # select a specific AdministrativeLevel
        region = region.admin(adm)

    bias_correct = "corrected" in task
    metric_stub = metric.replace("_corrected", "")
    if "gamma" in metric:
        output = get_path(
            "output",
            region.name,
            "era5",
            f"{region.name}-{ystart}-{year}-era5.{metric}.nc",
        )
    else:
        output = get_path(
            "output",
            region.name,
            "era5",
            f"{region.name}-{region.admin}-{year}-era5.{metric}.weekly_sum.nc",
        )
    if not overwrite and output.exists():
        return output
    match (metric_stub, bias_correct):
        case ("spi.gamma", _):
            ds = gamma_spi(region, f"{ystart}-{year}", bias_correct=bias_correct)
        case ("spei.gamma", _):
            ds = gamma_spei(region, f"{ystart}-{year}", bias_correct=bias_correct)
        case ("spi", False):
            assert isinstance(region, AdministrativeLevel)
            ds = process_spi(region, str(year))
        case ("spi", True):
            assert isinstance(region, AdministrativeLevel)
            ds = process_spi_corrected(region, str(year))
        case ("spei", False):
            assert isinstance(region, AdministrativeLevel)
            ds = process_spei_uncorrected(region, str(year))
        case ("spei", True):
            assert isinstance(region, AdministrativeLevel)
            ds = process_spei_corrected(region, str(year))
    if ds is None:
        raise RuntimeError(f"Task processing failed for {task}")
    ds.to_netcdf(output)
    return output


def run_tasks(
    task_group: str, tasks: list[str], show_paths: bool = True, overwrite: bool = True
) -> list[Path]:
    "Runs a list of tasks"

    with multiprocessing.Pool() as pool:
        paths = list(pool.map(functools.partial(run_task, overwrite=overwrite), tasks))
    if show_paths:
        logger.info("[%s] processed tasks: %s", task_group, ", ".join(tasks))
    else:
        logger.info("[%s] processed %d tasks", task_group, len(tasks))
    return paths


@register_process("era5", multiple_years=True)
def process_era5(
    region: AdministrativeLevel,
    date: str,
    skip_correction: bool = False,
    temporal_resolution: Literal["weekly", "daily"] = "daily",
    overwrite=False,
) -> list[Path]:
    """Overall era5 processor; runs core metrics and SPI and SPEI

    Parameters
    ----------
    region : AdministrativeLevel
        Administrative region to process
    date : str
        Range of years to calculate ERA5 metrics
    temporal_resolution : Literal['weekly', 'daily']
        Temporal resolution at which core metrics must be processed,
        must be one of 'daily' or 'weekly'. When processing at daily
        resolution, files are not combined as SPI and SPEI are always
        calculated at weekly resolution
    skip_correction : bool
        Whether to skip calculation of corrected metrics
    overwrite : bool
        Whether to overwrite existing generated files, default=False
    Returns
    -------
    list[Path]
        List of generated files
    """
    if not overwrite:
        logger.warning("""Keeping existing files, this may lead to incorrect or not updated data.
        For final release, always run with 'overwrite'""")
    ystart, yend = parse_year_range(date, warn_duration_less_than_years=15)
    yrange_str = f"{ystart}-{yend}"

    # Get population data for year range
    msg("==> Retrieving Worldpop population:", yrange_str)
    for year in range(ystart, yend + 1):
        get_worldpop(region, year)
    pool = get_dataset_pool(region)
    required_years = set(range(ystart, yend + 1))
    present_years = set(pool.years)
    if not required_years < present_years:
        raise FileNotFoundError(
            f"""Requested year range {ystart}-{yend}, but files missing for years: {", ".join(map(str, required_years - present_years))}
    Use `uv run dart-pipeline get era5 {region} <year>` to download data for <year>"""
        )
    if not skip_correction and (
        missing_tp_corrected := missing_tp_corrected_files(region.name, required_years)
    ):
        raise FileNotFoundError(
            "Calculation for bias corrected metrics requested, but missing tp_corrected_files:"
            + print_paths(missing_tp_corrected)
            + """

        See https://dart-pipeline.readthedocs.io/en/latest/bias-correction-precipitation.html on how to generate these files
        Alternatively, pass 'skip_correction' to skip calculating bias-corrected metrics"""
        )

    gamma_tasks = [
        f"{region.name}-{ystart}-{yend}-era5.{index}.gamma" for index in ["spi", "spei"]
    ]
    if not skip_correction:
        gamma_tasks += [
            f"{region.name}-{ystart}-{yend}-era5.{index}_corrected.gamma"
            for index in ["spi", "spei"]
        ]

    # Run gamma parameter estimation first, required for SPI and SPEI index calculations later
    msg("==> Estimating gamma parameters:", yrange_str)
    paths = run_tasks("GAMMA", gamma_tasks, overwrite=overwrite)

    index_tasks = [
        f"{region.name}-{region.admin}-{year}-era5.{index}"
        for year in range(ystart, yend + 1)
        for index in ["spi", "spei"]
    ]
    if not skip_correction:
        index_tasks += [
            f"{region.name}-{region.admin}-{year}-era5.{index}_corrected"
            for year in range(ystart, yend + 1)
            for index in ["spi", "spei"]
        ]

    msg("==> Calculating SPI and SPEI:", yrange_str)
    paths += run_tasks("INDEX", index_tasks, overwrite=overwrite)

    # Run core metrics -- there is already parallelisation within each year, so
    # we don't parallelise processing further
    # TODO: insert hook to run weekly agg
    match temporal_resolution:
        case "weekly":
            msg("==> Calculating core metrics (weekly):", yrange_str)
            for year in trange(ystart, yend + 1, desc="era5.core_weekly"):
                y_output = get_path(
                    "output",
                    region.name,
                    "era5",
                    f"{region.name}-{region.admin}-{year}-era5.core_weekly.nc",
                )
                if overwrite or not y_output.exists():
                    y_zs = era5_process_core_weekly(region, str(year))
                    y_zs.to_netcdf(y_output)
                paths.append(y_output)

            msg("==> Collating metrics:", yrange_str)
            ds = MetricCollection(f"{region.name}-{region.admin}").collate(
                (ystart, yend)
            )
            output = get_path(
                "output",
                region.name,
                "era5",
                f"{region.name}-{region.admin}-{ystart}-{yend}-era5.nc",
            )
            ds.attrs["DART_region"] = (
                f"{region.name} {region.pk} {region.tz} {region.bbox.int()}"
            )
            ds.to_netcdf(output)
            return [output]
        case "daily":
            msg("==> Calculating core metrics (daily):", yrange_str)
            for year in trange(ystart, yend + 1, desc="era5.core_daily"):
                y_output = get_path(
                    "output",
                    region.name,
                    "era5",
                    f"{region.name}-{region.admin}-{year}-era5.core_daily.nc",
                )
                gen_paths = []
                if overwrite or not y_output.exists():
                    gen_paths = era5_process_core_daily(region, str(year))
                    # y_zs.to_netcdf(y_output)
                paths.extend(gen_paths)
            return paths
