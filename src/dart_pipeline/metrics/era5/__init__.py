import logging
import functools
import multiprocessing
from pathlib import Path
from typing import Literal

import xarray as xr
import numpy as np

from geoglue.region import gadm
from geoglue.cds import ReanalysisSingleLevels, CdsPath
from tqdm import trange

from ...metrics import (
    register_metrics,
    register_fetch,
    register_process,
    print_paths,
)
from ...metrics.worldpop import get_worldpop
from ...util import iso3_admin_unpack, msg
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
def era5_fetch(iso3: str, date: str) -> CdsPath | None:
    iso3 = iso3.upper()
    year = int(date)
    prompt_cdsapi_key()
    data = ReanalysisSingleLevels(
        gadm(iso3, 1), VARIABLES, path=get_path("sources", iso3, "era5")
    )
    return data.get(year)


@register_process("era5.prep_bias_correct", multiple_years=True)
def prep_bias_correct(iso3: str, date: str) -> xr.Dataset:
    try:
        ystart, yend = date.split("-")
        ystart, yend = int(ystart), int(yend)
    except ValueError:
        raise ValueError("Date must be specified as a year range, e.g. 2000-2020")

    pool = get_dataset_pool(iso3)

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
            "output", iso3, "era5", f"{iso3}-{admin}-{year}-era5.{metric}.weekly_sum.nc"
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

    with multiprocessing.Pool() as pool:
        paths = list(pool.map(functools.partial(run_task, overwrite=overwrite), tasks))
    if show_paths:
        logger.info("[%s] processed tasks: %s", task_group, ", ".join(tasks))
    else:
        logger.info("[%s] processed %d tasks", task_group, len(tasks))
    return paths


@register_process("era5", multiple_years=True)
def process_era5(
    iso3: str,
    date: str,
    skip_correction: bool = False,
    temporal_resolution: Literal["weekly", "daily"] = "daily",
    overwrite=False,
) -> list[Path]:
    """Overall era5 processor; runs core metrics and SPI and SPEI

    Parameters
    ----------
    iso3 : str
        Country code or region name
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
    iso3, admin = iso3_admin_unpack(iso3)
    region = gadm(iso3, admin)
    yrange_str = f"{ystart}-{yend}"

    # Get population data for year range
    msg("==> Retrieving Worldpop population:", yrange_str)
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

    gamma_tasks = [
        f"{iso3}-{ystart}-{yend}-era5.{index}.gamma" for index in ["spi", "spei"]
    ]
    if not skip_correction:
        gamma_tasks += [
            f"{iso3}-{ystart}-{yend}-era5.{index}_corrected.gamma"
            for index in ["spi", "spei"]
        ]

    # Run gamma parameter estimation first, required for SPI and SPEI index calculations later
    msg("==> Estimating gamma parameters:", yrange_str)
    paths = run_tasks("GAMMA", gamma_tasks, overwrite=overwrite)

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
                    "output", iso3, "era5", f"{iso3}-{admin}-{year}-era5.core_weekly.nc"
                )
                if overwrite or not y_output.exists():
                    y_zs = era5_process_core_weekly(f"{iso3}-{admin}", str(year))
                    y_zs.to_netcdf(y_output)
                paths.append(y_output)

            msg("==> Collating metrics:", yrange_str)
            ds = MetricCollection(f"{iso3}-{admin}").collate((ystart, yend))
            output = get_path(
                "output", iso3, "era5", f"{iso3}-{admin}-{ystart}-{yend}-era5.nc"
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
                    "output", iso3, "era5", f"{iso3}-{admin}-{year}-era5.core_daily.nc"
                )
                gen_paths = []
                if overwrite or not y_output.exists():
                    gen_paths = era5_process_core_daily(f"{iso3}-{admin}", str(year))
                    # y_zs.to_netcdf(y_output)
                paths.extend(gen_paths)
            return paths
