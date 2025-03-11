"""
ERA5 Fetch module
"""

import zipfile
import datetime
from functools import cache
from pathlib import Path
from typing import Literal

import cdsapi
from geoglue.country import Country

from ...metrics import get_metrics
from ...util import source_path

from .types import ERA5HourlyPath, CDSRequest

DAYS = [f"{i:02d}" for i in range(1, 32)]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
TIMES = [f"{i:02d}:00" for i in range(24)]
ERA5_HOURLY_ACCUM_FILE = "data_stream-oper_stepType-accum.nc"
ERA5_HOURLY_INSTANT_FILE = "data_stream-oper_stepType-instant.nc"

DailyStatistic = Literal["daily_mean", "daily_min", "daily_max", "daily_sum"]
Resampling = Literal["remapdis", "remapbil"]


@cache
def era5_variables() -> list[str]:
    """This method loops through metric metadata and finds dependencies for era5
    metrics, collating them into a variables list. This list is then used to
    make a request that downloads all required variables for a particular year."""

    variables = set()
    for metric in (metrics := get_metrics("era5")):
        if depends := metrics[metric].get("depends"):
            variables.update(depends)
        else:
            variables.add(metric.removeprefix("era5."))
    return sorted(variables)


def era5_request(
    iso3: str, year: int, backend: Literal["gadm", "geoboundaries"] = "gadm"
) -> CDSRequest:
    "Returns cdsapi request dictionary for a given country ISO3, year"
    C = Country(iso3, backend=backend)
    return {
        "product_type": ["reanalysis"],
        "variable": era5_variables(),
        "year": [str(year)],
        "month": MONTHS,
        "day": DAYS,
        "time": TIMES,
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": C.era5_extents,
    }


def era5_extract_hourly_data(file: Path, extract_path: Path) -> ERA5HourlyPath:
    "Extracts hourly data from downloaded zip file"
    if file.suffix != ".zip":
        raise ValueError(f"Not a valid zip {file=}")
    instant_file, accum_file = None, None
    with zipfile.ZipFile(file, "r") as zf:
        zf.extractall(extract_path / file.stem)
    if (accum_file := extract_path / file.stem / ERA5_HOURLY_ACCUM_FILE).exists():
        accum_file = accum_file.rename(extract_path / (file.stem + ".accum.nc"))
    if (instant_file := extract_path / file.stem / ERA5_HOURLY_INSTANT_FILE).exists():
        instant_file = instant_file.rename(extract_path / (file.stem + ".instant.nc"))
    if instant_file or accum_file:
        return ERA5HourlyPath(instant=instant_file, accum=accum_file)
    else:
        raise ValueError(f"Error extracting hourly data from {file=}")


def era5_fetch_hourly(
    iso3: str, year: int, skip_exists: bool = True
) -> ERA5HourlyPath | None:
    """Fetches ERA5 data for a particular statistic and resampling combination

    ERA5 data is fetched in groups of variables, one corresponding to each
    statistic and resampling combination obtained from :meth:`get_groups`.
    After fetching, data is resampled to the ``target`` MemoryRaster (in our
    case, population) and written to disk.

    An API key is needed for this function to work, see instructions at
    https://cds.climate.copernicus.eu/how-to-api

    Parameters
    ----------
    iso3
        ISO3 code, used to request data in the appropriate extent
    year
        Data is downloaded for this year
    skip_exists
        Skip downloading if zipfile or extracted contents exist, default True

    Returns
    -------
        Path of netCDF file that was written to disk
    """
    cur_year = datetime.datetime.now().year
    if year < 1940 or year > cur_year:
        raise ValueError(f"ERA5 reanalysis data only available from 1940-{cur_year}")
    folder = source_path("meteorological/era5-reanalysis")
    outfile = folder / f"{iso3}-{year}-era5.zip"
    accum_file = folder / f"{iso3}-{year}-era5.accum.nc"
    instant_file = folder / f"{iso3}-{year}-era5.instant.nc"
    if accum_file.exists() and instant_file.exists():
        return ERA5HourlyPath(instant=instant_file, accum=accum_file)

    if not outfile.parent.exists():
        outfile.parent.mkdir(parents=True)
    if not skip_exists or not outfile.exists():
        client = cdsapi.Client()
        client.retrieve(
            "reanalysis-era5-single-levels",
            era5_request(iso3, year),
            outfile,
        )
    if outfile.exists():
        return era5_extract_hourly_data(outfile, folder)
