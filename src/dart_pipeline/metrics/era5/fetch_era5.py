"""
Collate module for API-based retrievals.

These require direct downloads to file.
"""

import datetime
from typing import Literal, TypedDict
from functools import cache
from pathlib import Path
from typing import TypedDict

import cdsapi
from geoglue.country import Country

from ...metrics import get_metrics
from ...util import source_path

DAYS = [f"{i:02d}" for i in range(1, 32)]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
TIMES = [f"{i:02d}:00" for i in range(1, 24)]
DailyStatistic = Literal["daily_mean", "daily_min", "daily_max", "daily_sum"]
Resampling = Literal["remapdis", "remapbil"]


class CDSRequest(TypedDict):
    product_type: list[str]
    variable: list[str]
    year: list[str]
    month: list[str]
    day: list[str]
    time: list[str]
    area: list[int]
    data_format: Literal["netcdf"]
    download_format: Literal["unarchived"]


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


def era5_request(iso3: str, year: int) -> CDSRequest:
    "Returns cdsapi request dictionary for a given country ISO3, year"
    C = Country(iso3, fetch_data=False)
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


def era5_fetch_hourly(iso3: str, year: int) -> Path | None:
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

    Returns
    -------
        Path of netCDF file that was written to disk
    """
    cur_year = datetime.datetime.now().year
    if year < 1940 or year > cur_year:
        raise ValueError(f"ERA5 reanalysis data only available from 1940-{cur_year}")
    client = cdsapi.Client()
    outfile = source_path("meteorological/era5-reanalysis", f"{iso3}-{year}-era5.zip")
    if not outfile.parent.exists():
        outfile.parent.mkdir(parents=True)
    client.retrieve(
        "reanalysis-era5-single-levels",
        era5_request(iso3, year),
        outfile,
    )
    if outfile.exists():
        return outfile
