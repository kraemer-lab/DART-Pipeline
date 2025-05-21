"""Utility library for DART pipeline."""

import copy
import json

import os
import sys
import shutil
import datetime
import calendar
import logging
from datetime import timedelta
from typing import Generator, Literal
from functools import cache
from pathlib import Path

import gzip
import pandas as pd
import py7zr
import pycountry
import requests
import xarray as xr
import geoglue.util
import geoglue.region
import geoglue.zonal_stats
from geoglue.memoryraster import MemoryRaster

from .constants import (
    COMPRESSED_FILE_EXTS,
)
from .types import Credentials, URLCollection
from .paths import get_path

logger = logging.getLogger(__name__)

VALID_ISO3 = [c.alpha_3 for c in pycountry.countries]

# Pandas display options
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", 40)
pd.set_option("display.width", 228)  # sierra


def zonal_stats(
    metric: str,
    unit: str,
    da: xr.DataArray,
    region: geoglue.region.Region,
    operation: str = "mean(coverage_weight=area_spherical_km2)",
    weights: MemoryRaster | None = None,
    include_cols: list[str] | None = None,
    fix_array: bool = False,
) -> pd.DataFrame:
    """Return zonal statistics for a particular DataArray.

    This is a wrapper around geoglue.zonal_stats to add metadata attributes
    such as metric, unit and region name to the dataframe.

    Parameters
    ----------
    metric : str
        Name of metric
    unit : str
        Unit of metric in udunits terminology. Unitless metrics should
        be assigned a unit of "1"
    da : xr.DataArray
        xarray DataArray to perform zonal statistics on. Must have
        'latitude', 'longitude' and a time coordinate
    region : geoglue.region.Region
        Region for which to calculate zonal statistics
    operation : str
        Zonal statistics operation. For a full list of operations, see
        https://isciences.github.io/exactextract/operations.html. Default
        operation is to calculate the mean with a spherical area coverage weight.
    weights : MemoryRaster | None
        Optional, if specified, uses the specified raster to perform weighted
        zonal statistics.
    include_cols : list[str] | None
        Optional, if specified, only includes these columns. If not specified,
        returns all columns except the geometry column
    fix_array : bool
        Whether to perform pre-processing steps, such as sorting longitude, latitude
        and setting CF-compliant attributes. These should not be required
        when processing downloaded weather data which should already be in
        compliant format. Optional, default=False

    Returns
    -------
    pd.DataFrame
        The DataFrame specified by the geometry in the `region` parameter, one
        additional column, `value` containing the zonal statistic for the corresponding geometry.
    """
    if fix_array:
        da = geoglue.util.sort_lonlat(da)  # type: ignore
        geoglue.util.set_lonlat_attrs(da)  # type: ignore
    geom = geoglue.region.read_region(region)
    df = geoglue.zonal_stats.zonal_stats(
        da, geom, operation, weights, include_cols=include_cols
    )
    df["region"] = region["name"]
    df["unit"] = unit
    df["metric"] = metric
    return df


def determine_netcdf_filename(metric: str, **kwargs) -> str:
    """Determines output netcdf file for a processor that returns xr.Dataset

    kwargs is expected to have `iso3`, and *may* have `date`. The output filename is determined as follows:

        ISO3-DATE-metric-KWARG_VALUES.nc

    where KWARG_VALUES is a hyphen delimited list of values in the rest of kwargs
    """
    iso3 = kwargs.pop("iso3")
    out = iso3
    if "date" in kwargs:
        out += "-" + kwargs.pop("date")
    out += "-" + metric
    if kwargs:
        out += "." + ".".join(kwargs.values())
    return out + ".nc"


def raise_on_missing_variables(ds: xr.Dataset, required_vars: list[str]):
    "Raises a ValueError if required variables are missing"
    vars = set(ds.variables) - set(ds.coords)
    if not set(required_vars) <= set(vars):
        raise ValueError("Required variables missing in dataset: {required_vars}")


def get_admin_from_dataframe(df: pd.DataFrame) -> int:
    "Gets admin level (1, 2, or 3) from data"

    if df.attrs.get("admin"):
        return df.attrs["admin"]
    if "GID_1" in df.columns:
        return max(i for i in (1, 2, 3) if f"GID_{i}" in df.columns)


def logfmt(d: dict) -> str:
    parts = []
    for k, v in d.items():
        if isinstance(v, str):
            if any(c in v for c in ' ="'):
                v = '"' + v.replace('"', '\\"') + '"'
        else:
            v = str(v)
        parts.append(f"{k}={v}")
    return " ".join(parts)


def iso3_admin_unpack(iso3_admin: str) -> tuple[str, int]:
    "Unpacks iso3-admin_level with verification"
    try:
        iso3, admin = iso3_admin.split("-")
    except ValueError:
        raise ValueError(
            "ISO3 and admin code should be specified concatenated with a hyphen, e.g. VNM-3, PER-2"
        )
    iso3 = iso3.upper()
    if iso3 not in VALID_ISO3:
        raise LookupError(f"Not a valid ISO3 code {iso3=}")
    admin = int(admin)
    if admin not in [1, 2, 3]:
        raise ValueError("Not a valid admin level, must be one of 1, 2, 3")
    return iso3, admin


def abort(bold_text: str, rest: str):
    print(f"❗ \033[1m{bold_text}\033[0m {rest}")
    sys.exit(1)


@cache
def days_in_year(year: int) -> Literal[365, 366]:
    "Returns number of days in year"
    return 366 if calendar.isleap(year) else 365


def get_country_name(iso3: str, common_name=True) -> str:
    if (country := pycountry.countries.get(alpha_3=iso3)) is None:
        raise ValueError(f"Country ISO3 not found: {iso3}")
    # By default, the function tries to return the country's common name
    if common_name:
        try:
            return country.common_name
        except AttributeError:
            return country.name
    # The default can be overridden and the country's "name" (not
    # "common name") can be returned explicitly
    else:
        return country.name


def only_one_from_collection(coll: URLCollection) -> URLCollection:
    coll_copy = copy.deepcopy(coll)
    coll_copy.files = [coll_copy.files[0]]
    return coll_copy


def use_range(value: int | float, min: int | float, max: int | float, message: str):
    if not min <= value <= max:
        raise ValueError(f"{message}: {min}-{max}")


def daterange(
    start_date: datetime.date, end_date: datetime.date
) -> Generator[datetime.date, None, None]:
    """Construct a date range for iterating over the days between two dates."""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def get_credentials(source: str, credentials: str | Path | None = None) -> Credentials:
    """
    Get a username and password pair from a credentials.json file.

    Parameters
    ----------
    source : str
        Name of the field that is accessible online but which is
        password-protected.
    credentials : str, pathlib.Path or None, default None
        Path (including filename) to the credentials file if different from the
        default (which is `credentials.json` in the `DART-Pipeline` directory).

    Returns
    -------
    username, password : str
        The username and password associated with the entry in the credentials
        file will be returned.

    Examples
    --------
    >>> get_credentials('aphrodite/daily-precip-v1901')
    ('example@email.com', '*******')

    Using an environment variable to store the credentials:

    $ export CREDENTIALS_JSON='{
        "APHRODITE Daily accumulated precipitation (V1901)": {
            "username": "example@email.com",
            "password": "*******"
        }
    }'
    $ python3
    >>> from dart_pipeline.util import get_credentials
    >>> metric = 'aphrodite/daily-precip-v1901'
    >>> get_credentials(metric, credentials='environ')
    ('example@email.com', '*******')
    """

    def credentials_from_string(s: str, source: str) -> tuple[str, str]:
        data = json.loads(s)
        if source in data:
            return data[source]["username"], data[source]["password"]
        else:
            raise KeyError("metric={metric!r} not found in credentials")

    # read credentials from environment if present
    if credentials_env := os.getenv("CREDENTIALS_JSON"):
        return credentials_from_string(credentials_env, source)

    # fallback to file if no environment variable set
    if credentials and not Path(credentials).exists():
        raise FileNotFoundError(
            """No credentials.json file was found. Either you have
                not created one or it is not in the specified location
                (the default location is the DART-Pipeline folder"""
        )
    else:
        return credentials_from_string(Path(credentials).read_text(), source)


def download_file(
    url: str,
    path: Path,
    auth: Credentials | None = None,
    unpack: bool = True,
    unpack_create_folder: bool = False,
) -> bool:
    """Download a file from a given URL to a given path."""
    if (r := requests.get(url, auth=auth)).status_code == 200:
        with open(path, "wb") as out:
            for bits in r.iter_content():
                out.write(bits)
        # Unpack file
        if unpack and any(str(path).endswith(ext) for ext in COMPRESSED_FILE_EXTS):
            logger.info(f"Unpacking downloaded file {path}")
            unpack_file(path, same_folder=not unpack_create_folder)
        return True
    else:
        logger.error(f"Failed to fetch {url}, status={r.status_code}")
        return False


def download_files(
    links: URLCollection,
    out_dir: Path,
    auth: Credentials | None = None,
    unpack: bool = True,
) -> list[bool]:
    """Download multiple files in a list."""
    out_dir = out_dir / links.relative_path
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    successes = []
    for file in links.files:
        url = links.base_url + "/" + file
        out_filepath = out_dir / Path(file).name
        successes.append(download_file(url, out_filepath, auth, unpack=unpack))

    return successes


def unpack_file(path: Path | str, same_folder: bool = False):
    """Unpack a zipped file."""
    path = Path(path)
    logger.info("unpacking:%s", path)
    logger.info("same_folder:%s", same_folder)
    match path.suffix:
        case ".7z":
            with py7zr.SevenZipFile(path, mode="r") as archive:
                archive.extractall(
                    path.parent if same_folder else path.parent / path.stem
                )
        case ".f90":
            pass
        case ".gpkg":
            pass
        case ".json":
            pass
        case ".gz":
            with gzip.open(path, "rb") as f_in:
                if same_folder:
                    extract_path = path.with_suffix("")
                else:
                    folder = str(path.name).replace(".gz", "")
                    file = str(path.name).replace(".gz", "")
                    extract_path = path.parent / Path(folder) / Path(file)
                    extract_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info("extract_path:%s", extract_path)
                try:
                    with open(extract_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                except gzip.BadGzipFile:
                    print(f"BadGzipFile: Not a gzipped file ({path.name})")
            return
        case _:
            extract_dir = path.parent if same_folder else path.parent / path.stem
            shutil.unpack_archive(path, str(extract_dir))


def get_shapefile(iso3: str, admin_level: Literal["0", "1", "2", "3"]) -> Path:
    """Get a shape file."""
    return get_path("sources", "gadm") / iso3 / f"gadm41_{iso3}_{admin_level}.shp"
