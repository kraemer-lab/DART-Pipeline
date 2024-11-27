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
from collections.abc import Iterable

from lxml import html
import gzip
import pandas as pd
import py7zr
import pycountry
import requests

from .constants import (
    DEFAULT_SOURCES_ROOT,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PLOTS_ROOT,
    COMPRESSED_FILE_EXTS,
    OUTPUT_COLUMNS
)
from .types import Credentials, URLCollection, DefaultPathProtocol

# Pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 40)
pd.set_option('display.width', 228)  # sierra


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
    credentials_path = (
        Path("credentials.json") if credentials is None else Path(credentials)
    )
    if not credentials_path.exists():
        raise FileNotFoundError(
            """No credentials.json file was found. Either you have
                not created one or it is not in the specified location
                (the default location is the DART-Pipeline folder"""
        )
    return credentials_from_string(credentials_path.read_text(), source)


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
            logging.info(f"Unpacking downloaded file {path}")
            unpack_file(path, same_folder=not unpack_create_folder)
        return True
    else:
        logging.error(f"Failed to fetch {url}, status={r.status_code}")
        return False


def download_files(
    links: URLCollection, out_dir: Path, auth: Credentials | None = None,
    unpack: bool = True
) -> list[bool]:
    """Download multiple files in a list."""
    out_dir = out_dir / links.relative_path
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    successes = []
    for file in links.files:
        url = links.base_url + "/" + file
        out_filepath = out_dir / Path(file).name
        successes.append(download_file(url, out_filepath, auth, unpack))

    return successes


def default_path_getter(env_var: str, default: str) -> DefaultPathProtocol:
    def default_path(source: str, path: str | Path | None = None) -> Path:
        root = Path(os.getenv(env_var, default))
        return root / source / path if path else root / source

    return default_path


source_path = default_path_getter("DART_PIPELINE_SOURCES", DEFAULT_SOURCES_ROOT)
output_path = default_path_getter("DART_PIPELINE_OUTPUT", DEFAULT_OUTPUT_ROOT)
plots_path = default_path_getter("DART_PIPELINE_PLOTS", DEFAULT_PLOTS_ROOT)


def walk(
    url: str, out_dir: Path, auth: Credentials | None = None
) -> list[tuple[str, list[str]]]:
    """
    Re-create `os.walk` and `Path.walk` for use with a website.

    Does not actually download the files, instead returns a list which
    can be passed to download_files()

    Parameters
    ----------
    url
        The base URL of the website being accessed.
    out_dir
        The folder to which the data will be downloaded.
    auth
        Credentials associated with the walk if required
    """
    # In general, use strings for creating and handling URLs as opposed to
    # urllib's URL objects or path objects.
    # - The requests.get() function expects a string
    # - The urllib module can be useful for more advanced URL manipulation but
    #   always use the urlunparse() function to convert back into a string
    # - Avoid os.path.join() because on Windows machines the slash will be the
    #   wrong way around
    # We want to be able to identify the parent URL
    idx = url.rindex("/")
    parent_url = url[:idx]

    if (page := requests.get(url, auth=auth)).status_code != 200:
        logging.warning(f"Status code {page.status_code}")
    webpage = html.fromstring(page.content)
    links = webpage.xpath("//a/@href")
    # Classify the links on the page
    sorters = []
    download_list = []
    children = []
    parents = []
    files = []
    if not isinstance(links, Iterable):
        raise ValueError(f"No links found in URL: {url}")
    for link in links:
        if not isinstance(link, str):
            continue
        if link in ["?C=N;O=D", "?C=M;O=A", "?C=S;O=A", "?C=D;O=A"]:
            sorters.append(link)
        elif link.endswith("/"):
            if parent_url.endswith(link.removesuffix("/")):
                # This is the parent's URL
                parents.append(link)
            else:
                children.append(link)
        else:
            files.append(link)
    # Remove hidden files
    files = [x for x in files if not x.startswith(".")]
    download_list = [(url, files)]

    for child in children:
        url_new = url + "/" + child.removesuffix("/")
        download_list += walk(url_new, out_dir, auth)
    return download_list


def bold_brackets(s: str) -> str:
    """Given a text with brackets such as [this], renders it in bold font"""
    return s.replace("[", "\033[1m").replace("]", "\033[0m")


def unpack_file(path: Path | str, same_folder: bool = False):
    """Unpack a zipped file."""
    path = Path(path)
    if str(path).endswith('.gz'):
        with gzip.open(path, 'rb') as f_in:
            if same_folder:
                extract_path = path.with_suffix('')
            else:
                folder = str(path.name).replace('.gz', '')
                file = str(path.name).replace('.gz', '')
                extract_path = path.parent / Path(folder) / Path(file)
                extract_path.parent.mkdir(parents=True, exist_ok=True)
            with open(extract_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return
    match path.suffix:
        case '.7z':
            with py7zr.SevenZipFile(path, mode="r") as archive:
                archive.extractall(
                    path.parent if same_folder else path.parent / path.stem
                )
        case '.f90':
            pass
        case _:
            extract_dir = path.parent if same_folder else path.parent / path.stem
            shutil.unpack_archive(path, str(extract_dir))


def update_or_create_output(
    new_df: pd.DataFrame, out: str | Path, return_df=False
):
    """Either update an existing CSV or create a new one."""
    # Validate the input
    if not isinstance(new_df, pd.DataFrame):
        raise TypeError('Expected a pandas DataFrame as input')
    if not isinstance(out, (str, Path)):
        raise TypeError('Expected a valid file path')

    # Check if the CSV file already exists
    if out.exists():
        # Import the existing CSV with everything as a string
        old_df = pd.read_csv(out, dtype=str)
        old_df.fillna('', inplace=True)
        # Convert the new data frame to str
        new_df = new_df.astype(str)
        new_df.fillna('', inplace=True)

        # Merge the new data with the existing data
        key_columns = OUTPUT_COLUMNS
        key_columns.remove('value')
        merged_df = old_df.merge(
            new_df, on=key_columns, suffixes=('_old', '_new'), how='outer'
        )

        # Update values where creation_date is the same but value is different
        updated_values = merged_df['value_new'].notna() & \
            (merged_df['value_old'] != merged_df['value_new'])
        merged_df.loc[updated_values, 'value_old'] = merged_df['value_new']

        # Keep the updated/old values and drop the helper column
        merged_df = merged_df.rename(columns={'value_old': 'value'})
        df = merged_df.drop(columns=['value_new'])
        df = df.drop_duplicates()

        # Add rows from the new data frame not in the old one
        mask = ~new_df.apply(tuple, axis=1).isin(old_df.apply(tuple, axis=1))
        df = pd.concat([df, new_df[mask]])

    # If a CSV of output data does not already exist
    else:
        # Output the data as-is
        df = new_df

    # Export
    logging.info(f'exporting:{out}')
    df.to_csv(out, index=False)

    # When testing we want to be able to inspect the data frame
    if return_df:
        return df


def get_shapefile(iso3: str, admin_level: Literal["0", "1", "2"]) -> Path:
    """Get a shape file."""
    return source_path(
        "geospatial/gadm",
        Path(iso3, f"gadm41_{iso3}_{admin_level}.shp"),
    )
