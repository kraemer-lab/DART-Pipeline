"""
Utility library for DART pipeline
"""

import json
import os
import shutil
import datetime
import logging
from datetime import timedelta
from typing import Generator
from pathlib import Path
from urllib.parse import urljoin

from lxml import html
import gzip
import py7zr
import requests

from .types import Credentials


def daterange(
    start_date: datetime.date, end_date: datetime.date
):
    """Construct a date range for iterating over the days between two dates."""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def get_credentials(metric: str, credentials: str | Path | None = None):
    """
    Get a username and password pair from a credentials.json file.

    Parameters
    ----------
    metric : str
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

    def credentials_from_string(s: str, metric: str) -> tuple[str, str]:
        data = json.loads(s)
        if metric in data:
            return data[metric]["username"], data[metric]["password"]
        else:
            raise KeyError("metric={metric!r} not found in credentials")

    # read credentials from environment if present
    if credentials_env := os.getenv("CREDENTIALS_JSON"):
        return credentials_from_string(credentials_env, metric)

    # fallback to file if no environment variable set
    credentials_path = (
        Path("../..", "credentials.json") if credentials is None else Path(credentials)
    )
    if not credentials_path.exists():
        raise FileNotFoundError(
            """No credentials.json file was found. Either you have
                not created one or it is not in the specified location
                (the default location is the DART-Pipeline folder"""
        )
    return credentials_from_string(credentials_path.read_text(), metric)


def download_file(url: str, path: Path, auth: Credentials | None = None) -> bool:
    """Download a file from a given URL to a given path."""
    print("Downloading", url, "->", path)
    if (r := requests.get(url, auth=auth)).status_code == 200:
        with open(path, "wb") as out:
            for bits in r.iter_content():
                out.write(bits)
        return True
    else:
        logging.error("Failed with status code", r.status_code)
        return False


def download_files(
    url: str, files: list[str], out_dir: Path, auth: Credentials | None = None
) -> list[bool]:
    """Download multiple files in a list."""
    # If the user requests it, only download the first file
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    return [
        download_file(urljoin(url, file), Path(out_dir, file), auth) for file in files
    ]


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
    for link in links:
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


def unpack_file(path: Path | str, same_folder: bool = False):
    """Unpack a zipped file."""
    print("Unpacking", path)
    path = Path(path)
    match path.suffix:
        case ".7z":
            with py7zr.SevenZipFile(path, mode="r") as archive:
                archive.extractall(path.parent if same_folder else path.stem)
        case ".gz":
            with gzip.open(path, "rb") as f_in, open(path.stem, "wb") as f_out:
                f_out.write(f_in.read())
        case _:
            extract_dir = path.parent if same_folder else path.stem
            print("to", extract_dir)
            shutil.unpack_archive(path, extract_dir)
