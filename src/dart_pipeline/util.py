"""
Utility library for DART pipeline
"""
# External libraries
from lxml import html
import gzip
import py7zr
import requests

# Built-in modules
from datetime import timedelta
from pathlib import Path
import json
import os
import shutil
import datetime

# Custom modules
from typing import Generator
from pathlib import Path
# Create the requirements file from the terminal with:
# $ python3 -m pip install pipreqs
# $ pipreqs '.' --force


def daterange(
    start_date: datetime.date, end_date: datetime.date
) -> Generator[datetime.date]:
    """Construct a date range for iterating over the days between two dates."""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def get_credentials(
    metric: str, credentials: str | Path | None = None
):
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
        Path('../..', "credentials.json") if credentials is None else Path(credentials)
    )
    if not credentials_path.exists():
        raise FileNotFoundError(
            """No credentials.json file was found. Either you have
                not created one or it is not in the specified location
                (the default location is the DART-Pipeline folder"""
        )
    return credentials_from_string(credentials_path.read_text(), metric)


def download_file(
    url: str, path: Path, username: str | None = None, password: str | None = None
) -> bool:
    """Download a file from a given URL to a given path."""
    print("Downloading", url)
    print("to", path)
    # Make a request for the data
    if username and password:
        r = requests.get(url, auth=(username, password))
    else:
        r = requests.get(url)
    # 401: Unauthorized
    # 200: OK
    if r.status_code == 200:
        with open(path, "wb") as out:
            for bits in r.iter_content():
                out.write(bits)
        return True
    else:
        print("Failed with status code", r.status_code)
        return False


def download_files(
    url: str,
    files: list[str],
    out_dir: Path,
    only_one: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> list[bool]:
    """Download multiple files in a list."""
    successes = []
    # If the user requests it, only download the first file
    if only_one:
        files = files[:1]
    # Download the files
    for file in files:
        # Create folder and intermediate folders
        path = Path(out_dir, relative_url)
        path.mkdir(parents=True, exist_ok=True)
        # Get the file
        file_url = base_url + "/" + relative_url + "/" + file
        path = Path(path, file)
        success = download_file(file_url, path, username, password)
        successes.append(success)

    return successes


def walk(
    url: str,
    only_one: bool = False,
    out_dir: Path = ".",
    username: str | None = None,
    password: str | None = None,
):
    """
    Re-create `os.walk` and `Path.walk` for use with a website.

    By default, all the files that are encountered by this walk function will
    be downloaded into a matching file structure on the local machine.

    Parameters
    ----------
    base_url : str
        The base URL of the website being accessed.
    relative_url : str
        The base_url plus the relative_url points to the folder on the server
        that will act as the starting point for the walk.
    only_one : bool, default False
        If True, only one file from each folder on the remote server will be
        downloaded (if `dry_run=True` it will not be downloaded but an empty
        file will created to represent it). This is to save on time and space
        when testing functionality.
    dry_run : bool, default False
        If True, the data will be downloaded. If False, no data will be
        downloaded but, instead, empty files will be created in the output
        folder with a naming system matching that of a wet run.
    out_dir : str, default '.'
        The folder to which the data will be downloaded.
    username : str, default None
        The username associated with an account that can access the data.
    password : str, default None
        The password associated with an account that can access the data.
    """
    # In general, use strings for creating and handling URLs as opposed to
    # urllib's URL objects or path objects.
    # - The requests.get() function expects a string
    # - The urllib module can be useful for more advanced URL manipulation but
    #   always use the urlunparse() function to convert back into a string
    # - Avoid os.path.join() because on Windows machines the slash will be the
    #   wrong way around
    url = base_url + "/" + relative_url

    # We want to be able to identify the parent URL
    idx = url.rindex("/")
    parent_url = url[:idx]

    if username and password:
        page = requests.get(url, auth=(username, password))
    else:
        page = requests.get(url)
    if page.status_code != 200:
        print(f"Status code {page.status_code}")
    webpage = html.fromstring(page.content)
    links = webpage.xpath("//a/@href")
    # Classify the links on the page
    sorters = []
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

    # Download the files
    download_files(
        base_url, relative_url, files, only_one, dry_run, out_dir, username, password
    )

    for child in children:
        relative_url_new = relative_url + "/" + child.removesuffix("/")
        walk(base_url, relative_url_new, only_one, dry_run, out_dir, username, password)


def unpack_file(path, same_folder=False):
    """Unpack a zipped file."""
    print("Unpacking", path)
    if Path(path).suffix == ".7z":
        if same_folder:
            output_folder = Path(path).parent
        else:
            output_folder = str(path).removesuffix(".7z")
        # Extract the 7z file
        with py7zr.SevenZipFile(path, mode="r") as archive:
            archive.extractall(output_folder)
    elif Path(path).suffix == ".gz":
        p_out = str(path).removesuffix(".gz")
        with gzip.open(path, "rb") as f_in, open(p_out, "wb") as f_out:
            f_out.write(f_in.read())
    else:
        if same_folder:
            print("to", path.parent)
            shutil.unpack_archive(path, path.parent)
        else:
            print("to", str(path).removesuffix(".zip"))
            shutil.unpack_archive(path, str(path).removesuffix(".zip"))
