"""
Script to collate raw data by downloading it from online sources.

See `DART dataset summarisation.xls` for information about the data fields
to be collated.

This script has been tested on Python 3.12 and more versions will be tested in
the future.

**Installation and Setup**

It is recommended to work in a virtual Python environment. Open a terminal in
the "A Collate Data" folder and run the following:

.. code-block::

    $ python3 -m venv venv
    $ source venv/bin/activate

Package requirements for this script are listed in `requirements.txt`. Install
these dependencies via:

.. code-block::

    $ python3 -m pip install -r requirements.txt

Password management is done by creating a file called `credentials.json` in the
top-level of the `DART-Pipeline` directory and adding login credentials into it
in the following format:

.. code-block::

    {
        "Example metric": {
            "username": "example@email.com",
            "password": "correct horse battery staple"
        }
    }

This file is automatically ignored by Git but can be imported into scripts.

**Example Usage**

To download Daily mean temperature product (V1808) meteorological data an
`APHRODITE account <http://aphrodite.st.hirosaki-u.ac.jp/download/>`_ is
needed and the username and password need to be added to the `credentials.json`
file as described above. The script can then be run as follows (note that these
examples use the `--only_one` and `--dry_run` flags which are meant for script
testing purposes only):

.. code-block::

    # Approx run time: 4.144
    $ python3 collate_data.py "APHRODITE temperature" --only_one --dry_run
    # Approx run time: 6:36.88
    $ python3 collate_data.py "APHRODITE temperature" --only_one

This will create a `Meteorological Data` folder inside the A folder into which
data will be downloaded.
"""
# External libraries
import cdsapi
from lxml import html
import py7zr
import gzip
import requests
# Built-in modules
import os
import shutil
from pathlib import Path
import argparse
import json
from datetime import date
# Custom modules
import utils
# Create the requirements file from the terminal with:
# $ python3 -m pip install pipreqs
# $ pipreqs '.' --force


def get_credentials(metric, base_dir='..', credentials=None):
    """
    Get a username and password pair from a credentials.json file.

    Parameters
    ----------
    metric : str
        Name of the field that is accessible online but which is
        password-protected.
    base_dir : str or pathlib.Path, default '..'
        The base directory of the Git project. It is assumed that the password
        store has been created and is located here.
    credentials : str, pathlib.Path or None, default None
        Path (including filename) to the credentials file is different from the
        default (which is `credentials.json` in the `DART-Pipeline` directory).

    Returns
    -------
    username, password : str
        The username and password associated with the entry in the credentials
        file will be returned.
    """
    # Construct the path to the credentials file
    if credentials is None:
        path = Path(base_dir, 'credentials.json')
    else:
        path = Path(credentials)
    # Open and parse the credentials file
    with open(path, 'r') as f:
        credentials = json.load(f)
    # Catch errors
    if metric not in credentials.keys():
        msg = f'No credentials for "{metric}" exists in the credentials ' + \
            f'file "{path}"'
        raise KeyError(msg)

    username = credentials[metric]['username']
    password = credentials[metric]['password']

    return username, password


def download_file(url, path, username=None, password=None):
    """Download a file from a given URL to a given path."""
    print('Downloading', url)
    print('to', path)
    # Make a request for the data
    if username and password:
        r = requests.get(url, auth=(username, password))
    else:
        r = requests.get(url)
    # 401: Unauthorized
    # 200: OK
    if r.status_code == 200:
        with open(path, 'wb') as out:
            for bits in r.iter_content():
                out.write(bits)
        return True
    else:
        print('Failed with status code', r.status_code)
        return False


def download_files(
    base_url, relative_url, files: list, only_one=False, dry_run=False,
    out_dir: str | Path = '.', username=None, password=None
):
    """Download multiple files in a list."""
    # If the user requests it, only download the first file
    if only_one:
        files = files[:1]
    # Download the files
    for file in files:
        # Create folder and intermediate folders
        path = Path(out_dir, relative_url)
        path.mkdir(parents=True, exist_ok=True)
        # Get the file
        if dry_run:
            path = Path(path, file)
            print(f'Touching: "{path}"')
            path.touch()
        else:
            file_url = base_url + '/' + relative_url + '/' + file
            path = Path(path, file)
            _ = download_file(file_url, path, username, password)


def walk(
    base_url, relative_url, only_one=False, dry_run=False, out_dir: str | Path
    = '.', username=None, password=None
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
    url = base_url + '/' + relative_url

    # We want to be able to identify the parent URL
    idx = url.rindex('/')
    parent_url = url[:idx]

    if username and password:
        page = requests.get(url, auth=(username, password))
    else:
        page = requests.get(url)
    if page.status_code != 200:
        print(f'Status code {page.status_code}')
    webpage = html.fromstring(page.content)
    links = webpage.xpath('//a/@href')
    # Classify the links on the page
    sorters = []
    children = []
    parents = []
    files = []
    for link in links:
        if link in ['?C=N;O=D', '?C=M;O=A', '?C=S;O=A', '?C=D;O=A']:
            sorters.append(link)
        elif link.endswith('/'):
            if parent_url.endswith(link.removesuffix('/')):
                # This is the parent's URL
                parents.append(link)
            else:
                children.append(link)
        else:
            files.append(link)
    # Remove hidden files
    files = [x for x in files if not x.startswith('.')]

    # Download the files
    download_files(
        base_url, relative_url, files, only_one, dry_run, out_dir, username,
        password
    )

    for child in children:
        relative_url_new = relative_url + '/' + child.removesuffix('/')
        walk(
            base_url, relative_url_new, only_one,
            dry_run, out_dir, username, password
        )


def download_gadm_data(file_format, out_dir, iso3, dry_run, level=None):
    """
    Download and unpack data from GADM.

    Parameters
    ----------
    file_format : {'Geopackage', 'Shapefile', 'GeoJSON'}
        The format of the raw data to be downloaded.
    out_dir : str or pathlib.Path
        Folder path to where the files will be downloaded.
    iso3 : str, default 'VNM'
        Alpha-3 country code as per ISO 3166 for the desired country. See the
        Wikipedia pages "`ISO 3166-1 alpha-3
        <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`_" and
        "`List of ISO 3166 country codes
        <https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes>`_".
    level : {'level0', 'level1', 'level2', 'level3', None}, default None
        Administrative levels available (depends on the country):

        - level 0: country
        - level 1: state (province)
        - level 2: county (district)
        - level 3: commune/ward (and equivalents)
    """
    # Construct the URL
    base_url = 'https://geodata.ucdavis.edu/gadm/gadm4.1'
    if file_format == 'Geopackage':
        relative_url = f'gpkg/gadm41_{iso3}.gpkg'
    elif file_format == 'Shapefile':
        relative_url = f'shp/gadm41_{iso3}_shp.zip'
    elif file_format == 'GeoJSON':
        if level == 'level0':
            relative_url = f'json/gadm41_{iso3}_0.json'
        elif level == 'level1':
            relative_url = f'json/gadm41_{iso3}_1.json.zip'
        elif level == 'level2':
            relative_url = f'json/gadm41_{iso3}_2.json.zip'
        elif level == 'level3':
            relative_url = f'json/gadm41_{iso3}_3.json.zip'
        else:
            raise ValueError(f'Unknown level "{level}"')
    else:
        raise ValueError(f'Unknown file format "{file_format}"')
    url = '/'.join([base_url, relative_url])

    # Construct the output file path
    base_name = Path(relative_url).name
    path = Path(out_dir, base_name)

    if dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f'Touching: "{path}"')
        path.touch()
    else:
        # Attempt to download the file
        succeded = download_file(url, path)

        # Unpack the file
        if succeded:
            # Unpack shape files
            if file_format == 'Shapefile':
                unpack_file(path, same_folder=False)
            # Unpack GeoJSON files
            if file_format == 'GeoJSON':
                # The level 0 data is not packed
                if level != 'level0':
                    unpack_file(path, same_folder=True)


def unpack_file(path, same_folder=False):
    """Unpack a zipped file."""
    print('Unpacking', path)
    if Path(path).suffix == '.7z':
        if same_folder:
            output_folder = Path(path).parent
        else:
            output_folder = str(path).removesuffix('.7z')
        # Extract the 7z file
        with py7zr.SevenZipFile(path, mode='r') as archive:
            archive.extractall(output_folder)
    elif Path(path).suffix == '.gz':
        p_out = str(path).removesuffix('.gz')
        with gzip.open(path, 'rb') as f_in, open(p_out, 'wb') as f_out:
            f_out.write(f_in.read())
    else:
        if same_folder:
            print('to', path.parent)
            shutil.unpack_archive(path, path.parent)
        else:
            print('to', str(path).removesuffix('.zip'))
            shutil.unpack_archive(path, str(path).removesuffix('.zip'))


def download_meteorological_data(
    data_name, only_one=False, dry_run=False, credentials=None, year=None
):
    """
    Download Meteorological data.

    APHRODITE products available for download:

    - `APHRO_JP V1207`: surface precipitation data over the land in Japan
    - `APHRO_JP V1801`: surface precipitation data over the land in Japan
    - `APHRO_MA V1801_R1`: updated version of `APHRO_JP V1801` covering Monsoon
        Asia (MA)
    - `APHRO_MA V1101EX_R1`: precipitation data for Monsoon Asia (MA) with an
        old algorithm but with updated data
    - `APHRO_MA V1808`: daily mean temperature product for Asia
    - `APHRO_MA V1901`: updated version of `V1101EX` and `V1801_R1`

    See `the Products' page
     <http://aphrodite.st.hirosaki-u.ac.jp/products.html>`_ for more.
    """
    if data_name == 'APHRODITE Daily accumulated precipitation (V1901)':
        download_aphrodite_precipitation_data(only_one, dry_run, credentials)
    elif data_name == 'APHRODITE Daily mean temperature product (V1808)':
        download_aphrodite_temperature_data(only_one, dry_run, credentials)
    elif data_name.startswith('CHIRPS: Rainfall Estimates from Rain Gauge an'):
        download_chirps_rainfall_data(only_one, dry_run)
    elif data_name == 'ERA5 atmospheric reanalysis':
        download_era5_reanalysis_data(only_one, dry_run)
    elif data_name.startswith('TerraClimate gridded temperature, precipitati'):
        download_terraclimate_data(only_one, dry_run, year)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def download_aphrodite_precipitation_data(
    only_one=False, dry_run=False, credentials=None
):
    """
    Download APHRODITE Daily accumulated precipitation (V1901).

    **Requires APHRODITE account**

    Run times:

    - `time python3 collate_data.py "APHRODITE precipitation"`: 9m20.565s
    - `time python3 collate_data.py "APHRODITE precipitation" -1`: 35.674s
    - `time python3 collate_data.py "APHRODITE precipitation" -1 -d`: 35.674s
    """
    data_type = 'Meteorological Data'
    data_name = 'APHRODITE Daily accumulated precipitation (V1901)'

    # Login credentials
    username, password = get_credentials(data_name, base_dir, credentials)

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs should be str, not urllib URL objects, because requests expects str
    base_url = 'http://aphrodite.st.hirosaki-u.ac.jp'

    # Dictionary of branch URLs for each resolution
    relative_urls = {
        '0.05 degree': 'product/APHRO_V1901/APHRO_MA/005deg',
        '0.25 degree': 'product/APHRO_V1901/APHRO_MA/025deg',
        '0.25 degree nc': 'product/APHRO_V1901/APHRO_MA/025deg_nc',
        '0.50 degree': 'product/APHRO_V1901/APHRO_MA/050deg',
        '0.50 degree nc': 'product/APHRO_V1901/APHRO_MA/050deg_nc',
    }

    # Dictionary of files at each relative URL
    lists_of_files = {
        '0.05 degree': ['APHRO_MA_PREC_CLM_005deg_V1901.ctl.gz'],
        '0.25 degree': [
            'APHRO_MA_025deg_V1901.2015.gz',
            'APHRO_MA_025deg_V1901.ctl.gz',
        ],
        '0.25 degree nc': ['APHRO_MA_025deg_V1901.2015.nc.gz'],
        '0.50 degree': [
            'APHRO_MA_050deg_V1901.2015.gz',
            'APHRO_MA_050deg_V1901.ctl.gz',
        ],
        '0.50 degree nc': ['APHRO_MA_050deg_V1901.2015.nc.gz'],
    }

    # Download the files
    for key in relative_urls.keys():
        relative_url = relative_urls[key]
        files = lists_of_files[key]
        download_files(
            base_url, relative_url, files, only_one, dry_run, out_dir,
            username, password
        )


def download_aphrodite_temperature_data(
    only_one=False, dry_run=False, credentials=None
):
    """
    Download APHRODITE Daily mean temperature product (V1808).

    **Requires APHRODITE account**

    Run times:

    - `time python3 collate_data.py "APHRODITE temperature"`: 87m58.039s
    - `time python3 collate_data.py "APHRODITE temperature" -1`: 6m36.88s
    - `time python3 collate_data.py "APHRODITE temperature" -1 -d`: 4.144s
    """
    data_type = 'Meteorological Data'
    data_name = 'APHRODITE Daily mean temperature product (V1808)'

    # Login credentials
    username, password = get_credentials(data_name, base_dir, credentials)

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs should be str (not urllib URL objects) because requests expects str
    base_url = 'http://aphrodite.st.hirosaki-u.ac.jp'
    # Dictionary of branch URLs for each resolution
    relative_urls = {
        '0.05 degree': 'product/APHRO_V1808_TEMP/APHRO_MA/005deg',
        '0.05 degree nc': 'product/APHRO_V1808_TEMP/APHRO_MA/005deg_nc',
        # '0.25 degree': 'product/APHRO_V1808_TEMP/APHRO_MA/025deg',
        # '0.25 degree nc': 'product/APHRO_V1808_TEMP/APHRO_MA/025deg_nc',
        '0.50 degree': 'product/APHRO_V1808_TEMP/APHRO_MA/050deg',
        # '0.50 degree nc': 'product/APHRO_V1808_TEMP/APHRO_MA/050deg_nc',
    }
    # Walk through the folder structure
    for key in relative_urls.keys():
        relative_url = relative_urls[key]
        walk(
            base_url, relative_url, only_one, dry_run,
            out_dir, username, password
        )

    # Download the 0.25 degree resolution data
    relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/025deg'
    files = [
        'APHRO_MA_TAVE_025deg_V1808.2015.gz',
        'APHRO_MA_TAVE_025deg_V1808.ctl.gz',
        'read_aphro_v1808.f90',
    ]
    download_files(
        base_url, relative_url, files, only_one, dry_run, out_dir, username,
        password
    )

    # Download the 0.25 degree nc resolution data
    relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/025deg_nc'
    files = [
        'APHRO_MA_TAVE_025deg_V1808.2015.nc.gz',
        'APHRO_MA_TAVE_025deg_V1808.nc.ctl.gz',
    ]
    download_files(
        base_url, relative_url, files, only_one, dry_run, out_dir, username,
        password
    )

    # Download the 0.50 degree nc resolution data
    relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/050deg_nc'
    files = [
        'APHRO_MA_TAVE_050deg_V1808.2015.nc.gz',
        'APHRO_MA_TAVE_050deg_V1808.nc.ctl.gz',
    ]
    download_files(
        base_url, relative_url, files, only_one, dry_run, out_dir, username,
        password
    )


def download_chirps_rainfall_data(only_one, dry_run):
    """
    Download CHIRPS Rainfall Estimates from Rain Gauge, Satellite Observations.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Download data in TIF format (.tif.gz), not COG format (.cog).

    Run times:

    - `time python3 collate_data.py "CHIRPS rainfall"`:
        - 05:30.123 (2024-01-01 to 2024-03-07)
        - 02:56.14 (2024-01-01 to 2024-03-11)
        - 02:55.466 (2024-01-01 to 2024-02-29)
    """
    data_type = 'Meteorological Data'
    data_name = 'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite ' + \
        'Observations'

    # Create output directory
    sanitised_data_name = data_name.replace(':', ' -')
    out_dir = Path(base_dir, 'A Collate Data', data_type, sanitised_data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Download data for 2024 onwards
    for year in [y for y in range(2023, int(date.today().year) + 1)]:
        # URLs should be str, not urllib URL objects, because requests expects
        # str
        base_url = 'https://data.chc.ucsb.edu'
        # I only know how to anayse tifs, not cogs
        fmt = 'tifs'

        relative_url = f'products/CHIRPS-2.0/global_daily/{fmt}/p05/{year}'
        # Walk through the folder structure
        walk(base_url, relative_url, only_one, dry_run, out_dir)

        relative_url = f'products/CHIRPS-2.0/global_monthly/{fmt}'
        # Walk through the folder structure
        walk(base_url, relative_url, only_one, dry_run, out_dir)

        relative_url = f'products/CHIRPS-2.0/global_annual/{fmt}'
        # Walk through the folder structure
        walk(base_url, relative_url, only_one, dry_run, out_dir)

        # Unpack the data
        for dirpath, dirnames, filenames in os.walk(out_dir):
            for filename in filenames:
                if filename.endswith('.tif.gz'):
                    path = Path(dirpath, filename)
                    unpack_file(path)


def download_era5_reanalysis_data(only_one, dry_run):
    """
    Download ERA5 atmospheric reanalysis data.

    How to use the Climate Data Store (CDS) Application Program Interface
    (API): https://cds.climate.copernicus.eu/api-how-to

    ```bash
    python3 -m pip install cdsapi
    cd ~/DART-Pipeline
    export PASSWORD_STORE_DIR=$PWD/.password-store
    pass insert "ERA5 atmospheric reanalysis"
    pass "ERA5 atmospheric reanalysis"
    ```

    Run times:

    - `time python3 collate_data.py "ERA5 reanalysis"`: 0m1.484s
    """
    data_type = 'Meteorological Data'
    data_name = 'ERA5 atmospheric reanalysis'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Use the Climate Data Store (CDS) Application Program Interface (API)
    # https://pypi.org/project/cdsapi/
    c = cdsapi.Client()
    request = {
        'date': '2013-01-01',  # The hyphens can be omitted
        # 1 is top level, 137 the lowest model level in ERA5. Use '/' to
        # separate values.
        'levelist': '1/10/100/137',
        'levtype': 'ml',
        # Full information at https://apps.ecmwf.int/codes/grib/param-db/
        # The native representation for temperature is spherical harmonics
        'param': '130',
        # Denotes ERA5. Ensemble members are selected by 'enda'
        'stream': 'oper',
        # You can drop :00:00 and use MARS short-hand notation, instead of
        # '00/06/12/18'
        'time': '00/to/23/by/6',
        'type': 'an',
        # North, West, South, East. Default: global
        'area': '80/-50/-25/0',
        # Latitude/longitude. Default: spherical harmonics or reduced Gaussian
        # grid
        'grid': '1.0/1.0',
        # Output needs to be regular lat-lon, so only works in combination
        # with 'grid'!
        'format': 'netcdf',
    }
    c.retrieve(
        # Requests follow MARS syntax
        # Keywords 'expver' and 'class' can be dropped. They are obsolete
        # since their values are imposed by 'reanalysis-era5-complete'
        'reanalysis-era5-complete',
        request,
        # Output file. Adapt as you wish.
        Path(out_dir, 'ERA5-ml-temperature-subarea.nc')
    )


def download_terraclimate_data(only_one, dry_run, year):
    """
    Download TerraClimate gridded temperature, precipitation, etc.

    Run times:

    - `time python3 collate_data.py "TerraClimate data"`:
        - 34m50.828s
        - 11m35.25s
    - `time python3 collate_data.py "TerraClimate data" -1 -d`: 4.606s
    """
    data_type = 'Meteorological Data'
    data_name = 'TerraClimate gridded temperature, precipitation, and other'

    if year is None:
        year = '2023'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs should be str, not urllib URL objects, because requests expects str
    base_url = 'https://climate.northwestknowledge.net'
    relative_url = 'TERRACLIMATE-DATA'

    # Download the following list of files
    files = [
        f'TerraClimate_aet_{year}.nc',
        f'TerraClimate_def_{year}.nc',
        f'TerraClimate_PDSI_{year}.nc',  # For 2023 the capitalisation
        f'TerraClimate_pdsi_{year}.nc',  # of "PDSI" changed
        f'TerraClimate_pet_{year}.nc',
        f'TerraClimate_ppt_{year}.nc',
        f'TerraClimate_q_{year}.nc',
        f'TerraClimate_soil_{year}.nc',
        f'TerraClimate_srad_{year}.nc',
        f'TerraClimate_swe_{year}.nc',
        f'TerraClimate_tmax_{year}.nc',
        f'TerraClimate_tmin_{year}.nc',
        f'TerraClimate_vap_{year}.nc',
        f'TerraClimate_vpd_{year}.nc',
        f'TerraClimate_ws_{year}.nc',
    ]
    download_files(base_url, relative_url, files, only_one, dry_run, out_dir)


def download_socio_demographic_data(data_name, only_one=False, dry_run=False):
    """Download socio-demographic data."""
    if data_name == 'WorldPop population density':
        download_worldpop_pop_density_data(only_one, dry_run)
    elif data_name == 'WorldPop population count':
        download_worldpop_pop_count_data(only_one, dry_run)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def download_worldpop_pop_density_data(only_one, dry_run):
    """
    Download WorldPop population density.

    All available datasets are detailed here:
    https://www.worldpop.org/rest/data

    Run times:

    - `time python3 collate_data.py "WorldPop pop density" -d`: 0m0.732s
    - `time python3 collate_data.py "WorldPop pop density":
        - 0m2.860s
        - 0m0.29s
    """
    data_type = 'Socio-Demographic Data'
    data_name = 'WorldPop population density'

    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')

    # Set additional parameters
    year = '2020'
    iso3 = 'VNM'
    base_url = 'https://data.worldpop.org'

    #
    # GeoDataFrame file
    #
    # Construct URL
    relative_url = 'GIS/Population_Density/Global_2000_2020_1km_UNadj/' + \
        f'{year}/{iso3}/{iso3.lower()}_pd_{year}_1km_UNadj_ASCII_XYZ.zip'
    url = '/'.join([base_url, relative_url])
    # Construct and create output path
    path = Path(base_dir, 'A Collate Data', data_type, data_name, relative_url)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Touch or download the GeoDataFrame file
    if dry_run:
        print(f'Touching: "{path}"')
        path.touch()
    else:
        succeded = download_file(url, path)
        # Unpack file
        if succeded:
            unpack_file(path, same_folder=True)

    #
    # GeoTIFF file
    #
    # Construct URL
    relative_url = 'GIS/Population_Density/Global_2000_2020_1km_UNadj/' + \
        f'{year}/{iso3}/{iso3.lower()}_pd_{year}_1km_UNadj.tif'
    url = '/'.join([base_url, relative_url])
    # Construct and create output path
    path = Path(base_dir, 'A Collate Data', data_type, data_name, relative_url)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Touch or download the GeoTIFF file
    if dry_run:
        print(f'Touching: "{path}"')
        path.touch()
    else:
        download_file(url, path)


def download_worldpop_pop_count_data(only_one, dry_run):
    """
    Download WorldPop population count.

    All available datasets are detailed here:
    https://www.worldpop.org/rest/data

    Run times:

    - `time python3 collate_data.py "WorldPop pop count"`: 406.2s
    """
    data_type = 'Socio-Demographic Data'
    data_name = 'WorldPop population count'

    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')

    # Set additional parameter
    base_url = 'https://data.worldpop.org'

    # Download GeoDataFrame file
    relative_url = 'GIS/Population/Individual_countries/VNM/' + \
        'Viet_Nam_100m_Population.7z'
    url = os.path.join(base_url, relative_url)
    path = Path(
        base_dir, 'A Collate Data', data_type, data_name, relative_url
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f'Touching: "{path}"')
        path.touch()
    else:
        print('Downloading', url)
        print('to', path)
        succeded = download_file(url, path)
        # Unpack file
        if succeded:
            unpack_file(path, same_folder=True)


def download_geospatial_data(data_name, only_one=False, dry_run=False):
    """Download Geospatial data."""
    if data_name == 'GADM administrative map':
        download_gadm_admin_map_data(only_one, dry_run)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def download_gadm_admin_map_data(only_one, dry_run=True):
    """
    Download GADM administrative map.

    Run times:

    - `time python3 collate_data.py "GADM admin map"`:
        - 2m0.457s
        - 0m31.094s
    """
    data_type = 'Geospatial Data'
    data_name = 'GADM administrative map'

    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')

    # Set additional parameters
    iso3 = 'VNM'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    download_gadm_data('Geopackage', out_dir, iso3, dry_run)
    download_gadm_data('Shapefile', out_dir, iso3, dry_run)
    download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level0')
    download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level1')
    download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level2')
    download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level3')


class EmptyObject:
    """Define an empty object for creating a fake args object for Sphinx."""

    def __init__(self):
        """Initialise."""
        self.data_name = ''
        self.only_one = False
        self.dry_run = False


shorthand_to_data_name = {
    # Meteorological Data
    'APHRODITE temperature':
    'APHRODITE Daily mean temperature product (V1808)',
    'APHRODITE precipitation':
    'APHRODITE Daily accumulated precipitation (V1901)',
    'CHIRPS rainfall':
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations',
    'TerraClimate data':
    'TerraClimate gridded temperature, precipitation, and other',
    'ERA5 reanalysis':
    'ERA5 atmospheric reanalysis',

    # Socio-Demographic Data
    'WorldPop pop density': 'WorldPop population density',
    'WorldPop pop count': 'WorldPop population count',

    # Geospatial Data
    'GADM admin map': 'GADM administrative map',
}

data_name_to_type = {
    # Meteorological Data
    'APHRODITE Daily mean temperature product (V1808)': 'Meteorological Data',
    'APHRODITE Daily accumulated precipitation (V1901)': 'Meteorological Data',
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations':
    'Meteorological Data',
    'TerraClimate gridded temperature, precipitation, and other':
    'Meteorological Data',
    'ERA5 atmospheric reanalysis': 'Meteorological Data',

    # Socio-Demographic Data
    'WorldPop population density': 'Socio-Demographic Data',
    'WorldPop population count': 'Socio-Demographic Data',

    # Geospatial Data
    'GADM administrative map': 'Geospatial Data',
}

# Establish the base directory
path = Path(__file__)
base_dir = utils.get_base_directory(path.parent)

# If running directly
if __name__ == '__main__':
    # Perform checks
    utils.check_os()
    utils.check_python()

    # Create command-line argument parser
    desc = 'Download data and store it locally for later processing.'
    parser = argparse.ArgumentParser(description=desc)

    # Add positional arguments
    message = 'The name of the data field to be downloaded and collated.'
    default = ''
    parser.add_argument('data_name', nargs='?', default=default, help=message)

    # Add optional arguments
    message = '''If set, only one item from each folder in the raw data
    will be downloaded/created.'''
    parser.add_argument('--only_one', '-1', action='store_true', help=message)
    message = '''If set, the raw data will not be downloaded. Instead, empty
    files will be created with the correct names and locations.'''
    parser.add_argument('--dry_run', '-d', action='store_true', help=message)
    message = '''Path (including filename) to the credentials file.
    Default is `credentials.json` in the `DART-Pipeline` directory.'''
    default = '../credentials.json'
    parser.add_argument('--credentials', '-c', default=default, help=message)
    message = '''If data from multiple years is available, choose a year from
    which to download.'''
    parser.add_argument('--year', '-y', default=None, help=message)

    # Parse arguments from terminal
    args = parser.parse_args()

    # Check
    if True:
        print('Arguments:')
        for arg in vars(args):
            print(f'{arg + ":":20s} {vars(args)[arg]}')

    # Extract the arguments
    data_name = args.data_name
    only_one = args.only_one
    dry_run = args.dry_run
    credentials = args.credentials
    year = args.year

    # Check that the data name is recognised
    if data_name in shorthand_to_data_name.keys():
        pass
    elif data_name in shorthand_to_data_name.values():
        pass
    elif data_name == '':
        pass
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')

    # Convert shorthand names to full names
    if data_name in shorthand_to_data_name.keys():
        data_name = shorthand_to_data_name[data_name]
    # Get macro data type
    data_type = ''
    if data_name in data_name_to_type.keys():
        data_type = data_name_to_type[data_name]

    if data_name == '':
        print('No data name has been provided. Exiting the programme.')
    elif data_type == 'Meteorological Data':
        download_meteorological_data(
            data_name, only_one, dry_run, credentials, year
        )
    elif data_type == 'Socio-Demographic Data':
        download_socio_demographic_data(data_name, only_one, dry_run)
    elif data_type == 'Geospatial Data':
        download_geospatial_data(data_name, only_one, dry_run)
    else:
        raise ValueError(f'Unrecognised data type "{data_type}"')

# If running via Sphinx
else:
    # Create a fake args object so Sphinx doesn't complain it doesn't have
    # command-line arguments
    args = EmptyObject()
