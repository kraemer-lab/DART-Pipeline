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
from bs4 import BeautifulSoup
from lxml import html
import cdsapi
import gzip
import py7zr
import pycountry
import requests
# Built-in modules
from datetime import date
from io import StringIO
from pathlib import Path
import argparse
import base64
import json
import os
import re
import shutil
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

    Examples
    --------
    >>> get_credentials('APHRODITE Daily accumulated precipitation (V1901)')
    ('example@email.com', '*******')

    Using an environment variable to store the credentials:

    $ export CREDENTIALS_JSON='{
        "APHRODITE Daily accumulated precipitation (V1901)": {
            "username": "example@email.com",
            "password": "*******"
        }
    }'
    $ python3
    >>> from test_collate_data import get_credentials
    >>> metric = 'APHRODITE Daily accumulated precipitation (V1901)'
    >>> get_credentials(metric, credentials='environ')
    ('example@email.com', '*******')
    """
    # Construct the path to the credentials file
    if credentials is None:
        path = Path(base_dir, 'credentials.json')
    elif credentials == 'environ':
        path = None
    else:
        path = Path(credentials)

    # If the credentials are located in a file
    if path:
        # Open and parse the credentials file
        try:
            with open(path, 'r') as f:
                credentials = json.load(f)
        except FileNotFoundError:
            msg = 'No credentials.json file was found. Either you have ' + \
                'not created one or it is not in the specified location ' + \
                '(the default location is the DART-Pipeline folder)'
            raise FileNotFoundError(msg)

    # If the credentials are in an environment variable
    if not path:
        io = StringIO(os.environ['CREDENTIALS_JSON'])
        credentials = json.load(io)

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
        if dry_run:
            path = Path(path, file)
            print(f'Touching: "{path}"')
            path.touch()
            success = True
        else:
            file_url = base_url + '/' + relative_url + '/' + file
            path = Path(path, file)
            success = download_file(file_url, path, username, password)
        successes.append(success)

    return successes


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
    Download and unpack GADM (Database of Global Administrative Areas) data.

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


def download_epidemiological_data(data_name, only_one, dry_run, year, iso3):
    """Download Epidemiological Data."""
    if data_name == 'Ministerio de Salud (Peru) data':
        download_ministerio_de_salud_peru_data(only_one, dry_run)


def download_ministerio_de_salud_peru_data(only_one, dry_run):
    """
    Download data from the Ministerio de Salud (Peru).

    https://www.dge.gob.pe/sala-situacional-dengue

    Run times:

    - `time python3 collate_data.py Peru`:
    - `time python3 collate_data.py Peru -1`:
    - `time python3 collate_data.py Peru -d`:
    - `time python3 collate_data.py Peru -1 -d`: 44.332s
    """
    data_type = 'Epidemiological Data'
    data_name = 'Ministerio de Salud (Peru) data'

    pages = [
        'sala_dengue_AMAZONAS',
        'sala_dengue_ANCASH',
        'sala_dengue_AREQUIPA',
        'sala_dengue_AYACUCHO',
        'sala_dengue_CAJAMARCA',
        'sala_dengue_CALLAO',
        'sala_dengue_CUSCO',
        'sala_dengue_HUANUCO',
        'sala_dengue_ICA',
        'sala_dengue_JUNIN',
        'sala_dengue_LA LIBERTAD',
        'sala_dengue_LAMBAYEQUE',
        'sala_dengue_LIMA',
        'sala_dengue_LORETO',
        'sala_dengue_MADRE DE DIOS',
        'sala_dengue_MOQUEGUA',
        'sala_dengue_PASCO',
        'sala_dengue_PIURA',
        'sala_dengue_PUNO',
        'sala_dengue_SAN MARTIN',
        'sala_dengue_TUMBES',
        'sala_dengue_UCAYALI',
        'Nacional_dengue',
    ]
    # If the user specifies that only one dataset should be downloaded
    if only_one:
        pages = pages[:1]
    for page in pages:
        url = 'https://www.dge.gob.pe/sala-situacional-dengue/uploads/' + \
            f'{page}.html'
        print(f'Accessing "{url}"')
        # Fetch webpage content
        response = requests.get(url)
        # Raise an exception for bad response status
        response.raise_for_status()
        # Parse HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find links with the onclick attribute in both <a> and <button> tags
        onclick_elements = soup.find_all(
            lambda tag: tag.name in ['a', 'button'] and tag.has_attr('onclick')
        )
        # Extract the onclick attribute values
        links = [element.get('onclick') for element in onclick_elements]
        # Raise an error if no links are found
        if len(links) == 0:
            raise ValueError('No links found on the page')

        for link in links:
            # Search the link for the data embedded in it
            regex_pattern = r"base64,(.*?)(?='\).then)"
            matches = re.findall(regex_pattern, link, re.DOTALL)
            if matches:
                base64_string = matches[0]
            else:
                raise ValueError('No data found embedded in the link')

            # Search the link for the filename
            regex_pattern = r"a\.download = '(.*?)';\s*a\.click"
            matches = re.findall(regex_pattern, link)
            if matches:
                # There is an actual filename for this data
                filename = matches[0]
            else:
                # Just use the page name for the file
                filename = page + '.xlsx'

            # Export
            path = Path(
                base_dir, 'A Collate Data', data_type, data_name, filename
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            if dry_run:
                # If doing a dry run, just touch the files
                print(f'Touching: "{path}"')
                path.touch()
            else:
                # Decode and export the data
                decoded_bytes = base64.b64decode(base64_string)
                with open(path, 'wb') as f:
                    print(f'Exporting "{path}"')
                    f.write(decoded_bytes)


def download_geospatial_data(data_name, only_one, dry_run, iso3):
    """Download Geospatial data."""
    if data_name == 'GADM administrative map':
        download_gadm_admin_map_data(only_one, dry_run, iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def download_gadm_admin_map_data(only_one, dry_run, iso3):
    """
    Download GADM administrative map.

    Run times:

    - `time python3 collate_data.py GADM -3 VNM`: 54.608s
    - `time python3 collate_data.py GADM -3 PER`: 1m2.167s
    - `time python3 collate_data.py GADM -3 GBR`: 13m22.114s
    """
    data_type = 'Geospatial Data'
    data_name = 'GADM administrative map'

    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')
    if iso3 == '':
        raise ValueError(f'No ISO3 code has been provided; use the "-3" flag')

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name, iso3)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Always download the shapefile
    download_gadm_data('Shapefile', out_dir, iso3, dry_run)
    # If only one type of data is requested, stop here. Otherwise, download
    # the other data types
    if only_one:
        pass
    else:
        download_gadm_data('Geopackage', out_dir, iso3, dry_run)
        download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level0')
        download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level1')
        download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level2')
        download_gadm_data('GeoJSON', out_dir, iso3, dry_run, level='level3')


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

    - `python3 collate_data.py "APHRODITE precipitation"`: 44.318s
    - `python3 collate_data.py "APHRODITE precipitation" -1`: 1m8.172s
    - `python3 collate_data.py "APHRODITE precipitation" -1 -d`: 0.206s
    - `python3 collate_data.py "APHRODITE precipitation" -c environ`: 48.823s
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
        '0.50 degree': 'product/APHRO_V1901/APHRO_MA/050deg',
        '0.50 degree nc': 'product/APHRO_V1901/APHRO_MA/050deg_nc',
        '0.25 degree': 'product/APHRO_V1901/APHRO_MA/025deg',
        '0.25 degree nc': 'product/APHRO_V1901/APHRO_MA/025deg_nc',
        '0.05 degree': 'product/APHRO_V1901/APHRO_MA/005deg',
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
        if only_one:
            break


def download_aphrodite_temperature_data(
    only_one=False, dry_run=False, credentials=None
):
    """
    Download APHRODITE Daily mean temperature product (V1808).

    **Requires APHRODITE account**

    Run times:

    - `time python3 collate_data.py "APHRODITE temperature"`: 1h27m58.039s
    - `time python3 collate_data.py "APHRODITE temperature" -1`: 1m47.492s
    - `time python3 collate_data.py "APHRODITE temperature" -1 -d`: 0.206s
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

    # Download the 0.50 degree nc resolution data
    relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/050deg_nc'
    files = [
        'APHRO_MA_TAVE_050deg_V1808.2015.nc.gz',  # 19 MB
        'APHRO_MA_TAVE_050deg_V1808.nc.ctl.gz',  # 347 B
    ]
    download_files(
        base_url, relative_url, files, only_one, dry_run, out_dir, username,
        password
    )

    if only_one:
        pass
    else:
        # Download the 0.50 degree resolution data
        relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/050deg'
        files = [
            'read_aphro_v1808.f90',  # 2.6 KB
        ]
        download_files(
            base_url, relative_url, files, only_one, dry_run, out_dir,
            username, password
        )

        # Download the 0.25 degree resolution nc data
        relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/025deg_nc'
        files = [
            'APHRO_MA_TAVE_025deg_V1808.2015.nc.gz',  # 64 MB
            'APHRO_MA_TAVE_025deg_V1808.nc.ctl.gz',  # 485 B
        ]
        download_files(
            base_url, relative_url, files, only_one, dry_run, out_dir,
            username, password
        )

        # Download the 0.25 degree resolution data
        relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/025deg'
        files = [
            'APHRO_MA_TAVE_025deg_V1808.2015.gz',  # 64 MB
            'APHRO_MA_TAVE_025deg_V1808.ctl.gz',  # 312 B
            'read_aphro_v1808.f90',  # 2.6 KB
        ]
        download_files(
            base_url, relative_url, files, only_one, dry_run, out_dir,
            username, password
        )

        # Download the 0.05 degree resolution nc data
        relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/005deg_nc'
        files = [
            'APHRO_MA_TAVE_CLM_005deg_V1808.nc.gz',  # 1.2 GB
        ]
        download_files(
            base_url, relative_url, files, only_one, dry_run, out_dir,
            username, password
        )

        # Download the 0.05 degree resolution data
        relative_url = 'product/APHRO_V1808_TEMP/APHRO_MA/005deg'
        files = [
            'APHRO_MA_TAVE_CLM_005deg_V1808.ctl.gz',  # 334 B
            'APHRO_MA_TAVE_CLM_005deg_V1808.grd.gz',  # 1.4 GB
            'read_aphro_clm_v1808.f90',  # 2.1 KB
        ]
        download_files(
            base_url, relative_url, files, only_one, dry_run, out_dir,
            username, password
        )


def download_chirps_rainfall_data(only_one, dry_run):
    """
    Download CHIRPS Rainfall Estimates from Rain Gauge, Satellite Observations.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Download data in TIF format (.tif.gz), not COG format (.cog).

    Run times:

    - `time python3 collate_data.py "CHIRPS rainfall"`:
        - 5m30.123s (2024-01-01 to 2024-03-07)
        - 2m56.14s (2024-01-01 to 2024-03-11)
        - 4m15.394s (2024-01-01 to 2024-03-31)
    - `time python3 collate_data.py "CHIRPS rainfall" -1`: 17.831s
    - `time python3 collate_data.py "CHIRPS rainfall" -1 -d`: 1.943s
    """
    data_type = 'Meteorological Data'
    data_name = 'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite ' + \
        'Observations'

    # Create output directory
    sanitised = data_name.replace(':', ' -')
    out_dir = Path(base_dir, 'A Collate Data', data_type, sanitised)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Download data for 2024 onwards
    for year in [y for y in range(2024, int(date.today().year) + 1)]:
        # URLs should be str, not urllib URL objects, because requests expects
        # str
        base_url = 'https://data.chc.ucsb.edu'
        # I only know how to anayse tifs, not cogs
        relative_url = f'products/CHIRPS-2.0/global_daily/tifs/p05/{year}'
        # Walk through the folder structure
        walk(base_url, relative_url, only_one, dry_run, out_dir)

    if not dry_run:
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

    Install and configure `cdsapi` by following the instructions here:
    https://pypi.org/project/cdsapi/

    A Climate Data Store account is needed.

    Run times:

    - `time python3 collate_data.py "ERA5 reanalysis"`: 1.293s
    - `time python3 collate_data.py "ERA5 reanalysis" -1`: 1.884s
    - `time python3 collate_data.py "ERA5 reanalysis" -1 -d`: 1.166s
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
        - 14m16.896s
    - `time python3 collate_data.py "TerraClimate data" -1`: 3m12.992s
    - `time python3 collate_data.py "TerraClimate data" -1 -d`: 0.204s
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


def download_socio_demographic_data(data_name, only_one, dry_run, iso3):
    """Download socio-demographic data."""
    if data_name == 'WorldPop population count':
        download_worldpop_pop_count_data(only_one, dry_run, iso3)
    elif data_name == 'WorldPop population density':
        download_worldpop_pop_density_data(only_one, dry_run, iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def download_worldpop_pop_count_data(only_one, dry_run, iso3):
    """
    Download WorldPop population count.

    All available WorldPop datasets are detailed here:
    https://www.worldpop.org/rest/data

    This function will download population data in GeoTIFF format (as files
    with the .tif extension) along with metadata files. A zipped file (with the
    .7z extension) will also be downloaded; this will contain the same GeoTIFF
    files along with .tfw and .tif.aux.xml files. Most users will not find
    these files useful and so unzipping the .7z file is usually unnecessary.

    Run times:

    - `time python3 collate_data.py "WorldPop pop count"`:
        - 6m46.2s
        - 17m40.154s
    - `time python3 collate_data.py "WorldPop pop count" -3 VNM`: 23m17.052s
    - `time python3 collate_data.py "WorldPop pop count" -3 PER`:
        - 46m47.78s
        - 1h15m44.285s
    - `time python3 collate_data.py "WorldPop pop count" -1 -3 VNM`: 1m14.778s
    """
    data_type = 'Socio-Demographic Data'
    data_name = 'WorldPop population count'

    # Set additional parameter
    base_url = 'https://data.worldpop.org'
    country = pycountry.countries.get(alpha_3=iso3).name
    country = country.replace(' ', '_')

    # Example URLs:
    # - https://data.worldpop.org/GIS/Population/Individual_countries/VNM/
    # - https://data.worldpop.org/GIS/Population/Individual_countries/PER/
    if only_one:
        # Download TIF file
        relative_url = f'GIS/Population/Individual_countries/{iso3}/' + \
            f'{country}_100m_Population/VNM_ppp_v2b_2020_UNadj.tif'
    else:
        # Download GeoDataFrame file (which includes the TIF file)
        relative_url = f'GIS/Population/Individual_countries/{iso3}/' + \
            f'{country}_100m_Population.7z'

    # Download files
    url = os.path.join(base_url, relative_url)
    path = Path(
        base_dir, 'A Collate Data', data_type, data_name, relative_url
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f'Touching: "{path}"')
        path.touch()
    else:
        succeded = download_file(url, path)
        # Unpack file
        if succeded:
            if str(path).endswith('.7z'):
                unpack_file(path, same_folder=False)


def download_worldpop_pop_density_data(only_one, dry_run, iso3):
    """
    Download WorldPop population density.

    All available datasets are detailed here:
    https://www.worldpop.org/rest/data

    Run times:

    - `time python3 collate_data.py "WorldPop pop density"`: 4.597s
    - `time python3 collate_data.py "WorldPop pop density" -3 VNM`:
        - 2.860s
        - 4.349s
    - `time python3 collate_data.py "WorldPop pop density" -3 PER`:
        - 6.723s
        - 18.760s
    - `time python3 collate_data.py "WorldPop pop density" -d`: 0.209s
    """
    data_type = 'Socio-Demographic Data'
    data_name = 'WorldPop population density'

    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')

    # Set additional parameters
    year = '2020'
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


class EmptyObject:
    """Define an empty object for creating a fake args object for Sphinx."""

    def __init__(self):
        """Initialise."""
        self.data_name = ''
        self.only_one = False
        self.dry_run = False


shorthand_to_data_name = {
    # Epidemiological Data
    'Peru': 'Ministerio de Salud (Peru) data',

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
    'GADM': 'GADM administrative map',
    'GADM admin map': 'GADM administrative map',
}

data_name_to_type = {
    # Epidemiological Data
    'Ministerio de Salud (Peru) data': 'Epidemiological Data',

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
    message = '''Country code in "ISO 3166-1 alpha-3" format.'''
    parser.add_argument('--iso3', '-3', default='', help=message)
    message = '''Show information to help with debugging.'''
    parser.add_argument('--verbose', '-v', help=message, action='store_true')

    # Parse arguments from terminal
    args = parser.parse_args()

    # Extract the arguments
    data_name = args.data_name
    only_one = args.only_one
    dry_run = args.dry_run
    credentials = args.credentials
    year = args.year
    iso3 = args.iso3.upper()
    verbose = args.verbose

    # Check
    if verbose:
        print('Arguments:')
        for arg in vars(args):
            print(f'{arg + ":":20s} {vars(args)[arg]}')

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
    elif data_type == 'Epidemiological Data':
        download_epidemiological_data(data_name, only_one, dry_run, year, iso3)
    elif data_type == 'Geospatial Data':
        download_geospatial_data(data_name, only_one, dry_run, iso3)
    elif data_type == 'Meteorological Data':
        download_meteorological_data(
            data_name, only_one, dry_run, credentials, year
        )
    elif data_type == 'Socio-Demographic Data':
        download_socio_demographic_data(data_name, only_one, dry_run, iso3)
    else:
        raise ValueError(f'Unrecognised data type "{data_type}"')

# If running via Sphinx
else:
    # Create a fake args object so Sphinx doesn't complain it doesn't have
    # command-line arguments
    args = EmptyObject()
