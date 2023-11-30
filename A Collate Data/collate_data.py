"""
Collate raw data.

See "DART dataset summarisation.xls" for information about the data fields
to be collated.

This documentation assumes Python 3.12 is being used. Other versions have not
been tested.

Package requirements for this script are listed in `requirements.txt` as
created with `pipreqs`:

.. code-block::

    $ python3.12 -m pip install pipreqs
    $ pipreqs '.' --force

These can be installed from the terminal via the following:

.. code-block::

    $ python3.12 -m pip install requests
    $ python3.12 -m pip install lxml
    $ python3.12 -m pip install cdsapi
    $ python3.12 -m pip install setuptools
    $ python3.12 -m pip install pandas
    $ python3.12 -m pip install py7zr
    $ python3.12 -m pip install passpy
    $ python3.12 -m pip install setuptools

Password management is done with gnupg (aka gnupg2, gnupg@2.4, gpg, gpg2):

- Setup on Ubuntu:

.. code-block::

    $ python3.12 -m pip install passpy
    $ sudo apt-get install gnupg2 -y
    $ gpg --gen-key
    $ gpg --list-keys
    $ sudo apt install pass
    $ export PASSWORD_STORE_DIR=$PWD/.password-store
    $ pass init <your GPG public key>
    $ passpy insert "name for password"
    $ passpy show "name for password"

- Setup on macOS:

.. code-block::

    $ python3.12 -m pip install passpy
    $ brew install gnupg
    $ gpg --gen-key
    $ gpg --list-keys
    $ brew install pass
    $ export PASSWORD_STORE_DIR=$PWD/.password-store
    $ pass init <your GPG public key>
    $ passpy insert "name for password"
    $ passpy show "name for password"

If you see `OSError: Unable to run gpg (gpg2) - it may not be available.` then
it might mean you haven't installed gpg or it might mean that macOS can't find
it. My solution is to use it from within Python, not from the Terminal.
"""
import requests
from lxml import html
import os
import shutil
import py7zr
from pathlib import Path
import passpy
import cdsapi
import json
import pandas as pd
import platform


def get_base_directory(path=os.path.abspath('.')):
    """Get the base directory for a git project."""
    while True:
        if '.git' in os.listdir(path):
            return path
        if path == os.path.dirname(path):
            return None  # If the current directory is the root, break the loop
        path = os.path.dirname(path)


def get_password(data_name, username, base_dir='.'):
    """Get a password from a passpy store."""
    store_dir = Path(base_dir, '.password-store')
    # Check what OS you are using
    if platform.system() == 'Linux':
        # GnuPG was installed via:
        # $ sudo apt-get install gnupg2 -y
        store = passpy.Store(store_dir=store_dir)
    elif platform.system() == 'Darwin':  # macOS
        # GnuPG was installed via:
        # $ brew install gnupg
        store = passpy.Store(store_dir=store_dir, gpg_bin='gpg')
    password = store.get_key(data_name)
    password = password[:-1]

    return password


def walk(
    base_url, relative_url, only_one=False, dry_run=False, out_dir='.',
    username=None, password=None
):
    """
    Re-create `os.walk()` and `Path.walk()` for use with a website.

    Parameters
    ----------
    dry_run : bool
        If True, the data will be downloaded. If False, no data will be
        downloaded but, instead, empty files will be created in the output
        folder with a naming system matching that of a wet run.
    only_one : bool
        If True, only one file from each folder on the remote server will be
        downloaded (or, if `dry_run=True`, have a TXT file created to represent
        it). This is to save on time and space when testing functionality.

    """
    url = os.path.join(base_url, relative_url)
    print(url)

    # We want to be able to identify the parent URL
    parent_url = os.path.dirname(url)

    page = requests.get(url, auth=(username, password))
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
    if only_one:
        files = files[:1]
    for file in files:
        # Create folder and intermediate folders
        path = Path(out_dir, relative_url)
        path.mkdir(parents=True, exist_ok=True)
        # Get the file
        if dry_run:
            path = Path(path, file)
            print(f'Creating: "{path}"')
            if not path.exists():
                path.touch()
        else:
            file_url = os.path.join(url, file)
            print(f'Downloading: "{file_url}"')
            path = Path(path, file)
            print(f'To: "{path}"')
            r = requests.get(file_url, auth=(username, password))
            # 401: Unauthorized
            # 200: OK
            if r.status_code == 200:
                with open(path, 'wb') as out:
                    for bits in r.iter_content():
                        out.write(bits)

    for child in children:
        relative_url_new = os.path.join(relative_url, child.removesuffix('/'))
        walk(
            base_url, relative_url_new, only_one,
            dry_run, out_dir, username, password
        )


def download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2):
    """
    Download data from www.worldpop.org.

    A summary of all the available data can be downloaded by running
    `collate_data.py`.

    Parameters
    ----------
    out_dir : str or path object
        Path to the folder to where the data will be downloaded.
    alias_1 : str
        The macro data type's 'alias' as used by the World Pop website.
    name_1 : str
        The macro data type's 'name' as used by the World Pop website.
    alias_2 : str
        The mico data type's 'alias' as used by the World Pop website.
    name_2 : str
        The mico data type's 'name' as used by the World Pop website.
    """
    base_url = 'https://www.worldpop.org/rest/data'
    url = os.path.join(base_url, alias_1, alias_2)
    page = requests.get(url)
    content = page.json()
    # Get the IDs of all the data
    df = pd.DataFrame()
    for datapoint in content['data']:
        new_row = {}
        for key in list(datapoint):
            new_row[key] = datapoint[key]
        # Append new row to master data frame
        new_row = pd.DataFrame(new_row, index=[1])
        df = pd.concat([df, new_row], ignore_index=True)
    path = Path(out_dir, name_1, name_2)
    path.mkdir(parents=True, exist_ok=True)
    path = Path(out_dir, name_1, name_2, f'{name_2}.csv')
    df.to_csv(path, index=False)
    # Get a unique list of the country ISO codes
    iso3s = []
    for datapoint in content['data']:
        if datapoint['iso3'] not in iso3s:
            iso3s.append(datapoint['iso3'])
    # Iterate over countries
    if iso3s[0] is not None:
        for iso3 in iso3s:
            # Download data for Vietnam
            if iso3 == 'VNM':
                # Initialise data frame
                df = pd.DataFrame()
                page = requests.get(url + f'?iso3={iso3}')
                content = page.json()
                # Full country name
                country = content['data'][0]['country']
                # Create folder for this country
                path = Path(out_dir, name_1, name_2, country)
                path.mkdir(parents=True, exist_ok=True)
                # Iterate through entries
                for datapoint in content['data']:
                    new_row = {}
                    for key in list(datapoint):
                        new_row[key] = str(datapoint[key])
                    # Append new row to master data frame
                    new_row = pd.DataFrame(new_row, index=[1])
                    df = pd.concat([df, new_row], ignore_index=True)
                    # Download data files
                    for file in datapoint['files']:
                        # Get the base name
                        basename = os.path.basename(file)
                        path = Path(out_dir, name_1, name_2, country, basename)
                        # Make a request for the data
                        r = requests.get(file)
                        # 401: Unauthorized
                        # 200: OK
                        if r.status_code == 200:
                            print(f'Downloading "{file}"')
                            print(f'to "{path}"')
                            with open(path, 'wb') as out:
                                out.write(r.content)
                        if Path(basename).suffix == '.7z':
                            # Unzip data
                            print(f'Unpacking "{path}"')
                            foldername = str(path).removesuffix('.7z')
                            # Extract the 7z file
                            with py7zr.SevenZipFile(path, mode='r') as archive:
                                archive.extractall(foldername)
                # Export summary of available data
                path = Path(out_dir, name_1, name_2, country, f'{country}.csv')
                df.to_csv(path, index=False)


# Establish the base directory
path = Path(__file__)
base_dir = get_base_directory(path.parent)

"""
Meteorological data
 └ APHRODITE Daily mean temperature product (V1808)

From the base directory:

```bash
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "APHRODITE Daily mean temperature product (V1808)"
$ pass "APHRODITE Daily mean temperature product (V1808)"
```
"""
if False:
    data_type = 'Meteorological Data'
    data_name = 'APHRODITE Daily mean temperature product (V1808)'
    # Login credentials
    username = 'rowan.nicholls@dtc.ox.ac.uk'
    password = get_password(data_name, username, base_dir)
    # Set parameters
    only_one = True
    dry_run = True

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs should be str (not urllib URL objects) because requests expects str
    base_url = 'http://aphrodite.st.hirosaki-u.ac.jp'
    # Dictionary of branch URLs for each resolution
    relative_urls = {
        '0.05 degree': 'product/APHRO_V1808_TEMP/APHRO_MA/005deg',
        '0.05 degree nc': 'product/APHRO_V1808_TEMP/APHRO_MA/005deg_nc',
        '0.25 degree': 'product/APHRO_V1808_TEMP/APHRO_MA/025deg',
        '0.25 degree nc': 'product/APHRO_V1808_TEMP/APHRO_MA/025deg_nc',
        '0.50 degree': 'product/APHRO_V1808_TEMP/APHRO_MA/050deg',
        '0.50 degree nc': 'product/APHRO_V1808_TEMP/APHRO_MA/050deg_nc',
    }
    # Walk through the folder structure
    for key in relative_urls.keys():
        relative_url = relative_urls[key]
        walk(
            base_url, relative_url, only_one, dry_run,
            out_dir, username, password
        )

"""
Meteorological data
 └ APHRODITE Daily accumulated precipitation (V1901)

From the base directory:

```bash
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "APHRODITE Daily accumulated precipitation (V1901)"
$ pass "APHRODITE Daily accumulated precipitation (V1901)"
```
"""
if False:
    data_type = 'Meteorological Data'
    data_name = 'APHRODITE Daily accumulated precipitation (V1901)'
    # Login credentials
    username = 'rowan.nicholls@dtc.ox.ac.uk'
    password = get_password(data_name, username, base_dir)
    # Set parameters
    only_one = True
    dry_run = True

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
    # Walk through the folder structure
    for key in relative_urls.keys():
        relative_url = relative_urls[key]
        walk(
            base_url, relative_url, only_one, dry_run,
            out_dir, username, password
        )

"""
Meteorological data
 └ CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations
"""
if False:
    data_type = 'Meteorological Data'
    data_name = 'CHIRPS - ' + \
        'Rainfall Estimates from Rain Gauge and Satellite Observations'
    # Set parameters
    dry_run = True
    only_one = True

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs should be str, not urllib URL objects, because requests expects str
    base_url = 'https://data.chc.ucsb.edu'
    relative_url = 'products/CHIRPS-2.0'
    # Walk through the folder structure
    walk(base_url, relative_url, only_one, dry_run, out_dir)

"""
Meteorological data
 └ TerraClimate gridded temperature, precipitation, and other water balance
variables
"""
if False:
    data_type = 'Meteorological Data'
    data_name = 'TerraClimate gridded temperature, precipitation, and other'
    # Set parameters
    dry_run = True
    only_one = True

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs should be str, not urllib URL objects, because requests expects str
    base_url = 'https://climate.northwestknowledge.net'
    relative_url = 'TERRACLIMATE-DATA'
    # Walk through the folder structure
    walk(base_url, relative_url, only_one, dry_run, out_dir)

"""
Meteorological data
 └ ERA5 atmospheric reanalysis

How to use the Climate Data Store (CDS) Application Program Interface (API):
https://cds.climate.copernicus.eu/api-how-to

```bash
$ python3.12 -m pip install cdsapi
$ cd ~/DART-Pipeline
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "ERA5 atmospheric reanalysis"
$ pass "ERA5 atmospheric reanalysis"
```
"""
if False:
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


"""
Socio-demographic data
 └ WorldPop population density
"""
if False:
    data_type = 'Socio-Demographic Data'
    data_name = 'WorldPop population density'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    if False:
        # Get overviews of all the data that is available
        base_url = 'https://www.worldpop.org/rest/data'
        page = requests.get(base_url)
        content = page.json()
        # Export as JSON
        path = Path(out_dir, 'Available Datasets.json')
        with open(path, 'w') as file:
            json.dump(content, file)
        # Construct a data frame overview of all the data that is available
        df = pd.DataFrame()
        for macro_data_type in content['data']:
            url = os.path.join(base_url, macro_data_type['alias'])
            page = requests.get(url)
            content = page.json()
            for micro_data_type in content['data']:
                new_row = {}
                new_row['alias_1'] = macro_data_type['alias']
                new_row['name_1'] = macro_data_type['name']
                new_row['title_1'] = macro_data_type['title']
                new_row['desc_1'] = macro_data_type['desc']
                new_row['alias_2'] = micro_data_type['alias']
                new_row['name_2'] = micro_data_type['name']
                # Append new row to master data frame
                new_row = pd.DataFrame(new_row, index=[1])
                df = pd.concat([df, new_row], ignore_index=True)
        path = Path(out_dir, 'Available Datasets.csv')
        df.to_csv(path, index=False)

    alias_1 = 'pop_density'
    name_1 = 'Population Density'
    if False:
        # Get the population density data
        alias_2 = 'pd_ic_1km'
        name_2 = 'Unconstrained individual countries (1km resolution)'
        download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)
    if False:
        alias_2 = 'pd_ic_1km_unadj'
        name_2 = 'Unconstrained individual countries UN adjusted (1km resolution)'
        download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)

"""
Socio-demographic data
 └ WorldPop population count
"""
if False:  # This block takes 406.2s or 949.1s to run
    data_type = 'Socio-Demographic Data'
    data_name = 'WorldPop population count'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Get the population count data
    alias_1 = 'pop'
    name_1 = 'Population Counts'
    alias_2 = 'pic'
    name_2 = 'Individual countries'
    download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)
    alias_2 = 'wpgp1km'
    name_2 = 'Unconstrained global mosaics 2000-2020 (1km resolution)'
    download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)


"""
Geospatial data
 └ GADM administrative map
"""


def download_gadm_data(file_format, out_dir, iso3='VNM', level=None):
    """
    Download from GADM.

    Parameters
    ----------
    file_format : {'Geopackage', 'Shapefile', 'GeoJSON'}
        The format of the raw data to be downloaded.
    out_dir : str or path object
        Folder path to where the files will be downloaded.
    iso3 : str, default 'VNM',
        Alpha-3 country code as per ISO 3166 for the desired country. List of
        country codes:
        https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
    level : {'level0', 'level1', 'level2', 'level3', None}, default None
        Administrative levels available (depends on the country):

        - level 0: country
        - level 1: state (province)
        - level 2: county (district)
        - level 3: commune/ward (and equivalents)
    """
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
    # Request the URL
    url = os.path.join(base_url, relative_url)
    r = requests.get(url)
    # 401: Unauthorized
    # 200: OK
    if r.status_code == 200:
        base_name = Path(relative_url).name
        path = Path(out_dir, base_name)
        print(f'Downloading {base_name}')
        print(f'to {out_dir}')
        with open(path, 'wb') as out:
            out.write(r.content)
    else:
        print(f'Status code {r.status_code} returned')

    # Unpack shape files
    if file_format == 'Shapefile':
        print(f'Unpacking {path}')
        shutil.unpack_archive(path, str(path).removesuffix('.zip'))


if True:  # This block takes 87.3s/13.2s to run
    data_type = 'Geospatial Data'
    data_name = 'GADM administrative map'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    download_gadm_data('Geopackage', out_dir, iso3='VNM')
    download_gadm_data('Shapefile', out_dir, iso3='VNM')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level0')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level1')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level2')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level3')
