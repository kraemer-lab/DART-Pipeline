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

Password management is done with GNU Privacy Guard (GnuPG or GPG) together with
pass and passpy. The passpy library would have been installed in the previous
step while GnuPG and pass can be installed via `apt` on Ubuntu or `brew` on
macOS:

.. code-block::

    # Ubuntu:
    $ sudo apt-get install gnupg2 -y
    $ sudo apt install pass

.. code-block::

    # macOS:
    $ brew install gnupg
    $ brew install pass

The rest of the password management setup is as follows:

.. code-block::

    $ gpg --gen-key
    $ gpg --list-keys
    $ cd ~/DART-Pipeline
    $ export PASSWORD_STORE_DIR=$PWD/.password-store
    $ pass init <your GnuPG public key>
    $ passpy insert "name for password"
    $ passpy show "name for password"

If you are on macOS and see `OSError: Unable to run gpg (gpg2) - it may not be
available` then it might mean you haven't installed GnuPG yet or it might mean
that you have but macOS can't find it. A solution is to use passpy (from within
Python) and to not use GnuPG (from the Terminal).

**Example Usage**

To download Daily mean temperature product (V1808) meteorological data an
`APHRODITE account <http://aphrodite.st.hirosaki-u.ac.jp/download/>`_ is
needed and the username and password need to be to hand. The password must be
stored in a `pass` password manager in the base directory:

.. code-block::

    $ cd ~/DART-Pipeline
    $ export PASSWORD_STORE_DIR=$PWD/.password-store
    $ pass insert "APHRODITE Daily mean temperature product (V1808)"

The script can then be run (note that these examples use the `--only_one` and
`--dry_run` flags which are meant for script testing purposes):

.. code-block::

    $ python3 collate_data.py --only_one --dry_run # Approx run time: 4.144
    $ python3 collate_data.py --only_one  # Approx run time: 6:36.88

This will create a `Meteorological Data` folder inside the A folder into which
data will be downloaded.
"""
# Create the requirements file with:
# $ python3 -m pip install pipreqs
# $ pipreqs '.' --force
import requests
from lxml import html
import os
import shutil
import py7zr
from pathlib import Path
import passpy
import cdsapi
import platform
import argparse
import utils


def get_password(data_name, username, base_dir='.'):
    """
    Get a password from a passpy store.

    Parameters
    ----------
    data_name : str
        Name of the field that is accessible online but which is
        password-protected.
    username : str
        The username associated with an account that can access the data.
    base_dir : str or pathlib.Path, default '.'
        The base directory of the Git project. It is assumed that the password
        store has been created and is located here.

    Returns
    -------
    password : str
        If successful, the password associated with the entry in the password
        store will be returned.
    """
    store_dir = Path(base_dir, '.password-store')
    # Check what OS you are using
    if platform.system() == 'Linux':
        # GnuPG was installed via:
        # $ sudo apt-get install gnupg2 -y
        store = passpy.Store(store_dir=str(store_dir))
    elif platform.system() == 'Darwin':  # macOS
        # GnuPG was installed via:
        # $ brew install gnupg
        store = passpy.Store(store_dir=str(store_dir), gpg_bin='gpg')
    else:
        raise ValueError(f'Unsupported OS detected: {platform.system()}')
    password = store.get_key(data_name)
    if password is not None:
        password = password[:-1]

    return password


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
    # Only take the first file
    if only_one:
        files = files[:1]

    # Download files on this webpage
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
            file_url = url + '/' + file
            print('Downloading', file_url)
            path = Path(path, file)
            print('to', path)
            if username and password:
                r = requests.get(file_url, auth=(username, password))
            else:
                r = requests.get(file_url)
            # 401: Unauthorized
            # 200: OK
            if r.status_code == 200:
                with open(path, 'wb') as out:
                    for bits in r.iter_content():
                        out.write(bits)
            else:
                print('Failed with status code', r.status_code)

    for child in children:
        relative_url_new = relative_url + '/' + child.removesuffix('/')
        walk(
            base_url, relative_url_new, only_one,
            dry_run, out_dir, username, password
        )


def download_gadm_data(file_format, out_dir, iso3='VNM', level=None):
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


def download_file(url, path):
    """Download a file from a given URL to a given path."""
    print('Downloading', url)
    print('to', path)
    # Make a request for the data
    r = requests.get(url)
    # 401: Unauthorized
    # 200: OK
    if r.status_code == 200:
        with open(path, 'wb') as out:
            out.write(r.content)
        return True
    else:
        print('Failed with status code', r.status_code)
        return False


def unpack_file(path, same_folder=False):
    """Unpack a zipped file."""
    print('Unpacking', path)
    if Path(path).suffix == '.7z':
        foldername = str(path).removesuffix('.7z')
        # Extract the 7z file
        with py7zr.SevenZipFile(path, mode='r') as archive:
            archive.extractall(foldername)
    else:
        if same_folder:
            print('to', path.parent)
            shutil.unpack_archive(path, path.parent)
        else:
            print('to', str(path).removesuffix('.zip'))
            shutil.unpack_archive(path, str(path).removesuffix('.zip'))


class EmptyObject:
    """Define an empty object for creating a fake args object for Sphinx."""

    def __init__(self):
        self.data_name = ''
        self.only_one = False
        self.dry_run = False


# If running directly
if __name__ == '__main__':
    # Perform checks
    utils.check_os()
    utils.check_python()

    # Create command-line argument parser
    desc = 'Download data and store it locally for later processing.'
    parser = argparse.ArgumentParser(description=desc)

    # Add optional arguments
    message = 'The name of the data field to be downloaded and collated.'
    default = ''
    parser.add_argument('--data_name', '-n', default=default, help=message)
    message = '''If set, only one item from each folder in the raw data
    will be downloaded/created.'''
    parser.add_argument('--only_one', '-1', action='store_true', help=message)
    message = '''If set, the raw data will not be downloaded. Instead, empty
    files will be created with the correct names and locations.'''
    parser.add_argument('--dry_run', '-d', action='store_true', help=message)

    # Parse arguments from terminal
    args = parser.parse_args()

# If running via Sphinx
else:
    # Create a fake args object so Sphinx doesn't complain it doesn't have
    # command-line arguments
    args = EmptyObject()

# Check
if True:
    print('Arguments:')
    for arg in vars(args):
        print(f'{arg + ":":20s} {vars(args)[arg]}')

data_name_to_type = {
    'APHRODITE Daily mean temperature product (V1808)': 'Meteorological Data',
    'APHRODITE Daily accumulated precipitation (V1901)': 'Meteorological Data',
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations':
    'Meteorological Data',
    'TerraClimate gridded temperature, precipitation, and other':
    'Meteorological Data',
    'ERA5 atmospheric reanalysis': 'Meteorological Data',
    'WorldPop population density': 'Socio-Demographic Data',
    'WorldPop population count': 'Socio-Demographic Data',
    'GADM administrative map': 'Geospatial Data',
}

# Establish the base directory
path = Path(__file__)
base_dir = utils.get_base_directory(path.parent)

"""
Meteorological data
 └ APHRODITE Daily mean temperature product (V1808)

**Requires APHRODITE account**

```bash
$ cd ~/DART-Pipeline
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "APHRODITE Daily mean temperature product (V1808)"
$ pass "APHRODITE Daily mean temperature product (V1808)"
```

Run times:

time python3 collate_data.py --only_one
6m36.88s

time python3 collate_data.py --only_one --dry_run
4.144s
"""
if args.data_name == 'APHRODITE Daily mean temperature product (V1808)':
    data_type = data_name_to_type[args.data_name]

    # Login credentials
    username = 'rowan.nicholls@dtc.ox.ac.uk'
    password = get_password(args.data_name, username, base_dir)
    # Set parameters
    only_one = args.only_one
    dry_run = args.dry_run

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, args.data_name)
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

**Requires APHRODITE account**

From the base directory:

```bash
$ cd ~/DART-Pipeline
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "APHRODITE Daily accumulated precipitation (V1901)"
$ pass "APHRODITE Daily accumulated precipitation (V1901)"
```

Run times:

- `time python3.12 collate_data.py -n "APHRODITE Daily accumulated
  precipitation (V1901)" --only_one`: 35.674s
"""
if args.data_name == 'APHRODITE Daily accumulated precipitation (V1901)':
    data_type = data_name_to_type[args.data_name]
    # Login credentials
    username = 'rowan.nicholls@dtc.ox.ac.uk'
    password = get_password(args.data_name, username, base_dir)
    # Set parameters
    only_one = args.only_one
    dry_run = args.dry_run

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, args.data_name)
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

Run times:

- `time python3.12 collate_data.py -n "CHIRPS: Rainfall Estimates from Rain
  Gauge and Satellite Observations" --only_one`: 1h21m59.41s
"""
if args.data_name.startswith('CHIRPS: Rainfall Estimates from Rain Gauge and'):
    # Get parameters from arguments
    data_name = args.data_name
    only_one = args.only_one
    dry_run = args.dry_run

    # Set additional parameters
    data_type = data_name_to_type[data_name]

    # Create output directory
    sanitised = data_name.replace(':', ' -')
    out_dir = Path(base_dir, 'A Collate Data', data_type, sanitised)
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

Run times:

- `$ time python3 collate_data.py -n "TerraClimate gridded temperature,
precipitation, and other" --only_one --dry_run`: 4.606s
"""
if args.data_name.startswith('TerraClimate gridded temperature, precipitatio'):
    # Get parameters from arguments
    data_name = args.data_name
    only_one = args.only_one
    dry_run = args.dry_run

    # Set additional parameters
    data_type = data_name_to_type[data_name]

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
$ python3 -m pip install cdsapi
$ cd ~/DART-Pipeline
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "ERA5 atmospheric reanalysis"
$ pass "ERA5 atmospheric reanalysis"
```

Run times:

- `$ time python3 collate_data.py -n "ERA5 atmospheric reanalysis"`: 7.738s
"""
if args.data_name == 'ERA5 atmospheric reanalysis':
    data_type = data_name_to_type[args.data_name]

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, args.data_name)
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

All available datasets are detailed here: https://www.worldpop.org/rest/data

Run times:

time python3 collate_data.py -n "WorldPop population density" --dry_run
- 0m0.732s

time python3 collate_data.py -n "WorldPop population density"
- 0m2.860s
- 0m0.29s
"""
if args.data_name == 'WorldPop population density':
    # Get parameters from arguments
    data_name = args.data_name
    only_one = args.only_one
    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')
    dry_run = args.dry_run

    # Set additional parameters
    data_type = data_name_to_type[data_name]
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

"""
Socio-demographic data
 └ WorldPop population count

All available datasets are detailed here: https://www.worldpop.org/rest/data

Run times:

- `$ time python3 collate_data.py -n "WorldPop population count"`: 406.2s
"""
if args.data_name == 'WorldPop population count':
    # Get parameters from arguments
    data_name = args.data_name
    only_one = args.only_one
    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')
    dry_run = args.dry_run

    # Set additional parameters
    data_type = data_name_to_type[data_name]
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

"""
Geospatial data
 └ GADM administrative map

time python3 collate_data.py -n "GADM administrative map"
- 2m0.457s
- 0m31.094s
"""
if args.data_name == 'GADM administrative map':
    # Get parameters from arguments
    data_name = args.data_name
    only_one = args.only_one
    if only_one:
        print('The --only_one/-1 flag has no effect for this metric')
    dry_run = args.dry_run
    if dry_run:
        print('The --dry_run/-d flag has no effect for this metric')

    # Set additional parameters
    data_type = data_name_to_type[data_name]
    iso3 = 'VNM'

    # Create output directory
    out_dir = Path(base_dir, 'A Collate Data', data_type, data_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    download_gadm_data('Geopackage', out_dir, iso3=iso3)
    download_gadm_data('Shapefile', out_dir, iso3=iso3)
    download_gadm_data('GeoJSON', out_dir, iso3=iso3, level='level0')
    download_gadm_data('GeoJSON', out_dir, iso3=iso3, level='level1')
    download_gadm_data('GeoJSON', out_dir, iso3=iso3, level='level2')
    download_gadm_data('GeoJSON', out_dir, iso3=iso3, level='level3')
