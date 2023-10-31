"""
Collate data.

See "DART dataset summarisation.xls"

$ python3.12 -m pip install pipreqs
$ pipreqs '.' --force

```
python3.12 -m pip install requests
python3.12 -m pip install lxml
python3.12 -m pip install cdsapi
python3.12 -m pip install setuptools
python3.12 -m pip install pandas
```

Password management: gnupg (aka gnupg2, gnupg@2.4, gpg, gpg2)

Ubuntu:
```bash
$ python3.12 -m pip install passpy
$ sudo apt-get install gnupg2 -y
$ gpg --gen-key
$ gpg --list-keys
$ sudo apt install pass
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass init <your GPG public key>
$ passpy insert "APHRODITE Daily mean temperature product (V1808)"
$ passpy show "APHRODITE Daily mean temperature product (V1808)"
```

macOS:
```bash
$ python3.12 -m pip install passpy
$ brew install gnupg
$ brew install pass
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass init <your GPG public key>
```

```python
store = passpy.Store(store_dir='.password-store', gpg_bin='gpg')
store.set_key("APHRODITE Daily accumulated precipitation (V1901)", 'c4KfKXrO')
```

If you see `OSError: Unable to run gpg (gpg2) - it may not be available.` then
it might mean you haven't installed gpg or it might mean that macOS can't find
it. My solution is to use it from within Python, not from the Terminal.
"""
import requests
from lxml import html
import os
from pathlib import Path
import passpy
import cdsapi
import json
import pandas as pd
import platform


def walk(trunk_url, branch_url, username=None, password=None):
    """
    Re-create os.walk for use on a website.

    Parameters
    ----------
    dry_run : bool
        If True, the data will be downloaded. If False, no data will be
        downloaded but, instead, TXT files will be created in the output folder
        with a naming system matching that of a wet run.
    only_one : bool
        If True, only one file from each folder on the remote server will be
        downloaded (or, if `dry_run=True`, have a TXT file created to represent
        it). This is to save on time and space when testing functionality.

    """
    # We want to be able to identify the parent URL
    parent_url = branch_url[:branch_url.rindex('/')]
    # We want to be able to identify links that are list sorters
    sorter_leaves = ['?C=N;O=D', '?C=M;O=A', '?C=S;O=A', '?C=D;O=A']

    page = requests.get(trunk_url + branch_url, auth=(username, password))
    webpage = html.fromstring(page.content)
    link_leaves = webpage.xpath('//a/@href')
    # Classify the links on the page
    sorters = []
    sub_dirs = []
    parent_dirs = []
    files = []
    for link_leaf in link_leaves:
        if link_leaf in sorter_leaves:
            sorters.append(link_leaf)
        elif link_leaf.endswith('/'):
            if (parent_url + '/').endswith(link_leaf):
                # This is the parent dir's URL
                parent_dirs.append(link_leaf)
            else:
                sub_dirs.append(link_leaf)
        else:
            files.append(link_leaf)
    # print(sorters)
    # print(sub_dirs)
    # print(parent_dirs)
    # print(files)
    # print(out_dir)
    # print(trunk_url + branch_url)
    if only_one:
        files = files[:1]
    for file in files:
        # Create folder and intermediate folders
        folderpath = Path(str(out_dir) + branch_url)
        os.makedirs(folderpath, exist_ok=True)
        # Get the file
        if dry_run:
            path = Path(folderpath, file)
            print(f'Creating: "{path}"')
            if not os.path.exists(path):
                with open(path, 'w'):
                    pass
        else:
            url = trunk_url + branch_url + '/' + file
            print(f'Downloading: "{url}"')
            path = Path(folderpath, file)
            print(f'To: "{path}"')
            r = requests.get(url, auth=(username, password))
            # 401: Unauthorized
            # 200: OK
            if r.status_code == 200:
                with open(path, 'wb') as out:
                    for bits in r.iter_content():
                        out.write(bits)

    for sub_dir in sub_dirs:
        walk(trunk_url, branch_url + '/' + sub_dir[:-1], username, password)


def download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2):
    root_url = 'https://www.worldpop.org/rest/data'
    page = requests.get(root_url + '/' + alias_1 + '/' + alias_2)
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
    os.makedirs(path, exist_ok=True)
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
                url = f'{root_url}/{alias_1}/{alias_2}?iso3={iso3}'
                page = requests.get(url)
                content = page.json()
                # Full country name
                country = content['data'][0]['country']
                # Create folder for this country
                path = Path(out_dir, name_1, name_2, country)
                os.makedirs(path, exist_ok=True)
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
                        r = requests.get(file)
                        # 401: Unauthorized
                        # 200: OK
                        if r.status_code == 200:
                            fn = file.split('/')[-1]
                            path = Path(out_dir, name_1, name_2, country, fn)
                            print(f'Downloading {file}')
                            print(f'to {path}')
                            with open(path, 'wb') as out:
                                out.write(r.content)
                # Export summary of available data
                path = Path(out_dir, name_1, name_2, country, f'{country}.csv')
                df.to_csv(path, index=False)


#
# Meteorological data
#

"""
APHRODITE Daily mean temperature product (V1808)

```bash
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "APHRODITE Daily mean temperature product (V1808)"
$ pass "APHRODITE Daily mean temperature product (V1808)"
```
"""
# Create output directory
field = 'APHRODITE Daily mean temperature product (V1808)'
out_dir = Path('Meteorological Data', field)
os.makedirs(out_dir, exist_ok=True)
# Set parameters
dry_run = True
only_one = True
# Login credentials
username = 'rowan.nicholls@dtc.ox.ac.uk'
# Check what OS you are using
if platform.system() == 'Linux':
    # GnuPG was installed via:
    # $ sudo apt-get install gnupg2 -y
    store = passpy.Store(store_dir='.password-store')
elif platform.system() == 'Darwin':  # macOS
    # GnuPG was installed via:
    # $ brew install gnupg
    store = passpy.Store(store_dir='.password-store', gpg_bin='gpg')
password = store.get_key(field)
password = password[:-1]
# URLs should be str, not urllib URL objects, because requests expects str
trunk_url = 'http://aphrodite.st.hirosaki-u.ac.jp'
branch_url = '/product/APHRO_V1808_TEMP'
if False:
    # Walk through the folder structure
    walk(trunk_url, branch_url, username, password)

"""
APHRODITE Daily accumulated precipitation (V1901)

```bash
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "APHRODITE Daily accumulated precipitation (V1901)"
$ pass "APHRODITE Daily accumulated precipitation (V1901)"
```
"""
# Create output directory
field = 'APHRODITE Daily accumulated precipitation (V1901)'
out_dir = Path('Meteorological Data', field)
os.makedirs(out_dir, exist_ok=True)
# Set parameters
dry_run = True
only_one = True
# Login credentials
username = 'rowan.nicholls@dtc.ox.ac.uk'
# Check what OS you are using
if platform.system() == 'Linux':
    # GnuPG was installed via:
    # $ sudo apt-get install gnupg2 -y
    store = passpy.Store(store_dir='.password-store')
elif platform.system() == 'Darwin':  # macOS
    # GnuPG was installed via:
    # $ brew install gnupg
    store = passpy.Store(store_dir='.password-store', gpg_bin='gpg')
password = store.get_key(field)
password = password[:-1]
# URLs should be str, not urllib URL objects, because requests expects str
trunk_url = 'http://aphrodite.st.hirosaki-u.ac.jp'
branch_url = '/product/APHRO_V1901'
if False:
    # Walk through the folder structure
    walk(trunk_url, branch_url, username, password)

"""
CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations
"""
# Create output directory
f = 'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite Observations'
out_dir = Path('Meteorological Data', f)
os.makedirs(out_dir, exist_ok=True)
# Set parameters
dry_run = True
only_one = True
# URLs should be str, not urllib URL objects, because requests expects str
trunk_url = 'https://data.chc.ucsb.edu'
branch_url = '/products/CHIRPS-2.0/'
if False:
    # Walk through the folder structure
    walk(trunk_url, branch_url)

"""
TerraClimate gridded temperature, precipitation, and other water balance
variables
"""
# Create output directory
f = 'TerraClimate gridded temperature, precipitation, and other'
out_dir = Path('Meteorological Data', f)
os.makedirs(out_dir, exist_ok=True)
# Set parameters
dry_run = True
only_one = False
# URLs should be str, not urllib URL objects, because requests expects str
trunk_url = 'https://climate.northwestknowledge.net'
branch_url = '/TERRACLIMATE-DATA'
if False:
    # Walk through the folder structure
    walk(trunk_url, branch_url)

"""
ERA5 atmospheric reanalysis

https://cds.climate.copernicus.eu/api-how-to

```bash
$ python3.12 -m pip install cdsapi
$ export PASSWORD_STORE_DIR=$PWD/.password-store
$ pass insert "ERA5 atmospheric reanalysis"
$ pass "ERA5 atmospheric reanalysis"
```
"""
# Create output directory
f = 'ERA5 atmospheric reanalysis'
out_dir = Path('Meteorological Data', f)
os.makedirs(out_dir, exist_ok=True)
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
if False:
    c.retrieve(
        # Requests follow MARS syntax
        # Keywords 'expver' and 'class' can be dropped. They are obsolete
        # since their values are imposed by 'reanalysis-era5-complete'
        'reanalysis-era5-complete',
        request,
        # Output file. Adapt as you wish.
        Path(out_dir, 'ERA5-ml-temperature-subarea.nc')
    )

#
# Socio-demographic data
#

"""
WorldPop population density
"""
# Create output directory
field = 'WorldPop population density'
out_dir = Path('Socio-Demographic Data', field)
os.makedirs(out_dir, exist_ok=True)

# Get overviews of all the data that is available
if False:
    root_url = 'https://www.worldpop.org/rest/data'
    page = requests.get(root_url)
    content = page.json()
    # Export as JSON
    filepath = Path(out_dir, 'Available Datasets.json')
    with open(filepath, 'w') as file:
        json.dump(content, file)
    # Construct a data frame overview of all the data that is available
    df = pd.DataFrame()
    for macro_data_type in content['data']:
        page = requests.get(root_url + '/' + macro_data_type['alias'])
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
    filepath = Path(out_dir, 'Available Datasets.csv')
    df.to_csv(filepath, index=False)

# Get the population density data
alias_1 = 'pop_density'
name_1 = 'Population Density'
alias_2 = 'pd_ic_1km'
name_2 = 'Unconstrained individual countries (1km resolution)'
if False:
    download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)
alias_2 = 'pd_ic_1km_unadj'
name_2 = 'Unconstrained individual countries UN adjusted (1km resolution)'
if False:
    download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)

"""
WorldPop population count
"""
# Create output directory
field = 'WorldPop population count'
out_dir = Path('Socio-Demographic Data', field)
os.makedirs(out_dir, exist_ok=True)

# Get the population count data
alias_1 = 'pop'
name_1 = 'Population Counts'
alias_2 = 'pic'
name_2 = 'Individual countries'
if False:
    download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)
alias_2 = 'wpgp1km'
name_2 = 'Unconstrained global mosaics 2000-2020 (1km resolution)'
if False:
    download_worldpop_data(out_dir, alias_1, name_1, alias_2, name_2)

#
# Geospatial data
#

"""
GADM administrative map

Administrative levels available (depends on the country):

- level 0: country
- level 1: state (province)
- level 2: county (district)
- level 3: commune/ward (and equivalents)
"""
# Create output directory
field = 'GADM administrative map'
out_dir = Path('Geospatial data', field)
os.makedirs(out_dir, exist_ok=True)


def download_gadm_data(file_format, out_dir, iso3='VNM', level=None):
    """
    Download from GADM.

    List of ISO 3166 country codes:
    https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
    """
    root_url = 'https://geodata.ucdavis.edu/gadm/gadm4.1/'
    if file_format == 'Geopackage':
        leaf_url = f'gpkg/gadm41_{iso3}.gpkg'
    elif file_format == 'Shapefile':
        leaf_url = f'shp/gadm41_{iso3}_shp.zip'
    elif file_format == 'GeoJSON':
        if level == 'level0':
            leaf_url = f'json/gadm41_{iso3}_0.json'
        elif level == 'level1':
            leaf_url = f'json/gadm41_{iso3}_1.json.zip'
        elif level == 'level2':
            leaf_url = f'json/gadm41_{iso3}_2.json.zip'
        elif level == 'level3':
            leaf_url = f'json/gadm41_{iso3}_3.json.zip'
        else:
            raise ValueError(f'Unknown level "{level}"')
    else:
        raise ValueError(f'Unknown file format "{file_format}"')
    # Request the URL
    url = root_url + leaf_url
    r = requests.get(url)
    # 401: Unauthorized
    # 200: OK
    if r.status_code == 200:
        filename = leaf_url.split('/')[-1]
        path = Path(out_dir, filename)
        print(f'Downloading {filename}')
        print(f'to {out_dir}')
        with open(path, 'wb') as out:
            out.write(r.content)


if False:
    download_gadm_data('Geopackage', out_dir, iso3='VNM')
    download_gadm_data('Shapefile', out_dir, iso3='VNM')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level0')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level1')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level2')
    download_gadm_data('GeoJSON', out_dir, iso3='VNM', level='level3')
