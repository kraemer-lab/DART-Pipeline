"""
Script to process raw data that has already been collated.

After the `collate_data.py` script in the "A Collate Data" folder has been run,
the `process_data.py` script in the "B Process Data" folder can be run. This
script has been tested on Python 3.12 and more versions will be tested in the
future.

**Installation and Setup**

As with the A-script, it is recommended to work in a Python virtual
environment specific to this script. Open a terminal in the "B Process Data"
folder and run the following:

.. code-block::

    $ python3 -m venv venv
    $ source venv/bin/activate

The package requirements for the B-script are listed in `requirements.txt` -
install these dependencies by running the following:

.. code-block::

    $ python3 -m pip install -r requirements.txt

Additionally, on macOS, the Geospatial Data Abstraction Library needs to be
installed from Homebrew:

.. code-block::

    $ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/
    install/HEAD/install.sh)"
    $ brew --version
    $ brew install gdal
    $ ogr2ogr --version

**Example Usage**

To process GADM administrative map geospatial data, run one or more of the
following commands (depending on the administrative level you are interested
in, a parameter controlled by the `-a` flag):

.. code-block::

    # Approx run time: 0m1.681s
    $ python3 process_data.py --data_name "GADM administrative map"
    # Approx run time: 0m5.659s
    $ python3 process_data.py --data_name "GADM administrative map" -a 1
    # Approx run time: 0m50.393s
    $ python3 process_data.py --data_name "GADM administrative map" -a 2
    # Approx run time: 8m54.418s
    $ python3 process_data.py --data_name "GADM administrative map" -a 3

These commands will create a "Geospatial Data" sub-folder and output data into
it.

In general, use `EPSG:9217 <https://epsg.io/9217>`_ or
`EPSG:4326 <https://epsg.io/4326>`_ for map projections and use the
`ISO 3166-1 alpha-3 <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`_
format for country codes.
"""
# External libraries
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from pyquadkey2 import quadkey
from rasterio.features import geometry_mask
from rasterio.mask import mask
from rasterio.transform import xy
from shapely.geometry import box, Point
import contextily
import geopandas as gpd
import matplotlib.ticker as mticker
import netCDF4 as nc
import numpy as np
import pandas as pd
import pycountry
import rasterio
# Built-in modules
from pathlib import Path
import argparse
import datetime
import math
import os
import warnings
# Custom modules
import utils
# Create the requirements file with:
# $ python3 -m pip install pipreqs
# $ pipreqs '.' --force

# If Wayland is being used on GNOME, use a different Matplotlib backend
if os.environ.get('WAYLAND_DISPLAY') is not None:
    # Set the Matplotlib backend to one that is compatible with Wayland
    plt.switch_backend('Agg')

# Settings
plt.rc('font', family='serif')
plt.rc('pgf', texsystem='xelatex')
plt.rc(
    'pgf', preamble=r'''
        \usepackage[utf8]{inputenc}
        \usepackage[T1]{fontenc}
        \usepackage{fontspec}
        \usepackage{lmodern}
    '''
)


def days_to_date(days_since_1900):
    """Convert a of number of days since 1900-01-01 into a date."""
    base_date = datetime.datetime(1900, 1, 1)
    target_date = base_date + datetime.timedelta(days=days_since_1900)

    return target_date


def pixel_to_latlon(x, y, transform):
    """
    Convert pixel coordinates to latitude and longitude.

    Parameters
    ----------
    x, y : list
        The x- and y-locations of the pixels to be converted to latitude and
        longitude.
    transform : Affine
        Affine transformation matrix as given in the GeoTIFF file.

    Returns
    -------
    lat, lon : array
        The latitude and longitude coordinates.
    """
    x, y = np.meshgrid(x, y)
    lon, lat = transform * (x, y)

    return lat, lon


def process_economic_data(data_name, iso3):
    """Process economic data."""
    if data_name == 'Relative Wealth Index':
        process_relative_wealth_index_data(iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_relative_wealth_index_data(iso3):
    """
    Process Relative Wealth Index data.

    Run times:
    - `time python3 process_data.py RWI -3 VNM`: 00:04.725
    """
    # Sanitise the inputs and update the user
    data_type = 'Economic Data'
    print(f'Data type:   {data_type}')
    data_name = 'Relative Wealth Index'
    print(f'Data name:   {data_name}')
    country = pycountry.countries.get(alpha_3=iso3).common_name
    print(f'Country:     {country}')
    print('')

    # Import raw data
    path = Path(
        base_dir, 'A Collate Data', 'Economic Data', 'Relative Wealth Index',
        iso3 + '.csv'
    )
    df = pd.read_csv(path)

    # Create plot
    plt.figure(figsize=utils.papersize_inches_a(5))
    plt.scatter(
        df['longitude'], df['latitude'], c=df['rwi'], cmap='viridis', s=0.8,
        marker='s'
    )
    # Add colourbar
    plt.colorbar(shrink=0.3, label='Relative Wealth Index (RWI)')
    # Set labels and title
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(
        rf'\centering\bf Relative Wealth Index\\\normalfont {country}\par',
        y=1.03
    )
    # Export
    path = Path(
        base_dir, 'B Process Data', 'Economic Data', 'Relative Wealth Index',
        iso3 + '.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()

    # Plot using contextily
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude)
    )
    _, ax = plt.subplots(figsize=utils.papersize_inches_a(5))
    gdf_plot = gdf.plot(
        ax=ax, column='rwi', marker='o', markersize=1, legend=True,
        legend_kwds={'shrink': 0.3, 'label': 'Relative Wealth Index (RWI)'}
    )
    contextily.add_basemap(
        ax, crs={'init': 'epsg:4326'},
        source=contextily.providers.OpenStreetMap.Mapnik
    )
    # Set labels and title
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(
        rf'\centering\bf Relative Wealth Index\\\normalfont {country}\par',
        y=1.03
    )
    # Export
    path = Path(
        base_dir, 'B Process Data', 'Economic Data', 'Relative Wealth Index',
        iso3 + ' - With Map.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()


def process_epidemiological_data(data_name, iso3, admin_level):
    """Process Epidemiological Data."""
    if data_name == 'Ministerio de Salud (Peru) data':
        process_ministerio_de_salud_peru_data(admin_level)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_ministerio_de_salud_peru_data(admin_level):
    """
    Process data from the Ministerio de Salud - Peru.

    Run times:

    - `time python3 process_data.py Peru`: 0m1.116s
    - `time python3 process_data.py Peru -a 1`: 0m3.814s

    Parameters
    ----------
    admin_level : {'0', '1'}
        The admin level as a string.
    """
    # Sanitise the inputs and update the user
    data_type = 'Epidemiological Data'
    print(f'Data type:   {data_type}')
    data_name = 'Ministerio de Salud (Peru) data'
    print(f'Data name:   {data_name}')
    iso3 = 'PER'
    country = pycountry.countries.get(alpha_3=iso3).name
    print(f'Country:     {country}')
    if not admin_level:
        admin_level = '0'
        print(f'Admin level: None, defaulting to {admin_level}')
    elif admin_level in ['0', '1']:
        print(f'Admin level: {admin_level}')
    else:
        raise ValueError(f'Invalid admin level: {admin_level}')

    # Find the raw data
    path = Path(base_dir, 'A Collate Data', data_type, data_name)
    if admin_level == '0':
        filepaths = [Path(path, 'casos_dengue_nacional.xlsx')]
    else:
        filepaths = []
        for dirpath, _, filenames in os.walk(path):
            filenames.sort()
            for filename in filenames:
                # Skip hidden files
                if filename.startswith('.'):
                    continue
                # Skip admin levels that have not been requested for analysis
                if filename == 'casos_dengue_nacional.xlsx':
                    continue
                filepaths.append(Path(dirpath, filename))

    # Initialise an output data frame
    master = pd.DataFrame()

    # Import the raw data
    for filepath in filepaths:
        df = pd.read_excel(filepath)

        # Get the name of the administrative divisions
        filename = filepath.name
        name = filename.removesuffix('.xlsx').split('_')[-1].capitalize()
        print(f'Processing {name} data')
        # Add to the output data frame
        df['admin_level_0'] = 'Peru'
        if admin_level == '1':
            df['admin_level_1'] = name
        region = df[f'admin_level_{admin_level}'].head(1)[0]

        # Convert 'year' and 'week' to datetime format
        df['date'] = pd.to_datetime(
            df['ano'].astype(str) + '-' + df['semana'].astype(str) + '-1',
            format='%G-%V-%u'
        )
        # Add to master data frame
        master = pd.concat([master, df], ignore_index=True)

        # Plot the individual region
        fig_region, ax_region = plt.subplots(figsize=utils.papersize_inches_a(6, 'landscape'))
        bl = df['tipo_dx'] == 'C'
        ax_region.plot(
            df[bl]['date'].values, df[bl]['n'].values, c='k', lw=1.2
        )
        ax_region.set_title(f'Confirmed Dengue Cases in {region}')
        ax_region.set_ylabel('Cases')
        ax_region.set_xlabel('Year')
        if len(df[bl]['date']) == 0:
            # If the department does not have any data
            pass
        elif len(df[bl]['date']) == 1:
            # If the department only have one data point, df['date'].max()
            # is infinite and a ValueError is triggered
            pass
        else:
            ax_region.set_xlim(df[bl]['date'].min(), df[bl]['date'].max())
            ax_region.set_ylim(0, df[bl]['n'].max() * 1.1)

        path = Path(
            base_dir, 'B Process Data', data_type, data_name,
            f'Admin Level {admin_level}', region + '.png'
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f'Exporting "{path}"')
        fig_region.savefig(path)
        plt.close(fig_region)

    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name,
        f'Admin Level {admin_level}', f'Admin Level {admin_level}.csv'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    master.to_csv(path, index=False)

    # Create a master plot
    if admin_level != '0':
        fig_all, ax_all = plt.subplots(figsize=utils.papersize_inches_a(6, 'landscape'))

        for filepath in filepaths:
            df = pd.read_excel(filepath)

            # Get the name of the administrative divisions
            filename = filepath.name
            region = filename.removesuffix('.xlsx').split('_')[-1].capitalize()

            # Convert 'year' and 'week' to datetime format
            df['date'] = pd.to_datetime(
                df['ano'].astype(str) + '-' + df['semana'].astype(str) + '-1',
                format='%G-%V-%u'
            )

            # Plot on master plot
            bl = df['tipo_dx'] == 'C'
            ax_all.plot(
                df[bl]['date'].values, df[bl]['n'].values, label=region
            )

        # Finish master plot
        ax_all.set_title('Confirmed Dengue Cases in Peru')
        ax_all.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        plt.subplots_adjust(right=0.75)
        ax_all.set_ylabel('Cases')
        ax_all.set_xlabel('Year')
        ax_all.set_xlim(df[bl]['date'].min(), df[bl]['date'].max())
        y_limits = ax_all.get_ylim()
        ax_all.set_ylim(0, y_limits[1])
        # Export
        path = Path(
            base_dir, 'B Process Data', data_type, data_name,
            f'Admin Level {admin_level}', f'Admin Level {admin_level}.png'
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f'Exporting "{path}"')
        fig_all.savefig(path)
        plt.close(fig_all)


def process_geospatial_data(data_name, admin_level, iso3):
    """
    Process Geospatial data.

    Only one type of geospatial data can be processed by this pipeline: GADM
    administrative maps.

    Parameters
    ----------
    data_name : str {'GADM administrative map', 'GADM admin map', 'GADM'}
        The name of the geospatial data to download.
    admin_level : str {'0', '1', '2', '3'}
        The administrative level of the country at which the geospatial data
        will be processed.
    iso3 : str
        [ISO 3166-1 alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3)
        three-letter country code.
    """
    if data_name == 'GADM administrative map':
        process_gadm_admin_map_data(admin_level, iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_gadm_admin_map_data(admin_level, iso3):
    """
    Process GADM administrative map data.

    Run times:

    - `time python3 process_data.py GADM -a 0 -3 VNM`: 1.790s
    - `time python3 process_data.py GADM -a 1 -3 VNM`: 7.327s
    - `time python3 process_data.py GADM -a 2 -3 VNM`: 1m12.668s
    - `time python3 process_data.py GADM -a 3 -3 VNM`: 20m26.797s
    - `time python3 process_data.py GADM -a 0 -3 PER`: 1.680s
    - `time python3 process_data.py GADM -a 1 -3 PER`: 3.983s
    - `time python3 process_data.py GADM -a 2 -3 PER`: 20.755s
    - `time python3 process_data.py GADM -a 3 -3 PER`: 3m16.707s
    """
    data_type = 'Geospatial Data'
    print(f'Data type:   {data_type}')
    data_name = 'GADM administrative map'
    print(f'Data name:   {data_name}')
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter('always')
        country = pycountry.countries.get(alpha_3=iso3).common_name
        # UserWarning: Country's common_name not found. Country name provided
        # instead.
        if (len(w) > 0) and (issubclass(w[-1].category, UserWarning)):
            country = pycountry.countries.get(alpha_3=iso3).name
    print(f'Country:     {country}')
    print('Admin level:', admin_level)
    print('')

    # Import the shape file
    filename = f'gadm41_{iso3}_{admin_level}.shp'
    path = Path(
        base_dir, 'A Collate Data', data_type, data_name, iso3,
        f'gadm41_{iso3}_shp', filename
    )
    gdf = gpd.read_file(path)

    # en.wikipedia.org/wiki/List_of_national_coordinate_reference_systems
    national_crs = {
        'GBR': 'EPSG:27700',
        'PER': 'EPSG:24892',  # Peru central zone
        'VNM': 'EPSG:4756',
    }
    try:
        gdf = gdf.to_crs(national_crs[iso3])
    except KeyError:
        pass

    # Plot
    fig = plt.figure(figsize=utils.papersize_inches_a(5))
    ax = fig.add_subplot()
    gdf.plot(ax=ax, color='white', edgecolor='black')
    plt.title(
        f'{country}\nAdmin Level {admin_level}',
        y=1.03
    )
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        f'Admin Level {admin_level}.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()

    # Initialise output data frame
    output = pd.DataFrame()
    # Iterate over the regions in the shape file
    for _, region in gdf.iterrows():
        # Initialise a new row for the output data frame
        new_row = {}
        new_row['Admin Level 0'] = region['COUNTRY']
        # Initialise the title
        title = region['COUNTRY']
        # Update the new row and the title if the admin level is high enough
        if int(admin_level) >= 1:
            new_row['Admin Level 1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            new_row['Admin Level 2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            new_row['Admin Level 3'] = region['NAME_3']
            title = region['NAME_3']

        # Plot
        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot()
        if region['geometry'].geom_type == 'MultiPolygon':
            for polygon in region['geometry'].geoms:
                x, y = polygon.exterior.xy
                plt.plot(x, y)
        elif region['geometry'].geom_type == 'Polygon':
            x, y = region['geometry'].exterior.xy
            plt.plot(x, y)
        ax.set_aspect('equal')
        plt.title(title)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        # Export
        filename = str(title).replace('/', '_') + '.png'
        path = Path(
            base_dir, 'B Process Data', data_type, data_name, iso3,
            f'Admin Level {admin_level}', filename
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f'Exporting "{path}"')
        plt.savefig(path)
        plt.close()

        # Calculate area in square metres
        area = region.geometry.area
        # Convert to square kilometers
        area_sq_km = area / 1e6
        # Add to output data frame
        new_row['Area [km²]'] = area_sq_km
        new_row_df = pd.DataFrame(new_row, index=[0])
        output = pd.concat([output, new_row_df], ignore_index=True)

    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        f'Admin Level {admin_level}', 'Area.csv'
    )
    print(f'Exporting "{path}"')
    output.to_csv(path, index=False)


def process_meteorological_data(
    data_name, year=None, month=None, day=None, verbose=False, test=False
):
    """Process meteorological data."""
    if data_name == 'APHRODITE Daily accumulated precipitation (V1901)':
        process_aphrodite_precipitation_data()
    elif data_name == 'APHRODITE Daily mean temperature product (V1808)':
        process_aphrodite_temperature_data()
    elif data_name.startswith('CHIRPS: Rainfall Estimates from Rain Gauge an'):
        process_chirps_rainfall_data(year, month, day, verbose, test)
    elif data_name == 'ERA5 atmospheric reanalysis':
        process_era5_reanalysis_data()
    elif data_name.startswith('TerraClimate gridded temperature, precipitati'):
        process_terraclimate_data(year, month, verbose, test)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_aphrodite_precipitation_data():
    """
    Process APHRODITE Daily accumulated precipitation (V1901) data.

    Run times:

    - `time python3 process_data.py "APHRODITE precipitation"`: 00:01.150
    """
    for res in ['025deg', '050deg']:
        dir_path = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'APHRODITE Daily accumulated precipitation (V1901)', 'product',
            'APHRO_V1901', 'APHRO_MA', res,
        )

        # Version of AphorTemp
        version = 'V1901'

        if res == '025deg':
            nx = 360
            ny = 280
        elif res == '050deg':
            nx = 180
            ny = 140
        else:
            raise ValueError('ERROR: Invalid resolution specified')

        year = 2015
        nday = utils.days_in_year(year)
        # Construct filename
        fname = Path(dir_path, f'APHRO_MA_{res}_{version}.{year}.gz')

        # Initialise output lists
        temp = []
        rstn = []

        print(f'Reading: {fname}')
        print('iday', 'temp', 'rstn')
        for iday in range(1, nday + 1):
            try:
                with open(fname, 'rb') as f:
                    # Seek to the appropriate position in the file for the
                    # current day's data
                    # 4 bytes per float, 2 variables (temp and rstn)
                    f.seek((iday - 1) * nx * ny * 4 * 2)
                    # Read the data for the current day
                    # 2 variables (temp and rstn)
                    data = np.fromfile(f, dtype=np.float32, count=nx * ny * 2)
                    # Replace undefined values with NaN
                    data = np.where(data == -99.9, np.nan, data)
                    data = np.where(data == -np.inf, np.nan, data)
                    data = np.where(data == np.inf, np.nan, data)
                    data = np.where(abs(data) < 0.000000001, np.nan, data)
                    data = np.where(abs(data) > 99999999999, np.nan, data)
                    # Reshape the data based on Fortran's column-major order
                    data = data.reshape((2, nx, ny), order='F')
                    temp_data = data[0, :, :]
                    rstn_data = data[1, :, :]
                    # Get the averages
                    mean_temp = np.nanmean(temp_data)
                    mean_rstn = np.nanmean(rstn_data)
                    # Print average values for temp and rstn
                    print(f'Day {iday}: ', end='')
                    print(f'Temp average = {mean_temp:.2f}, ', end='')
                    print(f'Rstn average = {mean_rstn:.2f}')
                    temp.append(mean_temp)
                    rstn.append(mean_rstn)
            except FileNotFoundError:
                # print(f'ERROR: File not found - {fname}')
                pass
            except ValueError:
                pass

        # Convert lists to DataFrame
        dct = {'temp': temp, 'rstn': rstn}
        df = pd.DataFrame(dct)

        # Export
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'APHRODITE Daily accumulated precipitation (V1901)',
            f'{res}.csv'
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path)


def process_aphrodite_temperature_data():
    """
    Process APHRODITE Daily mean temperature product (V1808) data.

    Run times:

    - `time python3 process_data.py "APHRODITE temperature"`: 00:03.018
    """
    for res in ['005deg', '025deg', '050deg_nc']:
        # Directory where data is stored
        dir_path = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'APHRODITE Daily mean temperature product (V1808)', 'product',
            'APHRO_V1808_TEMP', 'APHRO_MA', res,
        )

        # Version of AphorTemp
        version = 'V1808'

        # Product name
        # The name of the product uses the template "TAVE_YYYdeg" were YYY is
        # 025 or 050
        if res == '005deg':
            product = 'TAVE_CLM_005deg'
            nx = 1800
            ny = 1400
        elif res == '025deg':
            product = 'TAVE_025deg'
            nx = 360
            ny = 280
        elif res == '050deg_nc':
            product = 'TAVE_050deg'
            nx = 180
            ny = 140
        else:
            raise ValueError('ERROR: Invalid resolution specified')

        # Number of days and filename
        if product == 'TAVE_CLM_005deg':
            nday = 366
            # Construct filename
            fname = Path(dir_path, f'APHRO_MA_{product}_{version}.grd.gz')
        elif product == 'TAVE_025deg':
            year = 2015
            nday = utils.days_in_year(year)
            # Construct filename
            fname = Path(dir_path, f'APHRO_MA_{product}_{version}.{year}.gz')
        elif product == 'TAVE_050deg':
            year = 2015
            nday = utils.days_in_year(year)
            # Construct filename
            fname = f'APHRO_MA_{product}_{version}.{year}.nc.gz'
            fname = Path(dir_path, fname)
        else:
            raise ValueError('ERROR: Invalid product specified')

        # Initialise output lists
        temp = []
        rstn = []

        try:
            with open(fname, 'rb') as f:
                print(f'Reading: {fname}')
                print('iday', 'temp', 'rstn')
                for iday in range(1, nday + 1):
                    temp_data = np.fromfile(f, dtype=np.float32, count=nx * ny)
                    rstn_data = np.fromfile(f, dtype=np.float32, count=nx * ny)
                    temp_data = temp_data.reshape((nx, ny))
                    rstn_data = rstn_data.reshape((nx, ny))
                    print(iday, temp_data[0, 0], rstn_data[0, 0])
                    temp.append(temp_data[0, 0])
                    rstn.append(rstn_data[0, 0])
        except FileNotFoundError:
            # print(f'ERROR: File not found - {fname}')
            pass
        except ValueError:
            pass

        # Convert lists to DataFrame
        dct = {'temp': temp, 'rstn': rstn}
        df = pd.DataFrame(dct)

        # Export
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'APHRODITE Daily mean temperature product (V1808)',
            f'{res}.csv'
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path)


def process_chirps_rainfall_data(
    year, month=None, day=None, verbose=False, test=False
):
    """
    Process CHIRPS Rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Run times:

    - `time python3 process_data.py CHIRPS -y 2023 -v`: 1m33.389s
    - `time python3 process_data.py CHIRPS -y 2023 -m 5 -v`: 1m49.979s
    - `time python3 process_data.py CHIRPS -y 2023 -m 5 -d 1 -v`: 3m47.799s
    - `time python3 process_data.py CHIRPS -y 2023 -v -t`: 1m26.609s
    - `time python3 process_data.py CHIRPS -y 2023 -m 5 -v -t`: 1m21.545s
    - `time python3 process_data.py CHIRPS -y 2023 -m 5 -d 1 -v -t`: 2m8.233s
    """
    # Sanitise the inputs
    data_type = 'Meteorological Data'
    data_name = 'CHIRPS: Rainfall Estimates from Rain Gauge and ' + \
        'Satellite Observations'
    if not year:
        msg = 'No year provided. Use the "-y" flag.'
        raise ValueError(msg)
    if year:
        if not month:
            if day:
                msg = 'Year and day but no month provided. Use the "-m" flag.'
                raise ValueError(msg)

    # Re-format
    if month:
        month = f'{int(month):02d}'
    if day:
        day = f'{int(day):02d}'

    # Start constructing the import path
    path_root = Path(
        base_dir, 'A Collate Data', 'Meteorological Data',
        'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
        'Observations'
    )

    # Get the data sub-type to use
    if month:
        if day:
            print(
                f'Global Daily data will be processed for {year}-{month}-{day}'
            )
            filename = f'chirps-v2.0.{year}.{month}.{day}.tif'
            path_stem = Path('global_daily', year, month)
        else:
            print(f'Global Monthly data will be processed for {year}-{month}')
            filename = f'chirps-v2.0.{year}.{month}.tif'
            path_stem = Path('global_monthly', year)
    else:
        print(f'Global Annual data will be processed for {year}')
        filename = f'chirps-v2.0.{year}.tif'
        path_stem = Path('global_annual')

    # Open the CHIRPS .tif file
    path = Path(path_root, path_stem, filename)
    src = rasterio.open(path)
    print(f'Processing "{path}"')
    num_bands = src.count
    if num_bands != 1:
        msg = f'There is a number of bands other than 1: {num_bands}'
        raise ValueError(msg)

    # Get the data in the first band as an array
    data = src.read(1)
    # Get the affine transformation coefficients
    transform = src.transform
    # Get the size of the image
    rows, cols = src.height, src.width

    # Reshape the data into a 1D array
    rainfall = data.flatten()
    # Construct the coordinates for each pixel
    all_rows, all_cols = np.indices((rows, cols))
    lon, lat = xy(transform, all_rows.flatten(), all_cols.flatten())

    # Plot
    plt.figure(figsize=(20, 8))
    extent = [np.min(lon), np.max(lon), np.min(lat), np.max(lat)]
    # Hide nulls
    data[data == -9999] = 0
    cmap = plt.get_cmap('Blues')
    plt.imshow(data, extent=extent, cmap=cmap)
    plt.colorbar(label='Rainfall [mm]')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Rainfall Estimates')
    plt.grid(True)
    path_root = Path(
        base_dir, 'B Process Data', 'Meteorological Data',
        'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
        'Observations'
    )
    path = Path(path_root, path_stem, Path(filename).with_suffix('.png'))
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Saving "{path}"')
    plt.savefig(path)

    # If you're testing, don't create the plots
    if test:
        return

    # Plot - log transformed
    plt.figure(figsize=(20, 8))
    extent = [np.min(lon), np.max(lon), np.min(lat), np.max(lat)]
    # Hide nulls
    data[data == -9999] = 0
    # Log transform
    data = np.log(data)
    cmap = plt.get_cmap('Blues')
    plt.imshow(data, extent=extent, cmap=cmap)
    plt.colorbar(shrink=0.8, label='Rainfall [mm, log transformed]')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Rainfall Estimates - Log Transformed')
    plt.grid(True)
    path = str(path).removesuffix('.png') + ' - Log Transformed.png'
    print(f'Saving "{path}"')
    plt.savefig(path)


def process_era5_reanalysis_data():
    """
    Process ERA5 atmospheric reanalysis data.

    Run times:

    - `time python3 process_data.py "ERA5 reanalysis"`: 00:02.265
    """
    path = Path(
        base_dir, 'A Collate Data', 'Meteorological Data',
        'ERA5 atmospheric reanalysis', 'ERA5-ml-temperature-subarea.nc'
    )
    file = nc.Dataset(path, 'r')

    # Import variables as arrays
    longitude = file.variables['longitude'][:]
    latitude = file.variables['latitude'][:]
    level = file.variables['level'][:]
    time = file.variables['time'][:]
    temp = file.variables['t'][:]
    # Convert Kelvin to Celcius
    temp = temp - 273.15

    longitudes = []
    latitudes = []
    levels = []
    times = []
    temperatures = []
    for i, lon in enumerate(longitude):
        for j, lat in enumerate(latitude):
            for k, lev in enumerate(level):
                for l, t in enumerate(time):
                    longitudes.append(lon)
                    latitudes.append(lat)
                    levels.append(lev)
                    times.append(t)
                    temperatures.append(temp[l, k, j, i])

    # Convert lists to DataFrame
    dct = {
        'longitude': longitudes,
        'latitude': latitudes,
        'level': levels,
        'time': times,
        'temperature': temperatures,
    }
    df = pd.DataFrame(dct)

    # Export
    path = Path(
        base_dir, 'B Process Data', 'Meteorological Data',
        'ERA5 atmospheric reanalysis', 'ERA5-ml-temperature-subarea.csv'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

    # Close the file
    file.close()


def process_terraclimate_data(year, month, verbose=False, test=False):
    """
    Process TerraClimate gridded temperature, precipitation, etc.

    These raw data files are in the netCDF4 (`.nc`) format.

    Run times:

    - `time python3 process_data.py "TerraClimate data" -y 2023 -m 11`:
      23.644s
    """
    # Inform the user
    msg = datetime.datetime(int(year), int(month), 1)
    msg = msg.strftime('%B %Y')
    print(f'Processing data for {msg}')

    if test:
        metrics = ['aet']
    else:
        metrics = [
            'aet',  # water_evaporation_amount_mm
            'def',  # water_potential_evaporation_amount_minus_water_evaporatio
            'pdsi',  # palmer_drought_severity_index (unitless)
            'pet',  # water_potential_evaporation_amount_mm
            'ppt',  # precipitation_amount_mm
            'q',  # runoff_amount_mm
            'soil',  # soil_moisture_content_mm
            'srad',  # downwelling_shortwave_flux_in_air_W_per_m_squared
            'swe',  # liquid_water_content_of_surface_snow_mm
            'tmax',  # air_temperature_max_degC
            'tmin',  # air_temperature_min_degC
            'vap',  # water_vapor_partial_pressure_in_air_kPa
            'vpd',  # vapor_pressure_deficit_kPa
            'ws',  # wind_speed_m_per_s
        ]
    for year in [year]:
        for metric in metrics:
            filename = f'TerraClimate_{metric}_{year}.nc'
            print(f'Processing "{filename}"')
            path = Path(
                base_dir, 'A Collate Data', 'Meteorological Data',
                'TerraClimate gridded temperature, precipitation, and other',
                'TERRACLIMATE-DATA', filename
            )
            file = nc.Dataset(path, 'r')
            # print(file.variables.keys())
            # dict_keys(['lat', 'lon', 'time', 'crs', 'aet'])

            # Construct the metric name and unit
            if metric == 'pdsi':
                metric_name = 'Palmer Drought Severity Index'
                units = 'unitless'
            elif metric == 'tmax':
                metric_name = 'Maximum Air Temperature'
                units = '°C'
            elif metric == 'tmin':
                metric_name = 'Minimum Air Temperature'
                units = '°C'
            else:
                metric_name = file[metric].long_name
                metric_name = metric_name.replace('_', ' ').title()
                units = file[metric].units
                units = units.replace('W/m^2', 'W/m²')

            # Import variables as arrays
            longitude = file.variables['lon'][:]  # shape = (8640,)
            latitude = file.variables['lat'][:]  # shape = (4320,)
            time = file.variables['time']  # shape = (12,)
            raw_data = file.variables[metric]  # shape = (12, 4320, 8640)

            for month in [month]:
                # Convert the month number to an index
                i = int(month) - 1

                # Get the number of days since 1900-01-01
                t = int(time[i])

                # Get the date this data represents
                date = days_to_date(t)

                # Get the data for this timepoint, for all lat and lon
                data = raw_data[i, :, :]

                # Downsample to save memory
                data = data[::2, ::2]
                lon = longitude[::2]
                lat = latitude[::2]

                # Plot data
                fig = plt.figure(figsize=utils.papersize_inches_a(5, 'landscape'))
                ax = plt.axes()
                img = ax.imshow(data, cmap='GnBu')
                # Create the colour bar
                label = f'{metric_name} [{units}]'
                fig.colorbar(img, label=label, shrink=0.4)
                # Get the tick locations
                ylocs, _ = plt.yticks()
                xlocs, _ = plt.xticks()
                # Trim
                ylocs = ylocs[1:-1]
                xlocs = xlocs[1:-1]
                # Use the tick locations as indexes to get the lat and lon
                lat = lat[ylocs.astype(int)].round()
                lon = lon[xlocs.astype(int)].round()
                # Convert the axis ticks from pixels into lat and lon
                plt.yticks(ylocs, lat)
                plt.xticks(xlocs, lon)
                # Add labels
                plt.xlabel('Longitude')
                plt.ylabel('Latitude')
                B_Y = date.strftime('%B %Y')
                ax.set_title(
                    rf'\centering\bf {metric_name}\\\normalfont {B_Y}\par',
                    y=1.1
                )
                plt.tight_layout()
                # Export
                Y_m = date.strftime('%Y-%m')
                path = Path(
                    base_dir, 'B Process Data', 'Meteorological Data',
                    'TerraClimate', Y_m, metric_name
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                plt.savefig(path)
                plt.close()

            # Close the file
            file.close()


def process_socio_demographic_data(data_name, year, iso3, rt, test=False):
    """Process socio-demographic data."""
    if data_name == 'Meta population density':
        process_meta_pop_density_data(year, iso3)
    elif data_name == 'WorldPop population count':
        process_worldpop_pop_count_data(year, iso3, rt, test)
    elif data_name == 'WorldPop population density':
        process_worldpop_pop_density_data(year, iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_meta_pop_density_data(year, iso3):
    """
    Process Population Density Maps from Data for Good at Meta.

    Documentation: https://dataforgood.facebook.com/dfg/docs/
    high-resolution-population-density-maps-demographic-estimates-documentation

    Run times:

    - `python3 process_data.py "Meta pop density" -y 2020 -3 VNM`: 00:13.251
    """
    # Sanitise the inputs
    data_type = 'Socio-Demographic Data'
    print(f'Data type: {data_type}')
    data_name = 'Meta population density'
    print(f'Data name: {data_name}')
    print(f'Year:      {year}')
    if not iso3:
        raise ValueError('No ISO3 code has been provided; use the `-3` flag')
    country = pycountry.countries.get(alpha_3=iso3).common_name
    print(f'Country:   {country}')
    print('')

    metric = f'{iso3.lower()}_general_{year}'

    # Import the data
    path = Path(
        base_dir, 'A Collate Data', data_type, data_name, iso3,
        f'{metric}.csv'
    )
    df = pd.read_csv(path)

    # Log scale
    df = df[df[metric] > 0]
    metric_log = f'{metric}_log'
    df[metric_log] = np.log(df[metric])

    # Define the grid for the heatmap
    n = round(df['latitude'].max() - df['latitude'].min()) * 100
    lat_bins = np.linspace(df['latitude'].min(), df['latitude'].max(), n)
    n = round(df['longitude'].max() - df['longitude'].min()) * 100
    lon_bins = np.linspace(df['longitude'].min(), df['longitude'].max(), n)
    # Create a 2D histogram of the data
    heatmap, xedges, yedges = np.histogram2d(
        df['latitude'], df['longitude'],
        bins=[lat_bins, lon_bins],
        weights=df[metric_log],
        density=False
    )

    # Re-scale
    heatmap = (heatmap - heatmap.min()) / (heatmap.max() - heatmap.min())
    heatmap = heatmap * df[metric_log].max()

    # Create a custom colourmap
    # Define number of colours in the white and green regions of the colormap
    n_white = 40
    n_colours = 256 - n_white
    # Create the white section of the colormap
    # RGBA, A = 1 for opaque
    white = np.ones((n_white, 4))
    # Get the Greens colourmap data
    greens = plt.get_cmap('Greens', n_colours)
    # Combine the white and Greens colourmap data
    colours = np.vstack((white, greens(np.linspace(0.2, 1, n_colours))))
    # Create a new colormap
    cmap = LinearSegmentedColormap.from_list('WhiteGreens', colours)

    # Plot the heatmap
    fig, ax = plt.subplots(figsize=utils.papersize_inches_a(5))
    im = ax.imshow(
        heatmap, origin='lower', cmap=cmap,
        extent=[
            df['longitude'].min(), df['longitude'].max(),
            df['latitude'].min(), df['latitude'].max()
        ]
    )
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(f'Population Density\n{country} - {year}', y=1.03)

    # Manually create the colour bar
    ticks = np.linspace(df[metric_log].min(), df[metric_log].max(), 5)
    ticklabels = np.exp(ticks)
    ticklabels = ticklabels.astype(int)
    fig.colorbar(
        im,
        ticks=ticks,
        format=mticker.FixedFormatter(ticklabels),
        shrink=0.3,
        label='Number of People per square arcsecond'
    )

    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3, year,
        f'{country}.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()


def process_worldpop_pop_count_data(year, iso3, rt, test=False):
    """
    Process WorldPop population count.

    - EPSG:9217: https://epsg.io/9217
    - EPSG:4326: https://epsg.io/4326
    - EPSG = European Petroleum Survey Group

    Run times:

    - `python3 process_data.py "WorldPop pop count" -3 VNM -y 2020 -r ppp`:
        - 43.332s
    - `python3 process_data.py "WorldPop pop count" -3 PER -y 2020`:
        - 2m5.13s
        - 3m27.575s
    """
    # Sanitise the inputs
    data_type = 'Socio-Demographic Data'
    print('Data type:  ', data_type)
    data_name = 'WorldPop population count'
    print('Data name:  ', data_name)
    if not year:
        year = '2020'
        print('Year:       ', 'None, defaulting to 2020')
    else:
        print('Year:       ', year)
    if not iso3:
        raise ValueError('No ISO3 code has been provided; use the `-3` flag')
    country = pycountry.countries.get(alpha_3=iso3).name
    print('Country:    ', country)
    if not rt:
        rt = 'ppp'
        print('Resolution: ', 'None, defaulting to ppp')
    else:
        print('Resolution: ', rt)
    print('')

    # Import
    filename = Path(f'{iso3}_{rt}_v2b_{year}_UNadj.tif')
    path = Path(
        base_dir, 'A Collate Data', data_type, data_name, 'GIS', 'Population',
        'Individual_countries', iso3,
        country.replace(' ', '_') + '_100m_Population', filename,
    )
    # Load the data
    print(f'Processing "{filename}"')
    src = rasterio.open(path)

    # Get the affine transformation coefficients
    transform = src.transform
    # Read data from band 1
    if src.count != 1:
        raise ValueError(f'Unexpected number of bands: {src.count}')
    source_data = src.read(1)

    # Raw plot
    plt.figure(figsize=utils.papersize_inches_a(5))
    plt.imshow(source_data, cmap='GnBu')
    plt.title(
        rf'\centering\bf WorldPop Population Count' +
        rf'\\\normalfont {country} - {year}\par',
        y=1.03
    )
    plt.colorbar(shrink=0.3, label='Population')
    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        filename.stem + ' - Raw.png'
    )
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f'Exporting "{path}"')
        plt.savefig(path)
    # Save the tick details for the next plot
    ylocs, ylabels = plt.yticks()
    xlocs, xlabels = plt.xticks()
    # Trim
    ylocs = ylocs[1:-1]
    xlocs = xlocs[1:-1:2]
    # Finish
    plt.close()

    if test:
        return

    # Replace placeholder numbers with 0
    # (-3.4e+38 is the smallest single-precision floating-point number)
    df = pd.DataFrame(source_data)
    population_data = df[df != -3.4028234663852886e+38]
    """
    Sanity check: calculate the total population
    Google says that Vietnam's population was 96.65 million (2020)

    VNM_pph_v2b_2020.tif
    90,049,150 (2020)

    VNM_pph_v2b_2020_UNadj.tif
    96,355,010 (2020)

    VNM_ppp_v2b_2020.tif
    90,008,170 (2020)

    VNM_ppp_v2b_2020_UNadj.tif
    96,355,000 (2020)
    """
    print(f'Population as per {filename}: {population_data.sum().sum()}')

    # Plot - no normalisation
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        filename.stem + '.png'
    )
    if not path.exists():
        plt.imshow(population_data, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population')
        plt.ylabel('Latitude')
        plt.xlabel('Longitude')
        # Convert pixel coordinates to latitude and longitude
        lat, lon = pixel_to_latlon(xlocs, ylocs, transform)
        # Flatten into a list
        lat = [str(round(x[0], 1)) for x in lat]
        lon = [str(round(x, 1)) for x in lon[0]]
        # Convert the axis ticks from pixels into latitude and longitude
        plt.yticks(ylocs, lat)
        plt.xticks(xlocs, lon)
        # Export
        print(f'Exporting "{path}"')
        plt.savefig(path)
        plt.close()

    # Plot - log transformed
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        filename.stem + ' - Log Scale.png'
    )
    if not path.exists():
        population_data = np.log(population_data)
        plt.imshow(population_data, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population (log)')
        plt.ylabel('Latitude')
        plt.xlabel('Longitude')
        # Convert pixel coordinates to latitude and longitude
        lat, lon = pixel_to_latlon(xlocs, ylocs, transform)
        # Flatten into a list
        lat = [str(round(x[0], 1)) for x in lat]
        lon = [str(round(x, 1)) for x in lon[0]]
        # Convert the axis ticks from pixels into latitude and longitude
        plt.yticks(ylocs, lat)
        plt.xticks(xlocs, lon)
        # Export
        print(f'Exporting "{path}"')
        plt.savefig(path)
        plt.close()

    # Convert pixel coordinates to latitude and longitude
    cols = np.arange(source_data.shape[1])
    lon, _ = rasterio.transform.xy(transform, (1,), cols)
    rows = np.arange(source_data.shape[0])
    _, lat = rasterio.transform.xy(transform, rows, (1,))
    # Replace placeholder numbers with 0
    mask = source_data == -3.4028234663852886e+38
    source_data[mask] = 0
    # Create a DataFrame with latitude, longitude, and pixel values
    df = pd.DataFrame(source_data, index=lat, columns=lon)
    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        filename.stem + '.csv'
    )
    if not path.exists():
        print(f'Exporting "{path}"')
        df.to_csv(path)
    # Sanity checking
    if filename.stem == 'VNM_ppp_v2b_2020_UNadj':
        assert df.to_numpy().sum() == 96355088.0  # 96,355,088

    # Plot - transformed
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        filename.stem + ' - Transformed.png'
    )
    if not path.exists():
        plt.imshow(df, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population')
        plt.savefig(path)
        plt.close()


def process_worldpop_pop_density_data(year, iso3):
    """
    Process WorldPop population density.

    Run times:

    - `time python3 process_data.py "WorldPop pop density"`: 00:02.026
    - `time python3 process_data.py "WorldPop pop density" -3 PER`: 00:04.311
    - `time python3 process_data.py "WorldPop pop density" -y 2020 -3 VNM`:
        - 00:01.954
    """
    data_type = 'Socio-Demographic Data'
    print(f'Data type:   {data_type}')
    data_name = 'WorldPop population density'
    print(f'Data name:   {data_name}')
    print(f'Year:        {year}')
    country = pycountry.countries.get(alpha_3=iso3).common_name
    print(f'Country:     {country}')

    # Import the population density data
    iso3_lower = iso3.lower()
    filename = f'{iso3_lower}_pd_{year}_1km_UNadj_ASCII_XYZ'
    path = Path(
        base_dir, 'A Collate Data', data_type, data_name, 'GIS',
        'Population_Density', 'Global_2000_2020_1km_UNadj', year, iso3,
        Path(filename).with_suffix('.zip')
    )
    df = pd.read_csv(path)

    # Export as-is
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        Path(filename).with_suffix('.csv')
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    df.to_csv(path, index=False)

    # Plot
    fig, ax = plt.subplots(figsize=utils.papersize_inches_a(5))
    pt = df.pivot_table(index='Y', columns='X', values='Z')
    im = ax.imshow(pt, cmap='GnBu')
    ax.invert_yaxis()
    plt.title(
        rf'\centering\bf Population Density\\\normalfont {country}\par',
        y=1.03
    )
    label = f'Population Density {year}, UN Adjusted (pop/km²)'
    plt.colorbar(im, shrink=0.3, label=label)
    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        Path(filename).with_suffix('.png')
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()

    # Plot
    fig, ax = plt.subplots(figsize=utils.papersize_inches_a(5))
    plt.title(
        rf'\centering\bf Population Density - Log Transformed' +
        rf'\\\normalfont {country}\par',
        y=1.03
    )
    # Re-scale
    df = df[df['Z'] > 0]
    df['Z_rescaled'] = np.log(df['Z'])
    pt = df.pivot_table(index='Y', columns='X', values='Z_rescaled')
    im = ax.imshow(pt, cmap='GnBu')
    ax.invert_yaxis()
    # Manually create the colour bar
    ticks = np.linspace(df['Z_rescaled'].min(), df['Z_rescaled'].max(), 5)
    ticklabels = np.exp(ticks)
    ticklabels = ticklabels.astype(int)
    fig.colorbar(
        im,
        ticks=ticks,
        format=mticker.FixedFormatter(ticklabels),
        shrink=0.3,
        label=f'Population Density {year}, UN Adjusted (pop/km²)'
    )
    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        Path(filename + ' - Log Transformed').with_suffix('.png')
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()


def process_geospatial_meteorological_data(
    data_name, admin_level, iso3, year
):
    """Process Geospatial and Meteorological Data."""
    data_name_1 = 'GADM administrative map'
    data_name_2 = \
        'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations'
    if data_name == [data_name_1, data_name_2]:
        process_gadm_chirps_data(admin_level, iso3, year)
    else:
        raise ValueError(f'Unrecognised data names "{data_name}"')


def process_gadm_chirps_data(admin_level, iso3, year):
    """
    Process GADM administrative map and CHIRPS rainfall data.

    Run times:

    - `time python3 process_data.py GADM CHIRPS -a 0`: 1.869s
    - `time python3 process_data.py GADM CHIRPS -a 1`: 14.640s
    - `time python3 process_data.py GADM CHIRPS -a 2`: 2m36.276s
    - `time python3 process_data.py GADM CHIRPS -a 3`: 41m55.092s
    - `time python3 process_data.py GADM CHIRPS -a 0 -3 GBR`: 12.027s
    - `time python3 process_data.py GADM CHIRPS -a 1 -3 GBR`: 5.624s
    - `time python3 process_data.py GADM CHIRPS -a 2 -3 GBR`: 5.626s
    - `time python3 process_data.py GADM CHIRPS -a 3 -3 GBR`: 6.490s
    """
    # Sanitise the inputs
    data_type = 'Geospatial and Meteorological Data'
    data_name = 'GADM administrative map and CHIRPS rainfall data'
    if not admin_level:
        admin_level = '0'
    if not iso3:
        iso3 = 'VNM'
    country = pycountry.countries.get(alpha_3=iso3).name
    if not year:
        year = '2023'

    # Inform the user
    print('Data type:  ', data_type)
    print('Data names: ', data_name)
    print('Admin level:', admin_level)
    print('Country:    ', country)
    print('Year:       ', year)

    # Import the TIFF file
    path = Path(
        base_dir, 'A Collate Data', 'Meteorological Data',
        'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
        'Observations', 'global_daily', year, '05',
        f'chirps-v2.0.{year}.05.01.tif'
    )
    src = rasterio.open(path)
    # Read the first band
    data = src.read(1)
    # Replace negative values (no rainfall measured) with zeros
    data[data < 0] = 0
    # Create a bounding box from raster bounds
    bounds = src.bounds
    raster_bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)

    # Import the shape file
    path = Path(
        base_dir, 'A Collate Data', 'Geospatial Data',
        'GADM administrative map', iso3, f'gadm41_{iso3}_shp',
        f'gadm41_{iso3}_{admin_level}.shp'
    )
    gdf = gpd.read_file(path)
    # Transform the shape file to match the GeoTIFF's coordinate system
    gdf = gdf.to_crs(src.crs)

    # Get the aspect ratio for this region of the Earth
    miny = gdf.bounds['miny'].values[0]
    maxy = gdf.bounds['maxy'].values[0]
    # Calculate the lengths of lines of latitude and longitude at the centroid
    # of the polygon
    centroid_lat = (miny + maxy) / 2.0
    # Approximate length of one degree of latitude in meters
    lat_length = 111.32 * 1000
    # Approximate length of one degree of longitude in meters
    lon_length = 111.32 * 1000 * math.cos(math.radians(centroid_lat))
    # Calculate the stretch factor
    aspect_ratio = lat_length / lon_length

    # Initialise the output file
    output = pd.DataFrame()
    # Iterate over each region in the shape file
    for _, region in gdf.iterrows():
        geometry = region.geometry

        # Initialise a new row for the output data frame
        new_row = {}
        new_row['Admin Level 0'] = region['COUNTRY']
        # Initialise the title
        title = region['COUNTRY']
        # Update the new row and the title if the admin level is high enough
        if int(admin_level) >= 1:
            new_row['Admin Level 1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            new_row['Admin Level 2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            new_row['Admin Level 3'] = region['NAME_3']
            title = region['NAME_3']

        # Check if the rainfall data intersects this region
        if raster_bbox.intersects(geometry):
            # There is rainfall data for this region
            # Clip the data using the polygon of the current region
            region_data, region_transform = mask(src, [geometry], crop=True)
            # Replace negative values (where no rainfall was measured)
            region_data = np.where(region_data < 0, np.nan, region_data)
            region_shape = region_data.shape
            # Define the extent
            extent = [
                region_transform[2],
                region_transform[2] + region_transform[0] * region_shape[2],
                region_transform[5] + region_transform[4] * region_shape[1],
                region_transform[5],
            ]

            # Sum the pixel values to get the total for the region
            region_total = np.nansum(region_data)
            print(title, region_total)

            # Plot
            fig = plt.figure(figsize=utils.papersize_inches_a(4), dpi=144)
            ax = plt.axes()
            # Rainfall data
            img = ax.imshow(region_data[0], extent=extent, cmap='Blues')
            # Manually add colorbar
            fig.colorbar(img, shrink=0.2, label='Rainfall [mm]')
            # Shape data
            gpd.GeoSeries(geometry).plot(ax=ax, color='none')
            # Format
            ax.set_title(f'{title} Rainfall')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            # Adjust the aspect ratio to match this part of the Earth
            ax.set_aspect(aspect_ratio)
            # Export
            path = Path(
                base_dir, 'B Process Data', data_type, data_name, iso3,
                f'Admin Level {admin_level}', title + '.png'
            )
            os.makedirs(path.parent, exist_ok=True)
            plt.savefig(path)
            plt.close()

        else:
            # There is no rainfall data for this region
            region_total = 0
            print(title, region_total)

        # Add to output data frame
        new_row['Rainfall'] = region_total
        new_row_df = pd.DataFrame(new_row, index=[0])
        output = pd.concat([output, new_row_df], ignore_index=True)

    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        f'Admin Level {admin_level}', 'Rainfall.csv'
    )
    output.to_csv(path, index=False)


def process_geospatial_sociodemographic_data(
    data_name, admin_level, iso3, year, rt
):
    """Process Geospatial and Socio-Demographic Data."""
    data_name_1 = 'GADM administrative map'
    if data_name == [data_name_1, 'WorldPop population count']:
        process_gadm_worldpoppopulation_data(admin_level, iso3, year, rt)
    elif data_name == [data_name_1, 'WorldPop population density']:
        process_gadm_worldpopdensity_data(admin_level, iso3, year, rt)
    else:
        raise ValueError(f'Unrecognised data names "{data_name}"')


def process_gadm_worldpoppopulation_data(admin_level, iso3, year, rt):
    """
    Process GADM administrative map and WorldPop population count data.

    Run times:

    - `python3 process_data.py GADM "WorldPop pop count" -a 0`: 10.182s
    - `python3 process_data.py GADM "WorldPop pop count" -a 0 -3 PER`: 58.943s
    - `python3 process_data.py GADM "WorldPop pop count" -a 1 -3 PER`:
        - 1m28.149s
    - `python3 process_data.py GADM "WorldPop pop count" -a 1`: 1m36.789s
    - `python3 process_data.py GADM "WorldPop pop count" -a 2`: 17m21.086s
    - `python3 process_data.py GADM "WorldPop pop count" -a 2 -3 PER`:
        - 1m53.670s
    - `python3 process_data.py GADM "WorldPop pop count" -a 3 -3 PER`:
        - 7m20.111s
    """
    # Sanitise the inputs
    data_type = 'Geospatial and Socio-Demographic Data'
    data_name = 'GADM administrative map and WorldPop population count'
    if not admin_level:
        admin_level = '0'
    if not iso3:
        iso3 = 'VNM'
    country = pycountry.countries.get(alpha_3=iso3).name
    if not year:
        year = '2020'
    if not rt:
        rt = 'ppp'

    # Inform the user
    print('Data type:  ', data_type)
    print('Data names: ', data_name)
    print('Admin level:', admin_level)
    print('Country:    ', country)
    print('Year:       ', year)
    print('Resolution: ', rt)

    # Import the TIFF file
    filename = Path(f'{iso3}_{rt}_v2b_{year}_UNadj.tif')
    path = Path(
        base_dir, 'A Collate Data', 'Socio-Demographic Data',
        'WorldPop population count', 'GIS', 'Population',
        'Individual_countries', iso3
    )
    # Search for the actual folder that has the data
    folders = [d for d in os.listdir(path) if d.endswith('_100m_Population')]
    folder = folders[0]
    # Now we can construct the full path
    path = Path(path, folder, filename)
    # Now we can import it
    src = rasterio.open(path)
    # Read the first band
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data == -3.4028234663852886e+38] = 0
    # Create a bounding box from raster bounds
    bounds = src.bounds
    raster_bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
    # Sanity checking
    if (iso3 == 'VNM') and (year == '2020'):
        assert data.sum() == 96355088.0, \
            f'{data.sum()} != 96355088.0'  # 96,355,088
    if (iso3 == 'PER') and (year == '2020'):
        assert data.sum() == 32434896.0, \
            f'{data.sum()} != 32434896.0'  # 32,434,896

    # Import the shape file
    filename = f'gadm41_{iso3}_{admin_level}.shp'
    path = Path(
        base_dir, 'A Collate Data', 'Geospatial Data',
        'GADM administrative map', iso3, f'gadm41_{iso3}_shp', filename
    )
    gdf = gpd.read_file(path)
    # Transform the shape file to match the GeoTIFF's coordinate system
    # EPSG:4326 - WGS 84: latitude/longitude coordinate system based on the
    # Earth's center of mass
    gdf = gdf.to_crs(src.crs)

    # Get the aspect ratio for this region of the Earth
    miny = gdf.bounds['miny'].values[0]
    maxy = gdf.bounds['maxy'].values[0]
    # Calculate the lengths of lines of latitude and longitude at the centroid
    # of the polygon
    centroid_lat = (miny + maxy) / 2.0
    # Approximate length of one degree of latitude in meters
    lat_length = 111.32 * 1000
    # Approximate length of one degree of longitude in meters
    lon_length = 111.32 * 1000 * math.cos(math.radians(centroid_lat))
    # Calculate the stretch factor
    aspect_ratio = lat_length / lon_length

    # Initialise output data frame
    output = pd.DataFrame()
    # Iterate over the regions in the shape file
    for _, region in gdf.iterrows():
        geometry = region.geometry

        # Initialise a new row for the output data frame
        new_row = {}
        new_row['Admin Level 0'] = region['COUNTRY']
        # Initialise the title
        title = region['COUNTRY']
        # Update the new row and the title if the admin level is high enough
        if int(admin_level) >= 1:
            new_row['Admin Level 1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            new_row['Admin Level 2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            new_row['Admin Level 3'] = region['NAME_3']
            title = region['NAME_3']

        # Check if the population data intersects this region
        if raster_bbox.intersects(geometry):
            # There is population data for this region
            # Clip the data using the polygon of the current region
            region_data, region_transform = mask(src, [geometry], crop=True)
            # Replace negative values (if any exist)
            region_data = np.where(region_data < 0, np.nan, region_data)
            region_shape = region_data.shape
            # Define the extent
            extent = [
                region_transform[2],
                region_transform[2] + region_transform[0] * region_shape[2],
                region_transform[5] + region_transform[4] * region_shape[1],
                region_transform[5],
            ]

            # Sum the pixel values to get the total for the region
            region_total = np.nansum(region_data)
            print(title, region_total)

            # Plot
            fig = plt.figure(figsize=utils.papersize_inches_a(5), dpi=144)
            ax = plt.axes()
            # Rainfall data
            img = ax.imshow(region_data[0], extent=extent, cmap='viridis')
            # Manually add colorbar
            fig.colorbar(img, shrink=0.2, label=f'Population [{rt}]')
            # Shape data
            gpd.GeoSeries(geometry).plot(ax=ax, color='none')
            # Format
            ax.set_title(f'{title} Population')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            # Adjust the aspect ratio to match this part of the Earth
            ax.set_aspect(aspect_ratio)
            # Export
            path = Path(
                base_dir, 'B Process Data', data_type, data_name, iso3,
                f'Admin Level {admin_level}', title + '.png'
            )
            os.makedirs(path.parent, exist_ok=True)
            plt.savefig(path)
            plt.close()

        else:
            # There is no population data for this region
            region_total = 0
            print(title, region_total)

        # Add to output data frame
        new_row['Population'] = region_total
        # Export
        new_row_df = pd.DataFrame(new_row, index=[0])
        output = pd.concat([output, new_row_df], ignore_index=True)

    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        f'Admin Level {admin_level}', 'Population.csv'
    )
    output.to_csv(path, index=False)

    # # Calculate population density
    # # Import area
    # path = Path(
    #     base_dir, 'B Process Data', 'Geospatial Data',
    #     'GADM administrative map', iso3, f'Admin Level {admin_level}',
    #     'Area.csv'
    # )
    # area = pd.read_csv(path)
    # # Merge
    # level = int(admin_level)
    # on = [f'Admin Level {x}' for x in range(level, level + 1)]
    # df = pd.merge(output, area, how='outer', on=on)
    # # Calculate
    # df['Population Density'] = df['Population'] / df['Area [km²]']
    # # Export
    # path = Path(
    #     base_dir, 'B Process Data', data_type, data_name, iso3,
    #     f'Admin Level {admin_level}', 'Population Density.csv'
    # )
    # df.to_csv(path, index=False)


def process_gadm_worldpopdensity_data(admin_level, iso3, year, rt):
    """
    Process GADM administrative map and WorldPop population density data.

    Run times:

    - `time python3 process_data.py GADM "WorldPop pop density" -a 0`
        - 1.688s
    - `time python3 process_data.py GADM "WorldPop pop density" -a 1`
        - 13.474s
    - `time python3 process_data.py GADM "WorldPop pop density" -a 2`
        - 2m12.969s
    - `time python3 process_data.py GADM "WorldPop pop density" -a 3`
        - 21m20.179s
    """
    # Sanitise the inputs
    data_type = 'Geospatial and Socio-Demographic Data'
    data_name = 'GADM administrative map and WorldPop population density'
    if not admin_level:
        admin_level = '0'
    if not iso3:
        iso3 = 'VNM'
    country = pycountry.countries.get(alpha_3=iso3).name
    if not year:
        year = '2020'
    if not rt:
        rt = 'ppp'

    # Inform the user
    print('Data type:  ', data_type)
    print('Data names: ', data_name)
    print('Admin level:', admin_level)
    print('Country:    ', country)
    print('Year:       ', year)
    print('Resolution: ', rt)

    # Import the population density data
    filename = Path(f'{iso3.lower()}_pd_{year}_1km_UNadj.tif')
    path = Path(
        base_dir, 'A Collate Data', 'Socio-Demographic Data',
        'WorldPop population density', 'GIS', 'Population_Density',
        'Global_2000_2020_1km_UNadj', year, iso3, filename
    )
    src = rasterio.open(path)
    # Read data from band 1
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data < 0] = 0

    # Import the relevant shape file
    filename = f'gadm41_{iso3}_{admin_level}.shp'
    path = Path(
        base_dir, 'A Collate Data', 'Geospatial Data',
        'GADM administrative map', iso3, f'gadm41_{iso3}_shp', filename
    )
    gdf = gpd.read_file(path)

    # Get the aspect ratio for this region of the Earth
    miny = gdf.bounds['miny'].values[0]
    maxy = gdf.bounds['maxy'].values[0]
    # Calculate the lengths of lines of latitude and longitude at the centroid
    # of the polygon
    centroid_lat = (miny + maxy) / 2.0
    # Approximate length of one degree of latitude in meters
    lat_length = 111.32 * 1000
    # Approximate length of one degree of longitude in meters
    lon_length = 111.32 * 1000 * math.cos(math.radians(centroid_lat))
    # Calculate the stretch factor
    aspect_ratio = lat_length / lon_length

    # Iterate over the regions in the shape file
    for _, region in gdf.iterrows():
        geometry = region.geometry

        # Initialise new row
        new_row = {}
        new_row['Admin Level 0'] = region['COUNTRY']
        # Initialise the title
        title = region['COUNTRY']
        # Update the new row and the title if the admin level is high enough
        if 'NAME_1' in list(gdf):
            new_row['Admin Level 1'] = region['NAME_1']
            title = region['NAME_1']
        if 'NAME_2' in list(gdf):
            new_row['Admin Level 2'] = region['NAME_2']
            title = region['NAME_2']
        if 'NAME_3' in list(gdf):
            new_row['Admin Level 3'] = region['NAME_3']
            title = region['NAME_3']
        print(title)

        # Clip the data using the polygon of the current region
        region_data, region_transform = mask(src, [geometry], crop=True)
        # Replace negative values (if any exist)
        region_data = np.where(region_data < 0, np.nan, region_data)
        region_shape = region_data.shape
        # Define the extent
        extent = [
            region_transform[2],
            region_transform[2] + region_transform[0] * region_shape[2],
            region_transform[5] + region_transform[4] * region_shape[1],
            region_transform[5],
        ]

        # Plot
        fig = plt.figure(figsize=utils.papersize_inches_a(5), dpi=144)
        ax = plt.axes()
        if admin_level == '0':
            arr = region_data[0]
            arr[arr == 0] = np.nan
            # Re-scale
            arr = arr**0.01
            z = arr
        else:
            df = pd.DataFrame(region_data)
            df = df.replace(0, np.nan)
            df = df.dropna(how='all', axis=0)
            df = df.dropna(how='all', axis=1)
            # Re-scale
            df = df**0.01
            z = df
        img = ax.imshow(z, extent=extent, cmap='GnBu')
        # Manually create the colour bar
        ticks = np.linspace(np.nanmin(z), np.nanmax(z), 5)
        ticklabels = ticks**(1 / 0.01)
        ticklabels = ticklabels.astype(int)
        fig.colorbar(
            img,
            ticks=ticks,
            format=mticker.FixedFormatter(ticklabels),
            shrink=0.2,
            label=f'Population Density {year}, UN Adjusted (pop/km²)'
        )
        # Shape data
        gpd.GeoSeries(geometry).plot(ax=ax, color='none')
        # Format axes
        ax.set_title(f'{title} Population Density')
        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')
        # Adjust the aspect ratio to match this part of the Earth
        ax.set_aspect(aspect_ratio)
        # Export
        path = Path(
            base_dir, 'B Process Data', data_type, data_name, iso3,
            f'Admin Level {admin_level}', title + '.png'
        )
        os.makedirs(path.parent, exist_ok=True)
        plt.savefig(path)
        plt.close()


def process_economic_geospatial_sociodemographic_data(
    data_name, iso3, admin_level
):
    """Process Economic, Geospatial and Socio-Demographic Data."""
    data_name_1 = 'Relative Wealth Index'
    data_name_2 = 'GADM administrative map'
    data_name_3 = 'Meta population density'
    if data_name == [data_name_1, data_name_2, data_name_3]:
        process_pop_weighted_relative_wealth_index_data(iso3, admin_level)
    else:
        raise ValueError(f'Unrecognised data names "{data_name}"')


def get_admin_region(lat, lon, polygons):
    """
    Find the admin region in which a gridcell lies.

    Return the ID of administrative region in which the centre (given by
    latitude and longitude) of a 2.4km^2 gridcell lies.

    Parameters
    ----------
    lat : double
    lon : double
    polygons : dict

    Returns
    -------
    geo_id : str
    """
    point = Point(lon, lat)
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return 'null'


def process_pop_weighted_relative_wealth_index_data(iso3, admin_level='0'):
    """
    Process Population Weighted Relative Wealth Index.

    Adapted from:
    https://dataforgood.facebook.com/dfg/docs/
    tutorial-calculating-population-weighted-relative-wealth-index

    Run times:

    - `python3 process_data.py RWI GADM "Meta pop density" -3 VNM`: 2m36.11s
    """
    # Sanitise the inputs
    print('Data types:  Economic, Geospatial and Socio-Demographic')
    print('Data names:  Relative Wealth Index, GADM administrative ', end='')
    print('map and Meta population density')
    if not iso3:
        raise ValueError('No ISO3 code has been provided; use the `-3` flag')
    country = pycountry.countries.get(alpha_3=iso3).common_name
    print('Country:    ', country)
    print('Admin level:', admin_level)
    print('')

    # Import raw data
    shpfile = Path(
        base_dir, 'A Collate Data', 'Geospatial Data',
        'GADM administrative map', iso3, f'gadm41_{iso3}_shp',
        f'gadm41_{iso3}_2.shp'
    )
    rwifile = Path(
        base_dir, 'data', 'vnm_relative_wealth_index.csv'
    )
    popfile = Path(
        base_dir, 'A Collate Data', 'Socio-Demographic Data',
        'Meta population density', iso3, f'{iso3.lower()}_general_2020.csv'
    )

    # Create a dictionary of polygons where the key is the ID of the polygon
    # and the value is its geometry
    shapefile = gpd.read_file(shpfile)
    admin_geoid = f'GID_{admin_level}'
    polygons = dict(zip(shapefile[admin_geoid], shapefile['geometry']))

    # Classify the locations of the RWI values if this has not been done
    path = Path(
        base_dir, 'B Process Data', 'Economic Data', 'Relative Wealth Index',
        f'{iso3}.csv'
    )
    if path.exists():
        print('Locations of RWI values are already classified')
        rwi = pd.read_csv(path)
    else:
        print('Classifying locations of RWI values')
        rwi = pd.read_csv(rwifile)
        rwi['geo_id'] = rwi.apply(
            lambda x: get_admin_region(
                x['latitude'], x['longitude'], polygons
            ), axis=1
        )
        rwi = rwi[rwi['geo_id'] != 'null']

        path.parent.mkdir(parents=True, exist_ok=True)
        rwi.to_csv(path)

    # Convert population data from Meta into a data frame with the total
    # population for tiles of zoom level 14 (Bing tiles) using quadkeys
    population = pd.read_csv(popfile)
    colname = f'{iso3.lower()}_general_2020'
    population = population.rename(columns={colname: 'pop_2020'})
    population['quadkey'] = population.apply(
        lambda x: str(quadkey.from_geo((x['latitude'], x['longitude']), 14)),
        axis=1
    )
    bing_tile_z14_pop = population.groupby(
        'quadkey', as_index=False
    )['pop_2020'].sum()
    bing_tile_z14_pop['quadkey'] = \
        bing_tile_z14_pop['quadkey'].astype(np.int64)

    # Merge with the shape file
    shapefile = gpd.read_file(shpfile)
    rwi_pop = rwi.merge(
        bing_tile_z14_pop[['quadkey', 'pop_2020']], on='quadkey', how='inner'
    )
    # Aggregate
    geo_pop = rwi_pop.groupby('geo_id', as_index=False)['pop_2020'].sum()
    geo_pop = geo_pop.rename(columns={'pop_2020': 'geo_2020'})
    rwi_pop = rwi_pop.merge(geo_pop, on='geo_id', how='inner')
    # Convert to *population weighted* RWI
    rwi_pop['pop_weight'] = rwi_pop['pop_2020'] / rwi_pop['geo_2020']
    rwi_pop['rwi_weight'] = rwi_pop['rwi'] * rwi_pop['pop_weight']
    geo_rwi = rwi_pop.groupby('geo_id', as_index=False)['rwi_weight'].sum()
    shapefile_rwi = shapefile.merge(
        geo_rwi, left_on=admin_geoid, right_on='geo_id'
    )

    # Plot
    fig, ax = plt.subplots(figsize=utils.papersize_inches_a(5))
    shapefile_rwi.plot(
        ax=ax, column='rwi_weight', marker='o', markersize=1, legend=True,
        label='RWI score'
    )
    contextily.add_basemap(
        ax, crs={'init': 'epsg:4326'},
        source=contextily.providers.OpenStreetMap.Mapnik
    )
    plt.title('Relative Wealth Index')
    subtitle = f'{country} - Admin Level {admin_level}'
    plt.suptitle(subtitle, fontsize=12, fontweight='bold')
    path = Path(
        base_dir, 'B Process Data',
        'Economic, Geospatial and Socio-Demographic Data',
        'Relative Wealth Index, GADM administrative map and ' +
        'Meta population density',
        iso3, f'Admin Level {admin_level}.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Saving figure to "{path}"')
    plt.savefig(path, dpi=600)


class EmptyObject:
    """Define an empty object for creating a fake args object for Sphinx."""

    def __init__(self):
        """Initialise."""
        self.data_name = ''


shorthand_to_data_name = {
    # Economic data
    'RWI': 'Relative Wealth Index',

    # Epidemiological Data
    'Peru': 'Ministerio de Salud (Peru) data',

    # Meteorological Data
    'APHRODITE precipitation':
    'APHRODITE Daily accumulated precipitation (V1901)',
    'APHRODITE temperature':
    'APHRODITE Daily mean temperature product (V1808)',
    'CHIRPS':
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations',
    'CHIRPS rainfall':
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations',
    'CHIRPS':
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations',
    'TerraClimate data':
    'TerraClimate gridded temperature, precipitation, and other',
    'ERA5 reanalysis':
    'ERA5 atmospheric reanalysis',

    # Socio-Demographic Data
    'Meta pop density': 'Meta population density',
    'WorldPop pop density': 'WorldPop population density',
    'WorldPop pop count': 'WorldPop population count',

    # Geospatial Data
    'GADM admin map': 'GADM administrative map',
    'GADM': 'GADM administrative map',
}

data_name_to_type = {
    # Economic data
    'Relative Wealth Index': 'Economic Data',

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
    'Meta population density': 'Socio-Demographic Data',
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
    desc = 'Process data that has been previously downloaded and collated.'
    parser = argparse.ArgumentParser(description=desc)

    # Add positional arguments
    message = 'The name of the data field(s) to be processed.'
    default = []
    parser.add_argument('data_name', nargs='*', default=default, help=message)

    # Add optional arguments
    message = '''Some data fields have different data for each administrative
    level'''
    parser.add_argument('--admin_level', '-a', help=message)
    message = '''Some data fields have data available for multiple years.'''
    parser.add_argument('--year', '-y', help=message)
    message = '''Some data fields have data available for multiple months.'''
    parser.add_argument('--month', '-m', help=message)
    message = '''Some data fields have data available for multiple days.'''
    parser.add_argument('--day', '-d', help=message)
    message = '''"ppp" (people per pixel) or "pph" (people per hectare).'''
    parser.add_argument('--resolution_type', '-r', help=message)
    message = '''Country code in "ISO 3166-1 alpha-3" format.'''
    parser.add_argument('--iso3', '-3', help=message)
    message = '''Show information to help with debugging.'''
    parser.add_argument('--verbose', '-v', help=message, action='store_true')
    message = '''Run in test mode.'''
    parser.add_argument('--test', '-t', help=message, action='store_true')

    # Parse arguments from terminal
    args = parser.parse_args()

    # Extract the arguments
    data_name = args.data_name
    iso3 = args.iso3
    admin_level = args.admin_level
    year = args.year
    month = args.month
    day = args.day
    rt = args.resolution_type
    verbose = args.verbose
    test = args.test

    # Check
    if verbose:
        print('Arguments:')
        for arg in vars(args):
            print(f'{arg + ":":20s} {vars(args)[arg]}')

    # Convert shorthand names to full names
    for i, name in enumerate(data_name):
        if name in shorthand_to_data_name.keys():
            data_name[i] = shorthand_to_data_name[name]
    # Get macro data type
    data_type = []
    for name in data_name:
        if name in data_name_to_type.keys():
            data_type.append(data_name_to_type[name])

    if data_name == []:
        print('No data name has been provided. Exiting the programme.')
    elif data_type == ['Economic Data']:
        process_economic_data(data_name[0], iso3)
    elif data_type == ['Epidemiological Data']:
        process_epidemiological_data(data_name[0], iso3, admin_level)
    elif data_type == ['Geospatial Data']:
        process_geospatial_data(data_name[0], admin_level, iso3)
    elif data_type == ['Meteorological Data']:
        process_meteorological_data(
            data_name[0], year, month, day, verbose, test
        )
    elif data_type == ['Socio-Demographic Data']:
        process_socio_demographic_data(data_name[0], year, iso3, rt)

    elif data_type == ['Geospatial Data', 'Meteorological Data']:
        process_geospatial_meteorological_data(
            data_name, admin_level, iso3, year
        )
    elif data_type == ['Geospatial Data', 'Socio-Demographic Data']:
        process_geospatial_sociodemographic_data(
            data_name, admin_level, iso3, year, rt
        )

    elif data_type == [
        'Economic Data', 'Geospatial Data', 'Socio-Demographic Data'
    ]:
        process_economic_geospatial_sociodemographic_data(
            data_name, iso3, admin_level
        )

    else:
        raise ValueError(f'Unrecognised data type "{data_type}"')

# If running via Sphinx
else:
    # Create a fake args object so Sphinx doesn't complain it doesn't have
    # command-line arguments
    args = EmptyObject()
