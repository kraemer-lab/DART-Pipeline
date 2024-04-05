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
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from shapely.geometry import Point, Polygon
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
from rasterio.transform import xy
import netCDF4 as nc
# Built-in modules
import argparse
import gzip
import json
import os
from pathlib import Path
import struct
from datetime import datetime, timedelta
# Custom modules
import utils
# Create the requirements file with:
# $ python3 -m pip install pipreqs
# $ pipreqs '.' --force

# If Wayland is being used on GNOME, use a different Matplotlib backend
if os.environ.get('WAYLAND_DISPLAY') is not None:
    # Set the Matplotlib backend to one that is compatible with Wayland
    plt.switch_backend('Agg')


def days_to_date(days_since_1900):
    base_date = datetime(1900, 1, 1)
    target_date = base_date + timedelta(days=days_since_1900)

    return target_date


def plot_pop_density(df, folderpath, filename):
    """
    Plot the population for a region.

    Parameters
    ----------
    df : DataFrame
        The data to plot.
    folderpath : str or pathlib.Path
        The parent directory for the output plot.
    filename : str or pathlib.Path
        The filename for the output plot.
    """
    # Plot
    A = 3  # We want figures to be A3
    figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
    fig = plt.figure(figsize=figsize, dpi=144)
    ax = plt.axes()
    # Re-scale
    df['Z_rescaled'] = df['Z']**0.01
    # Plot
    pivotted = df.pivot(index='Y', columns='X', values='Z_rescaled')
    cax = plt.imshow(pivotted, cmap='GnBu')
    # Manually create the colour bar
    ticks = np.linspace(df['Z_rescaled'].min(), df['Z_rescaled'].max(), 5)
    ticklabels = ticks**(1 / 0.01)
    ticklabels = ticklabels.astype(int)
    fig.colorbar(
        cax,
        ticks=ticks,
        format=mticker.FixedFormatter(ticklabels),
        shrink=0.2,
        label='Population Density 2020, UN Adjusted (pop/km²)'
    )
    # Turn upside down
    plt.gca().invert_yaxis()
    # Remove ticks and tick labels
    plt.axis('off')
    # Correct aspect ratio
    ax.set_aspect('equal', adjustable='datalim')
    ax.autoscale()
    # Save
    path = Path(folderpath, filename)
    plt.savefig(path)
    plt.close()


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


def process_geospatial_data(data_name, admin_level, country_iso3):
    """Process Geospatial data."""
    if data_name == 'GADM administrative map':
        process_gadm_admin_map_data(admin_level, country_iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_gadm_admin_map_data(admin_level, country_iso3):
    """
    Process GADM administrative map data.

    Run times:

    - `time python3 process_data.py "GADM admin map" -a 0`: 0m1.036s
    - `time python3 process_data.py "GADM admin map"`: 0m3.830s
    - `time python3 process_data.py "GADM admin map" -a 2`: 0m33.953s
    - `time python3 process_data.py "GADM admin map" -a 3`: 12m30.51s
    """
    filenames = [f'gadm41_{country_iso3}_{admin_level}.shp']
    for filename in filenames:
        filename = Path(filename)
        relative_path = Path(
            'Geospatial Data', 'GADM administrative map',
            f'gadm41_{country_iso3}_shp'
        )

        # Import the shape file
        path = Path(base_dir, 'A Collate Data', relative_path, filename)
        gdf = gpd.read_file(path)

        # Plot
        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot()
        gdf.plot(ax=ax)
        name = gdf.loc[0, 'COUNTRY']
        plt.title(f'{name} - Admin Level {admin_level}')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        # Export
        path = Path(base_dir, 'B Process Data', relative_path, filename.stem)
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f'Exporting "{path}"')
        plt.savefig(path)
        plt.close()

        # Iterate over the regions in the shape file
        for i, row in gdf.iterrows():
            # Initialise new row
            new_row = {}
            # Populate the new row
            new_row['country'] = row['COUNTRY']
            title = row['COUNTRY']
            if 'NAME_1' in list(gdf):
                new_row['name_1'] = row['NAME_1']
                title = row['NAME_1']
            if 'NAME_2' in list(gdf):
                new_row['name_2'] = row['NAME_2']
                title = row['NAME_2']
            if 'NAME_3' in list(gdf):
                new_row['name_3'] = row['NAME_3']
                title = row['NAME_3']

            # Plot
            fig = plt.figure()
            ax = fig.add_subplot()
            if row['geometry'].geom_type == 'MultiPolygon':
                for polygon in row['geometry'].geoms:
                    x, y = polygon.exterior.xy
                    plt.plot(x, y)
            elif row['geometry'].geom_type == 'Polygon':
                x, y = row['geometry'].exterior.xy
                plt.plot(x, y)
            ax.set_aspect('equal')
            plt.title(title)
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
            # Export
            path = Path(
                base_dir, 'B Process Data', relative_path, filename.stem,
                str(title) + '.png'
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            print(f'Exporting "{path}"')
            plt.savefig(path)
            plt.close()


def process_meteorological_data(data_name):
    """Process meteorological data."""
    if data_name == 'APHRODITE Daily accumulated precipitation (V1901)':
        process_aphrodite_precipitation_data()
    elif data_name == 'APHRODITE Daily mean temperature product (V1808)':
        process_aphrodite_temperature_data()
    elif data_name.startswith('CHIRPS: Rainfall Estimates from Rain Gauge an'):
        process_chirps_rainfall_data()
    elif data_name == 'ERA5 atmospheric reanalysis':
        process_era5_reanalysis_data()
    elif data_name.startswith('TerraClimate gridded temperature, precipitati'):
        process_terraclimate_data()
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_aphrodite_precipitation_data():
    """
    Process APHRODITE Daily accumulated precipitation (V1901) data.

    Run times:

    - `time python3 process_data.py "APHRODITE precipitation"`: 0m1.150s
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
        # Check leap year
        if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
            nday = 366
        else:
            nday = 365
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
                print(f'ERROR: File not found - {fname}')
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

    - `time python3 process_data.py "APHRODITE temperature"`: 0m3.018s
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
            # Check leap year
            if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
                nday = 366
            else:
                nday = 365
            # Construct filename
            fname = Path(dir_path, f'APHRO_MA_{product}_{version}.{year}.gz')
        elif product == 'TAVE_050deg':
            year = 2015
            # Check leap year
            if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
                nday = 366
            else:
                nday = 365
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
            print(f'ERROR: File not found - {fname}')
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


def process_chirps_rainfall_data():
    """
    Process CHIRPS Rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.

    Run times:

    - `time python3 process_data.py "CHIRPS rainfall"`: s2m14.596s (one file)
    """
    path = Path(
        base_dir, 'A Collate Data', 'Meteorological Data',
        'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
        'Observations', 'products', 'CHIRPS-2.0', 'global_daily', 'tifs',
        'p05', '2024'
    )
    filepaths = list(path.iterdir())
    # Only process the GeoTIF files
    filepaths = [f for f in filepaths if f.suffix == '.tif']

    for filepath in filepaths:
        print(f'Processing "{filepath.name}"')
        # Open the CHIRPS .tif file
        src = rasterio.open(filepath)
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

        # Create a data frame
        df = pd.DataFrame({
            'longitude': lon,
            'latitude': lat,
            'rainfall': rainfall,
        })
        # Export
        path = Path(
            'Meteorological Data',
            'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
            'Observations', Path(filepath.name).with_suffix('.csv')
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)

        # Plot
        plt.figure(figsize=(20, 8))
        extent = [np.min(lon), np.max(lon), np.min(lat), np.max(lat)]
        # Hide nulls
        data[data == -9999] = 0
        cmap = plt.cm.get_cmap('Blues')
        plt.imshow(data, extent=extent, cmap=cmap)
        plt.colorbar(label='Rainfall [mm]')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title('Rainfall Estimates')
        plt.grid(True)
        path = Path(
            'Meteorological Data',
            'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
            'Observations', Path(filepath.name).with_suffix('.png')
        )
        plt.savefig(path)

        # Plot - log transformed
        plt.figure(figsize=(20, 8))
        extent = [np.min(lon), np.max(lon), np.min(lat), np.max(lat)]
        # Hide nulls
        data[data == -9999] = 0
        # Log transform
        data = np.log(data)
        cmap = plt.cm.get_cmap('Blues')
        plt.imshow(data, extent=extent, cmap=cmap)
        plt.colorbar(shrink=0.8, label='Rainfall [mm, log transformed]')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title('Rainfall Estimates - Log Transformed')
        plt.grid(True)
        path = str(path).removesuffix('.png') + ' - Log Transformed.png'
        plt.savefig(path)


def process_era5_reanalysis_data():
    """
    Process ERA5 atmospheric reanalysis data.

    Run times:

    - `time python3 process_data.py "ERA5 reanalysis"`: 0m2.265s
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


def process_terraclimate_data():
    """
    Process TerraClimate gridded temperature, precipitation, etc.

    Run times:

    - `time python3 process_data.py "TerraClimate data"`: 8m59.88s
    """
    metrics = [
        'aet',  # water_evaporation_amount_mm
        'def',  # water_potential_evaporation_amount_minus_water_evaporation_
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
    for year in ['2023']:
        for metric in metrics:
            filename = f'TerraClimate_{metric}_{year}.nc'
            print(f'Processing "{filename}"')
            path = Path(
                base_dir, 'A Collate Data', 'Meteorological Data',
                'TerraClimate gridded temperature, precipitation, and other',
                'TERRACLIMATE-DATA', filename
            )
            file = nc.Dataset(path, 'r')

            # Construct the metric name
            if metric == 'pdsi':
                metric_name = 'palmer_drought_severity_index'  # unitless
            elif metric == 'tmax':
                metric_name = 'air_temperature_max_degC'
            elif metric == 'tmin':
                metric_name = 'air_temperature_min_degC'
            else:
                units = file[metric].units
                units = units.replace('/', '_per_')
                units = units.replace('^2', '_squared')
                metric_name = file[metric].long_name + '_' + units

            # Import variables as arrays
            longitude = file.variables['lon'][:]
            latitude = file.variables['lat'][:]
            time = file.variables['time'][:]
            raw_data = file.variables[metric][:]

            for i, t in enumerate(time[-1:]):
                lat = np.repeat(latitude, 8640)
                lon = np.tile(longitude, 4320)
                # Get the data for this timepoint, for all lat and lon
                data = raw_data[i, :, :]
                data = data.reshape(4320 * 8640)

                # Stack the latitude, longitude and data arrays horizontally
                ar = np.column_stack((lat, lon, data))

                # Get the date this data represents
                date = days_to_date(t)
                date = date.strftime('%Y-%m-%d')

                # Export
                filename = metric_name + '.csv'
                print(f'Exporting "{date}/{filename}"')
                path = Path(
                    base_dir, 'B Process Data', 'Meteorological Data',
                    'TerraClimate', year, date, filename
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                header = f'latitude,longitude,{metric_name}'
                np.savetxt(path, ar, delimiter=',', header=header, fmt='%f')

            # Close the file
            file.close()


def process_socio_demographic_data(data_name, year, country_iso3, rt):
    """Process socio-demographic data."""
    if data_name == 'WorldPop population count':
        process_worldpop_pop_count_data(year, country_iso3, rt)
    elif data_name == 'WorldPop population density':
        process_worldpop_pop_density_data(year, country_iso3)
    else:
        raise ValueError(f'Unrecognised data name "{data_name}"')


def process_worldpop_pop_count_data(year, country_iso3, rt):
    """
    Process WorldPop population count.

    - EPSG:9217: https://epsg.io/9217
    - EPSG:4326: https://epsg.io/4326
    - EPSG = European Petroleum Survey Group

    Run times:

    - `time python3 process_data.py "WorldPop pop count"`: 43.332s
    """
    # Import
    filename = Path(f'{country_iso3}_{rt}_v2b_{year}_UNadj.tif')
    print(f'Processing "{filename}"')
    path = Path(
        base_dir, 'A Collate Data', 'Socio-Demographic Data',
        'WorldPop population count', 'GIS', 'Population',
        'Individual_countries', country_iso3, filename
    )
    # Load the data
    src = rasterio.open(path)
    # Get the affine transformation coefficients
    transform = src.transform
    # Read data from band 1
    if src.count != 1:
        raise ValueError(f'Unexpected number of bands: {src.count}')
    source_data = src.read(1)

    # Naive plot
    plt.imshow(source_data, cmap='GnBu')
    plt.title('WorldPop Population Count')
    plt.colorbar(shrink=0.8, label='Population')
    # Export
    path = Path(
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population count', country_iso3,
        filename.stem + ' - Naive.png'
    )
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(path)

    # Save the tick details for the next plot
    ylocs, ylabels = plt.yticks()
    xlocs, xlabels = plt.xticks()
    # Trim
    ylocs = ylocs[1:-1]
    xlocs = xlocs[1:-1:2]
    # Finish
    plt.close()

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
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population count', country_iso3,
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
        plt.savefig(path)
        plt.close()

    # Plot - log transformed
    path = Path(
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population count', country_iso3,
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
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population count', country_iso3,
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
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population count', country_iso3,
        filename.stem + ' - Transformed.png'
    )
    if not path.exists():
        plt.imshow(df, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population')
        plt.savefig(path)
        plt.close()


def process_worldpop_pop_density_data(year, country_iso3):
    """
    Process WorldPop population density.

    Run times:

    - `time python3 process_data.py "WorldPop pop density"`: 1.954s
    """
    # Import the population density data
    relative_path = Path(
        'Socio-Demographic Data', 'WorldPop population density', 'GIS',
        'Population_Density', 'Global_2000_2020_1km_UNadj', year, country_iso3,
    )
    iso3_lower = country_iso3.lower()
    filename = Path(f'{iso3_lower}_pd_{year}_1km_UNadj_ASCII_XYZ.zip')
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    df = pd.read_csv(path)

    # Export as-is
    filename = Path(filename.stem + '.csv')
    path = Path(
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population density', year, country_iso3,
        str(filename.stem) + '.csv'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    df.to_csv(path, index=False)

    # Plot
    A = 3  # We want figures to be A3
    figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
    fig, ax = plt.subplots(figsize=figsize)
    pt = df.pivot_table(index='Y', columns='X', values='Z')
    im = ax.imshow(pt, cmap='GnBu')
    ax.invert_yaxis()
    plt.title('Population Density - Naive')
    label = f'Population Density {year}, UN Adjusted (pop/km²)'
    plt.colorbar(im, shrink=0.2, label=label)
    # Remove ticks and tick labels
    plt.axis('off')
    # Export
    path = Path(
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population density', year, country_iso3,
        str(filename.stem) + ' - Naive.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()

    # Plot
    A = 3  # We want figures to be A3
    figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
    fig, ax = plt.subplots(figsize=figsize)
    plt.title('Population Density - Re-Scaled')
    # Re-scale
    df['Z_rescaled'] = df['Z']**0.01
    pt = df.pivot_table(index='Y', columns='X', values='Z_rescaled')
    im = ax.imshow(pt, cmap='GnBu')
    ax.invert_yaxis()
    # Manually create the colour bar
    ticks = np.linspace(df['Z_rescaled'].min(), df['Z_rescaled'].max(), 5)
    ticklabels = ticks**(1 / 0.01)
    ticklabels = ticklabels.astype(int)
    fig.colorbar(
        im,
        ticks=ticks,
        format=mticker.FixedFormatter(ticklabels),
        shrink=0.2,
        label=f'Population Density {year}, UN Adjusted (pop/km²)'
    )
    # Remove ticks and tick labels
    plt.axis('off')
    # Export
    path = Path(
        base_dir, 'B Process Data', 'Socio-Demographic Data',
        'WorldPop population density', year, country_iso3,
        str(filename.stem) + ' - Re-Scaled.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Exporting "{path}"')
    plt.savefig(path)
    plt.close()


def process_geospatial_meteorological_data(
    data_name, admin_level, iso3, year, rt
):
    """Process Geospatial and Meteorological Data."""
    data_name_1 = 'GADM administrative map'
    data_name_2 = \
        'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations'
    if data_name == [data_name_1, data_name_2]:
        process_gadm_chirps_data(admin_level, iso3, year, rt)
    else:
        raise ValueError(f'Unrecognised data names "{data_name}"')


def process_gadm_chirps_data(admin_level, iso3, year, rt):
    """
    Process GADM administrative map and CHIRPS rainfall data.

    Run times:

    - `python3 process_data.py GADM "CHIRPS rainfall" -a 0`: 00:01.763
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 1`: 00:14.640
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 2`: 02:36.276
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 3`: 41:55.092
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 0 -3 GBR`: 00:12.027
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 1 -3 GBR`: 00:05.624
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 2 -3 GBR`: 00:05.626
    - `python3 process_data.py GADM "CHIRPS rainfall" -a 3 -3 GBR`: 00:06.490
    """
    data_type = 'Geospatial and Meteorological Data'
    data_name = 'GADM administrative map and CHIRPS rainfall data'

    # Import the TIFF file
    filename = Path('chirps-v2.0.2024.01.01.tif')
    path = Path(
        base_dir, 'A Collate Data', 'Meteorological Data',
        'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
        'Observations',
        'products', 'CHIRPS-2.0', 'global_daily', 'tifs', 'p05', '2024',
        'chirps-v2.0.2024.01.01.tif'
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
    filename = f'gadm41_{iso3}_{admin_level}.shp'
    path = Path(
        base_dir, 'A Collate Data', 'Geospatial Data',
        'GADM administrative map', iso3, f'gadm41_{iso3}_shp', filename
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
            A = 4  # We want figures to be A4
            figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
            fig = plt.figure(figsize=figsize, dpi=144)
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

    - `python3 process_data.py GADM "WorldPop pop count" -a 0`:
        - 00:10.182
    - `python3 process_data.py GADM "WorldPop pop count" -a 0 -3 PER`:
        - 00:28.003
        - 00:58.943
    - `python3 process_data.py GADM "WorldPop pop count" -a 1`:
        - 01:36.789
    - `python3 process_data.py GADM "WorldPop pop count" -a 1 -3 PER`:
        - 01:11.465
        - 01:28.149
    - `python3 process_data.py GADM "WorldPop pop count" -a 2`:
        - 17:21.086
    - `python3 process_data.py GADM "WorldPop pop count" -a 2 -3 PER`:
        - 01:53.670
    - `python3 process_data.py GADM "WorldPop pop count" -a 3 -3 PER`:
        - 07:20.111
    """
    data_type = 'Geospatial and Socio-Demographic Data'
    data_name = 'GADM administrative map and WorldPop population count'

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
            A = 5  # We want figures to be A5
            figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
            fig = plt.figure(figsize=figsize, dpi=144)
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

    # Calculate population density
    # Import area
    path = Path(
        base_dir, 'B Process Data', 'Geospatial Data',
        'GADM administrative map', iso3, f'Admin Level {admin_level}',
        'Area.csv'
    )
    area = pd.read_csv(path)
    # Merge
    level = int(admin_level)
    on = [f'Admin Level {x}' for x in range(level, level + 1)]
    df = pd.merge(output, area, how='outer', on=on)
    # Calculate
    df['Population Density'] = df['Population'] / df['Area [km²]']
    # Export
    path = Path(
        base_dir, 'B Process Data', data_type, data_name, iso3,
        f'Admin Level {admin_level}', 'Population Density.csv'
    )
    df.to_csv(path, index=False)


def process_gadm_worldpopdensity_data(admin_level, iso3, year, rt):
    """
    Process GADM administrative map and WorldPop population density data.

    Run times:

    - `python3 process_data.py "GADM admin map" "WorldPop pop density" -a 0`
        - 00:01.688
    - `python3 process_data.py "GADM admin map" "WorldPop pop density" -a 1`
        - 00:13.474
    - `python3 process_data.py "GADM admin map" "WorldPop pop density" -a 2`
        - 02:12.969
    - `python3 process_data.py "GADM admin map" "WorldPop pop density" -a 3`
        - 21:20.179
    """
    data_type = 'Geospatial and Socio-Demographic Data'
    data_name = 'GADM administrative map and WorldPop population density'

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

    # Iterate over the regions in the shape file
    for _, region in gdf.iterrows():
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
        # Look at the polygons in the shapefile
        mask = geometry_mask(
            [region['geometry']], out_shape=data.shape,
            transform=src.transform, invert=True
        )
        # Use the mask to extract the region
        region_data = data * mask

        # Plot
        A = 3  # We want figures to be A3
        figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
        fig = plt.figure(figsize=figsize, dpi=144)
        ax = plt.axes()
        if admin_level == 0:
            arr = region_data
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
        img = ax.imshow(z, cmap='GnBu')
        # Manually create the colour bar
        ticks = np.linspace(z.min().min(), z.max().max(), 5)
        ticklabels = ticks**(1 / 0.01)
        ticklabels = ticklabels.astype(int)
        fig.colorbar(
            img,
            ticks=ticks,
            format=mticker.FixedFormatter(ticklabels),
            shrink=0.2,
            label=f'Population Density {year}, UN Adjusted (pop/km²)'
        )
        # Format axes
        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')
        plt.axis('off')
        # Export
        path = Path(
            base_dir, 'B Process Data', data_type, data_name, iso3,
            f'Admin Level {admin_level}', title + '.png'
        )
        os.makedirs(path.parent, exist_ok=True)
        plt.savefig(path)
        plt.close()


class EmptyObject:
    """Define an empty object for creating a fake args object for Sphinx."""

    def __init__(self):
        """Initialise."""
        self.data_name = ''


shorthand_to_data_name = {
    # Meteorological Data
    'APHRODITE precipitation':
    'APHRODITE Daily accumulated precipitation (V1901)',
    'APHRODITE temperature':
    'APHRODITE Daily mean temperature product (V1808)',
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
    message = '''"ppp" (people per pixel) or "pph" (people per hectare).'''
    parser.add_argument('--resolution_type', '-r', help=message)
    message = '''Country code in "ISO 3166-1 alpha-3" format.'''
    parser.add_argument('--iso3', '-3', help=message)
    message = '''Show information to help with debugging.'''
    parser.add_argument('--debug', '-d', help=message, action='store_true')

    # Parse arguments from terminal
    args = parser.parse_args()

    # Check
    if args.debug:
        print('Arguments:')
        for arg in vars(args):
            print(f'{arg + ":":20s} {vars(args)[arg]}')

    # Extract the arguments
    data_name = args.data_name
    iso3 = args.iso3
    admin_level = args.admin_level
    year = args.year
    rt = args.resolution_type

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
    elif data_type == ['Epidemiological Data']:
        process_epidemiological_data(data_name[0], iso3, admin_level)
    elif data_type == ['Geospatial Data']:
        process_geospatial_data(data_name[0], admin_level, iso3)
    elif data_type == ['Meteorological Data']:
        process_meteorological_data(data_name[0])
    elif data_type == ['Socio-Demographic Data']:
        process_socio_demographic_data(data_name[0], year, iso3, rt)

    elif data_type == ['Geospatial Data', 'Meteorological Data']:
        process_geospatial_meteorological_data(
            data_name, admin_level, iso3, year, rt
        )
    elif data_type == ['Geospatial Data', 'Socio-Demographic Data']:
        process_geospatial_sociodemographic_data(
            data_name, admin_level, iso3, year, rt
        )

    else:
        raise ValueError(f'Unrecognised data type "{data_type}"')

# If running via Sphinx
else:
    # Create a fake args object so Sphinx doesn't complain it doesn't have
    # command-line arguments
    args = EmptyObject()
