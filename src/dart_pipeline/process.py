"""
Functions to process raw data that has already been collated.

To process GADM administrative map geospatial data, run one or more of the
following commands

.. code-block::

    $ uv run dart-pipeline process geospatial/gadm admin-level=1


In general, use `EPSG:9217 <https://epsg.io/9217>`_ or
`EPSG:4326 <https://epsg.io/4326>`_ for map projections and use the
`ISO 3166-1 alpha-3 <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`_
format for country codes.
"""
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal, Callable
import logging
import os
import re

from matplotlib import pyplot as plt
from pandarallel import pandarallel
import geopandas as gpd
import netCDF4 as nc
import numpy as np
import pandas as pd
import rasterio
import rasterio.features
import rasterio.mask
import rasterio.transform
import shapely.geometry

from .plots import \
    plot_heatmap, plot_gadm_micro_heatmap, plot_gadm_macro_heatmap, \
    plot_timeseries, plot_scatter, plot_gadm_scatter
from .util import \
    source_path, days_in_year, output_path, get_country_name, get_shapefile
from .types import ProcessResult, PartialDate, AdminLevel
from .constants import TERRACLIMATE_METRICS, OUTPUT_COLUMNS

pandarallel.initialize(verbose=0)

TEST_MODE = os.getenv("DART_PIPELINE_TEST")
# Smallest single-precision floating-point number
MIN_FLOAT = -3.4028234663852886e38


def process_rwi(iso3: str, admin_level: str, plots=False):
    """Process Relative Wealth Index data."""
    source = 'economic/relative-wealth-index'
    iso3 = iso3.upper()
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('plots:%s', plots)

    # Create a dictionary of polygons where the key is the ID of the polygon
    # and the value is its geometry
    path = get_shapefile(iso3, admin_level)
    logging.info('Importing:%s', path)
    gdf = gpd.read_file(path)
    admin_geoid = f'GID_{admin_level}'
    polygons = dict(zip(gdf[admin_geoid], gdf['geometry']))

    # Import the relative wealth index data
    path = source_path(source, f'{iso3.lower()}_relative_wealth_index.csv')
    logging.info('Importing:%s', path)
    rwi = pd.read_csv(path)

    # Create a plot
    if plots:
        data = rwi.pivot(columns='longitude', index='latitude', values='rwi')
        origin = 'lower'
        min_lon = rwi['longitude'].min()
        max_lon = rwi['longitude'].max()
        min_lat = rwi['latitude'].min()
        max_lat = rwi['latitude'].max()
        extent = [min_lon, max_lon, min_lat, max_lat]
        limits = [min_lon, min_lat, max_lon, max_lat]
        zorder = 0
        country = get_country_name(iso3)
        title = f'Relative Wealth Index\n{country} - Admin Level {admin_level}'
        colourbar_label = 'Relative Wealth Index [unitless]'
        path = Path(output_path(source), f'{iso3}/admin_level_{admin_level}')
        plot_gadm_macro_heatmap(
            data, origin, extent, limits, gdf, zorder, title, colourbar_label,
            path
        )

    def get_admin(x):
        return get_admin_region(x['latitude'], x['longitude'], polygons)

    # Assign each latitude and longitude to an admin region
    rwi['geo_id'] = rwi.parallel_apply(get_admin, axis=1)  # type: ignore
    rwi = rwi[rwi['geo_id'] != 'null']

    # Get the mean RWI value for each region
    rwi = rwi.groupby('geo_id')['rwi'].mean().reset_index()

    # Dynamically choose which columns need to be added to the data
    region_columns = ['COUNTRY', 'NAME_1', 'NAME_2', 'NAME_3']
    admin_columns = region_columns[:int(admin_level) + 1]
    # Merge with the shapefile to get the region names
    rwi = rwi.merge(
        gdf[[admin_geoid] + admin_columns],
        left_on='geo_id', right_on=admin_geoid, how='left'
    )

    # Rename the columns
    columns = dict(zip(
        admin_columns, [f'admin_level_{i}' for i in range(len(admin_columns))]
    ))
    rwi = rwi.rename(columns=columns)
    rwi = rwi.rename(columns={'rwi': 'value'})
    # Add in the higher-level admin levels
    for i in range(int(admin_level) + 1, 4):
        rwi[f'admin_level_{i}'] = None
    # Add the metric name and unit
    rwi['metric'] = 'Relative Wealth Index'
    rwi['unit'] = 'unitless'
    # Re-order the columns
    output_columns = \
        [f'admin_level_{i}' for i in range(4)] + ['metric', 'value', 'unit']
    rwi = rwi[output_columns]

    return rwi, f'{iso3}.csv'


def process_dengueperu(
    admin_level: Literal['0', '1'] | None = None, plots=False
):
    """Process data from the Ministerio de Salud - Peru."""
    source = 'epidemiological/dengue/peru'
    if not admin_level:
        admin_level = '0'
        logging.info('admin_level:None (defaulting to %s)', admin_level)
    elif admin_level in ['0', '1']:
        logging.info('admin_level:%s', admin_level)
    else:
        raise ValueError(f'Invalid admin level: {admin_level}')

    # Find the raw data
    path = source_path(source)
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
    master = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Import the raw data
    for filepath in filepaths:
        logging.info('importing:%s', filepath)
        df = pd.read_excel(filepath)

        # Rename the headings
        columns = {
            'ano': 'year',
            'semana': 'week',
            'tipo_dx': 'metric',
            'n': 'value'
        }
        df = df.rename(columns=columns)

        # Define two metrics
        df.loc[df['metric'] == 'C', 'metric'] = 'Confirmed Dengue Cases'
        df.loc[df['metric'] == 'P', 'metric'] = 'Probable Dengue Cases'
        # Confirm no rows have been missed
        metrics = ['Confirmed Dengue Cases', 'Probable Dengue Cases']
        mask = ~df['metric'].isin(metrics)
        assert len(df[mask]) == 0

        # Get the name of the administrative divisions
        filename = filepath.name
        name = filename.removesuffix('.xlsx').split('_')[-1].capitalize()
        logging.info('processing:%s', name)
        # Add to the output data frame
        df['admin_level_0'] = 'Peru'
        if admin_level == '1':
            df['admin_level_1'] = name
        else:
            df['admin_level_1'] = ''
        df['admin_level_2'] = ''
        df['admin_level_3'] = ''

        # Add to master data frame
        master = pd.concat([master, df], ignore_index=True)

        # Plot
        if plots:
            df['date'] = pd.to_datetime(
                df['year'].astype(str) + df['week'].astype(str) + '1',
                format='%Y%U%w'
            )
            start = df.loc[0, 'year']
            end = df.loc[len(df) - 1, 'year']
            if admin_level == '0':
                title = f'Dengue Cases\nPeru - {start} to {end}'
            else:
                title = f'Dengue Cases\n{name} - {start} to {end}'
            path = Path(output_path(source), name + '.png')
            plot_timeseries(df, title, path)

    # Fill in additional columns
    master['iso3'] = 'PER'
    master['month'] = ''
    master['day'] = ''
    master['unit'] = 'cases'
    master['resolution'] = f'admin{admin_level}'
    master['creation_date'] = date.today()

    return master, 'dengue_peru.csv'


def process_gadm_aphroditetemperature(
    iso3: str, admin_level: Literal['0', '1', '2', '3'], partial_date: str,
    resolution=['025deg'], plots=False
):
    """
    Process GADM and APHRODITE Daily mean temperature product (V1808) data.

    Aggregates by given admin level for the given country (ISO3 code) and
    partial date.
    """
    sub_pipeline = 'geospatial/aphrodite-daily-mean-temp'
    pdate = PartialDate.from_string(partial_date)
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)

    version = 'V1808'
    year = pdate.year

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

    params = {
        '025deg': ('TAVE', '025deg', '', 360, 280),
        '025deg_nc': ('TAVE', '025deg', '.nc', 360, 280),
        '050deg_nc': ('TAVE', '050deg', '.nc', 180, 140),
        '005deg_nc': ('TAVE_CLM', '005deg', '.nc', 1800, 1400),
    }
    for data_type in resolution:
        product, res, ext, nx, ny = params[data_type]
        nday = days_in_year(int(year))
        # Record length
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = 60.125, -14.875
        xlon = x_start + np.arange(nx) * 0.25
        ylat = y_start + np.arange(ny) * 0.25

        # Open the file
        path = source_path('meteorological/aphrodite-daily-mean-temp', '')
        path = path / f'APHRO_MA_{product}_{res}_{version}.{year}{ext}'
        # Read binary data
        logging.info('importing:%s', path)
        with open(path, 'rb') as f:
            # Initialise arrays
            temp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read `temp` record
                temp_raw = np.fromfile(f, dtype='float32', count=recl)
                temp_raw = temp_raw.reshape((ny, nx))
                # Read `rstn` record
                rstn_raw = np.fromfile(f, dtype='float32', count=recl)
                rstn_raw = rstn_raw.reshape((ny, nx))
                # Store in arrays
                temp_data[iday, :, :] = temp_raw
                rstn_data[iday, :, :] = rstn_raw

        temp_data = temp_data.astype('float32')
        rstn_data = rstn_data.astype('float32')
        valid_xlon, valid_ylat = np.meshgrid(xlon, ylat, indexing='xy')

        # Iterate through days
        for iday in range(nday):
            this_date = datetime(int(year), 1, 1) + timedelta(days=iday)
            this_date = this_date.date()
            # Skip if the day doesn't match the partial date
            if pdate.month and pdate.month != this_date.month:
                continue
            if pdate.day and pdate.day != this_date.day:
                continue

            valid_mask = (rstn_data[iday, :, :] != 0.0) & \
                (temp_data[iday, :, :] != -99.90)
            valid_prcp = temp_data[iday][valid_mask]
            valid_lon = valid_xlon[valid_mask]
            valid_lat = valid_ylat[valid_mask]

            # Create rows in output for each sub-region
            to_append = []
            for idx, row in gdf.iterrows():
                # Extract the geometry of the current sub-region (polygon)
                region_geom = row.geometry

                # Filter to get data that falls within the sub-region geometry
                points = [
                    shapely.geometry.Point(lon, lat) for lon, lat in
                    zip(valid_lon, valid_lat)
                ]
                region_mask = np.array(
                    [region_geom.contains(point) for point in points]
                )

                # Filter data for this sub-region
                valid_temp_region = valid_prcp[region_mask]

                output_row = {
                    'iso3': iso3,
                    'admin_level_0': row['COUNTRY'],
                    'admin_level_1': row.get('NAME_1', ''),
                    'admin_level_2': row.get('NAME_2', ''),
                    'admin_level_3': row.get('NAME_3', ''),
                    'year': year,
                    'month': this_date.month,
                    'day': this_date.day,
                    'week': '',
                    'value': valid_temp_region.mean() if
                    len(valid_temp_region) > 0 else '',
                    'resolution': '0.25°' if res == '025deg' else '0.5°',
                    'metric': 'temperature',
                    'unit': '°C',
                    'creation_date': date.today()
                }
                to_append.append(pd.DataFrame([output_row]))
            # Concatenate the new rows to the output DataFrame
            if to_append:
                # Drop all-NA columns
                to_append = [df.dropna(axis=1, how='all') for df in to_append]
                # Drop empty data frames
                to_append = [df for df in to_append if not df.empty]
                output = pd.concat([output] + to_append, ignore_index=True)

            # Scatter plot
            if plots:
                title = f'Temperature\n{this_date}'
                colourbar_label = 'Temperature [°C]'
                folder = f'admin_level_{admin_level}/{res.replace('0', '0_')}'
                path = output_path(sub_pipeline) / folder / f'{this_date}.png'
                plot_gadm_scatter(
                    valid_lon, valid_lat, valid_prcp,
                    title, colourbar_label, path, gdf
                )

    return output, 'aphrodite-daily-mean-temp.csv'


def process_gadm_aphroditeprecipitation(
    iso3: str, admin_level: Literal['0', '1', '2', '3'], partial_date: str,
    resolution=['025deg', '050deg'], plots=False
):
    """
    Process GADM and APHRODITE Daily accumulated precipitation (V1901) data.

    Aggregates by given admin level for the given country (ISO3 code) and
    partial date.
    """
    sub_pipeline = 'geospatial/aphrodite-daily-precip'
    pdate = PartialDate.from_string(partial_date)
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

    version = 'V1901'
    year = pdate.year
    n_deg = {'025deg': (360, 280), '050deg': (180, 140)}
    for res in resolution:
        nx, ny = n_deg[res]
        nday = days_in_year(int(year))
        # Record length
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = 60.125, -14.875
        xlon = x_start + np.arange(nx) * 0.25
        ylat = y_start + np.arange(ny) * 0.25

        # Open the file
        path = source_path('meteorological/aphrodite-daily-precip', '')
        file_path = path / f'APHRO_MA_{res}_{version}.{year}'
        # Read binary data
        with open(file_path, 'rb') as f:
            # Initialise arrays
            prcp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read `prcp` record
                prcp_raw = np.fromfile(f, dtype='float32', count=recl)
                prcp_raw = prcp_raw.reshape((ny, nx))
                # Read `rstn` record
                rstn_raw = np.fromfile(f, dtype='float32', count=recl)
                rstn_raw = rstn_raw.reshape((ny, nx))
                # Store in arrays
                prcp_data[iday, :, :] = prcp_raw
                rstn_data[iday, :, :] = rstn_raw

        prcp_data = prcp_data.astype('float32')
        rstn_data = rstn_data.astype('float32')
        valid_xlon, valid_ylat = np.meshgrid(xlon, ylat, indexing='xy')

        # Iterate through days
        for iday in range(nday):
            this_date = datetime(int(year), 1, 1) + timedelta(days=iday)
            this_date = this_date.date()
            # Skip if the day doesn't match the partial date
            if pdate.month and pdate.month != this_date.month:
                continue
            if pdate.day and pdate.day != this_date.day:
                continue

            valid_mask = (rstn_data[iday, :, :] != 0.0) & \
                (prcp_data[iday, :, :] != -99.90)
            valid_prcp = prcp_data[iday][valid_mask]
            valid_lon = valid_xlon[valid_mask]
            valid_lat = valid_ylat[valid_mask]

            # Create rows in output for each sub-region
            to_append = []
            for idx, row in gdf.iterrows():
                # Extract the geometry of the current sub-region (polygon)
                region_geom = row.geometry

                # Filter to get data that falls within the sub-region geometry
                points = [
                    shapely.geometry.Point(lon, lat) for lon, lat in
                    zip(valid_lon, valid_lat)
                ]
                region_mask = np.array(
                    [region_geom.contains(point) for point in points]
                )

                # Filter data for this sub-region
                _ = valid_lon[region_mask]
                _ = valid_lat[region_mask]
                valid_prcp_region = valid_prcp[region_mask]

                output_row = {
                    'iso3': iso3,
                    'admin_level_0': row['COUNTRY'],
                    'admin_level_1': row.get('NAME_1', ''),
                    'admin_level_2': row.get('NAME_2', ''),
                    'admin_level_3': row.get('NAME_3', ''),
                    'year': year,
                    'month': this_date.month,
                    'day': this_date.day,
                    'week': '',
                    'value': valid_prcp_region.sum(),
                    'resolution': '0.25°' if res == '025deg' else '0.5°',
                    'metric': 'precipitation',
                    'unit': 'mm',
                    'creation_date': date.today()
                }
                to_append.append(pd.DataFrame([output_row]))
            # Concatenate the new rows to the output DataFrame
            if to_append:
                # Drop all-NA columns
                to_append = [df.dropna(axis=1, how='all') for df in to_append]
                # Drop empty data frames
                to_append = [df for df in to_append if not df.empty]
                output = pd.concat([output] + to_append, ignore_index=True)

            # Scatter plot
            if plots:
                title = f'Precipitation\n{this_date}'
                colourbar_label = 'Precipitation [mm]'
                folder = f'admin_level_{admin_level}/{res.replace('0', '0_')}'
                path = output_path(sub_pipeline) / folder / f'{this_date}.png'
                plot_gadm_scatter(
                    valid_lon, valid_lat, valid_prcp,
                    title, colourbar_label, path, gdf
                )

    return output, 'aphrodite-daily-precip.csv'


def process_gadm_admin_map_data(iso3: str, admin_level: AdminLevel):
    """Process GADM administrative map data."""
    gdf = gpd.read_file(get_shapefile(iso3, admin_level))

    # en.wikipedia.org/wiki/List_of_national_coordinate_reference_systems
    national_crs = {
        "GBR": "EPSG:27700",
        "PER": "EPSG:24892",  # Peru central zone
        "VNM": "EPSG:4756",
    }
    try:
        gdf = gdf.to_crs(national_crs[iso3])
    except KeyError:
        pass

    # Initialise output data frame
    output = pd.DataFrame()
    # Iterate over the regions in the shape file
    for _, region in gdf.iterrows():
        # Initialise a new row for the output data frame
        new_row = {"Admin Level 0": region["COUNTRY"]}
        # Initialise the title
        # Update the new row and the title if the admin level is high enough
        if int(admin_level) >= 1:
            new_row["Admin Level 1"] = region["NAME_1"]
        if int(admin_level) >= 2:
            new_row["Admin Level 2"] = region["NAME_2"]
        if int(admin_level) >= 3:
            new_row["Admin Level 3"] = region["NAME_3"]

        # Calculate area in square metres
        area = region.geometry.area
        # Convert to square kilometres
        area_sq_km = area / 1e6
        # Add to output data frame
        new_row["Area [km²]"] = area_sq_km
        new_row_df = pd.DataFrame(new_row, index=[0])
        output = pd.concat([output, new_row_df], ignore_index=True)

    return output, f"{iso3}/admin{admin_level}_area.csv"


def process_gadm_worldpopcount(
    iso3, partial_date: str, admin_level: AdminLevel = '0', rt: str = 'ppp',
    plots=False
):
    """Process GADM administrative map and WorldPop population count data."""
    sub_pipeline = 'geospatial/worldpop-count'
    iso3 = iso3.upper()
    logging.info('iso3:%s', iso3)
    logging.info('partial_date:%s', partial_date)
    logging.info('admin_level:%s', admin_level)
    logging.info('rt:%s', rt)
    logging.info('plots:%s', plots)

    pdate = PartialDate.from_string(partial_date)
    if pdate.day:
        msg = f'The date {partial_date} includes a day. Provide only a ' + \
            'year in YYYY format.'
        raise ValueError(msg)
    if pdate.month:
        msg = f'The date {partial_date} includes a month. Provide only a ' + \
            'year in YYYY format.'
        raise ValueError(msg)

    # Import the GeoTIFF file
    source = 'sociodemographic/worldpop-count'
    path = Path(iso3, f'{iso3}_{rt}_v2b_{pdate.year}_UNadj.tif')
    path = source_path(source, path)
    logging.info('importing:%s', path)
    try:
        src = rasterio.open(path)
    except rasterio.errors.RasterioIOError:
        # Could not find the file
        file_found = False
        for year in range(datetime.today().year, 1990, -1):
            filename = f'{iso3}_{rt}_v2b_{year}_UNadj.tif'
            if filename in os.listdir(path.parent):
                new_path = source_path(source, Path(iso3, filename))
                logging.info('importing:%s', new_path)
                print(f'File {path} not found.')
                print(f'Importing {new_path} instead')
                src = rasterio.open(new_path)
                file_found = True
                break
        if not file_found:
            msg = f'{path}: No such file or directory. Either it has not ' + \
                'been downloaded or data does not exist for this year or ' + \
                'any year since 1990.'
            raise rasterio.errors.RasterioIOError(msg)

    # Rasterio stores image layers in 'bands'
    # Get the data in the first band as an array
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data == MIN_FLOAT] = 0
    # Hide nulls
    data[data == -9999] = 0

    # Create a bounding box from raster bounds
    bounds = src.bounds
    raster_bbox = shapely.geometry.box(
        bounds.left, bounds.bottom, bounds.right, bounds.top
    )

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)
    # Transform the shape file to match the GeoTIFF's coordinate system
    gdf = gdf.to_crs(src.crs)
    # EPSG:4326 - WGS 84: latitude/longitude coordinate system based on the
    # Earth's center of mass

    # Initialise an output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Iterate over each region in the shape file
    for i, region in gdf.iterrows():
        # Add the region name to the output data frame
        output.loc[i, 'admin_level_0'] = region['COUNTRY']
        # Initialise the graph title
        title = region['COUNTRY']
        # Add more region names and update the graph title if the admin level
        # is high enough to warrant it
        if int(admin_level) >= 1:
            output.loc[i, 'admin_level_1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            output.loc[i, 'admin_level_2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            output.loc[i, 'admin_level_3'] = region['NAME_3']
            title = region['NAME_3']

        # Add date information to the output data frame
        output.loc[i, 'year'] = pdate.year

        # Check if the population data intersects this region
        geometry = region.geometry
        if raster_bbox.intersects(geometry):
            # There is population data for this region
            # Clip the data using the polygon of the current region
            region_data, _ = rasterio.mask.mask(src, [geometry], crop=True)
            # Replace negative values (if any exist)
            region_data = np.where(region_data < 0, np.nan, region_data)
            # Sum the pixel values to get the total for the region
            region_total = np.nansum(region_data)
        else:
            # There is no population data for this region
            region_total = 0
        logging.info('region:%s', title)
        logging.info('region_total:%s', region_total)
        # Add the result to the output data frame
        metric = 'population'
        output.loc[i, 'metric'] = metric
        output.loc[i, 'value'] = region_total
        unit = 'people'
        output.loc[i, 'unit'] = unit

    # Create a plot
    if plots:
        data[data == 0] = np.nan
        data = np.log(data)
        origin = 'upper'
        min_lon, min_lat, max_lon, max_lat = gdf.total_bounds
        extent = [min_lon, max_lon, min_lat, max_lat]
        limits = [min_lon, min_lat, max_lon, max_lat]
        if admin_level in ['2', '3']:
            zorder = 0
        else:
            zorder = 1
        name = get_country_name(iso3, common_name=True)
        title = f'{metric}\n{name} - {pdate.year}'
        colourbar_label = f'Average {metric} per pixel [{unit}]'
        path = output_path(sub_pipeline, f'{iso3}/admin_level_{admin_level}')
        plot_gadm_macro_heatmap(
            data, origin, extent, limits, gdf, zorder, title, colourbar_label,
            path, log_plot=True
        )

    output['iso3'] = iso3
    if rt == 'ppp':
        output['resolution'] = 'people per pixel'
    elif rt == 'pph':
        output['resolution'] = 'people per hectare'
    output['creation_date'] = date.today()

    # Return
    df = output.fillna('')
    sub_pipeline = sub_pipeline.replace('/', '_')
    filename = f'{iso3}_{sub_pipeline}_{date.today()}_{pdate}.csv'
    return df, filename


def process_aphrodite_temperature_data(year=None, plots=False) -> \
        list[ProcessResult]:
    """Process APHRODITE Daily mean temperature product (V1808) data."""
    sub_pipeline = 'meteorological/aphrodite-daily-mean-temp'
    version = 'V1808'

    if not year:
        # Regex pattern to match the resolution, version and year in filenames
        pattern = r'APHRO_MA_TAVE_(\d+deg)_V(\d+)\.(\d+)'
        # Find the latest year for which there is data
        years = []
        path = source_path(sub_pipeline, '')
        for filename in Path(path).iterdir():
            match = re.match(pattern, str(filename.name))
            if match:
                _, _, year = match.groups()
                years.append(int(year))
        # Get the latest year
        year = str(max(years))

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

    params = {
        '025deg': ('TAVE', '025deg', '', 360, 280),
        '025deg_nc': ('TAVE', '025deg', '.nc', 360, 280),
        '050deg_nc': ('TAVE', '050deg', '.nc', 180, 140),
        '005deg_nc': ('TAVE_CLM', '005deg', '.nc', 1800, 1400),
    }
    for data_type in ['025deg']:
        product, res, ext, nx, ny = params[data_type]
        nday = days_in_year(int(year))
        # Record length
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = 60.125, -14.875
        xlon = x_start + np.arange(nx) * 0.25
        ylat = y_start + np.arange(ny) * 0.25

        # Open the file
        path = source_path(sub_pipeline, '')
        path = path / f'APHRO_MA_{product}_{res}_{version}.{year}{ext}'
        # Read binary data
        logging.info('opening:%s', path)
        with open(path, 'rb') as f:
            # Initialise arrays
            temp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read `temp` record
                temp_raw = np.fromfile(f, dtype='float32', count=recl)
                temp_raw = temp_raw.reshape((ny, nx))
                # Read `rstn` record
                rstn_raw = np.fromfile(f, dtype='float32', count=recl)
                rstn_raw = rstn_raw.reshape((ny, nx))
                # Store in arrays
                temp_data[iday, :, :] = temp_raw
                rstn_data[iday, :, :] = rstn_raw

        temp_data = temp_data.astype('float32')
        rstn_data = rstn_data.astype('float32')
        valid_xlon, valid_ylat = np.meshgrid(xlon, ylat, indexing='xy')

        # Iterate through days
        for iday in range(nday):
            valid_mask = (rstn_data[iday, :, :] != 0.0) & \
                (temp_data[iday, :, :] != -99.90)
            valid_temp = temp_data[iday][valid_mask]
            valid_lon = valid_xlon[valid_mask]
            valid_lat = valid_ylat[valid_mask]

            this_date = datetime(int(year), 1, 1) + timedelta(days=iday)
            this_date = this_date.date()

            # Scatter plot
            if plots:
                title = f'Temperature\n{this_date}'
                colourbar_label = 'Temperature [°C]'
                folder = res.replace('0', '0_')
                path = output_path(sub_pipeline) / folder / f'{this_date}.png'
                plot_scatter(
                    valid_lon, valid_lat, valid_temp, title, colourbar_label,
                    path
                )

            i = len(output)
            output.loc[i, 'year'] = year
            output.loc[i, 'month'] = this_date.month
            output.loc[i, 'day'] = this_date.day
            output.loc[i, 'value'] = valid_temp.mean()
            if res == '025deg':
                output.loc[i, 'resolution'] = '0.25°'
            elif res == '050deg':
                output.loc[i, 'resolution'] = '0.5°'

    output['iso3'] = ''
    output['admin_level_0'] = ''
    output['admin_level_1'] = ''
    output['admin_level_2'] = ''
    output['admin_level_3'] = ''
    output['week'] = ''
    output['metric'] = 'temperature'
    output['unit'] = '°C'
    output['creation_date'] = date.today()

    return output, 'aphrodite-daily-mean-temp.csv'


def process_aphrodite_precipitation_data(
    year=None, resolution=['025deg', '050deg'], plots=False
) -> list[ProcessResult]:
    """Process APHRODITE Daily accumulated precipitation (V1901) data."""
    sub_pipeline = 'meteorological/aphrodite-daily-precip'
    base_path = source_path(sub_pipeline, '')
    version = 'V1901'
    if not year:
        # Regex pattern to match the resolution, version and year in filenames
        pattern = r'APHRO_MA_(\d+deg)_V(\d+)\.(\d+)$'
        # Find the latest year for which there is data
        years = []
        for filename in Path(base_path).iterdir():
            match = re.match(pattern, str(filename.name))
            if match:
                _, _, year = match.groups()
                years.append(int(year))
        # Get the latest year
        year = str(max(years))

    # Initialise output data frame
    output = pd.DataFrame(columns=OUTPUT_COLUMNS)

    n_deg = {'025deg': (360, 280), '050deg': (180, 140)}
    for res in resolution:
        nx, ny = n_deg[res]
        nday = days_in_year(int(year))
        # Record length
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = 60.125, -14.875
        xlon = x_start + np.arange(nx) * 0.25
        ylat = y_start + np.arange(ny) * 0.25

        # Open the file
        file_path = Path(base_path) / Path(f'APHRO_MA_{res}_{version}.{year}')
        # Read binary data
        with open(file_path, 'rb') as f:
            # Initialise arrays
            prcp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read `prcp` record
                prcp_raw = np.fromfile(f, dtype='float32', count=recl)
                prcp_raw = prcp_raw.reshape((ny, nx))
                # Read `rstn` record
                rstn_raw = np.fromfile(f, dtype='float32', count=recl)
                rstn_raw = rstn_raw.reshape((ny, nx))
                # Store in arrays
                prcp_data[iday, :, :] = prcp_raw
                rstn_data[iday, :, :] = rstn_raw

        prcp_data = prcp_data.astype('float32')
        rstn_data = rstn_data.astype('float32')
        valid_xlon, valid_ylat = np.meshgrid(xlon, ylat, indexing='xy')

        # Iterate through days
        for iday in range(nday):
            valid_mask = (rstn_data[iday, :, :] != 0.0) & \
                (prcp_data[iday, :, :] != -99.90)
            valid_prcp = prcp_data[iday][valid_mask]
            valid_lon = valid_xlon[valid_mask]
            valid_lat = valid_ylat[valid_mask]

            this_date = datetime(int(year), 1, 1) + timedelta(days=iday)
            this_date = this_date.date()

            # Scatter plot
            if plots:
                title = f'Precipitation\n{this_date}'
                colourbar_label = 'Precipitation [mm]'
                folder = res.replace('0', '0_')
                path = output_path(sub_pipeline) / folder / f'{this_date}.png'
                plot_scatter(
                    valid_lon, valid_lat, valid_prcp, title, colourbar_label,
                    path
                )

            i = len(output)
            output.loc[i, 'year'] = year
            output.loc[i, 'month'] = this_date.month
            output.loc[i, 'day'] = this_date.day
            output.loc[i, 'value'] = valid_prcp.sum()
            if res == '025deg':
                output.loc[i, 'resolution'] = '0.25°'
            elif res == '050deg':
                output.loc[i, 'resolution'] = '0.5°'

    output['metric'] = 'precipitation'
    output['unit'] = 'mm'
    output['creation_date'] = date.today()

    return output, 'aphrodite-daily-precip.csv'


def get_chirps_rainfall_data_path(date: PartialDate) -> Path:
    """Get the path to a CHIRPS rainfall data file."""
    file = None
    match date.scope:
        case "daily":
            file = Path(
                "global_daily",
                str(date.year),
                date.zero_padded_month,
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case "monthly":
            file = Path(
                "global_monthly",
                str(date.year),
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case "annual":
            file = Path("global_annual", f"chirps-v2.0.{date}.tif")

    path = source_path("meteorological/chirps-rainfall", file)
    if not path.exists():
        raise FileNotFoundError(f"CHIRPS rainfall data not found: {path}")

    return path


def process_chirps_rainfall(partial_date: str, plots=False) -> ProcessResult:
    """
    Process CHIRPS Rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    source = 'meteorological/chirps-rainfall'
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logging.info('importing:%s', file)
    src = rasterio.open(file)

    # Initialise the data frame that will store the output data for each region
    columns = ['year', 'month', 'day', 'rainfall']
    output = pd.DataFrame(columns=columns)

    # Add date information to the output data frame
    output.loc[0, 'year'] = pdate.year
    if pdate.month:
        output.loc[0, 'month'] = pdate.month
    if pdate.day:
        output.loc[0, 'day'] = pdate.day

    # Rasterio stores image layers in 'bands'
    # Get the data in the first band as an array
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data == MIN_FLOAT] = 0
    # Hide nulls
    data[data == -9999] = 0
    # Add the result to the output data frame
    output.loc[0, 'rainfall'] = np.nansum(data)

    # Create a plot
    if plots:
        title = f'Rainfall\n{pdate}'
        colourbar_label = 'Rainfall [mm]'
        path = Path(
            output_path(source), str(pdate).replace('-', '/'),
            str(pdate) + '.png'
        )
        plot_heatmap(data, title, colourbar_label, path)

    # Export
    return output, 'chirps-rainfall.csv'


def process_era5_reanalysis_data() -> ProcessResult:
    """Process ERA5 atmospheric reanalysis data."""
    source = "meteorological/era5-atmospheric-reanalysis"
    path = source_path(source, "ERA5-ml-temperature-subarea.nc")
    file = nc.Dataset(path, "r")  # type: ignore

    # Import variables as arrays
    longitude = file.variables["longitude"][:]
    latitude = file.variables["latitude"][:]
    level = file.variables["level"][:]
    time = file.variables["time"][:]
    temp = file.variables["t"][:]
    # Convert Kelvin to Celsius
    temp = temp - 273.15

    longitudes = []
    latitudes = []
    levels = []
    times = []
    temperatures = []
    for i, lon in enumerate(longitude):
        for j, lat in enumerate(latitude):
            for k, lev in enumerate(level):
                for m, t in enumerate(time):
                    longitudes.append(lon)
                    latitudes.append(lat)
                    levels.append(lev)
                    times.append(t)
                    temperatures.append(temp[m, k, j, i])

    dct = {
        "longitude": longitudes,
        "latitude": latitudes,
        "level": levels,
        "time": times,
        "temperature": temperatures,
    }
    df = pd.DataFrame(dct)
    file.close()

    return df, "ERA5-ml-temperature-subarea.csv"


def process_terraclimate(
    partial_date: str, iso3: str, admin_level: str, plots=False
):
    """
    Process TerraClimate data.

    This metric incorporates TerraClimate gridded temperature, precipitation,
    and other water balance variables. The data is stored in NetCDF (`.nc`)
    files for which the `netCDF4` library is needed.
    """
    source = 'meteorological/terraclimate'
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('plots:%s', plots)

    # Initialise output data frame
    columns = [
        'admin_level_0', 'admin_level_1', 'admin_level_2', 'admin_level_3',
        'year', 'month'
    ]
    output = pd.DataFrame(columns=columns)

    # Import a shapefile
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)

    # Iterate over the metrics
    for metric in TERRACLIMATE_METRICS:
        # Import the raw data
        if (pdate.year == 2023) and (metric == 'pdsi'):
            # In 2023 the capitalization of pdsi changed
            metric = 'PDSI'
        path = source_path(source, f'TerraClimate_{metric}_{pdate.year}.nc')
        logging.info('importing:%s', path)
        ds = nc.Dataset(path)

        # Extract the variables
        lat = ds.variables['lat'][:]
        lon = ds.variables['lon'][:]
        time = ds.variables['time'][:]  # Time in days since 1900-01-01
        raw = ds.variables[metric]

        # Check if a standard name is provided for this metric
        try:
            standard_name = raw.standard_name
        except AttributeError:
            standard_name = metric

        # Apply scale factor
        data = raw[:].astype(np.float32)
        data = data * raw.scale_factor + raw.add_offset
        # Replace fill values with NaN
        data[data == raw._FillValue] = np.nan

        # Convert time to actual dates
        base_time = datetime(1900, 1, 1)
        months = [base_time + timedelta(days=t) for t in time]

        for i, month in enumerate(months):
            # If a month has been specified on the command line
            if pdate.month:
                # If this data come from a month that does not match the
                # requested month
                if pdate.month != month.month:
                    # Skip this iteration
                    continue

            # Extract the data for the chosen month
            this_month = data[i, :, :]

            # Plot
            if plots:
                origin = 'upper'
                extent = [lon.min(), lon.max(), lat.min(), lat.max()]
                limits = gdf.total_bounds
                zorder = 1
                month_str = month.strftime('%B %Y')
                title = f'{raw.description}\n{iso3} - {month_str}'
                colourbar_label = f'{raw.description} [{raw.units}]'
                path = Path(
                    output_path(source), str(pdate).replace('-', '/'),
                    f'admin_level_{admin_level}', title + '.png'
                )
                plot_gadm_macro_heatmap(
                    this_month, origin, extent, limits, gdf, zorder, title,
                    colourbar_label, path
                )

            # Iterate over the regions in the shape file
            for j, region in gdf.iterrows():
                geometry = region.geometry

                # Initialise a new row for the output data frame
                idx = i * len(months) + j
                output.loc[idx, 'admin_level_0'] = region['COUNTRY']
                output.loc[idx, 'admin_level_1'] = None
                output.loc[idx, 'admin_level_2'] = None
                output.loc[idx, 'admin_level_3'] = None
                output.loc[idx, 'year'] = month.year
                output.loc[idx, 'month'] = month.month
                # Initialise the graph title
                title = region['COUNTRY']
                # Update the new row and the title if the admin level is high
                # enough
                if int(admin_level) >= 1:
                    output.loc[idx, 'admin_level_1'] = region['NAME_1']
                    title = region['NAME_1']
                if int(admin_level) >= 2:
                    output.loc[idx, 'admin_level_2'] = region['NAME_2']
                    title = region['NAME_2']
                if int(admin_level) >= 3:
                    output.loc[idx, 'admin_level_3'] = region['NAME_3']
                    title = region['NAME_3']

                # Define transform for geometry_mask based on grid resolution
                transform = rasterio.transform.from_origin(
                    lon.min(), lat.max(), abs(lon[1] - lon[0]),
                    abs(lat[1] - lat[0])
                )

                # Create a mask that is True for points outside the geometries
                mask = rasterio.features.geometry_mask(
                    [geometry], transform=transform, out_shape=this_month.shape
                )
                masked_data = np.ma.masked_array(this_month, mask=mask)

                # Plot
                if plots and (admin_level == 0):
                    month_str = month.strftime('%B %Y')
                    title = f'{raw.description}\n{title} - {month_str}'
                    colourbar_label = f'{raw.description} [{raw.units}]'
                    extent = [lon.min(), lon.max(), lat.min(), lat.max()]
                    plot_gadm_micro_heatmap(
                        source, masked_data, gdf, pdate, title,
                        colourbar_label, region, extent
                    )

                # Add to output data frame
                output.loc[idx, standard_name] = np.nansum(masked_data)

        # Close the NetCDF file after use
        ds.close()

    # # Export
    # path = Path(output_path(source), year, iso3 + '.csv')
    # print('Exporting', path)
    # output.to_csv(path, index=False)

    return output, f'{iso3}.csv'


def process_worldpop_pop_count_data(
    iso3: str, year: int = 2020, rt: str = 'ppp'
) -> ProcessResult:
    """
    Process WorldPop population count.

    - EPSG:9217: https://epsg.io/9217
    - EPSG:4326: https://epsg.io/4326
    - EPSG = European Petroleum Survey Group
    """
    sub_pipeline = 'sociodemographic/worldpop-count'
    country = get_country_name(iso3)
    logging.info('year:%s', year)
    logging.info('iso3:%s', iso3)
    logging.info('resolution_type:%s', rt)

    filename = Path(f'{iso3}_{rt}_v2b_{year}_UNadj.tif')
    path = source_path(sub_pipeline, iso3) / filename
    logging.info('importing:%s', path)
    src = rasterio.open(path)
    # Read data from band 1
    if src.count != 1:
        raise ValueError(f'Unexpected number of bands: {src.count}')
    source_data = src.read(1)

    # Replace placeholder numbers with 0
    # (-3.4e+38 is the smallest single-precision floating-point number)
    df = pd.DataFrame(source_data)
    population_data = df[df != MIN_FLOAT]
    population = population_data.sum().sum()
    logging.info('population:%s', population)

    # Initialise an output data frame
    df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Populate output data frame
    df.loc[0, 'iso3'] = iso3
    df.loc[0, 'admin_level_0'] = country
    df.loc[0, 'year'] = year
    df.loc[0, 'metric'] = 'population'
    df.loc[0, 'unit'] = 'people'
    df.loc[0, 'value'] = population
    if rt == 'ppp':
        df.loc[0, 'resolution'] = 'people per pixel'
    elif rt == 'pph':
        df.loc[0, 'resolution'] = 'people per hectare'
    df.loc[0, 'creation_date'] = date.today()

    return df.fillna(''), 'worldpop-count.csv'


def process_worldpop_pop_density_data(iso3: str, year: int) -> ProcessResult:
    """
    Process WorldPop population density.
    """
    source = "sociodemographic/worldpop-density"
    print(f"Source:      {source}")
    print(f"Year:        {year}")
    print(f"Country:     {iso3}")

    # Import the population density data
    iso3_lower = iso3.lower()
    filename = Path(f"{iso3_lower}_pd_{year}_1km_UNadj_ASCII_XYZ")
    base_path = source_path(
        source, f"population-density/Global_2000_2020_1km_UNadj/{year}/{iso3}"
    )
    df = pd.read_csv(base_path / filename.with_suffix(".zip"))
    return df, f"{iso3}/{filename.with_suffix('.csv')}"


def process_gadm_chirps_rainfall(
    iso3: str, admin_level: Literal['0', '1'], partial_date: str, plots=False
):
    """
    Process GADM administrative map and CHIRPS rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    pdate = PartialDate.from_string(partial_date)
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logging.info('importing:%s', file)
    src = rasterio.open(file)

    # Create a bounding box from raster bounds
    bounds = src.bounds
    raster_bbox = shapely.geometry.box(
        bounds.left, bounds.bottom, bounds.right, bounds.top
    )

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)
    # Transform the shape file to match the GeoTIFF's coordinate system
    gdf = gdf.to_crs(src.crs)
    # EPSG:4326 - WGS 84: latitude/longitude coordinate system based on the
    # Earth's center of mass

    # Initialise the data frame that will store the output data for each region
    columns = [
        'admin_level_0', 'admin_level_1', 'admin_level_2', 'admin_level_3',
        'year', 'month', 'day', 'rainfall'
    ]
    output = pd.DataFrame(columns=columns)

    # Iterate over each region in the shape file
    for i, region in gdf.iterrows():
        # Add the region name to the output data frame
        output.loc[i, 'admin_level_0'] = region['COUNTRY']
        # Initialise the graph title
        title = region['COUNTRY']
        # Add more region names and update the graph title if the admin level
        # is high enough to warrant it
        if int(admin_level) >= 1:
            output.loc[i, 'admin_level_1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            output.loc[i, 'admin_level_2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            output.loc[i, 'admin_level_3'] = region['NAME_3']
            title = region['NAME_3']

        # Add date information to the output data frame
        output.loc[i, 'year'] = pdate.year
        if pdate.month:
            output.loc[i, 'month'] = pdate.month
        if pdate.day:
            output.loc[i, 'day'] = pdate.day

        # Check if the rainfall data intersects this region
        geometry = region.geometry
        if raster_bbox.intersects(geometry):
            # There is rainfall data for this region
            # Clip the data using the polygon of the current region
            region_data, _ = rasterio.mask.mask(src, [geometry], crop=True)
            # Replace negative values (if any exist)
            region_data = np.where(region_data < 0, np.nan, region_data)
            # Sum the pixel values to get the total for the region
            region_total = np.nansum(region_data)
        else:
            # No rainfall data for this region
            region_total = 0
        logging.info('region:%s', title)
        logging.info('region_total:%s', region_total)
        # Add the result to the output data frame
        output.loc[i, 'rainfall'] = region_total

        if plots:
            # Get the bounds of the region
            min_lon, min_lat, max_lon, max_lat = geometry.bounds
            # Plot
            _, ax = plt.subplots()
            ar = region_data[0]
            ar[ar == 0] = np.nan
            im = ax.imshow(
                ar, cmap='coolwarm', origin='upper',
                extent=[min_lon, max_lon, min_lat, max_lat]
            )
            # Add the geographical borders
            gdf.plot(ax=ax, color='none', edgecolor='gray')
            gpd.GeoDataFrame([region]).plot(ax=ax, color='none', edgecolor='k')
            plt.colorbar(im, ax=ax, label='Rainfall [mm]')
            ax.set_title(f'Rainfall\n{title} - {pdate}')
            ax.set_xlim(min_lon, max_lon)
            ax.set_ylim(min_lat, max_lat)
            ax.set_ylabel('Latitude')
            ax.set_xlabel('Longitude')
            # Make the plot title file-system safe
            title = re.sub(r'[<>:"/\\|?*]', '_', title)
            title = title.strip()
            # Export
            path = Path(
                output_path('geospatial/chirps-rainfall'),
                str(pdate).replace('-', '/'), title + '.png'
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            logging.info('exporting:%s', path)
            plt.savefig(path)
            plt.close()

    # Export
    return output, f'{iso3}.csv'


def get_admin_region(lat: float, lon: float, polygons) -> str:
    """
    Find the admin region in which a grid cell lies.

    Return the ID of administrative region in which the centre (given by
    latitude and longitude) of a 2.4km^2 grid cell lies.
    """
    point = shapely.geometry.Point(lon, lat)
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return "null"


PROCESSORS: dict[str, Callable[..., ProcessResult | list[ProcessResult]]] = {
    "economic/relative-wealth-index": process_rwi,
    "epidemiological/dengue/peru": process_dengueperu,
    'geospatial/aphrodite-daily-mean-temp': process_gadm_aphroditetemperature,
    'geospatial/aphrodite-daily-precip': process_gadm_aphroditeprecipitation,
    "geospatial/chirps-rainfall": process_gadm_chirps_rainfall,
    "geospatial/gadm": process_gadm_admin_map_data,
    "geospatial/worldpop-count": process_gadm_worldpopcount,
    "meteorological/aphrodite-daily-mean-temp": process_aphrodite_temperature_data,
    "meteorological/aphrodite-daily-precip": process_aphrodite_precipitation_data,
    "meteorological/chirps-rainfall": process_chirps_rainfall,
    "meteorological/era5-reanalysis": process_era5_reanalysis_data,
    "meteorological/terraclimate": process_terraclimate,
    "sociodemographic/worldpop-count": process_worldpop_pop_count_data,
    "sociodemographic/worldpop-density": process_worldpop_pop_density_data,
}
