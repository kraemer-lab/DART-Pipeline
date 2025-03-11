"""Module for processing WorldPop population count data."""
from datetime import datetime, date
from pathlib import Path
import logging
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.mask
import shapely.geometry

from dart_pipeline.util import (
    source_path, get_shapefile, get_country_name, output_path
)
from dart_pipeline.plots import plot_gadm_macro_heatmap
from dart_pipeline.constants import OUTPUT_COLUMNS, MIN_FLOAT
from dart_pipeline.types import PartialDate, AdminLevel


def process_gadm_worldpopcount(
    iso3, partial_date: str, admin_level: AdminLevel = '0', rt: str = 'ppp',
    plots=False
):
    """Process GADM administrative map and WorldPop population count data."""
    sub_pipeline = 'geospatial/worldpop-count'
    iso3 = iso3.upper()
    logging.info('iso3:%s', iso3)
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', partial_date)
    logging.info('admin_level:%s', admin_level)
    logging.info('rt:%s', rt)
    logging.info('plots:%s', plots)

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
        output.loc[i, 'GID_0'] = region['GID_0']
        output.loc[i, 'COUNTRY'] = region['COUNTRY']
        # Initialise the graph title
        title = region['COUNTRY']
        # Add more region names and update the graph title if the admin level
        # is high enough to warrant it
        if int(admin_level) >= 1:
            output.loc[i, 'GID_1'] = region['GID_1']
            output.loc[i, 'NAME_1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            output.loc[i, 'GID_2'] = region['GID_2']
            output.loc[i, 'NAME_2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            output.loc[i, 'GID_3'] = region['GID_3']
            output.loc[i, 'NAME_3'] = region['NAME_3']
            title = region['NAME_3']

        # Add date information to the output data frame
        output.loc[i, 'year'] = pdate.year
        output.loc[i, 'month'] = pdate.year
        output.loc[i, 'day'] = pdate.year
        output.loc[i, 'week'] = None

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
        metric = 'Population Count'
        output.loc[i, 'metric'] = metric
        output.loc[i, 'value'] = region_total
        output.loc[i, 'unit'] = None

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
        colourbar_label = metric
        path = output_path(sub_pipeline, f'{iso3}/admin_level_{admin_level}')
        plot_gadm_macro_heatmap(
            data, origin, extent, limits, gdf, zorder, title, colourbar_label,
            path, log_plot=True
        )

    output['creation_date'] = date.today()

    sub_pipeline = sub_pipeline.replace('/', '_')
    filename = f'{iso3}_{sub_pipeline}_{partial_date}_{date.today()}.csv'

    # Export
    return output.fillna(''), filename
