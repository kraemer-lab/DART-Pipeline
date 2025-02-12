from datetime import date
from pathlib import Path
from typing import Literal
import logging

import geopandas as gpd
import netCDF4 as nc
import numpy as np
import pandas as pd
import rasterio

from dart_pipeline.constants import BASE_DIR, OUTPUT_COLUMNS, \
    DEFAULT_SOURCES_ROOT
from dart_pipeline.plots import plot_gadm_micro_heatmap
from dart_pipeline.types import PartialDate
from dart_pipeline.util import get_shapefile, output_path


def process_gadm_era5reanalysis(
    dataset, iso3: str, admin_level: Literal['0', '1', '2', '3'],
    partial_date: str, plots=False

):
    """Process GADM and ERA5 atmospheric reanalysis data."""
    sub_pipeline = 'geospatial/era5-reanalysis'
    logging.info('dataset:%s', dataset)
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    logging.info('plots:%s', plots)

    # Initialise the output data frame
    df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)

    # Find the meteorological data
    filepaths = []
    folder = Path(
        BASE_DIR, DEFAULT_SOURCES_ROOT, 'meteorological', 'era5-reanalysis'
    )
    for path in folder.iterdir():
        if path.name == f'{dataset}_{str(pdate)}.nc':
            # The data file has been found
            filepaths.append(path)
            break
        if path.name == f'{dataset}_{str(pdate)}':
            # The data folder has been found
            filepaths = path.iterdir()

    # Process the data
    idx = 0
    for i, path in enumerate(sorted(filepaths)):
        logging.info('importing:%s', path)
        ds = nc.Dataset(path, 'r')  # type: ignore

        # Extract the variables
        lat = ds.variables['latitude'][:]
        lon = ds.variables['longitude'][:]
        # Typically the data variable will be at the front
        variable = list(ds.variables)[0]
        raw = ds.variables[variable]
        # Extract the 2D array that holds the data
        data = raw[0, :, :]
        metric = ds.variables[variable].long_name
        unit = ds.variables[variable].units

        # Iterate over the regions in the shape file
        for j, region in gdf.iterrows():
            geometry = region.geometry

            # Populate a row for the output data frame
            df.loc[idx, 'admin_level_0'] = region['COUNTRY']
            region_name = region['COUNTRY']
            # Update the new row and the region name if the admin level is high
            # enough
            if int(admin_level) >= 1:
                df.loc[idx, 'admin_level_1'] = region['NAME_1']
                region_name = region['NAME_1']
            else:
                df.loc[idx, 'admin_level_1'] = None
            if int(admin_level) >= 2:
                df.loc[idx, 'admin_level_2'] = region['NAME_2']
                region_name = region['NAME_2']
            else:
                df.loc[idx, 'admin_level_2'] = None
            if int(admin_level) >= 3:
                df.loc[idx, 'admin_level_3'] = region['NAME_3']
                region_name = region['NAME_3']
            else:
                df.loc[idx, 'admin_level_3'] = None
            # Populate the year and month
            df.loc[idx, 'year'] = pdate.year
            df.loc[idx, 'month'] = pdate.month
            df.loc[idx, 'day'] = pdate.day

            # Define transform for geometry_mask based on grid resolution
            transform = rasterio.transform.from_origin(
                lon.min(), lat.max(), abs(lon[1] - lon[0]),
                abs(lat[1] - lat[0])
            )

            # Create a mask that is True for points outside the geometries
            mask = rasterio.features.geometry_mask(
                [geometry], transform=transform, out_shape=data.shape
            )
            masked_data = np.ma.masked_array(data, mask=mask)

            # Plot
            if plots and (admin_level == '0'):
                title = f'{metric}\n{region_name} - {partial_date}'
                colourbar_label = f'{metric} [{unit}]'
                extent = [lon.min(), lon.max(), lat.min(), lat.max()]
                path = Path(
                    output_path(sub_pipeline),
                    str(pdate).replace('-', '/'),
                    f'{region_name} - {metric}.png'
                )
                plot_gadm_micro_heatmap(
                    masked_data, gdf, pdate, title, colourbar_label, region,
                    extent, path
                )

            # Add to output data frame
            df.loc[idx, 'iso3'] = iso3
            df.loc[idx, 'metric'] = metric
            df.loc[idx, 'value'] = np.nansum(masked_data)
            df.loc[idx, 'unit'] = unit
            df.loc[idx, 'resolution'] = f'Admin Level {admin_level}'
            df.loc[idx, 'creation_date'] = date.today()

            # Increment the row
            idx += 1

        # Close the NetCDF file after use
        ds.close()

    return df.fillna(''), 'era5-reanalysis.csv'
