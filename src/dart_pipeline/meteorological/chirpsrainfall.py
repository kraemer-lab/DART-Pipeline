"""Module for processing CHIRPS Rainfall data."""
from datetime import date
from pathlib import Path
import logging

import numpy as np
import pandas as pd
import rasterio

from dart_pipeline.constants import MIN_FLOAT, OUTPUT_COLUMNS
from dart_pipeline.plots import plot_heatmap
from dart_pipeline.types import PartialDate, ProcessResult
from dart_pipeline.util import output_path, source_path


def get_chirps_rainfall_data_path(date: PartialDate) -> Path:
    """Get the path to a CHIRPS rainfall data file."""
    file = None
    match date.scope:
        case 'daily':
            file = Path(
                'global_daily',
                str(date.year),
                date.zero_padded_month,
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case 'monthly':
            file = Path(
                'global_monthly',
                str(date.year),
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case 'annual':
            file = Path('global_annual', f'chirps-v2.0.{date}.tif')

    path = source_path('meteorological/chirps-rainfall', file)
    if not path.exists():
        raise FileNotFoundError(f'CHIRPS rainfall data not found: {path}')

    return path


def process_chirpsrainfall(partial_date: str, plots=False) -> ProcessResult:
    """
    Process CHIRPS Rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    sub_pipeline = 'meteorological/chirps-rainfall'
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logging.info('importing:%s', file)
    src = rasterio.open(file)

    # Initialise the data frame that will store the output data for each region
    df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Add geospatial information to output data frame
    df.loc[0, 'GID_0'] = ''
    df.loc[0, 'COUNTRY'] = ''
    df.loc[0, 'GID_1'] = ''
    df.loc[0, 'NAME_1'] = ''
    df.loc[0, 'GID_2'] = ''
    df.loc[0, 'NAME_2'] = ''
    df.loc[0, 'GID_3'] = ''
    df.loc[0, 'NAME_3'] = ''
    # Add date information to the output data frame
    df.loc[0, 'year'] = pdate.year
    if pdate.month:
        df.loc[0, 'month'] = pdate.month
    if pdate.day:
        df.loc[0, 'day'] = pdate.day
    df.loc[0, 'week'] = ''

    df.loc[0, 'metric'] = 'meteorological.chirps-rainfall'
    # Rasterio stores image layers in 'bands'
    # Get the data in the first band as an array
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data == MIN_FLOAT] = 0
    # Hide nulls
    data[data == -9999] = 0
    # Add the result to the output data frame
    df.loc[0, 'value'] = np.nansum(data)
    df.loc[0, 'unit'] = 'mm'
    df.loc[0, 'creation_date'] = date.today()

    # Create a plot
    if plots:
        title = f'Rainfall\n{pdate}'
        colourbar_label = 'Rainfall [mm]'
        path = Path(
            output_path(sub_pipeline), str(pdate).replace('-', '/'),
            str(pdate) + '.png'
        )
        plot_heatmap(data, title, colourbar_label, path)

    sub_pipeline = sub_pipeline.replace('/', '_')
    filename = f'{sub_pipeline}_{pdate.year}_{date.today()}.csv'

    return df, filename
