"""Module for processing APHRODITE precipitation (V1901) data."""
from datetime import date, datetime, timedelta
from pathlib import Path
import re

import numpy as np
import pandas as pd

from dart_pipeline.plots import plot_scatter
from dart_pipeline.util import \
    source_path, days_in_year, output_path
from dart_pipeline.types import ProcessResult
from dart_pipeline.constants import OUTPUT_COLUMNS

# No data in APHRODITE data
# See APHRO_MA_025deg_V1901.ctl and others
NO_DATA = -99.90


def process_aphroditeprecipitation(
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

    params = {
        # Parameters from APHRO_MA_025deg_V1901.ctl
        '025deg': {
            'n_deg': (360, 280),
            'start_coords': (60.125, -14.875),
            'scale_factor': 0.25
        },
        # Parameters from APHRO_MA_050deg_V1901.ctl
        '050deg': {
            'n_deg': (180, 140),
            'start_coords': (60.25, -14.75),
            'scale_factor': 0.5
        }
    }
    for res in resolution:
        nday = days_in_year(int(year))
        # Record length
        nx, ny = params[res]['n_deg']
        recl = nx * ny
        # Longitude and latitude bounds
        x_start, y_start = params[res]['start_coords']
        scale_factor = params[res]['scale_factor']
        xlon = x_start + np.arange(nx) * scale_factor
        ylat = y_start + np.arange(ny) * scale_factor

        # Open the file
        file_path = Path(base_path) / Path(f'APHRO_MA_{res}_{version}.{year}')
        # Read binary data
        with open(file_path, 'rb') as f:
            # Initialise arrays
            prcp_data = np.zeros((nday, ny, nx))
            rstn_data = np.zeros((nday, ny, nx))

            for iday in range(nday):
                # Read next batch of prcp values of size nx * ny
                prcp_raw = np.fromfile(f, dtype='float32', count=recl)
                prcp_raw = prcp_raw.reshape((ny, nx))
                # Read next batch of rstn values of size nx * ny
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

            valid_mask = (rstn_data[iday, :, :] != 0.0) & \
                (prcp_data[iday, :, :] != NO_DATA)
            valid_prcp = prcp_data[iday][valid_mask]
            valid_lon = valid_xlon[valid_mask]
            valid_lat = valid_ylat[valid_mask]

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

    output['metric'] = 'aphrodite-daily-precip'
    output['unit'] = 'mm'
    output['creation_date'] = date.today()

    return output, 'aphrodite-daily-precip.csv'
