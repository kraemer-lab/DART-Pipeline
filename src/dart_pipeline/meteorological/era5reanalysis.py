from datetime import date
import logging

import netCDF4 as nc
import numpy as np
import pandas as pd

from dart_pipeline.types import PartialDate
from dart_pipeline.plots import plot_heatmap
from dart_pipeline.constants import BASE_DIR, OUTPUT_COLUMNS, \
    DEFAULT_SOURCES_ROOT, DEFAULT_OUTPUT_ROOT


def process_era5reanalysis(dataset, partial_date, plots=False):
    """Process ERA5 atmospheric reanalysis data."""
    sub_pipeline = 'meteorological/era5-reanalysis'
    pdate = PartialDate.from_string(partial_date)
    logging.info('dataset:%s', dataset)
    logging.info('partial_date:%s', pdate)
    logging.info('plots:%s', plots)

    # Find the data
    filepaths = []
    folder = BASE_DIR / DEFAULT_SOURCES_ROOT / sub_pipeline
    for path in folder.iterdir():
        if path.name == f'{dataset}_{str(pdate)}.nc':
            # The data file has been found
            filepaths.append(path)
            break
        if path.name == f'{dataset}_{str(pdate)}':
            # The data folder has been found
            filepaths = path.iterdir()

    # Initialise the output data frame
    df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Process the data
    for i, path in enumerate(sorted(filepaths)):
        logging.info('importing:%s', path)
        ds = nc.Dataset(path, 'r')  # type: ignore

        # Typically the data variable will be at the front
        variable = list(ds.variables)[0]
        data = ds.variables[variable][:]
        mean_value = np.mean(data[~np.isnan(data)])
        metric = ds.variables[variable].long_name
        unit = ds.variables[variable].units

        if plots:
            title = f'{metric}\n{partial_date}'
            colourbar_label = f'{metric} [{unit}]'
            path = BASE_DIR / DEFAULT_OUTPUT_ROOT / sub_pipeline / \
                str(pdate).replace('-', '/') / \
                (title.replace('\n', ' - ') + '.png')
            plot_heatmap(data[0, :, :], title, colourbar_label, path)

        # Add to output data frame
        df.loc[i, 'iso3'] = ''
        df.loc[i, 'admin_level_0'] = ''
        df.loc[i, 'admin_level_1'] = ''
        df.loc[i, 'admin_level_2'] = ''
        df.loc[i, 'admin_level_3'] = ''
        df.loc[i, 'year'] = pdate.year
        df.loc[i, 'month'] = pdate.month
        df.loc[i, 'day'] = pdate.day
        df.loc[i, 'week'] = ''
        df.loc[i, 'metric'] = metric
        df.loc[i, 'value'] = mean_value
        df.loc[i, 'unit'] = unit
        df.loc[i, 'resolution'] = 'global'
        df.loc[i, 'creation_date'] = date.today()

        ds.close()

    return df.fillna(''), 'era5-reanalysis.csv'
