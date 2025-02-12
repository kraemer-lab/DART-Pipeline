from datetime import date
from pathlib import Path
import logging

import pandas as pd
import rasterio
import rasterio.mask

from dart_pipeline.util import (
    source_path, get_country_name
)
from dart_pipeline.constants import OUTPUT_COLUMNS, MIN_FLOAT
from dart_pipeline.types import ProcessResult


def process_worldpopcount(
    iso3: str, year: int = 2020, rt: str = 'ppp'
) -> ProcessResult:
    """
    Process WorldPop population count.

    - EPSG:9217: https://epsg.io/9217
    - EPSG:4326: https://epsg.io/4326
    - EPSG = European Petroleum Survey Group
    """
    sub_pipeline = 'sociodemographic/worldpop-count'
    iso3 = iso3.upper()
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
    df.loc[0, 'unit'] = 'unitless'
    df.loc[0, 'value'] = population
    if rt == 'ppp':
        df.loc[0, 'resolution'] = 'people per pixel'
    elif rt == 'pph':
        df.loc[0, 'resolution'] = 'people per hectare'
    df.loc[0, 'creation_date'] = date.today()

    sub_pipeline = sub_pipeline.replace('/', '_')
    filename = f'{iso3}_{sub_pipeline}_{year}_{date.today()}.csv'

    return df.fillna(''), filename
