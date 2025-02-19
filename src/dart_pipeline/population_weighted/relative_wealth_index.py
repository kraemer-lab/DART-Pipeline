"""
Module for processing population-weighted Relative Wealth Index.

See the tutorial here:
https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-weigh
ted-relative-wealth-index
"""
import logging

from pyquadkey2 import quadkey
from shapely.geometry import Point
import contextily
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from dart_pipeline.types import ProcessResult, PartialDate, AdminLevel
from dart_pipeline.util import get_country_name, get_shapefile, source_path, \
    output_path


def get_point_in_polygon(lat: float, lon: float, polygons: dict) -> str:
    """
    Find the administrative region ID containing a given point.

    Args:
        lat (float): Latitude of the point.
        lon (float): Longitude of the point.
        polygons (dict): Dictionary mapping region IDs to their polygon
            geometries.

    Returns:
        str: The ID of the region containing the point, or 'null' if not found.
    """
    point = Point(lon, lat)
    for geo_id, polygon in polygons.items():
        if polygon.contains(point):
            return geo_id

    return 'null'


def process_row(row, polygons, zoom=15):
    lat, lon = float(row['latitude']), float(row['longitude'])
    geo_id = get_point_in_polygon(lat, lon, polygons)
    qk = quadkey.from_geo((lat, lon), zoom)

    return pd.Series([geo_id, qk])


def process_gadm_popdensity_rwi(
    iso3: str, partial_date: str, admin_level: AdminLevel = '2', plots=False
) -> ProcessResult:
    """
    Process population-weighted Relative Wealth Index data.

    Original author: Prathyush Sambaturu

    Purpose: Preprocess and aggregate Relative Wealth Index scores for
    administrative regions (admin2 or admin3) of Vietnam. The code for
    aggregation is adapted from the following tutorial:
    https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-
    weighted-relative-wealth-index
    """
    sub_pipeline = 'population-weighted/relative-wealth-index'
    logging.info('iso3:%s', iso3)
    country_name = get_country_name(iso3)
    logging.info('country_name:%s', country_name)
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    year = pdate.year
    logging.info('admin_level:%s', admin_level)
    logging.info('plots:%s', plots)

    # Import the GADM shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    shapefile = gpd.read_file(path)
    # Get the polygons from the shape file and create a dictionary mapping the
    # region IDs to their polygon geometries
    admin_geoid = f'GID_{admin_level}'
    polygons = dict(zip(shapefile[admin_geoid], shapefile['geometry']))

    # Import the Relative Wealth Index data
    source = 'economic/relative-wealth-index'
    path = source_path(source, f'{iso3.lower()}_relative_wealth_index.csv')
    logging.info('importing:%s', path)
    rwi = pd.read_csv(path)
    # Assign each RWI value to an administrative region
    rwi[['geo_id', 'quadkey']] = rwi.apply(
        lambda x: process_row(x, polygons), axis=1
    )
    rwi = rwi[rwi['geo_id'] != 'null']

    # Import population density data
    source = 'sociodemographic/meta-pop-density'
    path = source_path(
        source, f'{iso3.upper()}/{iso3.lower()}_general_{year}.csv'
    )
    logging.info('importing:%s', path)
    population = pd.read_csv(path)
    population = population.rename(
        columns={f'{iso3.lower()}_general_{year}': f'pop_{year}'}
    )
    # Aggregates the data by Bing tiles at zoom level 14
    population['quadkey'] = population.apply(
        lambda x: str(quadkey.from_geo((x['latitude'], x['longitude']), 14)),
        axis=1
    )
    bing_tile_z14 = population.groupby(
        'quadkey', as_index=False
    )[f'pop_{year}'].sum()
    bing_tile_z14['quadkey'] = bing_tile_z14['quadkey'].astype(str)

    # Merge the RWI and population density data
    rwi_pop = rwi.merge(
        bing_tile_z14[['quadkey', f'pop_{year}']], on='quadkey', how='inner'
    )
    geo_pop = rwi_pop.groupby('geo_id', as_index=False)[f'pop_{year}'].sum()
    geo_pop = geo_pop.rename(columns={f'pop_{year}': f'geo_{year}'})
    rwi_pop = rwi_pop.merge(geo_pop, on='geo_id', how='inner')
    rwi_pop['pop_weight'] = rwi_pop[f'pop_{year}'] / rwi_pop[f'geo_{year}']
    rwi_pop['rwi_weight'] = rwi_pop['rwi'] * rwi_pop['pop_weight']
    geo_rwi = rwi_pop.groupby('geo_id', as_index=False)['rwi_weight'].sum()

    # Merge the population-weight RWI data with the GADM shapefile
    rwi = shapefile.merge(geo_rwi, left_on=admin_geoid, right_on='geo_id')

    print(rwi.head())

    # Plot
    if plots:
        _, ax = plt.subplots(figsize=(15, 12))
        rwi.plot(
            ax=ax,
            column='rwi_weight',
            marker='o',
            markersize=1,
            legend=True,
            label='RWI score'
        )
        contextily.add_basemap(
            ax, crs={'init': 'epsg:4326'},
            source=contextily.providers.OpenStreetMap.Mapnik
        )
        plt.title('Relative Wealth Index scores of regions in Vietnam')
        plt.legend()
        # Export
        path = output_path(sub_pipeline, f'{iso3}/admin_level_{admin_level}')
        path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(path)
        plt.close()

    # sub_pipeline = sub_pipeline.replace('/', '_')
    # filename = f'{iso3}_{sub_pipeline}_{year}_{date.today()}.csv'

    # return df.fillna(''), filename
