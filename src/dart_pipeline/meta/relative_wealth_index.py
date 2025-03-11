"""
Processing and aggregation of population-weighted Relative Wealth Index.

See the tutorial here:
https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-weigh
ted-relative-wealth-index

Originally adapted by Prathyush Sambaturu.
"""
from datetime import date
import logging

from pyquadkey2 import quadkey
from shapely.geometry import Point
import contextily
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

from dart_pipeline.constants import OUTPUT_COLUMNS
from dart_pipeline.types import ProcessResult, PartialDate, AdminLevel
from dart_pipeline.util import get_country_name, get_shapefile, source_path, \
    output_path, populate_output_df_admin_levels, populate_output_df_temporal


def get_geo_id(lat: float, lon: float, polygons: dict) -> str:
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
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return 'null'


def get_quadkey(x, zoom_level):
    """Get the quadkey for a latitude and longitude at a zoom level."""
    return str(quadkey.from_geo((x['latitude'], x['longitude']), zoom_level))


def process_gadm_popdensity_rwi(
    iso3: str, partial_date: str = '2020', admin_level: AdminLevel = '2',
    plots=False
) -> ProcessResult:
    """
    Process population-weighted Relative Wealth Index and geospatial data.

    Purpose: Preprocess and aggregate Relative Wealth Index scores for
    administrative regions (admin2 or admin3) of Vietnam. The code for
    aggregation is adapted from the following tutorial:
    https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-
    weighted-relative-wealth-index

    Originally adapted by Prathyush Sambaturu.
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

    # Zoom level 14 is ~2.4km Bing tile
    zoom_level = 14

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
    rwi['geo_id'] = rwi.apply(
        lambda x: get_geo_id(x['latitude'], x['longitude'], polygons),
        axis=1
    )
    rwi = rwi[rwi['geo_id'] != 'null']
    rwi['quadkey'] = rwi.apply(lambda x: get_quadkey(x, zoom_level), axis=1)

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
        lambda x: get_quadkey(x, zoom_level), axis=1
    )
    population = population.groupby(
        'quadkey', as_index=False
    )[f'pop_{year}'].sum()

    # Merge the RWI and population density data
    rwi_pop = rwi.merge(population, on='quadkey', how='inner')
    geo_pop = rwi_pop.groupby('geo_id', as_index=False)[f'pop_{year}'].sum()
    geo_pop = geo_pop.rename(columns={f'pop_{year}': f'geo_{year}'})
    rwi_pop = rwi_pop.merge(geo_pop, on='geo_id', how='inner')
    rwi_pop['pop_weight'] = rwi_pop[f'pop_{year}'] / rwi_pop[f'geo_{year}']
    rwi_pop['rwi_weight'] = rwi_pop['rwi'] * rwi_pop['pop_weight']
    geo_rwi = rwi_pop.groupby('geo_id', as_index=False)['rwi_weight'].sum()

    # Merge the population-weight RWI data with the GADM shapefile
    rwi = shapefile.merge(geo_rwi, left_on=admin_geoid, right_on='geo_id')

    # Plot
    if plots:
        _, ax = plt.subplots()
        rwi.plot(
            ax=ax,
            column='rwi_weight',
            legend=True,
        )
        contextily.add_basemap(
            ax, crs='EPSG:4326',
            source=contextily.providers.OpenStreetMap.Mapnik
        )
        plt.title('Population-Weighted Relative Wealth Index')
        plt.xlabel('Longitude')
        plt.xticks(rotation=30)
        plt.ylabel('Latitude')
        # Export
        path = output_path(sub_pipeline, f'{iso3}/admin_level_{admin_level}')
        path.parent.mkdir(parents=True, exist_ok=True)
        logging.info('exporting:%s', path)
        plt.savefig(path)
        plt.close()

    # Format the output data frame
    rwi = rwi.rename(columns={'GID_0': 'ISO3'})
    rwi = populate_output_df_admin_levels(rwi, admin_level)
    rwi = populate_output_df_temporal(rwi, pdate)
    rwi['metric'] = 'meta.relative_wealth_index'
    rwi = rwi.rename(columns={'rwi_weight': 'value'})
    rwi['unit'] = 'unitless'
    rwi['creation_date'] = date.today()
    # Re-order the columns
    rwi = rwi[OUTPUT_COLUMNS]

    sub_pipeline = sub_pipeline.replace('/', '_')
    filename = f'{iso3}_{sub_pipeline}_{year}_{date.today()}.csv'

    return rwi.fillna(''), filename
