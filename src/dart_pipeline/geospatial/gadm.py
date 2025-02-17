"""Module for processing Database of Global Administrative Areas data."""
from datetime import date
from pathlib import Path
import logging

from matplotlib import pyplot as plt
import geopandas as gpd
import pandas as pd

from dart_pipeline.constants import OUTPUT_COLUMNS
from dart_pipeline.util import get_shapefile, get_country_name, output_path
from dart_pipeline.types import AdminLevel


def process_gadm(iso3: str, admin_level: AdminLevel, plots=False):
    """Process GADM administrative map data."""
    sub_pipeline = 'geospatial/gadm'
    iso3 = iso3.upper()
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('plots:%s', plots)

    # Import
    gdf = gpd.read_file(get_shapefile(iso3, admin_level))

    # Re-project the data
    # https://spatialreference.org/
    national_crs = {
        'GBR': 27700,  # EPSG:27700 - OSGB36/British National Grid
        'PER': 24892,  # EPSG:24892 - PSAD56/Peru central zone
        'VNM': 2045,  # EPSG:2045 - Hanoi 1972/Gauss-Kruger zone 19
    }
    try:
        gdf = gdf.to_crs(epsg=national_crs[iso3])
    except KeyError:
        pass

    # Initialise the output data frame
    df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Iterate over the regions in the shape file
    for i, region in gdf.iterrows():
        df.loc[i, 'iso3'] = iso3

        # Populate a row for the output data frame
        df.loc[i, 'admin_level_0'] = region['COUNTRY']
        # Update the new row and the region name if the admin level is high
        # enough
        if int(admin_level) >= 1:
            df.loc[i, 'admin_level_1'] = region['NAME_1']
        else:
            df.loc[i, 'admin_level_1'] = None
        if int(admin_level) >= 2:
            df.loc[i, 'admin_level_2'] = region['NAME_2']
        else:
            df.loc[i, 'admin_level_2'] = None
        if int(admin_level) >= 3:
            df.loc[i, 'admin_level_3'] = region['NAME_3']
        else:
            df.loc[i, 'admin_level_3'] = None

        # Populate the year and month
        df.loc[i, 'year'] = None
        df.loc[i, 'month'] = None
        df.loc[i, 'day'] = None
        df.loc[i, 'week'] = None

        # Calculate area in square metres
        area = region.geometry.area
        # Convert to square kilometres
        area_sq_km = area / 1e6

        # Add to output data frame
        df.loc[i, 'metric'] = 'area'
        df.loc[i, 'value'] = area_sq_km
        df.loc[i, 'unit'] = 'km²'
        df.loc[i, 'resolution'] = None
        df.loc[i, 'creation_date'] = date.today()

    if plots:
        # Re-project to a geographic coordinate system (GCS)
        gdf = gdf.to_crs(epsg=4326)
        # Plot only the boundaries (without filling regions)
        fig, ax = plt.subplots(figsize=(10, 6))
        gdf.boundary.plot(ax=ax, color='black', linewidth=1)
        country_name = get_country_name(iso3)
        title = f'{country_name}\nAdmin Level {admin_level}'
        ax.set_title(title)
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        # Ensure x/y limits are properly set
        if not gdf.empty and gdf.total_bounds.any():
            ax.set_xlim(gdf.total_bounds[0], gdf.total_bounds[2])
            ax.set_ylim(gdf.total_bounds[1], gdf.total_bounds[3])
        # Export
        path = Path(
            output_path(sub_pipeline), str(iso3),
            f'{country_name} - Admin Level {admin_level}.png'
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        logging.info('exporting:%s', path)
        plt.savefig(path)
        plt.close()

    sub_pipeline = sub_pipeline.replace('/', '_')

    return df.fillna(''), f'{iso3}_{sub_pipeline}_{date.today()}.csv'
